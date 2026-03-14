use crate::logic::worker::ProcessorMessage;
use futures::StreamExt;
use lapin::{Connection, ConnectionProperties, options::*, types::FieldTable};
use opentelemetry::global;
use ractor::ActorRef;
use serde::Deserialize;
use shared::telemetry::AmqpExtractor;
use tracing::{Instrument, error, info, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Deserialize, Debug)]
pub struct DocumentPayload {
    pub url: String,
    pub blob_id: String,
    pub mime_type: String,
}

pub async fn start_document_consumer(amqp_addr: &str, processor_ref: ActorRef<ProcessorMessage>) {
    let conn = Connection::connect(amqp_addr, ConnectionProperties::default())
        .await
        .expect("Processor Consumer failed to connect to RabbitMQ");

    let channel = conn
        .create_channel()
        .await
        .expect("Failed to create channel");

    channel
        .queue_declare(
            "document_queue".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare document_queue");

    let mut consumer = channel
        .basic_consume(
            "document_queue".into(),
            "processor_consumer".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to start RabbitMQ consumer");

    info!("🎧 Processor started consuming from 'document_queue'");

    tokio::spawn(async move {
        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    let empty_headers = FieldTable::default();
                    let headers = delivery
                        .properties
                        .headers()
                        .as_ref()
                        .unwrap_or(&empty_headers);
                    let parent_cx = global::get_text_map_propagator(|propagator| {
                        propagator.extract(&AmqpExtractor(headers))
                    });
                    let consume_span = info_span!("consume_rabbitmq_message");
                    let _ = consume_span.set_parent(parent_cx);

                    async {
                        if let Ok(payload) =
                            serde_json::from_slice::<DocumentPayload>(&delivery.data)
                        {
                            let msg = ProcessorMessage::ProcessDocument {
                                url: payload.url,
                                blob_id: payload.blob_id,
                                mime_type: payload.mime_type,
                            };

                            if let Err(e) = processor_ref.cast(msg) {
                                error!("Failed to route message to Processor Actor: {}", e);
                                let _ = delivery.nack(BasicNackOptions::default()).await;
                            } else {
                                let _ = delivery.ack(BasicAckOptions::default()).await;
                            }
                        } else {
                            error!("Failed to deserialize DocumentPayload");
                            let _ = delivery.nack(BasicNackOptions::default()).await;
                        }
                    }
                    .instrument(consume_span)
                    .await;
                }
                Err(e) => error!("Error in RabbitMQ consumer stream: {:?}", e),
            }
        }
    });
}
