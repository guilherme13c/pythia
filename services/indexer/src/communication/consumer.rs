use crate::logic::worker::IndexerMessage;
use futures::StreamExt;
use lapin::{Connection, ConnectionProperties, options::*, types::FieldTable};
use opentelemetry::global;
use ractor::ActorRef;
use serde::Deserialize;
use shared::telemetry::AmqpExtractor;
use tracing::{Instrument, error, info, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Deserialize, Clone, Debug)]
pub struct VectorPayload {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub chunks: Vec<String>,
    pub embeddings: Vec<Vec<f32>>,
}

pub async fn start_vector_consumer(amqp_addr: &str, indexer_ref: ActorRef<IndexerMessage>) {
    let conn = Connection::connect(amqp_addr, ConnectionProperties::default())
        .await
        .expect("Indexer Consumer failed to connect to RabbitMQ");

    let channel = conn
        .create_channel()
        .await
        .expect("Failed to create channel");

    channel
        .queue_declare(
            "vector_queue".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare vector_queue");

    let mut consumer = channel
        .basic_consume(
            "vector_queue".into(),
            "indexer_consumer".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to start RabbitMQ consumer");

    info!("Indexer started consuming from 'vector_queue'");

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
                        if let Ok(payload) = serde_json::from_slice::<VectorPayload>(&delivery.data)
                        {
                            let msg = IndexerMessage::Store {
                                url: payload.url,
                                title: payload.title,
                                description: payload.description,
                                chunks: payload.chunks,
                                embeddings: payload.embeddings,
                            };

                            if let Err(e) = indexer_ref.cast(msg) {
                                error!("Failed to route message to Indexer Actor: {}", e);
                                let _ = delivery.nack(BasicNackOptions::default()).await;
                            } else {
                                let _ = delivery.ack(BasicAckOptions::default()).await;
                            }
                        } else {
                            error!("Failed to deserialize VectorPayload");
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
