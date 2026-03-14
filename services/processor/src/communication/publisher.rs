use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, options::*, types::FieldTable,
};
use opentelemetry::global;
use serde::Serialize;
use shared::telemetry::AmqpInjector;
use std::future::Future;
use std::pin::Pin;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Serialize, Clone, Debug)]
pub struct VectorMessage {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub chunks: Vec<String>,
    pub embeddings: Vec<Vec<f32>>,
}

pub trait VectorPublisher: Send + Sync {
    fn publish(
        &self,
        message: VectorMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;
}

pub struct RabbitMqVectorPublisher {
    channel: Channel,
}

impl RabbitMqVectorPublisher {
    pub async fn new(amqp_addr: &str) -> Result<Self, String> {
        let conn = Connection::connect(amqp_addr, ConnectionProperties::default())
            .await
            .map_err(|e| format!("Failed to connect to RabbitMQ: {}", e))?;

        let channel = conn.create_channel().await.map_err(|e| e.to_string())?;

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
            .map_err(|e| format!("Failed to declare queue: {}", e))?;

        Ok(Self { channel })
    }
}

impl VectorPublisher for RabbitMqVectorPublisher {
    fn publish(
        &self,
        message: VectorMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        let channel = self.channel.clone();

        Box::pin(async move {
            let payload = serde_json::to_vec(&message).map_err(|e| e.to_string())?;

            let mut headers = FieldTable::default();
            global::get_text_map_propagator(|propagator| {
                let cx = tracing::Span::current().context();
                propagator.inject_context(&cx, &mut AmqpInjector(&mut headers))
            });

            channel
                .basic_publish(
                    "".into(),
                    "document_queue".into(),
                    BasicPublishOptions::default(),
                    &payload,
                    BasicProperties::default()
                        .with_delivery_mode(2)
                        .with_headers(headers),
                )
                .await
                .map_err(|e| format!("Failed to publish: {}", e))?;

            Ok(())
        })
    }
}
