use lapin::types::{AMQPValue, FieldTable, ShortString};
use opentelemetry::{
    KeyValue,
    propagation::{Extractor, Injector},
};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator};
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

pub struct AmqpInjector<'a>(pub &'a mut FieldTable);

impl<'a> Injector for AmqpInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0
            .insert(ShortString::from(key), AMQPValue::LongString(value.into()));
    }
}

pub struct AmqpExtractor<'a>(pub &'a FieldTable);

impl<'a> Extractor for AmqpExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.inner().get(key).and_then(|value| {
            if let AMQPValue::LongString(s) = value {
                std::str::from_utf8(s.as_bytes()).ok()
            } else {
                None
            }
        })
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .inner()
            .keys()
            .map(|k: &ShortString| k.as_str())
            .collect()
    }
}

pub fn init_telemetry(
    service_name: &str,
    otlp_endpoint: Option<String>,
) -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let fmt_layer = tracing_subscriber::fmt::layer();

    if let Some(endpoint) = otlp_endpoint {
        let resource = Resource::builder()
            .with_attributes(vec![KeyValue::new(
                "service.name",
                service_name.to_string(),
            )])
            .build();

        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .expect("Failed to build OTLP exporter");

        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build();

        opentelemetry::global::set_tracer_provider(provider.clone());

        use opentelemetry::trace::TracerProvider as _;
        let tracer = provider.tracer(service_name.to_string());

        let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        Registry::default()
            .with(env_filter)
            .with(fmt_layer)
            .with(telemetry_layer)
            .init();

        Some(provider)
    } else {
        Registry::default().with(env_filter).with(fmt_layer).init();
        None
    }
}
