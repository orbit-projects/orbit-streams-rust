use orbit_streams_rust::{BatchProcessor, ProcessorFailure, StreamBatch, validate_batch};
use serde::Deserialize;
use std::sync::RwLock;

struct AppendSuffix {
    configuration: RwLock<Configuration>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    #[serde(default = "suffix_operation")]
    operation: String,
    #[serde(default)]
    suffix: String,
    #[serde(default = "one_round")]
    rounds: u32,
}

fn suffix_operation() -> String {
    "suffix".into()
}

fn one_round() -> u32 {
    1
}

impl BatchProcessor for AppendSuffix {
    fn activate(&self, configuration_json: &[u8]) -> Result<(), ProcessorFailure> {
        let config: Configuration =
            serde_json::from_slice(configuration_json).map_err(|_| ProcessorFailure)?;
        if config.suffix.len() > 4096
            || !matches!(config.operation.as_str(), "suffix" | "checksum")
            || !(1..=100_000).contains(&config.rounds)
        {
            return Err(ProcessorFailure);
        }
        *self.configuration.write().map_err(|_| ProcessorFailure)? = config;
        Ok(())
    }

    fn process(&self, mut batch: StreamBatch) -> Result<StreamBatch, ProcessorFailure> {
        let configuration = self.configuration.read().map_err(|_| ProcessorFailure)?;
        for record in &mut batch.records {
            match configuration.operation.as_str() {
                "suffix" => {
                    let suffix = configuration.suffix.as_bytes();
                    if record.value.len().saturating_add(suffix.len()) > 256 * 1024 {
                        return Err(ProcessorFailure);
                    }
                    record.value.reserve(suffix.len());
                    record.value.extend_from_slice(suffix);
                }
                "checksum" => {
                    let mut checksum = 2_166_136_261u32;
                    for _ in 0..configuration.rounds {
                        for byte in &record.value {
                            checksum = (checksum ^ u32::from(*byte)).wrapping_mul(16_777_619);
                        }
                    }
                    if record.value.len().saturating_add(4) > 256 * 1024 {
                        return Err(ProcessorFailure);
                    }
                    record.value.extend_from_slice(&checksum.to_be_bytes());
                }
                _ => return Err(ProcessorFailure),
            }
        }
        validate_batch(&batch).map_err(|_| ProcessorFailure)?;
        Ok(batch)
    }
}

#[tokio::main]
async fn main() {
    let processor = AppendSuffix {
        configuration: RwLock::new(Configuration {
            operation: "suffix".into(),
            suffix: String::new(),
            rounds: 1,
        }),
    };
    if orbit_streams_rust::plugin::run(std::sync::Arc::new(processor))
        .await
        .is_err()
    {
        std::process::exit(1);
    }
}
