//! Core process-host connection and bounded stream capability dispatch.

use crate::{BatchProcessor, ProcessorFailure, StreamBatch, validate_batch};
use prost::Message;
use prost_types::Any;
use proto::plugin_v1::{
    CapabilityResult, CommandResult, HealthProbe, PluginCommand, PluginFrame, PluginHealth,
    PluginHello, PluginMetadata, Shutdown, core_frame::Payload as CorePayload,
    plugin_command::Kind as CommandKind, plugin_frame::Payload as PluginPayload,
    plugin_health::Status as HealthStatus,
};
use serde::Deserialize;
use std::io::{BufRead, Read};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;

use crate::proto;

const MAX_BOOTSTRAP_BYTES: usize = 64 * 1024;
const MAX_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_MESSAGE_BYTES: usize = 1088 * 1024;
const MAX_IN_FLIGHT_CALLS: usize = 16;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    endpoint: String,
    token: String,
    metadata: PluginMetadataJson,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginMetadataJson {
    id: String,
    name: String,
    version: String,
    api_version: String,
    #[serde(default)]
    dependencies: Vec<String>,
    #[serde(default)]
    optional_dependencies: Vec<String>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    required_capabilities: Vec<String>,
}

struct RuntimeState {
    active: bool,
}

/// Connect a Rust batch processor to a Core-supervised local gRPC plugin host.
///
/// Core owns child supervision, configuration delivery, authentication, health probes, and
/// shutdown. The process transport is trusted same-user loopback and is not a sandbox.
pub async fn run<P: BatchProcessor>(processor: Arc<P>) -> Result<(), &'static str> {
    let bootstrap = read_bootstrap().map_err(|_| "invalid Orbit plugin bootstrap")?;
    validate_bootstrap(&bootstrap)?;
    let endpoint = Endpoint::from_shared(format!("http://{}", bootstrap.endpoint))
        .map_err(|_| "invalid Orbit plugin endpoint")?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|_| "could not connect to Orbit Core")?;
    let (sender, receiver) = mpsc::channel::<PluginFrame>(MAX_IN_FLIGHT_CALLS + 2);
    let mut request = Request::new(ReceiverStream::new(receiver));
    let bearer = format!("Bearer {}", bootstrap.token);
    let bearer_value =
        MetadataValue::try_from(bearer.as_str()).map_err(|_| "invalid Orbit plugin credential")?;
    request.metadata_mut().insert("authorization", bearer_value);

    let mut client = proto::plugin_v1::process_plugin_client::ProcessPluginClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    sender
        .send(PluginFrame {
            payload: Some(PluginPayload::Hello(PluginHello {
                protocol_major: 1,
                protocol_minor: 0,
                metadata: Some(PluginMetadata {
                    id: bootstrap.metadata.id,
                    name: bootstrap.metadata.name,
                    version: bootstrap.metadata.version,
                    api_version: bootstrap.metadata.api_version,
                    dependencies: bootstrap.metadata.dependencies,
                    optional_dependencies: bootstrap.metadata.optional_dependencies,
                    capabilities: bootstrap.metadata.capabilities,
                    required_capabilities: bootstrap.metadata.required_capabilities,
                }),
            })),
        })
        .await
        .map_err(|_| "could not send Orbit plugin handshake")?;
    let mut response = client
        .connect(request)
        .await
        .map_err(|_| "could not connect to Orbit Core")?
        .into_inner();

    let state = Arc::new(RwLock::new(RuntimeState { active: false }));
    let slots = Arc::new(Semaphore::new(MAX_IN_FLIGHT_CALLS));
    let mut calls = JoinSet::new();
    let result = async {
        while let Some(frame) = response
            .message()
            .await
            .map_err(|_| "Orbit Core process connection ended")?
        {
            while calls.try_join_next().is_some() {}
            let Some(payload) = frame.payload else {
                continue;
            };
            match payload {
                CorePayload::Command(command) => {
                    // Keep lifecycle transitions ordered with capability work. The Core host
                    // waits for its active calls before deactivation; draining here also makes
                    // the child safe if another host sends commands concurrently.
                    while calls.join_next().await.is_some() {}
                    let request_id = command.request_id;
                    let result = handle_command(&*processor, &state, command).await;
                    sender
                        .send(PluginFrame {
                            payload: Some(PluginPayload::CommandResult(CommandResult {
                                request_id,
                                succeeded: result,
                                error_code: if result {
                                    String::new()
                                } else {
                                    "failed".into()
                                },
                                error_message: String::new(),
                            })),
                        })
                        .await
                        .map_err(|_| "could not reply to Orbit Core")?;
                }
                CorePayload::HealthProbe(HealthProbe { request_id }) => {
                    let active = state.read().await.active;
                    sender
                        .send(PluginFrame {
                            payload: Some(PluginPayload::Health(PluginHealth {
                                request_id,
                                status: if active {
                                    HealthStatus::Healthy as i32
                                } else {
                                    HealthStatus::Unhealthy as i32
                                },
                                message: if active { "ready" } else { "inactive" }.into(),
                            })),
                        })
                        .await
                        .map_err(|_| "could not reply to Orbit Core")?;
                }
                CorePayload::CapabilityCall(call) => {
                    let permit = slots
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| "Rust plugin call capacity is closed")?;
                    let processor = Arc::clone(&processor);
                    let state = Arc::clone(&state);
                    let sender = sender.clone();
                    calls.spawn(async move {
                        send_capability_result(sender, processor, state, call, permit).await;
                    });
                }
                CorePayload::Shutdown(Shutdown {}) => break,
            }
        }
        Ok(())
    }
    .await;
    while calls.join_next().await.is_some() {}
    result
}

async fn send_capability_result<P: BatchProcessor>(
    sender: mpsc::Sender<PluginFrame>,
    processor: Arc<P>,
    state: Arc<RwLock<RuntimeState>>,
    call: proto::plugin_v1::CapabilityCall,
    _permit: OwnedSemaphorePermit,
) {
    let request_id = call.request_id;
    let result = dispatch_batch(processor, state, call).await;
    let _ = sender
        .send(PluginFrame {
            payload: Some(PluginPayload::CapabilityResult(CapabilityResult {
                request_id,
                response: result.as_ref().ok().cloned(),
                error_code: result.err().unwrap_or_default(),
            })),
        })
        .await;
}

fn read_bootstrap() -> Result<Bootstrap, ()> {
    let mut bytes = Vec::with_capacity(1024);
    std::io::stdin()
        .lock()
        .take((MAX_BOOTSTRAP_BYTES + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .map_err(|_| ())?;
    if bytes.len() > MAX_BOOTSTRAP_BYTES || bytes.last() != Some(&b'\n') {
        return Err(());
    }
    serde_json::from_slice(&bytes).map_err(|_| ())
}

fn validate_bootstrap(bootstrap: &Bootstrap) -> Result<(), &'static str> {
    let (host, port) = bootstrap
        .endpoint
        .split_once(':')
        .ok_or("invalid Orbit plugin endpoint")?;
    if host != "127.0.0.1"
        || port.is_empty()
        || port.len() > 5
        || !port.bytes().all(|b| b.is_ascii_digit())
        || port
            .parse::<u16>()
            .ok()
            .filter(|value| *value > 0)
            .is_none()
        || bootstrap.token.len() != 64
        || !bootstrap.token.bytes().all(|b| b.is_ascii_hexdigit())
        || bootstrap.metadata.id.is_empty()
        || bootstrap.metadata.name.is_empty()
        || bootstrap.metadata.version.is_empty()
        || bootstrap.metadata.api_version.is_empty()
    {
        return Err("invalid Orbit plugin bootstrap");
    }
    Ok(())
}

async fn handle_command<P: BatchProcessor>(
    processor: &P,
    state: &RwLock<RuntimeState>,
    command: PluginCommand,
) -> bool {
    if command.request_id == 0 || command.configuration_json.len() > MAX_CONFIG_BYTES {
        return false;
    }
    match CommandKind::try_from(command.kind) {
        Ok(CommandKind::Activate) => {
            if processor.activate(&command.configuration_json).is_err() {
                return false;
            }
            state.write().await.active = true;
            true
        }
        Ok(CommandKind::Deactivate) => {
            if processor.deactivate().is_err() {
                return false;
            }
            state.write().await.active = false;
            true
        }
        _ => false,
    }
}

async fn dispatch_batch<P: BatchProcessor>(
    processor: Arc<P>,
    state: Arc<RwLock<RuntimeState>>,
    call: proto::plugin_v1::CapabilityCall,
) -> Result<Any, String> {
    if call.request_id == 0 || call.method != "/orbit.streams.v1.BatchProcessor/Process" {
        return Err("unsupported-method".into());
    }
    if !state.read().await.active {
        return Err("inactive".into());
    }
    let any = call.request.ok_or_else(|| "invalid-request".to_owned())?;
    if any.type_url != "type.googleapis.com/orbit.streams.v1.StreamBatch"
        || any.value.len() > MAX_MESSAGE_BYTES
    {
        return Err("invalid-request".into());
    }
    let batch =
        StreamBatch::decode(any.value.as_slice()).map_err(|_| "invalid-request".to_owned())?;
    validate_batch(&batch).map_err(|_| "invalid-request".to_owned())?;
    let max_records = batch.max_records;
    let max_bytes = batch.max_bytes;
    let output = tokio::task::spawn_blocking(move || processor.process(batch))
        .await
        .map_err(|_| "processor-failed".to_owned())?
        .map_err(|ProcessorFailure| "processor-failed".to_owned())?;
    if output.max_records != max_records || output.max_bytes != max_bytes {
        return Err("invalid-response".into());
    }
    validate_batch(&output).map_err(|_| "invalid-response".to_owned())?;
    let encoded = output.encode_to_vec();
    if encoded.len() > MAX_CONFIG_BYTES {
        return Err("invalid-response".into());
    }
    Ok(Any {
        type_url: "type.googleapis.com/orbit.streams.v1.StreamBatch".into(),
        value: encoded,
    })
}
