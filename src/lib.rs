//! Rust SDK for bounded Orbit stream batches and Core-supervised process plugins.

pub mod plugin;

/// Generated capability and Core process protocol messages.
pub mod proto {
    /// Core's versioned plugin lifecycle transport.
    pub mod plugin_v1 {
        tonic::include_proto!("orbit.plugin.v1");
    }

    /// Provider-neutral Orbit stream-processing contract, owned by `orbit-streams`.
    pub mod streams_v1 {
        tonic::include_proto!("orbit.streams.v1");
    }
}

pub use proto::streams_v1::{StreamBatch, StreamRecord};

/// Hard limits shared with the provider-neutral Python package.
pub const MAX_RECORD_BYTES: usize = 256 * 1024;
pub const MAX_BATCH_BYTES: usize = 1024 * 1024;
pub const MAX_RECORDS_PER_BATCH: usize = 1024;
pub const MAX_HEADERS: usize = 16;
pub const MAX_HEADER_BYTES: usize = 4096;

/// A bounded stream SDK validation error. Error values never include record payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamError {
    InvalidBatch,
    InvalidRecord,
    LimitExceeded,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidBatch => "invalid stream batch",
            Self::InvalidRecord => "invalid stream record",
            Self::LimitExceeded => "stream processing limit exceeded",
        })
    }
}

impl std::error::Error for StreamError {}

/// Validate a capability request or response using the Python contract's hard bounds.
pub fn validate_batch(batch: &StreamBatch) -> Result<(), StreamError> {
    let count = batch.records.len();
    let max_records = batch.max_records as usize;
    let max_bytes = batch.max_bytes as usize;
    if max_records == 0
        || max_records > MAX_RECORDS_PER_BATCH
        || max_bytes == 0
        || max_bytes > MAX_BATCH_BYTES
        || count > max_records
    {
        return Err(StreamError::InvalidBatch);
    }
    let mut total = 0usize;
    for record in &batch.records {
        validate_record(record)?;
        total = total
            .saturating_add(record.record_id.len())
            .saturating_add(record.value.len())
            .saturating_add(record.key.as_ref().map_or(0, Vec::len))
            .saturating_add(record.content_type.len());
        for (name, value) in &record.headers {
            total = total.saturating_add(name.len()).saturating_add(value.len());
        }
        if total > max_bytes {
            return Err(StreamError::LimitExceeded);
        }
    }
    Ok(())
}

fn validate_record(record: &StreamRecord) -> Result<(), StreamError> {
    if record.record_id.is_empty()
        || record.record_id.len() > 128
        || !record
            .record_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        || !record.record_id.as_bytes()[0].is_ascii_alphanumeric()
        || !record.record_id.as_bytes()[record.record_id.len() - 1].is_ascii_alphanumeric()
        || record.value.len() > MAX_RECORD_BYTES
        || record
            .key
            .as_ref()
            .is_some_and(|key| key.len() > MAX_HEADER_BYTES)
        || record.timestamp_ms < 0
        || record.content_type.is_empty()
        || record.content_type.len() > 128
        || !record.content_type.bytes().all(|b| (33..=126).contains(&b))
        || record.headers.len() > MAX_HEADERS
    {
        return Err(StreamError::InvalidRecord);
    }
    let mut header_bytes = 0usize;
    for (name, value) in &record.headers {
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_.-".contains(&b))
            || !name.as_bytes()[0].is_ascii_lowercase() && !name.as_bytes()[0].is_ascii_digit()
            || value.len() > 256
            || value.chars().any(|c| c <= '\u{1f}' || c == '\u{7f}')
        {
            return Err(StreamError::InvalidRecord);
        }
        header_bytes = header_bytes
            .saturating_add(name.len())
            .saturating_add(value.len());
    }
    if header_bytes > MAX_HEADER_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    Ok(())
}

/// User-defined synchronous batch transform implemented with the Rust SDK.
pub trait BatchProcessor: Send + Sync + 'static {
    /// Called once after Core sends its validated JSON configuration.
    fn activate(&self, _configuration_json: &[u8]) -> Result<(), ProcessorFailure> {
        Ok(())
    }

    /// Transform or filter one validated bounded batch. The caller owns delivery semantics.
    fn process(&self, batch: StreamBatch) -> Result<StreamBatch, ProcessorFailure>;

    /// Called before Core's process host shuts down.
    fn deactivate(&self) -> Result<(), ProcessorFailure> {
        Ok(())
    }
}

/// Sanitized processor failure; arbitrary provider details are not sent to Core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessorFailure;

impl std::fmt::Display for ProcessorFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stream processor failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn batch(records: Vec<StreamRecord>) -> StreamBatch {
        StreamBatch {
            records,
            max_records: MAX_RECORDS_PER_BATCH as u32,
            max_bytes: MAX_BATCH_BYTES as u32,
        }
    }

    fn record() -> StreamRecord {
        StreamRecord {
            record_id: "record-1".into(),
            value: vec![1, 2, 3],
            timestamp_ms: 5,
            key: None,
            content_type: "application/octet-stream".into(),
            headers: HashMap::new(),
        }
    }

    #[test]
    fn accepts_a_valid_bounded_batch() {
        assert_eq!(validate_batch(&batch(vec![record()])), Ok(()));
    }

    #[test]
    fn rejects_oversized_values_and_invalid_identifiers() {
        let mut value = record();
        value.value.resize(MAX_RECORD_BYTES + 1, 0);
        assert_eq!(
            validate_batch(&batch(vec![value])),
            Err(StreamError::InvalidRecord)
        );

        let mut id = record();
        id.record_id = "bad/id".into();
        assert_eq!(
            validate_batch(&batch(vec![id])),
            Err(StreamError::InvalidRecord)
        );
    }

    #[test]
    fn enforces_limits_on_outputs_as_well_as_inputs() {
        let mut invalid = batch(vec![record()]);
        invalid.max_bytes = (MAX_BATCH_BYTES + 1) as u32;
        assert_eq!(validate_batch(&invalid), Err(StreamError::InvalidBatch));

        let mut invalid = record();
        invalid
            .headers
            .insert("tenant".into(), "private\nvalue".into());
        assert_eq!(
            validate_batch(&batch(vec![invalid])),
            Err(StreamError::InvalidRecord)
        );
    }
}
