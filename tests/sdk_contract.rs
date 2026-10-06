use orbit_streams_rust::{
    MAX_BATCH_BYTES, MAX_RECORDS_PER_BATCH, StreamBatch, StreamRecord, validate_batch,
};
use std::collections::HashMap;

#[test]
fn public_sdk_accepts_the_shared_stream_contract() {
    let batch = StreamBatch {
        records: vec![StreamRecord {
            record_id: "event-1".into(),
            value: b"payload".to_vec(),
            timestamp_ms: 1,
            key: Some(b"key".to_vec()),
            content_type: "application/octet-stream".into(),
            headers: HashMap::from([("tenant".into(), "example".into())]),
        }],
        max_records: MAX_RECORDS_PER_BATCH as u32,
        max_bytes: MAX_BATCH_BYTES as u32,
    };

    assert!(validate_batch(&batch).is_ok());
}
