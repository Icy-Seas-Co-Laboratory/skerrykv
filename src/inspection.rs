//! Non-sensitive inspection records shared by native and MCP adapters.
//! Segment pages follow raw UUID/index order; they are not chronological snapshots.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub segment_id: String,
    pub state: String,
    /// Current logical file length; this is not an allocated disk-block measurement.
    pub physical_bytes: u64,
    /// End of committed record bytes, excluding any footer or growth capacity.
    pub committed_bytes: u64,
    pub object_count: u64,
    pub live_payload_bytes: u64,
    pub created_timestamp: u64,
    pub sealed_timestamp: Option<u64>,
    pub whole_segment_digest: Option<String>,
    /// Committed bytes divided by configured segment capacity. An imported
    /// segment may exceed the current configured capacity.
    pub utilization: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentPage {
    pub segments: Vec<SegmentInfo>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub hash: String,
    pub exists: bool,
    pub payload_length: Option<u64>,
    pub segment_id: Option<String>,
    pub payload_offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    pub segment_id: String,
    pub verified_bytes: u64,
    pub object_count: u64,
    /// Describes the metadata snapshot at verification start. A concurrent seal
    /// may finish later without changing the verified committed prefix.
    pub sealed: bool,
}
