//! list result.

// List result (pagination)
// ---------------------------------------------------------------------------

/// Result of a paginated list-objects operation.
#[derive(Debug, Clone)]
pub struct ListObjectsResult {
    pub objects: Vec<ObjectSummary>,
    pub next_continuation_token: Option<String>,
    pub is_truncated: bool,
}

/// Summary of an object returned in a listing.
#[derive(Debug, Clone)]
pub struct ObjectSummary {
    pub key: String,
    pub etag: String,
    pub size: usize,
    pub last_modified: u64,
}

// ---------------------------------------------------------------------------
