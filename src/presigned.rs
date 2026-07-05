//! presigned.


// Presigned URL
// ---------------------------------------------------------------------------

/// A presigned URL token for time-limited access.
#[derive(Debug, Clone)]
pub struct PresignedUrl {
    pub url: String,
    pub bucket: String,
    pub key: String,
    pub expires_at: u64,
}

// ---------------------------------------------------------------------------
