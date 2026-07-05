//! lifecycle.

// Lifecycle
// ---------------------------------------------------------------------------

/// A lifecycle rule that expires objects older than a given duration.
#[derive(Debug, Clone)]
pub struct LifecycleRule {
    pub id: String,
    pub prefix: String,
    pub expiration_days: u64,
    pub enabled: bool,
}

// ---------------------------------------------------------------------------
