//! bucket.

use crate::lifecycle::LifecycleRule;
use crate::object::Object;
use std::collections::BTreeMap;

// Bucket
// ---------------------------------------------------------------------------

/// A storage bucket containing objects.
#[derive(Debug, Clone)]
pub struct Bucket {
    pub name: String,
    pub created_at: u64,
    pub versioning_enabled: bool,
    pub objects: BTreeMap<String, Object>,
    pub lifecycle_rules: Vec<LifecycleRule>,
}

// ---------------------------------------------------------------------------
