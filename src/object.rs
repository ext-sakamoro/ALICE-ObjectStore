//! object.

use crate::metadata::Metadata;

// Object / Version
// ---------------------------------------------------------------------------

/// A single version of an object.
#[derive(Debug, Clone)]
pub struct ObjectVersion {
    pub version_id: String,
    pub data: Vec<u8>,
    pub etag: String,
    pub metadata: Metadata,
    pub last_modified: u64,
    pub delete_marker: bool,
}

/// An object stored in a bucket, potentially with multiple versions.
#[derive(Debug, Clone)]
pub struct Object {
    pub key: String,
    pub versions: Vec<ObjectVersion>,
}

impl Object {
    pub(crate) fn current(&self) -> Option<&ObjectVersion> {
        self.versions.last().filter(|v| !v.delete_marker)
    }
}

// ---------------------------------------------------------------------------
