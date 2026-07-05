//! multipart.

use crate::metadata::Metadata;
use std::collections::BTreeMap;

// Multipart upload
// ---------------------------------------------------------------------------

/// A part uploaded as part of a multipart upload.
#[derive(Debug, Clone)]
pub struct Part {
    pub part_number: u32,
    pub data: Vec<u8>,
    pub etag: String,
}

/// State for an in-progress multipart upload.
#[derive(Debug, Clone)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub metadata: Metadata,
    pub parts: BTreeMap<u32, Part>,
}

// ---------------------------------------------------------------------------
