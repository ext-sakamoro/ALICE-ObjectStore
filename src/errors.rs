//! errors.

use std::fmt;

// Error
// ---------------------------------------------------------------------------

/// Errors returned by the object store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    BucketNotFound(String),
    BucketAlreadyExists(String),
    ObjectNotFound(String),
    VersionNotFound(String, String),
    UploadNotFound(String),
    PartNotFound(u32),
    InvalidPartOrder,
    NoParts,
    PresignedUrlExpired,
    PresignedUrlInvalid,
    InvalidLifecycleRule(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BucketNotFound(b) => write!(f, "bucket not found: {b}"),
            Self::BucketAlreadyExists(b) => write!(f, "bucket already exists: {b}"),
            Self::ObjectNotFound(k) => write!(f, "object not found: {k}"),
            Self::VersionNotFound(k, v) => write!(f, "version not found: {k} v={v}"),
            Self::UploadNotFound(id) => write!(f, "upload not found: {id}"),
            Self::PartNotFound(n) => write!(f, "part not found: {n}"),
            Self::InvalidPartOrder => write!(f, "parts must be in ascending order"),
            Self::NoParts => write!(f, "no parts uploaded"),
            Self::PresignedUrlExpired => write!(f, "presigned URL expired"),
            Self::PresignedUrlInvalid => write!(f, "presigned URL invalid"),
            Self::InvalidLifecycleRule(msg) => write!(f, "invalid lifecycle rule: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

pub type Result<T> = std::result::Result<T, StoreError>;

// ---------------------------------------------------------------------------
