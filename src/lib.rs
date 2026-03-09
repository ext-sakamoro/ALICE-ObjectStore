#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

//! ALICE-ObjectStore: S3-compatible object storage engine.
//!
//! Pure Rust implementation with buckets, objects, multipart upload,
//! versioning, lifecycle policies, metadata, `ETags`, presigned URLs,
//! and pagination.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
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
// ETag helper
// ---------------------------------------------------------------------------

/// Compute a simple `ETag` from data bytes (FNV-1a 64-bit, hex-encoded).
fn compute_etag(data: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("\"{hash:016x}\"")
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// User-defined metadata attached to an object.
pub type Metadata = HashMap<String, String>;

// ---------------------------------------------------------------------------
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
    fn current(&self) -> Option<&ObjectVersion> {
        self.versions.last().filter(|v| !v.delete_marker)
    }
}

// ---------------------------------------------------------------------------
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
// ObjectStore
// ---------------------------------------------------------------------------

/// The main object store.
#[derive(Debug, Default)]
pub struct ObjectStore {
    buckets: BTreeMap<String, Bucket>,
    uploads: HashMap<String, MultipartUpload>,
    presigned_urls: HashMap<String, PresignedUrl>,
    next_upload_id: u64,
    next_version_id: u64,
    next_presigned_id: u64,
}

impl ObjectStore {
    /// Create a new empty object store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    // -- time helper --------------------------------------------------------

    fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
    }

    fn next_upload_id(&mut self) -> String {
        self.next_upload_id += 1;
        format!("upload-{}", self.next_upload_id)
    }

    fn next_version_id(&mut self) -> String {
        self.next_version_id += 1;
        format!("v{}", self.next_version_id)
    }

    fn next_presigned_token(&mut self) -> String {
        self.next_presigned_id += 1;
        format!("psurl-{}", self.next_presigned_id)
    }

    // -- Bucket CRUD --------------------------------------------------------

    /// Create a new bucket.
    ///
    /// # Errors
    /// Returns `BucketAlreadyExists` if the name is taken.
    pub fn create_bucket(&mut self, name: &str) -> Result<()> {
        if self.buckets.contains_key(name) {
            return Err(StoreError::BucketAlreadyExists(name.to_owned()));
        }
        self.buckets.insert(
            name.to_owned(),
            Bucket {
                name: name.to_owned(),
                created_at: Self::now_epoch(),
                versioning_enabled: false,
                objects: BTreeMap::new(),
                lifecycle_rules: Vec::new(),
            },
        );
        Ok(())
    }

    /// Delete a bucket. The bucket must exist.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if it does not exist.
    pub fn delete_bucket(&mut self, name: &str) -> Result<()> {
        self.buckets
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| StoreError::BucketNotFound(name.to_owned()))
    }

    /// Check whether a bucket exists.
    #[must_use]
    pub fn head_bucket(&self, name: &str) -> bool {
        self.buckets.contains_key(name)
    }

    /// List all bucket names.
    #[must_use]
    pub fn list_buckets(&self) -> Vec<String> {
        self.buckets.keys().cloned().collect()
    }

    // -- Versioning ---------------------------------------------------------

    /// Enable or disable versioning on a bucket.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    pub fn set_versioning(&mut self, bucket: &str, enabled: bool) -> Result<()> {
        let b = self
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        b.versioning_enabled = enabled;
        Ok(())
    }

    /// Check whether versioning is enabled on a bucket.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    pub fn get_versioning(&self, bucket: &str) -> Result<bool> {
        self.buckets
            .get(bucket)
            .map(|b| b.versioning_enabled)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))
    }

    // -- Object CRUD --------------------------------------------------------

    /// Put an object into a bucket.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    ///
    /// # Panics
    /// Will not panic; internal `expect` is guarded by prior existence check.
    pub fn put_object(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        metadata: Option<Metadata>,
    ) -> Result<String> {
        if !self.buckets.contains_key(bucket) {
            return Err(StoreError::BucketNotFound(bucket.to_owned()));
        }

        let etag = compute_etag(&data);
        let versioning_enabled = self.buckets[bucket].versioning_enabled;
        let version_id = if versioning_enabled {
            self.next_version_id()
        } else {
            "null".to_owned()
        };

        let version = ObjectVersion {
            version_id,
            data,
            etag: etag.clone(),
            metadata: metadata.unwrap_or_default(),
            last_modified: Self::now_epoch(),
            delete_marker: false,
        };

        let b = self.buckets.get_mut(bucket).expect("bucket exists");
        let obj = b.objects.entry(key.to_owned()).or_insert_with(|| Object {
            key: key.to_owned(),
            versions: Vec::new(),
        });

        if versioning_enabled {
            obj.versions.push(version);
        } else {
            obj.versions = vec![version];
        }

        Ok(etag)
    }

    /// Get the current version of an object.
    ///
    /// # Errors
    /// Returns `BucketNotFound` or `ObjectNotFound`.
    pub fn get_object(&self, bucket: &str, key: &str) -> Result<&ObjectVersion> {
        let b = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        b.objects
            .get(key)
            .and_then(Object::current)
            .ok_or_else(|| StoreError::ObjectNotFound(key.to_owned()))
    }

    /// Get a specific version of an object.
    ///
    /// # Errors
    /// Returns `BucketNotFound`, `ObjectNotFound`, or `VersionNotFound`.
    pub fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<&ObjectVersion> {
        let b = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        let obj = b
            .objects
            .get(key)
            .ok_or_else(|| StoreError::ObjectNotFound(key.to_owned()))?;
        obj.versions
            .iter()
            .find(|v| v.version_id == version_id)
            .ok_or_else(|| StoreError::VersionNotFound(key.to_owned(), version_id.to_owned()))
    }

    /// Head (metadata only) for the current version.
    ///
    /// # Errors
    /// Returns `BucketNotFound` or `ObjectNotFound`.
    pub fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectSummary> {
        let v = self.get_object(bucket, key)?;
        Ok(ObjectSummary {
            key: key.to_owned(),
            etag: v.etag.clone(),
            size: v.data.len(),
            last_modified: v.last_modified,
        })
    }

    /// Delete an object. With versioning, inserts a delete marker.
    ///
    /// # Errors
    /// Returns `BucketNotFound` or `ObjectNotFound`.
    pub fn delete_object(&mut self, bucket: &str, key: &str) -> Result<()> {
        let b = self
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;

        if b.versioning_enabled {
            let obj = b
                .objects
                .get_mut(key)
                .ok_or_else(|| StoreError::ObjectNotFound(key.to_owned()))?;
            let vid = {
                self.next_version_id += 1;
                format!("v{}", self.next_version_id)
            };
            obj.versions.push(ObjectVersion {
                version_id: vid,
                data: Vec::new(),
                etag: String::new(),
                metadata: Metadata::new(),
                last_modified: Self::now_epoch(),
                delete_marker: true,
            });
            Ok(())
        } else {
            b.objects
                .remove(key)
                .map(|_| ())
                .ok_or_else(|| StoreError::ObjectNotFound(key.to_owned()))
        }
    }

    /// Delete a specific version of an object.
    ///
    /// # Errors
    /// Returns `BucketNotFound`, `ObjectNotFound`, or `VersionNotFound`.
    pub fn delete_object_version(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<()> {
        let b = self
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        let obj = b
            .objects
            .get_mut(key)
            .ok_or_else(|| StoreError::ObjectNotFound(key.to_owned()))?;
        let idx = obj
            .versions
            .iter()
            .position(|v| v.version_id == version_id)
            .ok_or_else(|| StoreError::VersionNotFound(key.to_owned(), version_id.to_owned()))?;
        obj.versions.remove(idx);
        if obj.versions.is_empty() {
            b.objects.remove(key);
        }
        Ok(())
    }

    /// Copy an object within or across buckets.
    ///
    /// # Errors
    /// Returns `BucketNotFound` or `ObjectNotFound`.
    pub fn copy_object(
        &mut self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<String> {
        let v = self.get_object(src_bucket, src_key)?;
        let data = v.data.clone();
        let metadata = v.metadata.clone();
        self.put_object(dst_bucket, dst_key, data, Some(metadata))
    }

    // -- List objects (pagination) ------------------------------------------

    /// List objects in a bucket with optional prefix filter and pagination.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        max_keys: usize,
        continuation_token: Option<&str>,
    ) -> Result<ListObjectsResult> {
        let b = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;

        let prefix_str = prefix.unwrap_or("");
        let iter = b.objects.iter().filter(|(k, _)| k.starts_with(prefix_str));

        // Skip past continuation token
        let iter: Box<dyn Iterator<Item = (&String, &Object)>> =
            if let Some(token) = continuation_token {
                Box::new(iter.skip_while(move |(k, _)| k.as_str() <= token))
            } else {
                Box::new(iter)
            };

        let mut objects = Vec::new();
        let mut last_key = None;
        let mut count = 0;

        for (key, obj) in iter {
            if count >= max_keys {
                return Ok(ListObjectsResult {
                    objects,
                    next_continuation_token: last_key,
                    is_truncated: true,
                });
            }
            if let Some(v) = obj.current() {
                objects.push(ObjectSummary {
                    key: key.clone(),
                    etag: v.etag.clone(),
                    size: v.data.len(),
                    last_modified: v.last_modified,
                });
                last_key = Some(key.clone());
                count += 1;
            }
        }

        Ok(ListObjectsResult {
            objects,
            next_continuation_token: None,
            is_truncated: false,
        })
    }

    /// List object versions in a bucket.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    pub fn list_object_versions(&self, bucket: &str, key: &str) -> Result<Vec<&ObjectVersion>> {
        let b = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        let obj = b
            .objects
            .get(key)
            .ok_or_else(|| StoreError::ObjectNotFound(key.to_owned()))?;
        Ok(obj.versions.iter().collect())
    }

    // -- Multipart upload ---------------------------------------------------

    /// Initiate a multipart upload.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    pub fn create_multipart_upload(
        &mut self,
        bucket: &str,
        key: &str,
        metadata: Option<Metadata>,
    ) -> Result<String> {
        if !self.buckets.contains_key(bucket) {
            return Err(StoreError::BucketNotFound(bucket.to_owned()));
        }
        let upload_id = self.next_upload_id();
        self.uploads.insert(
            upload_id.clone(),
            MultipartUpload {
                upload_id: upload_id.clone(),
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                metadata: metadata.unwrap_or_default(),
                parts: BTreeMap::new(),
            },
        );
        Ok(upload_id)
    }

    /// Upload a part for a multipart upload.
    ///
    /// # Errors
    /// Returns `UploadNotFound` if the upload ID is invalid.
    pub fn upload_part(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> Result<String> {
        let upload = self
            .uploads
            .get_mut(upload_id)
            .ok_or_else(|| StoreError::UploadNotFound(upload_id.to_owned()))?;
        let etag = compute_etag(&data);
        upload.parts.insert(
            part_number,
            Part {
                part_number,
                data,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    /// List uploaded parts for a multipart upload.
    ///
    /// # Errors
    /// Returns `UploadNotFound` if the upload ID is invalid.
    pub fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, String)>> {
        let upload = self
            .uploads
            .get(upload_id)
            .ok_or_else(|| StoreError::UploadNotFound(upload_id.to_owned()))?;
        Ok(upload
            .parts
            .iter()
            .map(|(&n, p)| (n, p.etag.clone()))
            .collect())
    }

    /// Complete a multipart upload, assembling parts in order.
    ///
    /// `part_numbers` specifies the order; they must be ascending.
    ///
    /// # Errors
    /// Returns `UploadNotFound`, `NoParts`, `InvalidPartOrder`, or `PartNotFound`.
    pub fn complete_multipart_upload(
        &mut self,
        upload_id: &str,
        part_numbers: &[u32],
    ) -> Result<String> {
        if part_numbers.is_empty() {
            return Err(StoreError::NoParts);
        }
        for w in part_numbers.windows(2) {
            if w[0] >= w[1] {
                return Err(StoreError::InvalidPartOrder);
            }
        }

        let upload = self
            .uploads
            .remove(upload_id)
            .ok_or_else(|| StoreError::UploadNotFound(upload_id.to_owned()))?;

        let mut combined = Vec::new();
        for &pn in part_numbers {
            let part = upload.parts.get(&pn).ok_or(StoreError::PartNotFound(pn))?;
            combined.extend_from_slice(&part.data);
        }

        let metadata = upload.metadata;
        self.put_object(&upload.bucket, &upload.key, combined, Some(metadata))
    }

    /// Abort a multipart upload, discarding all parts.
    ///
    /// # Errors
    /// Returns `UploadNotFound` if the upload ID is invalid.
    pub fn abort_multipart_upload(&mut self, upload_id: &str) -> Result<()> {
        self.uploads
            .remove(upload_id)
            .map(|_| ())
            .ok_or_else(|| StoreError::UploadNotFound(upload_id.to_owned()))
    }

    // -- Lifecycle policies -------------------------------------------------

    /// Add a lifecycle rule to a bucket.
    ///
    /// # Errors
    /// Returns `BucketNotFound` or `InvalidLifecycleRule`.
    pub fn put_lifecycle_rule(&mut self, bucket: &str, rule: LifecycleRule) -> Result<()> {
        if rule.expiration_days == 0 {
            return Err(StoreError::InvalidLifecycleRule(
                "expiration_days must be > 0".to_owned(),
            ));
        }
        let b = self
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        b.lifecycle_rules.push(rule);
        Ok(())
    }

    /// List lifecycle rules for a bucket.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    pub fn get_lifecycle_rules(&self, bucket: &str) -> Result<&[LifecycleRule]> {
        let b = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        Ok(&b.lifecycle_rules)
    }

    /// Remove all lifecycle rules from a bucket.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    pub fn delete_lifecycle_rules(&mut self, bucket: &str) -> Result<()> {
        let b = self
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        b.lifecycle_rules.clear();
        Ok(())
    }

    /// Apply lifecycle rules, removing expired objects.
    /// `now` is the current epoch seconds.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    ///
    /// # Panics
    /// Will not panic; internal `expect` is guarded by prior existence check.
    pub fn apply_lifecycle(&mut self, bucket: &str, now: u64) -> Result<Vec<String>> {
        let b = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;

        let rules: Vec<LifecycleRule> = b
            .lifecycle_rules
            .iter()
            .filter(|r| r.enabled)
            .cloned()
            .collect();

        let mut expired_keys = Vec::new();
        let b = self.buckets.get_mut(bucket).expect("bucket exists");

        for rule in &rules {
            let expiration_secs = rule.expiration_days * 86400;
            let keys_to_remove: Vec<String> = b
                .objects
                .iter()
                .filter(|(k, _)| k.starts_with(&rule.prefix))
                .filter_map(|(k, obj)| {
                    obj.current().and_then(|v| {
                        if now.saturating_sub(v.last_modified) >= expiration_secs {
                            Some(k.clone())
                        } else {
                            None
                        }
                    })
                })
                .collect();

            for key in keys_to_remove {
                b.objects.remove(&key);
                expired_keys.push(key);
            }
        }

        Ok(expired_keys)
    }

    // -- Presigned URLs -----------------------------------------------------

    /// Generate a presigned URL token for GET access.
    ///
    /// # Errors
    /// Returns `BucketNotFound` or `ObjectNotFound`.
    pub fn generate_presigned_url(
        &mut self,
        bucket: &str,
        key: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        // Verify object exists
        let _ = self.get_object(bucket, key)?;

        let token = self.next_presigned_token();
        let url = format!("https://{bucket}.s3.example.com/{key}?token={token}");
        self.presigned_urls.insert(
            token,
            PresignedUrl {
                url: url.clone(),
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                expires_at: Self::now_epoch() + expires_in_secs,
            },
        );
        Ok(url)
    }

    /// Validate a presigned URL and return the object data if valid.
    ///
    /// # Errors
    /// Returns `PresignedUrlInvalid` or `PresignedUrlExpired`.
    pub fn access_presigned_url(&self, url: &str) -> Result<&ObjectVersion> {
        let token = url
            .split("token=")
            .nth(1)
            .ok_or(StoreError::PresignedUrlInvalid)?;

        let ps = self
            .presigned_urls
            .get(token)
            .ok_or(StoreError::PresignedUrlInvalid)?;

        if Self::now_epoch() > ps.expires_at {
            return Err(StoreError::PresignedUrlExpired);
        }

        self.get_object(&ps.bucket, &ps.key)
    }

    /// Validate a presigned URL at a specific time.
    ///
    /// # Errors
    /// Returns `PresignedUrlInvalid` or `PresignedUrlExpired`.
    pub fn access_presigned_url_at(&self, url: &str, now: u64) -> Result<&ObjectVersion> {
        let token = url
            .split("token=")
            .nth(1)
            .ok_or(StoreError::PresignedUrlInvalid)?;

        let ps = self
            .presigned_urls
            .get(token)
            .ok_or(StoreError::PresignedUrlInvalid)?;

        if now > ps.expires_at {
            return Err(StoreError::PresignedUrlExpired);
        }

        self.get_object(&ps.bucket, &ps.key)
    }

    // -- Object metadata operations -----------------------------------------

    /// Update metadata on an existing object (current version).
    ///
    /// # Errors
    /// Returns `BucketNotFound` or `ObjectNotFound`.
    pub fn update_metadata(&mut self, bucket: &str, key: &str, metadata: Metadata) -> Result<()> {
        let b = self
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        let obj = b
            .objects
            .get_mut(key)
            .ok_or_else(|| StoreError::ObjectNotFound(key.to_owned()))?;
        let v = obj
            .versions
            .last_mut()
            .ok_or_else(|| StoreError::ObjectNotFound(key.to_owned()))?;
        v.metadata = metadata;
        Ok(())
    }

    /// Get metadata for the current version of an object.
    ///
    /// # Errors
    /// Returns `BucketNotFound` or `ObjectNotFound`.
    pub fn get_metadata(&self, bucket: &str, key: &str) -> Result<&Metadata> {
        let v = self.get_object(bucket, key)?;
        Ok(&v.metadata)
    }

    // -- Bucket object count ------------------------------------------------

    /// Return the number of objects in a bucket.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    pub fn object_count(&self, bucket: &str) -> Result<usize> {
        let b = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        Ok(b.objects.len())
    }

    /// Return total size of all current object versions in a bucket.
    ///
    /// # Errors
    /// Returns `BucketNotFound` if the bucket does not exist.
    pub fn bucket_size(&self, bucket: &str) -> Result<usize> {
        let b = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::BucketNotFound(bucket.to_owned()))?;
        let total = b
            .objects
            .values()
            .filter_map(|obj| obj.current().map(|v| v.data.len()))
            .sum();
        Ok(total)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> ObjectStore {
        ObjectStore::new()
    }

    // -- Bucket tests -------------------------------------------------------

    #[test]
    fn create_bucket() {
        let mut s = store();
        assert!(s.create_bucket("b1").is_ok());
    }

    #[test]
    fn create_duplicate_bucket() {
        let mut s = store();
        s.create_bucket("b1").unwrap();
        assert_eq!(
            s.create_bucket("b1").unwrap_err(),
            StoreError::BucketAlreadyExists("b1".into())
        );
    }

    #[test]
    fn delete_bucket() {
        let mut s = store();
        s.create_bucket("b1").unwrap();
        assert!(s.delete_bucket("b1").is_ok());
    }

    #[test]
    fn delete_nonexistent_bucket() {
        let mut s = store();
        assert_eq!(
            s.delete_bucket("nope").unwrap_err(),
            StoreError::BucketNotFound("nope".into())
        );
    }

    #[test]
    fn head_bucket_exists() {
        let mut s = store();
        s.create_bucket("b1").unwrap();
        assert!(s.head_bucket("b1"));
    }

    #[test]
    fn head_bucket_not_exists() {
        let s = store();
        assert!(!s.head_bucket("nope"));
    }

    #[test]
    fn list_buckets_empty() {
        let s = store();
        assert!(s.list_buckets().is_empty());
    }

    #[test]
    fn list_buckets_multiple() {
        let mut s = store();
        s.create_bucket("a").unwrap();
        s.create_bucket("b").unwrap();
        s.create_bucket("c").unwrap();
        assert_eq!(s.list_buckets(), vec!["a", "b", "c"]);
    }

    // -- Object tests -------------------------------------------------------

    #[test]
    fn put_and_get_object() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "key1", b"hello".to_vec(), None).unwrap();
        let obj = s.get_object("b", "key1").unwrap();
        assert_eq!(obj.data, b"hello");
    }

    #[test]
    fn put_object_no_bucket() {
        let mut s = store();
        assert_eq!(
            s.put_object("nope", "k", vec![], None).unwrap_err(),
            StoreError::BucketNotFound("nope".into())
        );
    }

    #[test]
    fn get_object_not_found() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert_eq!(
            s.get_object("b", "nope").unwrap_err(),
            StoreError::ObjectNotFound("nope".into())
        );
    }

    #[test]
    fn put_object_returns_etag() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let etag = s.put_object("b", "k", b"data".to_vec(), None).unwrap();
        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));
    }

    #[test]
    fn etag_deterministic() {
        let e1 = compute_etag(b"same");
        let e2 = compute_etag(b"same");
        assert_eq!(e1, e2);
    }

    #[test]
    fn etag_differs_for_different_data() {
        let e1 = compute_etag(b"aaa");
        let e2 = compute_etag(b"bbb");
        assert_ne!(e1, e2);
    }

    #[test]
    fn head_object_returns_summary() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"12345".to_vec(), None).unwrap();
        let summary = s.head_object("b", "k").unwrap();
        assert_eq!(summary.key, "k");
        assert_eq!(summary.size, 5);
    }

    #[test]
    fn delete_object_without_versioning() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"x".to_vec(), None).unwrap();
        s.delete_object("b", "k").unwrap();
        assert!(s.get_object("b", "k").is_err());
    }

    #[test]
    fn delete_object_not_found() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert_eq!(
            s.delete_object("b", "nope").unwrap_err(),
            StoreError::ObjectNotFound("nope".into())
        );
    }

    #[test]
    fn overwrite_object() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"v1".to_vec(), None).unwrap();
        s.put_object("b", "k", b"v2".to_vec(), None).unwrap();
        assert_eq!(s.get_object("b", "k").unwrap().data, b"v2");
    }

    #[test]
    fn put_empty_data() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", vec![], None).unwrap();
        assert!(s.get_object("b", "k").unwrap().data.is_empty());
    }

    #[test]
    fn put_large_data() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let data = vec![0xAB; 1_000_000];
        s.put_object("b", "k", data.clone(), None).unwrap();
        assert_eq!(s.get_object("b", "k").unwrap().data.len(), 1_000_000);
    }

    #[test]
    fn copy_object_same_bucket() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "src", b"data".to_vec(), None).unwrap();
        s.copy_object("b", "src", "b", "dst").unwrap();
        assert_eq!(s.get_object("b", "dst").unwrap().data, b"data");
    }

    #[test]
    fn copy_object_cross_bucket() {
        let mut s = store();
        s.create_bucket("b1").unwrap();
        s.create_bucket("b2").unwrap();
        s.put_object("b1", "k", b"cross".to_vec(), None).unwrap();
        s.copy_object("b1", "k", "b2", "k").unwrap();
        assert_eq!(s.get_object("b2", "k").unwrap().data, b"cross");
    }

    #[test]
    fn copy_preserves_metadata() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let mut meta = Metadata::new();
        meta.insert("foo".into(), "bar".into());
        s.put_object("b", "k", b"d".to_vec(), Some(meta)).unwrap();
        s.copy_object("b", "k", "b", "k2").unwrap();
        assert_eq!(
            s.get_object("b", "k2")
                .unwrap()
                .metadata
                .get("foo")
                .unwrap(),
            "bar"
        );
    }

    // -- Metadata tests -----------------------------------------------------

    #[test]
    fn put_with_metadata() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let mut meta = Metadata::new();
        meta.insert("content-type".into(), "text/plain".into());
        s.put_object("b", "k", b"hi".to_vec(), Some(meta)).unwrap();
        let m = s.get_metadata("b", "k").unwrap();
        assert_eq!(m.get("content-type").unwrap(), "text/plain");
    }

    #[test]
    fn update_metadata() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"x".to_vec(), None).unwrap();
        let mut meta = Metadata::new();
        meta.insert("tag".into(), "updated".into());
        s.update_metadata("b", "k", meta).unwrap();
        assert_eq!(
            s.get_metadata("b", "k").unwrap().get("tag").unwrap(),
            "updated"
        );
    }

    #[test]
    fn get_metadata_not_found() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert!(s.get_metadata("b", "nope").is_err());
    }

    #[test]
    fn update_metadata_no_bucket() {
        let mut s = store();
        assert!(s.update_metadata("nope", "k", Metadata::new()).is_err());
    }

    #[test]
    fn update_metadata_no_object() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert!(s.update_metadata("b", "nope", Metadata::new()).is_err());
    }

    // -- Versioning tests ---------------------------------------------------

    #[test]
    fn versioning_default_disabled() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert!(!s.get_versioning("b").unwrap());
    }

    #[test]
    fn enable_versioning() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.set_versioning("b", true).unwrap();
        assert!(s.get_versioning("b").unwrap());
    }

    #[test]
    fn versioning_no_bucket() {
        let mut s = store();
        assert!(s.set_versioning("nope", true).is_err());
    }

    #[test]
    fn versioned_put_keeps_history() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.set_versioning("b", true).unwrap();
        s.put_object("b", "k", b"v1".to_vec(), None).unwrap();
        s.put_object("b", "k", b"v2".to_vec(), None).unwrap();
        let versions = s.list_object_versions("b", "k").unwrap();
        assert_eq!(versions.len(), 2);
    }

    #[test]
    fn versioned_get_returns_latest() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.set_versioning("b", true).unwrap();
        s.put_object("b", "k", b"v1".to_vec(), None).unwrap();
        s.put_object("b", "k", b"v2".to_vec(), None).unwrap();
        assert_eq!(s.get_object("b", "k").unwrap().data, b"v2");
    }

    #[test]
    fn get_specific_version() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.set_versioning("b", true).unwrap();
        s.put_object("b", "k", b"v1".to_vec(), None).unwrap();
        let versions = s.list_object_versions("b", "k").unwrap();
        let vid = &versions[0].version_id;
        let v = s.get_object_version("b", "k", vid).unwrap();
        assert_eq!(v.data, b"v1");
    }

    #[test]
    fn version_not_found() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"d".to_vec(), None).unwrap();
        assert!(s.get_object_version("b", "k", "bad-version").is_err());
    }

    #[test]
    fn delete_with_versioning_adds_marker() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.set_versioning("b", true).unwrap();
        s.put_object("b", "k", b"d".to_vec(), None).unwrap();
        s.delete_object("b", "k").unwrap();
        // Object should not be found (delete marker is latest)
        assert!(s.get_object("b", "k").is_err());
        // But versions still exist
        let versions = s.list_object_versions("b", "k").unwrap();
        assert_eq!(versions.len(), 2);
        assert!(versions.last().unwrap().delete_marker);
    }

    #[test]
    fn delete_specific_version() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.set_versioning("b", true).unwrap();
        s.put_object("b", "k", b"v1".to_vec(), None).unwrap();
        s.put_object("b", "k", b"v2".to_vec(), None).unwrap();
        let versions = s.list_object_versions("b", "k").unwrap();
        let vid = versions[0].version_id.clone();
        s.delete_object_version("b", "k", &vid).unwrap();
        let remaining = s.list_object_versions("b", "k").unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn delete_last_version_removes_object() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"only".to_vec(), None).unwrap();
        let versions = s.list_object_versions("b", "k").unwrap();
        let vid = versions[0].version_id.clone();
        s.delete_object_version("b", "k", &vid).unwrap();
        assert!(s.get_object("b", "k").is_err());
    }

    #[test]
    fn version_id_is_null_without_versioning() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"d".to_vec(), None).unwrap();
        let v = s.get_object("b", "k").unwrap();
        assert_eq!(v.version_id, "null");
    }

    #[test]
    fn version_ids_are_unique() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.set_versioning("b", true).unwrap();
        s.put_object("b", "k", b"v1".to_vec(), None).unwrap();
        s.put_object("b", "k", b"v2".to_vec(), None).unwrap();
        let versions = s.list_object_versions("b", "k").unwrap();
        assert_ne!(versions[0].version_id, versions[1].version_id);
    }

    // -- Pagination tests ---------------------------------------------------

    #[test]
    fn list_objects_empty_bucket() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let res = s.list_objects("b", None, 10, None).unwrap();
        assert!(res.objects.is_empty());
        assert!(!res.is_truncated);
    }

    #[test]
    fn list_objects_all() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        for i in 0..5 {
            s.put_object("b", &format!("key{i}"), vec![i as u8], None)
                .unwrap();
        }
        let res = s.list_objects("b", None, 10, None).unwrap();
        assert_eq!(res.objects.len(), 5);
        assert!(!res.is_truncated);
    }

    #[test]
    fn list_objects_paginated() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        for i in 0..5 {
            s.put_object("b", &format!("key{i}"), vec![i as u8], None)
                .unwrap();
        }
        let page1 = s.list_objects("b", None, 2, None).unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        assert!(page1.next_continuation_token.is_some());

        let page2 = s
            .list_objects("b", None, 2, page1.next_continuation_token.as_deref())
            .unwrap();
        assert_eq!(page2.objects.len(), 2);
        assert!(page2.is_truncated);

        let page3 = s
            .list_objects("b", None, 2, page2.next_continuation_token.as_deref())
            .unwrap();
        assert_eq!(page3.objects.len(), 1);
        assert!(!page3.is_truncated);
    }

    #[test]
    fn list_objects_with_prefix() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "images/a.png", vec![], None).unwrap();
        s.put_object("b", "images/b.png", vec![], None).unwrap();
        s.put_object("b", "docs/readme.txt", vec![], None).unwrap();
        let res = s.list_objects("b", Some("images/"), 10, None).unwrap();
        assert_eq!(res.objects.len(), 2);
    }

    #[test]
    fn list_objects_no_bucket() {
        let s = store();
        assert!(s.list_objects("nope", None, 10, None).is_err());
    }

    #[test]
    fn list_objects_prefix_no_match() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "foo", vec![], None).unwrap();
        let res = s.list_objects("b", Some("bar"), 10, None).unwrap();
        assert!(res.objects.is_empty());
    }

    #[test]
    fn list_objects_max_keys_one() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        for i in 0..3 {
            s.put_object("b", &format!("k{i}"), vec![], None).unwrap();
        }
        let res = s.list_objects("b", None, 1, None).unwrap();
        assert_eq!(res.objects.len(), 1);
        assert!(res.is_truncated);
    }

    #[test]
    fn list_object_versions_no_bucket() {
        let s = store();
        assert!(s.list_object_versions("nope", "k").is_err());
    }

    #[test]
    fn list_object_versions_no_object() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert!(s.list_object_versions("b", "nope").is_err());
    }

    // -- Multipart upload tests ---------------------------------------------

    #[test]
    fn multipart_basic() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "big", None).unwrap();
        s.upload_part(&uid, 1, b"part1".to_vec()).unwrap();
        s.upload_part(&uid, 2, b"part2".to_vec()).unwrap();
        s.complete_multipart_upload(&uid, &[1, 2]).unwrap();
        assert_eq!(s.get_object("b", "big").unwrap().data, b"part1part2");
    }

    #[test]
    fn multipart_no_bucket() {
        let mut s = store();
        assert!(s.create_multipart_upload("nope", "k", None).is_err());
    }

    #[test]
    fn upload_part_no_upload() {
        let mut s = store();
        assert!(s.upload_part("bad", 1, vec![]).is_err());
    }

    #[test]
    fn complete_no_parts() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        s.upload_part(&uid, 1, b"d".to_vec()).unwrap();
        assert_eq!(
            s.complete_multipart_upload(&uid, &[]).unwrap_err(),
            StoreError::NoParts
        );
    }

    #[test]
    fn complete_invalid_order() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        s.upload_part(&uid, 1, b"a".to_vec()).unwrap();
        s.upload_part(&uid, 2, b"b".to_vec()).unwrap();
        assert_eq!(
            s.complete_multipart_upload(&uid, &[2, 1]).unwrap_err(),
            StoreError::InvalidPartOrder
        );
    }

    #[test]
    fn complete_missing_part() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        s.upload_part(&uid, 1, b"a".to_vec()).unwrap();
        assert_eq!(
            s.complete_multipart_upload(&uid, &[1, 2]).unwrap_err(),
            StoreError::PartNotFound(2)
        );
    }

    #[test]
    fn abort_multipart() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        s.upload_part(&uid, 1, b"x".to_vec()).unwrap();
        s.abort_multipart_upload(&uid).unwrap();
        assert!(s.upload_part(&uid, 2, vec![]).is_err());
    }

    #[test]
    fn abort_nonexistent_upload() {
        let mut s = store();
        assert!(s.abort_multipart_upload("nope").is_err());
    }

    #[test]
    fn list_parts() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        s.upload_part(&uid, 1, b"a".to_vec()).unwrap();
        s.upload_part(&uid, 3, b"c".to_vec()).unwrap();
        let parts = s.list_parts(&uid).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].0, 1);
        assert_eq!(parts[1].0, 3);
    }

    #[test]
    fn list_parts_no_upload() {
        let s = store();
        assert!(s.list_parts("nope").is_err());
    }

    #[test]
    fn multipart_with_metadata() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let mut meta = Metadata::new();
        meta.insert("key".into(), "val".into());
        let uid = s.create_multipart_upload("b", "k", Some(meta)).unwrap();
        s.upload_part(&uid, 1, b"d".to_vec()).unwrap();
        s.complete_multipart_upload(&uid, &[1]).unwrap();
        assert_eq!(s.get_metadata("b", "k").unwrap().get("key").unwrap(), "val");
    }

    #[test]
    fn multipart_overwrite_part() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        s.upload_part(&uid, 1, b"old".to_vec()).unwrap();
        s.upload_part(&uid, 1, b"new".to_vec()).unwrap();
        s.complete_multipart_upload(&uid, &[1]).unwrap();
        assert_eq!(s.get_object("b", "k").unwrap().data, b"new");
    }

    #[test]
    fn multipart_three_parts() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        s.upload_part(&uid, 1, b"A".to_vec()).unwrap();
        s.upload_part(&uid, 2, b"B".to_vec()).unwrap();
        s.upload_part(&uid, 3, b"C".to_vec()).unwrap();
        s.complete_multipart_upload(&uid, &[1, 2, 3]).unwrap();
        assert_eq!(s.get_object("b", "k").unwrap().data, b"ABC");
    }

    #[test]
    fn complete_multipart_duplicate_order() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        s.upload_part(&uid, 1, b"a".to_vec()).unwrap();
        assert_eq!(
            s.complete_multipart_upload(&uid, &[1, 1]).unwrap_err(),
            StoreError::InvalidPartOrder
        );
    }

    // -- Lifecycle tests ----------------------------------------------------

    #[test]
    fn add_lifecycle_rule() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_lifecycle_rule(
            "b",
            LifecycleRule {
                id: "r1".into(),
                prefix: "logs/".into(),
                expiration_days: 30,
                enabled: true,
            },
        )
        .unwrap();
        let rules = s.get_lifecycle_rules("b").unwrap();
        assert_eq!(rules.len(), 1);
    }

    #[test]
    fn lifecycle_invalid_zero_days() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert_eq!(
            s.put_lifecycle_rule(
                "b",
                LifecycleRule {
                    id: "r".into(),
                    prefix: String::new(),
                    expiration_days: 0,
                    enabled: true,
                },
            )
            .unwrap_err(),
            StoreError::InvalidLifecycleRule("expiration_days must be > 0".into())
        );
    }

    #[test]
    fn lifecycle_no_bucket() {
        let mut s = store();
        assert!(s
            .put_lifecycle_rule(
                "nope",
                LifecycleRule {
                    id: "r".into(),
                    prefix: String::new(),
                    expiration_days: 1,
                    enabled: true,
                },
            )
            .is_err());
    }

    #[test]
    fn delete_lifecycle_rules() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_lifecycle_rule(
            "b",
            LifecycleRule {
                id: "r1".into(),
                prefix: String::new(),
                expiration_days: 1,
                enabled: true,
            },
        )
        .unwrap();
        s.delete_lifecycle_rules("b").unwrap();
        assert!(s.get_lifecycle_rules("b").unwrap().is_empty());
    }

    #[test]
    fn delete_lifecycle_no_bucket() {
        let mut s = store();
        assert!(s.delete_lifecycle_rules("nope").is_err());
    }

    #[test]
    fn apply_lifecycle_expires_objects() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "logs/old.txt", b"old".to_vec(), None)
            .unwrap();
        s.put_object("b", "logs/new.txt", b"new".to_vec(), None)
            .unwrap();
        s.put_object("b", "keep.txt", b"keep".to_vec(), None)
            .unwrap();

        s.put_lifecycle_rule(
            "b",
            LifecycleRule {
                id: "r1".into(),
                prefix: "logs/".into(),
                expiration_days: 1,
                enabled: true,
            },
        )
        .unwrap();

        // Simulate future time: now + 2 days
        let far_future = ObjectStore::now_epoch() + 2 * 86400;
        let expired = s.apply_lifecycle("b", far_future).unwrap();
        assert_eq!(expired.len(), 2);
        // "keep.txt" should remain
        assert!(s.get_object("b", "keep.txt").is_ok());
    }

    #[test]
    fn apply_lifecycle_disabled_rule_ignored() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "logs/a", b"d".to_vec(), None).unwrap();

        s.put_lifecycle_rule(
            "b",
            LifecycleRule {
                id: "r".into(),
                prefix: "logs/".into(),
                expiration_days: 1,
                enabled: false,
            },
        )
        .unwrap();

        let far_future = ObjectStore::now_epoch() + 2 * 86400;
        let expired = s.apply_lifecycle("b", far_future).unwrap();
        assert!(expired.is_empty());
    }

    #[test]
    fn apply_lifecycle_no_bucket() {
        let mut s = store();
        assert!(s.apply_lifecycle("nope", 0).is_err());
    }

    #[test]
    fn lifecycle_multiple_rules() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "logs/a", b"d".to_vec(), None).unwrap();
        s.put_object("b", "tmp/b", b"d".to_vec(), None).unwrap();

        s.put_lifecycle_rule(
            "b",
            LifecycleRule {
                id: "r1".into(),
                prefix: "logs/".into(),
                expiration_days: 1,
                enabled: true,
            },
        )
        .unwrap();
        s.put_lifecycle_rule(
            "b",
            LifecycleRule {
                id: "r2".into(),
                prefix: "tmp/".into(),
                expiration_days: 1,
                enabled: true,
            },
        )
        .unwrap();

        let far_future = ObjectStore::now_epoch() + 2 * 86400;
        let expired = s.apply_lifecycle("b", far_future).unwrap();
        assert_eq!(expired.len(), 2);
    }

    #[test]
    fn get_lifecycle_rules_no_bucket() {
        let s = store();
        assert!(s.get_lifecycle_rules("nope").is_err());
    }

    // -- Presigned URL tests ------------------------------------------------

    #[test]
    fn presigned_url_basic() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"secret".to_vec(), None).unwrap();
        let url = s.generate_presigned_url("b", "k", 3600).unwrap();
        assert!(url.contains("token="));
        let obj = s.access_presigned_url(&url).unwrap();
        assert_eq!(obj.data, b"secret");
    }

    #[test]
    fn presigned_url_no_object() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert!(s.generate_presigned_url("b", "nope", 100).is_err());
    }

    #[test]
    fn presigned_url_no_bucket() {
        let mut s = store();
        assert!(s.generate_presigned_url("nope", "k", 100).is_err());
    }

    #[test]
    fn presigned_url_invalid_token() {
        let s = store();
        assert_eq!(
            s.access_presigned_url("https://bad?token=nope")
                .unwrap_err(),
            StoreError::PresignedUrlInvalid
        );
    }

    #[test]
    fn presigned_url_no_token_param() {
        let s = store();
        assert_eq!(
            s.access_presigned_url("https://bad").unwrap_err(),
            StoreError::PresignedUrlInvalid
        );
    }

    #[test]
    fn presigned_url_expired() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"d".to_vec(), None).unwrap();
        let url = s.generate_presigned_url("b", "k", 10).unwrap();
        // Check at now + 20 seconds
        let future = ObjectStore::now_epoch() + 20;
        assert_eq!(
            s.access_presigned_url_at(&url, future).unwrap_err(),
            StoreError::PresignedUrlExpired
        );
    }

    #[test]
    fn presigned_url_valid_before_expiry() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "k", b"d".to_vec(), None).unwrap();
        let url = s.generate_presigned_url("b", "k", 3600).unwrap();
        let now = ObjectStore::now_epoch();
        let obj = s.access_presigned_url_at(&url, now).unwrap();
        assert_eq!(obj.data, b"d");
    }

    // -- Object count / size ------------------------------------------------

    #[test]
    fn object_count_empty() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert_eq!(s.object_count("b").unwrap(), 0);
    }

    #[test]
    fn object_count_after_puts() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "a", vec![], None).unwrap();
        s.put_object("b", "b", vec![], None).unwrap();
        assert_eq!(s.object_count("b").unwrap(), 2);
    }

    #[test]
    fn object_count_no_bucket() {
        let s = store();
        assert!(s.object_count("nope").is_err());
    }

    #[test]
    fn bucket_size_empty() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert_eq!(s.bucket_size("b").unwrap(), 0);
    }

    #[test]
    fn bucket_size_sum() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "a", b"123".to_vec(), None).unwrap();
        s.put_object("b", "b", b"45".to_vec(), None).unwrap();
        assert_eq!(s.bucket_size("b").unwrap(), 5);
    }

    #[test]
    fn bucket_size_no_bucket() {
        let s = store();
        assert!(s.bucket_size("nope").is_err());
    }

    // -- Error display tests ------------------------------------------------

    #[test]
    fn error_display_bucket_not_found() {
        let e = StoreError::BucketNotFound("x".into());
        assert_eq!(format!("{e}"), "bucket not found: x");
    }

    #[test]
    fn error_display_bucket_already_exists() {
        let e = StoreError::BucketAlreadyExists("x".into());
        assert_eq!(format!("{e}"), "bucket already exists: x");
    }

    #[test]
    fn error_display_object_not_found() {
        let e = StoreError::ObjectNotFound("k".into());
        assert_eq!(format!("{e}"), "object not found: k");
    }

    #[test]
    fn error_display_version_not_found() {
        let e = StoreError::VersionNotFound("k".into(), "v1".into());
        assert_eq!(format!("{e}"), "version not found: k v=v1");
    }

    #[test]
    fn error_display_upload_not_found() {
        let e = StoreError::UploadNotFound("u".into());
        assert_eq!(format!("{e}"), "upload not found: u");
    }

    #[test]
    fn error_display_part_not_found() {
        let e = StoreError::PartNotFound(5);
        assert_eq!(format!("{e}"), "part not found: 5");
    }

    #[test]
    fn error_display_invalid_part_order() {
        let e = StoreError::InvalidPartOrder;
        assert_eq!(format!("{e}"), "parts must be in ascending order");
    }

    #[test]
    fn error_display_no_parts() {
        let e = StoreError::NoParts;
        assert_eq!(format!("{e}"), "no parts uploaded");
    }

    #[test]
    fn error_display_presigned_expired() {
        let e = StoreError::PresignedUrlExpired;
        assert_eq!(format!("{e}"), "presigned URL expired");
    }

    #[test]
    fn error_display_presigned_invalid() {
        let e = StoreError::PresignedUrlInvalid;
        assert_eq!(format!("{e}"), "presigned URL invalid");
    }

    #[test]
    fn error_display_invalid_lifecycle() {
        let e = StoreError::InvalidLifecycleRule("bad".into());
        assert_eq!(format!("{e}"), "invalid lifecycle rule: bad");
    }

    // -- Misc / edge cases --------------------------------------------------

    #[test]
    fn etag_empty_data() {
        let e = compute_etag(b"");
        assert!(!e.is_empty());
    }

    #[test]
    fn store_default_is_empty() {
        let s = ObjectStore::new();
        assert!(s.list_buckets().is_empty());
    }

    #[test]
    fn multiple_buckets_independent() {
        let mut s = store();
        s.create_bucket("b1").unwrap();
        s.create_bucket("b2").unwrap();
        s.put_object("b1", "k", b"in-b1".to_vec(), None).unwrap();
        assert!(s.get_object("b2", "k").is_err());
    }

    #[test]
    fn head_object_no_bucket() {
        let s = store();
        assert!(s.head_object("nope", "k").is_err());
    }

    #[test]
    fn head_object_no_key() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert!(s.head_object("b", "nope").is_err());
    }

    #[test]
    fn copy_object_no_src_bucket() {
        let mut s = store();
        s.create_bucket("dst").unwrap();
        assert!(s.copy_object("nope", "k", "dst", "k").is_err());
    }

    #[test]
    fn copy_object_no_src_key() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert!(s.copy_object("b", "nope", "b", "dst").is_err());
    }

    #[test]
    fn delete_object_no_bucket() {
        let mut s = store();
        assert!(s.delete_object("nope", "k").is_err());
    }

    #[test]
    fn delete_version_no_bucket() {
        let mut s = store();
        assert!(s.delete_object_version("nope", "k", "v").is_err());
    }

    #[test]
    fn delete_version_no_key() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        assert!(s.delete_object_version("b", "nope", "v").is_err());
    }

    #[test]
    fn disable_versioning() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.set_versioning("b", true).unwrap();
        s.set_versioning("b", false).unwrap();
        assert!(!s.get_versioning("b").unwrap());
    }

    #[test]
    fn get_versioning_no_bucket() {
        let s = store();
        assert!(s.get_versioning("nope").is_err());
    }

    #[test]
    fn many_objects_pagination_full_scan() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        for i in 0..20 {
            s.put_object("b", &format!("obj-{i:03}"), vec![i as u8], None)
                .unwrap();
        }
        let mut all = Vec::new();
        let mut token = None;
        loop {
            let res = s.list_objects("b", None, 7, token.as_deref()).unwrap();
            all.extend(res.objects);
            if res.is_truncated {
                token = res.next_continuation_token;
            } else {
                break;
            }
        }
        assert_eq!(all.len(), 20);
    }

    #[test]
    fn error_is_std_error() {
        let e: Box<dyn std::error::Error> = Box::new(StoreError::BucketNotFound("x".into()));
        assert!(!e.to_string().is_empty());
    }

    #[test]
    fn error_clone_and_eq() {
        let e1 = StoreError::NoParts;
        let e2 = e1.clone();
        assert_eq!(e1, e2);
    }

    #[test]
    fn upload_part_returns_etag() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        let etag = s.upload_part(&uid, 1, b"data".to_vec()).unwrap();
        assert!(etag.starts_with('"'));
    }

    #[test]
    fn multipart_partial_completion() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        let uid = s.create_multipart_upload("b", "k", None).unwrap();
        s.upload_part(&uid, 1, b"A".to_vec()).unwrap();
        s.upload_part(&uid, 2, b"B".to_vec()).unwrap();
        s.upload_part(&uid, 3, b"C".to_vec()).unwrap();
        // Only use parts 1 and 3
        s.complete_multipart_upload(&uid, &[1, 3]).unwrap();
        assert_eq!(s.get_object("b", "k").unwrap().data, b"AC");
    }

    #[test]
    fn presigned_url_contains_bucket_and_key() {
        let mut s = store();
        s.create_bucket("my-bucket").unwrap();
        s.put_object("my-bucket", "path/to/file.txt", b"d".to_vec(), None)
            .unwrap();
        let url = s
            .generate_presigned_url("my-bucket", "path/to/file.txt", 60)
            .unwrap();
        assert!(url.contains("my-bucket"));
        assert!(url.contains("path/to/file.txt"));
    }

    #[test]
    fn etag_16_hex_chars() {
        let e = compute_etag(b"test");
        // Format: "XXXXXXXXXXXXXXXX" (16 hex chars + 2 quotes)
        assert_eq!(e.len(), 18);
    }

    #[test]
    fn lifecycle_rule_with_empty_prefix_matches_all() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.put_object("b", "a", b"d".to_vec(), None).unwrap();
        s.put_object("b", "b", b"d".to_vec(), None).unwrap();
        s.put_lifecycle_rule(
            "b",
            LifecycleRule {
                id: "all".into(),
                prefix: String::new(),
                expiration_days: 1,
                enabled: true,
            },
        )
        .unwrap();
        let far = ObjectStore::now_epoch() + 2 * 86400;
        let expired = s.apply_lifecycle("b", far).unwrap();
        assert_eq!(expired.len(), 2);
    }

    #[test]
    fn versioned_object_etags_differ() {
        let mut s = store();
        s.create_bucket("b").unwrap();
        s.set_versioning("b", true).unwrap();
        s.put_object("b", "k", b"v1".to_vec(), None).unwrap();
        s.put_object("b", "k", b"v2".to_vec(), None).unwrap();
        let versions = s.list_object_versions("b", "k").unwrap();
        assert_ne!(versions[0].etag, versions[1].etag);
    }
}
