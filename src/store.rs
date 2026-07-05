//! store.

use crate::bucket::Bucket;
use crate::errors::{Result, StoreError};
use crate::etag::compute_etag;
use crate::lifecycle::LifecycleRule;
use crate::list_result::{ListObjectsResult, ObjectSummary};
use crate::metadata::Metadata;
use crate::multipart::{MultipartUpload, Part};
use crate::object::{Object, ObjectVersion};
use crate::presigned::PresignedUrl;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    pub(crate) fn now_epoch() -> u64 {
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
