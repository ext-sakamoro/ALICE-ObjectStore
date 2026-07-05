//! Integration tests.

#![allow(
    clippy::wildcard_imports,
    clippy::too_many_lines,
    clippy::float_cmp,
    clippy::unwrap_used,
    clippy::indexing_slicing
)]

use crate::bucket::*;
use crate::errors::*;
use crate::etag::*;
use crate::lifecycle::*;
use crate::list_result::*;
use crate::metadata::*;
use crate::multipart::*;
use crate::object::*;
use crate::presigned::*;
use crate::store::*;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
