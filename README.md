**English** | [日本語](README_JP.md)

# ALICE-ObjectStore

**ALICE S3-Compatible Object Storage Engine** — Pure Rust implementation with buckets, objects, multipart upload, versioning, lifecycle policies, presigned URLs, and pagination.

Part of [Project A.L.I.C.E.](https://github.com/anthropics/alice) ecosystem.

## Features

- **Bucket Management** — Create, delete, and list buckets
- **Object CRUD** — Put, get, delete, and list objects with key-based access
- **Multipart Upload** — Chunked upload with part management and completion
- **Versioning** — Object version history with version ID tracking
- **Lifecycle Policies** — Automatic object expiration and transition rules
- **ETags** — FNV-1a hash-based content integrity verification
- **Presigned URLs** — Time-limited access URL generation and validation
- **Metadata** — User-defined key-value metadata on objects
- **Pagination** — Cursor-based object listing for large buckets

## Architecture

```
ObjectStore
 ├── Bucket
 │    ├── versioning_enabled: bool
 │    ├── lifecycle_rules: Vec<LifecycleRule>
 │    └── objects: BTreeMap<String, Object>
 │
 ├── Object
 │    ├── key: String
 │    └── versions: Vec<ObjectVersion>
 │         ├── version_id
 │         ├── data + etag
 │         ├── metadata
 │         └── delete_marker
 │
 └── MultipartUpload
      ├── upload_id
      ├── parts: BTreeMap<u32, Part>
      └── complete() → Object
```

## Quick Start

```rust
use alice_objectstore::ObjectStore;

let mut store = ObjectStore::new();
store.create_bucket("my-bucket").unwrap();
store.put_object("my-bucket", "key.txt", b"hello").unwrap();
let obj = store.get_object("my-bucket", "key.txt").unwrap();
```

## License

`AGPL-3.0 OR LicenseRef-Commercial` — dual-licensed. Pick either.

| Option | Terms | Use it when |
|--------|-------|-------------|
| **AGPL-3.0** | [LICENSE-AGPL](LICENSE-AGPL) — free, no reporting obligation | Your project is itself AGPL-compatible open source, or you are only using it internally |
| **Commercial License** | [LICENSE-COMMERCIAL.md](LICENSE-COMMERCIAL.md) — paid, removes the copyleft | Closed-source product, proprietary SaaS, edge / firmware distribution, plugin redistribution, or a platform NDA that forbids source disclosure |

AGPL is a strong copyleft: a product, firmware image, or service that links
`alice-objectstore` and is distributed or served to users must be released under the AGPL
as well. That is intentional for the open ecosystem, and the Commercial
License exists for the cases where it is not something you are able to do.

Commercial licence enquiries: <contact@extoria.co.jp>
