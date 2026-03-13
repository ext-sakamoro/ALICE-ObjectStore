[English](README.md) | **日本語**

# ALICE-ObjectStore

**ALICE S3互換オブジェクトストレージエンジン** — バケット、オブジェクト、マルチパートアップロード、バージョニング、ライフサイクルポリシー、署名付きURL、ページネーションを備えた純Rust実装。

[Project A.L.I.C.E.](https://github.com/anthropics/alice) エコシステムの一部。

## 機能

- **バケット管理** — バケットの作成・削除・一覧
- **オブジェクトCRUD** — キーベースのPut、Get、Delete、List操作
- **マルチパートアップロード** — パート管理・完了処理付きのチャンクアップロード
- **バージョニング** — バージョンID追跡によるオブジェクト履歴管理
- **ライフサイクルポリシー** — オブジェクトの自動有効期限・遷移ルール
- **ETag** — FNV-1aハッシュベースのコンテンツ整合性検証
- **署名付きURL** — 時間制限付きアクセスURLの生成・検証
- **メタデータ** — オブジェクトへのユーザー定義キーバリューメタデータ
- **ページネーション** — 大規模バケット向けカーソルベースのオブジェクト一覧

## アーキテクチャ

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

## クイックスタート

```rust
use alice_objectstore::ObjectStore;

let mut store = ObjectStore::new();
store.create_bucket("my-bucket").unwrap();
store.put_object("my-bucket", "key.txt", b"hello").unwrap();
let obj = store.get_object("my-bucket", "key.txt").unwrap();
```

## ライセンス

AGPL-3.0
