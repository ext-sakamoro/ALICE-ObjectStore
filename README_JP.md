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

`AGPL-3.0 OR LicenseRef-Commercial` — デュアルライセンス どちらかを選べる

| 選択肢 | 条文 | こういう時 |
|--------|------|-----------|
| **AGPL-3.0** | [LICENSE-AGPL](LICENSE-AGPL) — 無償、報告義務なし | 自分の project も AGPL 互換の OSS、または社内利用のみ |
| **商用ライセンス** | [LICENSE-COMMERCIAL.md](LICENSE-COMMERCIAL.md) — 有償、コピーレフト義務を解除 | クローズドソース製品 / 商用 SaaS / エッジ・ファームウェア配布 / plugin 再配布 / ソース開示を禁じるプラットフォーム NDA |

AGPL は強いコピーレフト: `alice-objectstore` を link して配布 / 提供する製品・ファームウェア・
サービスは AGPL で公開する義務がある これはオープンなエコシステムのための意図的な
選択で、それが実行できない場合のために商用ライセンスを用意している

商用ライセンスの問い合わせ: <contact@extoria.co.jp>
