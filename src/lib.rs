//! ALICE-ObjectStore: S3-compatible object storage engine.

#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(
    clippy::module_name_repetitions,
    clippy::doc_markdown,
    clippy::wildcard_imports,
    clippy::too_many_lines,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::similar_names,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::return_self_not_must_use
)]

pub mod bucket;
pub mod errors;
pub mod etag;
pub mod lifecycle;
pub mod list_result;
pub mod metadata;
pub mod multipart;
pub mod object;
pub mod prelude;
pub mod presigned;
pub mod store;

#[cfg(test)]
mod integration_tests;

// Backward-compat re-exports.
pub use crate::bucket::*;
pub use crate::errors::*;
pub use crate::lifecycle::*;
pub use crate::list_result::*;
pub use crate::metadata::*;
pub use crate::multipart::*;
pub use crate::object::*;
pub use crate::presigned::*;
pub use crate::store::*;
