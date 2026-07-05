//! etag.

// ETag helper
// ---------------------------------------------------------------------------

/// Compute a simple `ETag` from data bytes (FNV-1a 64-bit, hex-encoded).
pub(crate) fn compute_etag(data: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("\"{hash:016x}\"")
}

// ---------------------------------------------------------------------------
