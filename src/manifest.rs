use std::fmt;
use std::ops::Range;
use std::path::PathBuf;

use crate::error::ProxyError;

const MAGIC: [u8; 4] = *b"XS3M";
const VERSION: u8 = 1;
const HASH_ALGO_SHA256: u8 = 1;
const HEADER_SIZE: usize = 26;
const HASH_LEN: usize = 32;

/// Binary manifest describing how an object is split into content-addressed chunks.
#[derive(Clone)]
pub struct Manifest {
    pub chunk_size: u64,
    pub total_size: u64,
    pub hashes: Vec<[u8; HASH_LEN]>,
}

impl Manifest {
    pub fn num_chunks(&self) -> usize {
        self.hashes.len()
    }

    /// Compute the range of chunk indexes that overlap `[byte_start, byte_end]` (inclusive).
    pub fn chunks_for_range(&self, byte_start: u64, byte_end: u64) -> Range<usize> {
        let first = (byte_start / self.chunk_size) as usize;
        let last = std::cmp::min(
            (byte_end / self.chunk_size) as usize,
            self.num_chunks().saturating_sub(1),
        );
        first..last + 1
    }

    /// Serialize to compact binary format.
    pub fn serialize(&self) -> Vec<u8> {
        let num = self.hashes.len() as u32;
        let mut buf = Vec::with_capacity(HEADER_SIZE + HASH_LEN * self.hashes.len());
        buf.extend_from_slice(&MAGIC);
        buf.push(VERSION);
        buf.push(HASH_ALGO_SHA256);
        buf.extend_from_slice(&self.chunk_size.to_le_bytes());
        buf.extend_from_slice(&self.total_size.to_le_bytes());
        buf.extend_from_slice(&num.to_le_bytes());
        for h in &self.hashes {
            buf.extend_from_slice(h);
        }
        buf
    }

    /// Deserialize from binary format.
    pub fn deserialize(data: &[u8]) -> Result<Self, ProxyError> {
        if data.len() < HEADER_SIZE {
            return Err(ProxyError::Internal("manifest too short".into()));
        }
        if data[0..4] != MAGIC {
            return Err(ProxyError::Internal("manifest: bad magic".into()));
        }
        if data[4] != VERSION {
            return Err(ProxyError::Internal(format!(
                "manifest: unsupported version {}",
                data[4]
            )));
        }
        if data[5] != HASH_ALGO_SHA256 {
            return Err(ProxyError::Internal(format!(
                "manifest: unsupported hash algo {}",
                data[5]
            )));
        }
        let chunk_size = u64::from_le_bytes(data[6..14].try_into().unwrap());
        let total_size = u64::from_le_bytes(data[14..22].try_into().unwrap());
        let num_chunks = u32::from_le_bytes(data[22..26].try_into().unwrap()) as usize;

        let expected_len = HEADER_SIZE + HASH_LEN * num_chunks;
        if data.len() < expected_len {
            return Err(ProxyError::Internal(format!(
                "manifest: expected {} bytes, got {}",
                expected_len,
                data.len()
            )));
        }

        let mut hashes = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let offset = HEADER_SIZE + i * HASH_LEN;
            let mut h = [0u8; HASH_LEN];
            h.copy_from_slice(&data[offset..offset + HASH_LEN]);
            hashes.push(h);
        }

        Ok(Self {
            chunk_size,
            total_size,
            hashes,
        })
    }
}

impl fmt::Debug for Manifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Manifest")
            .field("chunk_size", &self.chunk_size)
            .field("total_size", &self.total_size)
            .field("num_chunks", &self.hashes.len())
            .finish()
    }
}

/// Derive the filesystem path for a content-addressed chunk relative to data_dir.
/// Layout: `{prefix}{h[0]}/{h[1]}/{h[2]}/{h[3]}/{full_hex}`
pub fn hash_to_chunk_path(hash: &[u8; HASH_LEN], prefix: &str) -> PathBuf {
    let hex = hex_encode(hash);
    PathBuf::from(format!(
        "{}{}/{}/{}/{}/{}",
        prefix,
        &hex[0..1],
        &hex[1..2],
        &hex[2..3],
        &hex[3..4],
        hex
    ))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> Manifest {
        Manifest {
            chunk_size: 8_388_608,
            total_size: 20_000_000,
            hashes: vec![[0xab; 32], [0xcd; 32], [0xef; 32]],
        }
    }

    #[test]
    fn roundtrip_serialize_deserialize() {
        let m = sample_manifest();
        let bytes = m.serialize();
        let m2 = Manifest::deserialize(&bytes).unwrap();
        assert_eq!(m2.chunk_size, m.chunk_size);
        assert_eq!(m2.total_size, m.total_size);
        assert_eq!(m2.hashes.len(), m.hashes.len());
        assert_eq!(m2.hashes[0], m.hashes[0]);
        assert_eq!(m2.hashes[2], m.hashes[2]);
    }

    #[test]
    fn manifest_size() {
        let m = sample_manifest();
        let bytes = m.serialize();
        assert_eq!(bytes.len(), 26 + 32 * 3);
    }

    #[test]
    fn chunks_for_range_single() {
        let m = sample_manifest();
        let r = m.chunks_for_range(0, 100);
        assert_eq!(r, 0..1);
    }

    #[test]
    fn chunks_for_range_spanning() {
        let m = sample_manifest();
        // bytes 8M..17M spans chunks 1 and 2
        let r = m.chunks_for_range(8_388_608, 17_000_000);
        assert_eq!(r, 1..3);
    }

    #[test]
    fn deserialize_rejects_short() {
        assert!(Manifest::deserialize(&[0; 10]).is_err());
    }

    #[test]
    fn deserialize_rejects_bad_magic() {
        let mut data = sample_manifest().serialize();
        data[0] = 0;
        assert!(Manifest::deserialize(&data).is_err());
    }

    #[test]
    fn hash_to_chunk_path_format() {
        let hash = [0xab; 32];
        let p = hash_to_chunk_path(&hash, "data/");
        assert_eq!(
            p.to_str().unwrap(),
            "data/a/b/a/b/abababababababababababababababababababababababababababababababab"
        );
    }
}
