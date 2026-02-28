use std::fmt;
use std::ops::Range;
use std::path::PathBuf;

use crate::error::ProxyError;

const MAGIC: [u8; 4] = *b"XS3M";
const VERSION: u8 = 1;
const ID_ALGO_UUID: u8 = 2;
const HEADER_SIZE: usize = 26;
pub const ID_LEN: usize = 16;

/// Binary manifest describing how an object is split into UUID-identified chunks.
#[derive(Clone)]
pub struct Manifest {
    pub chunk_size: u64,
    pub total_size: u64,
    pub chunk_ids: Vec<[u8; ID_LEN]>,
}

impl Manifest {
    pub fn num_chunks(&self) -> usize {
        self.chunk_ids.len()
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
        let num = self.chunk_ids.len() as u32;
        let mut buf = Vec::with_capacity(HEADER_SIZE + ID_LEN * self.chunk_ids.len());
        buf.extend_from_slice(&MAGIC);
        buf.push(VERSION);
        buf.push(ID_ALGO_UUID);
        buf.extend_from_slice(&self.chunk_size.to_le_bytes());
        buf.extend_from_slice(&self.total_size.to_le_bytes());
        buf.extend_from_slice(&num.to_le_bytes());
        for id in &self.chunk_ids {
            buf.extend_from_slice(id);
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
        if data[5] != ID_ALGO_UUID {
            return Err(ProxyError::Internal(format!(
                "manifest: unsupported id algo {}",
                data[5]
            )));
        }
        let chunk_size = u64::from_le_bytes(data[6..14].try_into().unwrap());
        let total_size = u64::from_le_bytes(data[14..22].try_into().unwrap());
        let num_chunks = u32::from_le_bytes(data[22..26].try_into().unwrap()) as usize;

        let expected_len = HEADER_SIZE + ID_LEN * num_chunks;
        if data.len() < expected_len {
            return Err(ProxyError::Internal(format!(
                "manifest: expected {} bytes, got {}",
                expected_len,
                data.len()
            )));
        }

        let mut chunk_ids = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let offset = HEADER_SIZE + i * ID_LEN;
            let mut id = [0u8; ID_LEN];
            id.copy_from_slice(&data[offset..offset + ID_LEN]);
            chunk_ids.push(id);
        }

        Ok(Self {
            chunk_size,
            total_size,
            chunk_ids,
        })
    }
}

impl fmt::Debug for Manifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Manifest")
            .field("chunk_size", &self.chunk_size)
            .field("total_size", &self.total_size)
            .field("num_chunks", &self.chunk_ids.len())
            .finish()
    }
}

/// Derive the filesystem path for a UUID-identified chunk relative to data_dir.
/// Layout: `{prefix}{h[0]}/{h[1]}/{h[2]}/{h[3]}/{full_hex}`
#[allow(dead_code)]
pub fn id_to_chunk_path(id: &[u8; ID_LEN], prefix: &str) -> PathBuf {
    let hex = hex_encode(id);
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

/// Derive the S3 object key for a UUID-identified chunk.
/// Same layout as `id_to_chunk_path` but returns a String (no PathBuf).
pub fn id_to_s3_key(id: &[u8; ID_LEN], prefix: &str) -> String {
    let hex = hex_encode(id);
    format!(
        "{}{}/{}/{}/{}/{}",
        prefix,
        &hex[0..1],
        &hex[1..2],
        &hex[2..3],
        &hex[3..4],
        hex
    )
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Public hex encoding for chunk IDs (used in logging).
pub fn hex_encode_id(id: &[u8; ID_LEN]) -> String {
    hex_encode(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> Manifest {
        Manifest {
            chunk_size: 8_388_608,
            total_size: 20_000_000,
            chunk_ids: vec![[0xab; 16], [0xcd; 16], [0xef; 16]],
        }
    }

    #[test]
    fn roundtrip_serialize_deserialize() {
        let m = sample_manifest();
        let bytes = m.serialize();
        let m2 = Manifest::deserialize(&bytes).unwrap();
        assert_eq!(m2.chunk_size, m.chunk_size);
        assert_eq!(m2.total_size, m.total_size);
        assert_eq!(m2.chunk_ids.len(), m.chunk_ids.len());
        assert_eq!(m2.chunk_ids[0], m.chunk_ids[0]);
        assert_eq!(m2.chunk_ids[2], m.chunk_ids[2]);
    }

    #[test]
    fn manifest_size() {
        let m = sample_manifest();
        let bytes = m.serialize();
        assert_eq!(bytes.len(), 26 + 16 * 3);
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
    fn id_to_chunk_path_format() {
        let id = [0xab; 16];
        let p = id_to_chunk_path(&id, "data/");
        let hex: String = id.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex.len(), 32);
        let expected = format!("data/{}/{}/{}/{}/{}", &hex[0..1], &hex[1..2], &hex[2..3], &hex[3..4], hex);
        assert_eq!(p.to_str().unwrap(), expected);
    }
}
