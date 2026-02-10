use crate::error::ProxyError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end_inclusive: u64,
}

impl ByteRange {
    pub fn len(&self) -> u64 {
        self.end_inclusive - self.start + 1
    }
}

/// Parse a single-range HTTP Range header value against a known object size.
/// Multi-range requests are rejected (returns RangeNotSatisfiable).
pub fn parse_range_header(
    header: Option<&str>,
    total_size: u64,
) -> Result<Option<ByteRange>, ProxyError> {
    let Some(value) = header else {
        return Ok(None);
    };
    if total_size == 0 {
        return Err(ProxyError::RangeNotSatisfiable);
    }
    let value = value.trim();
    if !value.starts_with("bytes=") {
        return Err(ProxyError::RangeNotSatisfiable);
    }
    let spec = &value["bytes=".len()..];
    if spec.contains(',') {
        return Err(ProxyError::RangeNotSatisfiable);
    }
    let (left, right) = spec.split_once('-').ok_or(ProxyError::RangeNotSatisfiable)?;
    let left = left.trim();
    let right = right.trim();

    let range = if left.is_empty() {
        let suffix_len: u64 = right
            .parse()
            .map_err(|_| ProxyError::RangeNotSatisfiable)?;
        if suffix_len == 0 {
            return Err(ProxyError::RangeNotSatisfiable);
        }
        let len = suffix_len.min(total_size);
        ByteRange {
            start: total_size - len,
            end_inclusive: total_size - 1,
        }
    } else {
        let start: u64 = left
            .parse()
            .map_err(|_| ProxyError::RangeNotSatisfiable)?;
        if start >= total_size {
            return Err(ProxyError::RangeNotSatisfiable);
        }
        let end_inclusive = if right.is_empty() {
            total_size - 1
        } else {
            right
                .parse::<u64>()
                .map_err(|_| ProxyError::RangeNotSatisfiable)?
        };
        if end_inclusive < start {
            return Err(ProxyError::RangeNotSatisfiable);
        }
        ByteRange {
            start,
            end_inclusive: end_inclusive.min(total_size - 1),
        }
    };

    Ok(Some(range))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_header_returns_none() {
        assert_eq!(parse_range_header(None, 1000).unwrap(), None);
    }

    #[test]
    fn simple_range() {
        let r = parse_range_header(Some("bytes=0-499"), 1000)
            .unwrap()
            .unwrap();
        assert_eq!(r.start, 0);
        assert_eq!(r.end_inclusive, 499);
        assert_eq!(r.len(), 500);
    }

    #[test]
    fn open_ended() {
        let r = parse_range_header(Some("bytes=500-"), 1000)
            .unwrap()
            .unwrap();
        assert_eq!(r.start, 500);
        assert_eq!(r.end_inclusive, 999);
        assert_eq!(r.len(), 500);
    }

    #[test]
    fn suffix() {
        let r = parse_range_header(Some("bytes=-200"), 1000)
            .unwrap()
            .unwrap();
        assert_eq!(r.start, 800);
        assert_eq!(r.end_inclusive, 999);
    }

    #[test]
    fn end_clamped() {
        let r = parse_range_header(Some("bytes=900-99999"), 1000)
            .unwrap()
            .unwrap();
        assert_eq!(r.end_inclusive, 999);
    }

    #[test]
    fn start_past_end_is_error() {
        assert!(parse_range_header(Some("bytes=1000-"), 1000).is_err());
    }

    #[test]
    fn multi_range_rejected() {
        assert!(parse_range_header(Some("bytes=0-100,200-300"), 1000).is_err());
    }

    #[test]
    fn zero_suffix_rejected() {
        assert!(parse_range_header(Some("bytes=-0"), 1000).is_err());
    }

    #[test]
    fn zero_size_object_rejected() {
        assert!(parse_range_header(Some("bytes=0-0"), 0).is_err());
    }
}
