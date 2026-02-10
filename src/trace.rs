//! Optional JSONL trace writer for diagnosing download/reader timing.
//!
//! Enable via `--debug-trace <path>`.  Each line is a self-contained JSON
//! object with at least a `t` field (seconds since proxy start, f64) and
//! an `event` field.
//!
//! Example output:
//! ```jsonl
//! {"t":0.001,"event":"request","key":"10g","size":10737418240,...}
//! {"t":0.003,"event":"dl_chunk_start","worker":0,"chunk":0,...}
//! {"t":0.078,"event":"dl_s3_connected","worker":0,"chunk":0,"latency_ms":75.1}
//! {"t":2.710,"event":"dl_chunk_done","worker":0,"chunk":0,"bytes":8388608,...}
//! {"t":0.080,"event":"rd_chunk_enter","chunk":0,...}
//! {"t":0.080,"event":"rd_wait","chunk":0,"need":1,"have":0}
//! {"t":2.715,"event":"rd_chunk_done","chunk":0,"bytes":8388608,"ms":2635.0}
//! ```

use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use serde_json::Value;

pub struct TraceWriter {
    file: Mutex<std::io::BufWriter<std::fs::File>>,
    epoch: Instant,
}

impl TraceWriter {
    pub fn new(path: &std::path::Path) -> std::io::Result<Self> {
        let file = std::fs::File::create(path)?;
        Ok(Self {
            file: Mutex::new(std::io::BufWriter::with_capacity(64 * 1024, file)),
            epoch: Instant::now(),
        })
    }

    /// Write a JSON event line.  The `t` field is injected automatically.
    pub fn log(&self, mut obj: Value) {
        let t = self.epoch.elapsed().as_secs_f64();
        if let Some(map) = obj.as_object_mut() {
            // Insert at front so `t` appears first in output (BTreeMap is sorted).
            // serde_json's default Map is BTreeMap, so key "t" sorts early.
            map.insert("t".into(), Value::from(t));
        }
        let mut f = self.file.lock();
        let _ = serde_json::to_writer(&mut *f, &obj);
        let _ = writeln!(&mut *f);
    }
}

/// Zero-cost trace helper.  The closure is only called when tracing is enabled,
/// so `json!()` allocations are skipped entirely in the common (no-trace) path.
#[inline]
pub fn trace_log(trace: &Option<Arc<TraceWriter>>, f: impl FnOnce() -> Value) {
    if let Some(tw) = trace {
        tw.log(f());
    }
}
