use std::io::Write;
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::Value;

pub struct TraceWriter {
    inner: Mutex<Box<dyn Write + Send>>,
}

impl TraceWriter {
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        Self {
            inner: Mutex::new(writer),
        }
    }
}

pub fn trace_log(trace: &Option<Arc<TraceWriter>>, make_entry: impl FnOnce() -> Value) {
    if let Some(tw) = trace {
        let mut entry = make_entry();
        if let Some(obj) = entry.as_object_mut() {
            obj.insert(
                "ts".to_string(),
                Value::Number(
                    serde_json::Number::from_f64(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs_f64(),
                    )
                    .unwrap_or(serde_json::Number::from(0)),
                ),
            );
        }
        if let Ok(mut line) = serde_json::to_vec(&entry) {
            line.push(b'\n');
            let _ = tw.inner.lock().write_all(&line);
        }
    }
}
