//! Mount-s3 prefetch strategy benchmark.
//!
//! Reads chunk files from /data/data/bench/{chunk_size}m/pool{N}/ with
//! different parallelism levels. Each pool must only be read once per
//! mount-s3 cache lifetime to measure cold performance.
//!
//! Usage:
//!   bench-read --data-dir /data/data --chunk-mb 8 --pool 0 --parallelism 8
//!   bench-read --data-dir /data/data --chunk-mb 64 --pool 1 --parallelism 16
//!   bench-read --strategy sequential --chunk-mb 8 --pool 2

use std::cmp::min;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

mod semaphore {
    use std::sync::{Condvar, Mutex};

    pub struct Semaphore {
        count: Mutex<usize>,
        cv: Condvar,
    }

    impl Semaphore {
        pub fn new(count: usize) -> Self {
            Self { count: Mutex::new(count), cv: Condvar::new() }
        }
        pub fn acquire(&self) {
            let mut c = self.count.lock().unwrap();
            while *c == 0 {
                c = self.cv.wait(c).unwrap();
            }
            *c -= 1;
        }
        pub fn release(&self) {
            let mut c = self.count.lock().unwrap();
            *c += 1;
            self.cv.notify_one();
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut data_dir = PathBuf::from("/data/data");
    let mut chunk_mb: u64 = 8;
    let mut pool: u32 = 0;
    let mut parallelism: usize = 8;
    let mut piece_kb: usize = 256;
    let mut strategy = "parallel-stream".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => { i += 1; data_dir = PathBuf::from(&args[i]); }
            "--chunk-mb" => { i += 1; chunk_mb = args[i].parse().unwrap(); }
            "--pool" => { i += 1; pool = args[i].parse().unwrap(); }
            "--parallelism" => { i += 1; parallelism = args[i].parse().unwrap(); }
            "--piece-kb" => { i += 1; piece_kb = args[i].parse().unwrap(); }
            "--strategy" => { i += 1; strategy = args[i].clone(); }
            "--help" | "-h" => {
                eprintln!("bench-read [--data-dir PATH] [--chunk-mb 8|16|32|64] [--pool N]");
                eprintln!("           [--parallelism N] [--piece-kb N] [--strategy NAME]");
                eprintln!();
                eprintln!("Strategies: sequential, parallel-buffer, parallel-stream");
                std::process::exit(0);
            }
            other => { eprintln!("unknown arg: {other}"); std::process::exit(1); }
        }
        i += 1;
    }

    let dir = data_dir.join(format!("bench/{}m/pool{}", chunk_mb, pool));
    let piece_size = piece_kb * 1024;

    let mut files: Vec<PathBuf> = Vec::new();
    match std::fs::read_dir(&dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                files.push(entry.path());
            }
        }
        Err(e) => {
            eprintln!("ERROR: cannot read {}: {e}", dir.display());
            eprintln!("Hint: upload with bench/upload_chunks.py first");
            std::process::exit(1);
        }
    }
    files.sort();

    if files.is_empty() {
        eprintln!("ERROR: no files in {}", dir.display());
        std::process::exit(1);
    }

    let num_chunks = files.len();
    let expected_size = chunk_mb as usize * 1024 * 1024;

    eprintln!(
        "bench: strategy={strategy} chunk_mb={chunk_mb} pool={pool} \
         parallelism={parallelism} piece_kb={piece_kb} chunks={num_chunks}"
    );

    match strategy.as_str() {
        "sequential" => run_sequential(&files, expected_size, piece_size),
        "parallel-buffer" => run_parallel_buffer(&files, expected_size, piece_size, parallelism),
        "parallel-stream" => run_parallel_stream(&files, expected_size, piece_size, parallelism),
        other => {
            eprintln!("unknown strategy: {other}");
            std::process::exit(1);
        }
    }
}

struct ChunkStats {
    slot: usize,
    open_us: u64,
    first_read_us: u64,
    total_read_us: u64,
    bytes: u64,
}

fn read_chunk(path: &std::path::Path, expected_size: usize, piece_size: usize, slot: usize) -> ChunkStats {
    let t0 = Instant::now();
    let f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => panic!("open {}: {e}", path.display()),
    };
    let open_us = t0.elapsed().as_micros() as u64;

    let len = expected_size;
    let mut buf = vec![0u8; len];
    let mut pos = 0usize;
    let read_start = Instant::now();
    let mut first_read_us = 0u64;

    while pos < len {
        let piece = min(len - pos, piece_size);
        match f.read_exact_at(&mut buf[pos..pos + piece], pos as u64) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("read {} at {pos}: {e}", path.display()),
        }
        if pos == 0 {
            first_read_us = read_start.elapsed().as_micros() as u64;
        }
        pos += piece;
    }
    let total_read_us = read_start.elapsed().as_micros() as u64;

    ChunkStats { slot, open_us, first_read_us, total_read_us, bytes: pos as u64 }
}

fn print_stats(label: &str, wall_ms: u64, stats: &[ChunkStats]) {
    let total_bytes: u64 = stats.iter().map(|s| s.bytes).sum();
    let throughput = if wall_ms > 0 { total_bytes / 1024 / wall_ms } else { 0 };

    let mut open: Vec<u64> = stats.iter().map(|s| s.open_us / 1000).collect();
    let mut first: Vec<u64> = stats.iter().map(|s| s.first_read_us / 1000).collect();
    let mut total: Vec<u64> = stats.iter().map(|s| s.total_read_us / 1000).collect();
    let mut chunk_tp: Vec<u64> = stats.iter().map(|s| {
        if s.total_read_us > 0 { s.bytes * 1000 / s.total_read_us } else { 0 }
    }).collect();

    open.sort(); first.sort(); total.sort(); chunk_tp.sort();

    println!("--- {label} ---");
    println!("wall_ms={wall_ms} total_mb={} throughput_mbps={throughput} chunks={}",
        total_bytes / (1024 * 1024), stats.len());
    println!("open_ms:       min={:4} avg={:4} p50={:4} p95={:4} max={:4}",
        pct(&open, 0), avg(&open), pct(&open, 50), pct(&open, 95), pct(&open, 100));
    println!("first_read_ms: min={:4} avg={:4} p50={:4} p95={:4} max={:4}",
        pct(&first, 0), avg(&first), pct(&first, 50), pct(&first, 95), pct(&first, 100));
    println!("total_read_ms: min={:4} avg={:4} p50={:4} p95={:4} max={:4}",
        pct(&total, 0), avg(&total), pct(&total, 50), pct(&total, 95), pct(&total, 100));
    println!("chunk_mbps:    min={:4} avg={:4} p50={:4} p95={:4} max={:4}",
        pct(&chunk_tp, 0) / 1024, avg(&chunk_tp) / 1024, pct(&chunk_tp, 50) / 1024,
        pct(&chunk_tp, 95) / 1024, pct(&chunk_tp, 100) / 1024);

    for s in stats {
        let mbps = if s.total_read_us > 0 { s.bytes / 1024 / (s.total_read_us / 1000).max(1) } else { 0 };
        println!("  slot={:3} open={:4}ms first={:4}ms total={:4}ms mbps={:4}",
            s.slot, s.open_us / 1000, s.first_read_us / 1000, s.total_read_us / 1000, mbps);
    }
}

fn run_sequential(files: &[PathBuf], expected_size: usize, piece_size: usize) {
    let t0 = Instant::now();
    let mut stats = Vec::with_capacity(files.len());

    for (i, path) in files.iter().enumerate() {
        stats.push(read_chunk(path, expected_size, piece_size, i));
    }

    print_stats("sequential", t0.elapsed().as_millis() as u64, &stats);
}

fn run_parallel_buffer(files: &[PathBuf], expected_size: usize, piece_size: usize, par: usize) {
    let t0 = Instant::now();
    let work: Arc<std::sync::Mutex<Vec<(usize, PathBuf)>>> =
        Arc::new(std::sync::Mutex::new(files.iter().enumerate().map(|(i, p)| (i, p.clone())).collect()));
    let (tx, rx) = std::sync::mpsc::channel::<ChunkStats>();

    let mut handles = Vec::new();
    for _ in 0..par {
        let wq = work.clone();
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            loop {
                let item = wq.lock().unwrap().pop();
                match item {
                    Some((slot, path)) => { let _ = tx.send(read_chunk(&path, expected_size, piece_size, slot)); }
                    None => break,
                }
            }
        }));
    }
    drop(tx);

    let mut stats: Vec<ChunkStats> = rx.into_iter().collect();
    for h in handles { h.join().unwrap(); }

    stats.sort_by_key(|s| s.slot);
    print_stats(&format!("parallel-buffer({par})"), t0.elapsed().as_millis() as u64, &stats);
}

fn run_parallel_stream(files: &[PathBuf], expected_size: usize, piece_size: usize, par: usize) {
    let t0 = Instant::now();
    let num = files.len();

    let mut senders: Vec<Option<std::sync::mpsc::SyncSender<ChunkStats>>> = Vec::with_capacity(num);
    let mut receivers = Vec::with_capacity(num);
    for _ in 0..num {
        let (tx, rx) = std::sync::mpsc::sync_channel::<ChunkStats>(1);
        senders.push(Some(tx));
        receivers.push(rx);
    }

    let file_paths: Vec<PathBuf> = files.to_vec();
    let sem = Arc::new(semaphore::Semaphore::new(par));

    let producer = std::thread::spawn(move || {
        let mut handles = Vec::new();
        for (slot, path) in file_paths.into_iter().enumerate() {
            sem.acquire();
            let tx = senders[slot].take().unwrap();
            let s = sem.clone();
            handles.push(std::thread::spawn(move || {
                let stats = read_chunk(&path, expected_size, piece_size, slot);
                let _ = tx.send(stats);
                s.release();
            }));
        }
        for h in handles { h.join().unwrap(); }
    });

    let mut stats = Vec::with_capacity(num);
    let mut total_stall_us: u64 = 0;
    let mut max_stall_us: u64 = 0;
    let mut max_stall_slot: usize = 0;
    let mut stall_count: usize = 0;

    for (slot, rx) in receivers.into_iter().enumerate() {
        let wait_start = Instant::now();
        let s = rx.recv().unwrap();
        let stall_us = wait_start.elapsed().as_micros() as u64;

        if stall_us > 5000 {
            stall_count += 1;
            eprintln!("  stall: slot={slot} stall_ms={} open_ms={} first_ms={} total_ms={}",
                stall_us / 1000, s.open_us / 1000, s.first_read_us / 1000, s.total_read_us / 1000);
        }
        total_stall_us += stall_us;
        if stall_us > max_stall_us { max_stall_us = stall_us; max_stall_slot = slot; }
        stats.push(s);
    }

    producer.join().unwrap();
    let wall_ms = t0.elapsed().as_millis() as u64;

    print_stats(&format!("parallel-stream({par})"), wall_ms, &stats);
    println!("stream: stall_total_ms={} stall_max_ms={} stall_max_slot={max_stall_slot} stalled={stall_count}/{num}",
        total_stall_us / 1000, max_stall_us / 1000);
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = (p * (sorted.len() - 1)) / 100;
    sorted[idx]
}

fn avg(data: &[u64]) -> u64 {
    if data.is_empty() { return 0; }
    data.iter().sum::<u64>() / data.len() as u64
}
