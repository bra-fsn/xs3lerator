use std::process::Command;

fn main() {
    let sha = std::env::var("GITHUB_SHA")
        .ok()
        .map(|s| s[..7.min(s.len())].to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "--short", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .filter(|s| !s.is_empty())
        });

    match sha {
        Some(sha) => println!("cargo:rustc-env=GIT_SHA=-{sha}"),
        None => println!("cargo:rustc-env=GIT_SHA="),
    }

    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
}
