use std::process::{exit, Command};

const CONTRACTS_PATH: &str = "../../contracts";

fn main() {
    if std::env::var("SKIP_CONTRACT_BUILD").unwrap_or_default() == "true" {
        return;
    }

    if !CONTRACTS_PATH.is_empty() {
        std::env::set_current_dir(CONTRACTS_PATH).unwrap_or_else(|err| {
            eprintln!("Error changing to subdirectory: {}", err);
            exit(1);
        });
    }

    Command::new("npm")
        .arg("ci")
        .status()
        .unwrap_or_else(|err| {
            eprintln!("Error executing npm ci: {}", err);
            exit(1);
        });

    Command::new("npm")
        .arg("run")
        .arg("build")
        .status()
        .unwrap_or_else(|err| {
            eprintln!("Error executing npm run build: {}", err);
            exit(1);
        });

    std::env::set_current_dir("..").unwrap_or_else(|err| {
        eprintln!("Error changing back to the original directory: {}", err);
        exit(1);
    });

    println!("cargo:rerun-if-changed={}", CONTRACTS_PATH);
}
