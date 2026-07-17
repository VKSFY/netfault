use std::env;
use std::net::SocketAddr;
use std::process::ExitCode;

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

mod proxy;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn parse_args() -> Result<(SocketAddr, SocketAddr)> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        anyhow::bail!(
            "usage: {} <listen_addr> <target_addr>\n  e.g. {} 127.0.0.1:8080 127.0.0.1:9000",
            args.first().map(String::as_str).unwrap_or("netfault"),
            args.first().map(String::as_str).unwrap_or("netfault"),
        );
    }
    let listen: SocketAddr = args[1]
        .parse()
        .with_context(|| format!("invalid listen address: {}", args[1]))?;
    let target: SocketAddr = args[2]
        .parse()
        .with_context(|| format!("invalid target address: {}", args[2]))?;
    Ok((listen, target))
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();

    let (listen, target) = match parse_args() {
        Ok(pair) => pair,
        Err(err) => {
            eprintln!("{err:#}");
            return ExitCode::from(2);
        }
    };

    if let Err(err) = proxy::run(listen, target).await {
        tracing::error!(error = %err, "proxy exited with error");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
