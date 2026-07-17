use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use netfault::config::{CliOverrides, Config, FileConfig};
use netfault::proxy;

/// A TCP proxy that injects network faults (latency, drops, corruption, closes)
/// for testing how applications behave under adverse network conditions.
#[derive(Parser, Debug)]
#[command(name = "netfault", version, about, long_about = None)]
struct Cli {
    /// Path to a TOML config file. If omitted, --listen and --target are required.
    #[arg(short = 'c', long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Local address to bind (overrides `listen` in the config file).
    #[arg(short = 'l', long, value_name = "ADDR")]
    listen: Option<SocketAddr>,

    /// Target address to forward to (overrides `target` in the config file).
    #[arg(short = 't', long, value_name = "ADDR")]
    target: Option<SocketAddr>,

    /// PRNG seed for reproducible fault injection (overrides `seed` in the config file).
    /// If neither source specifies one, a random seed is drawn and logged at startup.
    #[arg(short = 's', long, value_name = "N")]
    seed: Option<u64>,
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn load_config(cli: Cli) -> Result<Config> {
    let file = match cli.config.as_deref() {
        Some(path) => FileConfig::from_toml_file(path)?,
        None => FileConfig::default(),
    };
    let overrides = CliOverrides {
        listen: cli.listen,
        target: cli.target,
        seed: cli.seed,
    };
    Config::resolve(file, overrides)
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();

    let cli = Cli::parse();
    let config = match load_config(cli) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("config error: {err:#}");
            return ExitCode::from(2);
        }
    };

    tracing::info!(
        listen = %config.listen,
        target = %config.target,
        seed = config.seed,
        "netfault starting"
    );
    tracing::debug!(?config.client_to_server, ?config.server_to_client, "fault config");

    if let Err(err) = proxy::run(Arc::new(config)).await {
        tracing::error!(error = %err, "proxy exited with error");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
