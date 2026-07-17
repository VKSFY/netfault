use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use netfault::config::{CliOverrides, Config, FileConfig};
use netfault::proxy;
use netfault::stats::{Stats, StatsSnapshot};

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

/// Print an end-of-run summary derived from the shared `Stats` counters.
/// Written to stdout (not the log) so it's the last thing a user sees and is
/// easy to grep/parse.
fn print_summary(seed: u64, snap: &StatsSnapshot) {
    let c2s = &snap.client_to_server;
    let s2c = &snap.server_to_client;
    println!();
    println!("============================");
    println!(" netfault shutdown summary");
    println!("============================");
    println!("seed                   : {seed}");
    println!("connections handled    : {}", snap.connections_handled);
    println!("bytes forwarded c -> s : {}", c2s.bytes_forwarded);
    println!("bytes forwarded s -> c : {}", s2c.bytes_forwarded);
    println!("faults injected (c2s):");
    println!("  latency events       : {}", c2s.latency_events);
    println!("  chunks dropped       : {}", c2s.chunks_dropped);
    println!("  chunks corrupted     : {}", c2s.chunks_corrupted);
    println!("  close fault fired    : {}", c2s.close_fault_fired);
    println!("faults injected (s2c):");
    println!("  latency events       : {}", s2c.latency_events);
    println!("  chunks dropped       : {}", s2c.chunks_dropped);
    println!("  chunks corrupted     : {}", s2c.chunks_corrupted);
    println!("  close fault fired    : {}", s2c.close_fault_fired);
    println!("============================");
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

    let seed = config.seed;
    let stats = Arc::new(Stats::default());
    let shutdown = CancellationToken::new();

    // Ctrl+C on Windows and SIGINT on Unix both come through here. Signal
    // handling runs in its own task so the proxy's accept loop can `select!`
    // on the shutdown token without also owning the signal machinery.
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => tracing::info!("received Ctrl+C; requesting shutdown"),
            Err(err) => tracing::error!(error = %err, "failed to install Ctrl+C handler"),
        }
        signal_shutdown.cancel();
    });

    let run_result = proxy::run(Arc::new(config), Arc::clone(&stats), shutdown).await;

    print_summary(seed, &stats.snapshot());

    if let Err(err) = run_result {
        tracing::error!(error = %err, "proxy exited with error");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
