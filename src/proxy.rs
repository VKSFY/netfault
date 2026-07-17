use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, info_span, warn, Instrument, Span};

use crate::config::{Config, FaultConfig};
use crate::fault::{derive_seed, FaultPipeline, InjectionStats};

const BUFFER_SIZE: usize = 8 * 1024;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Direction of a forwarding half.
#[derive(Debug, Clone, Copy)]
enum Direction {
    ClientToServer,
    ServerToClient,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Direction::ClientToServer => "c2s",
            Direction::ServerToClient => "s2c",
        }
    }

    /// Stable tag used to seed the per-direction RNG.
    fn seed_tag(self) -> u64 {
        match self {
            Direction::ClientToServer => 0,
            Direction::ServerToClient => 1,
        }
    }
}

/// Bind to `config.listen`, accept forever, and forward each connection to `config.target`.
pub async fn run(config: Arc<Config>) -> Result<()> {
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to bind listener on {}", config.listen))?;
    info!(listen = %config.listen, target = %config.target, "netfault proxy listening");
    serve(listener, config).await
}

/// Serve on an already-bound listener. Split out from `run` so callers that
/// need atomic port acquisition (integration tests using `127.0.0.1:0`,
/// systemd socket activation, etc.) can bind themselves and hand the socket in.
pub async fn serve(listener: TcpListener, config: Arc<Config>) -> Result<()> {
    loop {
        let (client, client_addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                // Accept errors are typically transient (e.g. EMFILE). Log and continue
                // rather than bringing down the whole proxy.
                warn!(error = %err, "accept() failed; continuing");
                continue;
            }
        };

        let id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let span = info_span!("conn", id = id, client = %client_addr);
        let conn_config = Arc::clone(&config);
        tokio::spawn(
            async move {
                if let Err(err) = handle_connection(id, client, client_addr, conn_config).await {
                    warn!(error = %format!("{err:#}"), "connection ended with error");
                }
            }
            .instrument(span),
        );
    }
}

async fn handle_connection(
    conn_id: u64,
    client: TcpStream,
    client_addr: SocketAddr,
    config: Arc<Config>,
) -> Result<()> {
    let target = config.target;
    info!(%client_addr, %target, "connection opened");

    // Disable Nagle on both sides so small fault-injected chunks flush promptly.
    if let Err(err) = client.set_nodelay(true) {
        debug!(error = %err, "set_nodelay(client) failed; continuing");
    }

    let server = TcpStream::connect(target)
        .await
        .with_context(|| format!("failed to connect to target {target}"))?;
    if let Err(err) = server.set_nodelay(true) {
        debug!(error = %err, "set_nodelay(server) failed; continuing");
    }

    let (client_r, client_w) = client.into_split();
    let (server_r, server_w) = server.into_split();

    let cancel = CancellationToken::new();
    let c2s_pipeline = build_pipeline(
        &config.client_to_server,
        config.seed,
        conn_id,
        Direction::ClientToServer,
    );
    let s2c_pipeline = build_pipeline(
        &config.server_to_client,
        config.seed,
        conn_id,
        Direction::ServerToClient,
    );

    let c2s_span =
        info_span!(parent: Span::current(), "dir", side = Direction::ClientToServer.as_str());
    let s2c_span =
        info_span!(parent: Span::current(), "dir", side = Direction::ServerToClient.as_str());

    let c2s_cancel = cancel.clone();
    let s2c_cancel = cancel.clone();

    let c2s = tokio::spawn(
        forward(
            client_r,
            server_w,
            c2s_pipeline,
            c2s_cancel,
            Direction::ClientToServer,
        )
        .instrument(c2s_span),
    );
    let s2c = tokio::spawn(
        forward(
            server_r,
            client_w,
            s2c_pipeline,
            s2c_cancel,
            Direction::ServerToClient,
        )
        .instrument(s2c_span),
    );

    let c2s_out = collect_forward_result("c2s", c2s.await);
    let s2c_out = collect_forward_result("s2c", s2c.await);

    info!(
        c2s_bytes = c2s_out.bytes,
        s2c_bytes = s2c_out.bytes,
        c2s_dropped = c2s_out.stats.dropped,
        s2c_dropped = s2c_out.stats.dropped,
        c2s_corrupted = c2s_out.stats.corrupted,
        s2c_corrupted = s2c_out.stats.corrupted,
        c2s_latency_events = c2s_out.stats.latency_events,
        s2c_latency_events = s2c_out.stats.latency_events,
        closed_by_fault = c2s_out.stats.closed + s2c_out.stats.closed > 0,
        "connection closed"
    );
    Ok(())
}

fn build_pipeline(
    cfg: &FaultConfig,
    master_seed: u64,
    conn_id: u64,
    dir: Direction,
) -> FaultPipeline {
    let seed = derive_seed(master_seed, conn_id, dir.seed_tag());
    FaultPipeline::new(cfg.clone(), seed)
}

struct ForwardOutput {
    bytes: u64,
    stats: InjectionStats,
}

fn collect_forward_result(
    dir: &'static str,
    joined: Result<Result<ForwardOutput>, tokio::task::JoinError>,
) -> ForwardOutput {
    match joined {
        Ok(Ok(out)) => out,
        Ok(Err(err)) => {
            warn!(dir, error = %format!("{err:#}"), "forward task errored");
            ForwardOutput {
                bytes: 0,
                stats: InjectionStats::default(),
            }
        }
        Err(join_err) => {
            error!(dir, error = %join_err, "forward task panicked");
            ForwardOutput {
                bytes: 0,
                stats: InjectionStats::default(),
            }
        }
    }
}

/// Read from `reader`, run each chunk through `pipeline`, and write the result
/// to `writer`. Terminates on EOF, I/O error, `pipeline` requesting close, or
/// `cancel` firing (which typically means the other direction's pipeline asked
/// to close and we should tear down the whole connection).
async fn forward(
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    mut pipeline: FaultPipeline,
    cancel: CancellationToken,
    dir: Direction,
) -> Result<ForwardOutput> {
    let mut buf = vec![0u8; BUFFER_SIZE];
    let mut total: u64 = 0;
    let mut closed_by_fault = false;

    loop {
        let n = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                debug!(dir = dir.as_str(), "cancelled by peer direction");
                break;
            }
            r = reader.read(&mut buf) => match r {
                Ok(0) => break,
                Ok(n) => n,
                Err(err) => {
                    return Err(
                        anyhow::Error::from(err).context(format!("read failed on {}", dir.as_str()))
                    );
                }
            },
        };

        let chunk = buf[..n].to_vec();
        let outcome = pipeline.process(chunk).await;

        if let Some(payload) = outcome.payload {
            if let Err(err) = writer.write_all(&payload).await {
                return Err(
                    anyhow::Error::from(err).context(format!("write failed on {}", dir.as_str()))
                );
            }
            total += payload.len() as u64;
        }

        if outcome.close_after {
            closed_by_fault = true;
            debug!(dir = dir.as_str(), "close fault fired");
            cancel.cancel();
            break;
        }
    }

    // Half-close the write side so the peer sees EOF. Best-effort — if the
    // connection is already dead (e.g. fault-close on the other direction just
    // dropped the socket) the shutdown will error and we log at debug.
    if let Err(err) = writer.shutdown().await {
        debug!(dir = dir.as_str(), error = %err, "shutdown() failed");
    }

    let stats = pipeline.stats();
    if closed_by_fault {
        info!(dir = dir.as_str(), "closed by fault");
    }
    Ok(ForwardOutput {
        bytes: total,
        stats,
    })
}
