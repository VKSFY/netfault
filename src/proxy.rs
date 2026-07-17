use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tracing::{debug, error, info, info_span, warn, Instrument, Span};

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
}

/// Bind to `listen`, accept forever, and forward each connection to `target`.
pub async fn run(listen: SocketAddr, target: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind listener on {listen}"))?;
    info!(%listen, %target, "netfault proxy listening");

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
        tokio::spawn(
            async move {
                if let Err(err) = handle_connection(client, client_addr, target).await {
                    warn!(error = %format!("{err:#}"), "connection ended with error");
                }
            }
            .instrument(span),
        );
    }
}

async fn handle_connection(
    client: TcpStream,
    client_addr: SocketAddr,
    target: SocketAddr,
) -> Result<()> {
    info!(%client_addr, %target, "connection opened");

    // Disable Nagle on the client side so small chunks flush promptly. This makes
    // observed fault-injection behavior (latency, drops) match the config more
    // closely under low-throughput workloads.
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

    let c2s_span =
        info_span!(parent: Span::current(), "dir", side = Direction::ClientToServer.as_str());
    let s2c_span =
        info_span!(parent: Span::current(), "dir", side = Direction::ServerToClient.as_str());

    let c2s =
        tokio::spawn(forward(client_r, server_w, Direction::ClientToServer).instrument(c2s_span));
    let s2c =
        tokio::spawn(forward(server_r, client_w, Direction::ServerToClient).instrument(s2c_span));

    // Wait for both directions to complete. If one side errors, log it but keep
    // waiting on the other so we still report accurate byte counts.
    let c2s_bytes = match c2s.await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => {
            warn!(dir = "c2s", error = %format!("{err:#}"), "forward task errored");
            0
        }
        Err(join_err) => {
            error!(dir = "c2s", error = %join_err, "forward task panicked");
            0
        }
    };
    let s2c_bytes = match s2c.await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => {
            warn!(dir = "s2c", error = %format!("{err:#}"), "forward task errored");
            0
        }
        Err(join_err) => {
            error!(dir = "s2c", error = %join_err, "forward task panicked");
            0
        }
    };

    info!(c2s_bytes, s2c_bytes, "connection closed");
    Ok(())
}

/// Copy bytes from `reader` to `writer` a chunk at a time. Returns total bytes forwarded.
///
/// Reads are chunked (not `tokio::io::copy_bidirectional`) so that later milestones
/// can insert fault-injection logic between the read and the write on each chunk.
async fn forward(
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    dir: Direction,
) -> Result<u64> {
    let mut buf = vec![0u8; BUFFER_SIZE];
    let mut total: u64 = 0;
    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(err) => {
                return Err(
                    anyhow::Error::from(err).context(format!("read failed on {}", dir.as_str()))
                );
            }
        };
        if let Err(err) = writer.write_all(&buf[..n]).await {
            return Err(
                anyhow::Error::from(err).context(format!("write failed on {}", dir.as_str()))
            );
        }
        total += n as u64;
    }
    // Half-close the write side so the peer sees EOF.
    if let Err(err) = writer.shutdown().await {
        debug!(dir = dir.as_str(), error = %err, "shutdown() failed");
    }
    Ok(total)
}
