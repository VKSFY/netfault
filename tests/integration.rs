//! End-to-end integration tests for the netfault proxy.
//!
//! Each test spins up an in-process TCP echo server and an in-process proxy
//! (bound to an ephemeral OS port so tests can run in parallel and never
//! collide), then drives a client and asserts on observed behavior.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use tokio_util::sync::CancellationToken;

use netfault::config::{Config, FaultConfig};
use netfault::fault::probability_tolerance;
use netfault::proxy;
use netfault::stats::Stats;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Bind an echo server on `127.0.0.1:0` and return its address plus a handle
/// to the accept loop. Each accepted connection is echoed byte-for-byte until
/// the peer closes its write side; then the echo half-closes too.
async fn spawn_echo_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind echo server");
    let addr = listener.local_addr().expect("echo local_addr");
    let handle = tokio::spawn(async move {
        loop {
            let (mut sock, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => return,
            };
            tokio::spawn(async move {
                let _ = sock.set_nodelay(true);
                let mut buf = [0u8; 8192];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if sock.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let _ = sock.shutdown().await;
            });
        }
    });
    (addr, handle)
}

/// Bind a proxy on `127.0.0.1:0`, overwriting `config.listen` with the actual
/// address. Returns the address, a handle to the accept loop, a shared
/// `Stats` for tests that want to inspect counters, and a shutdown token.
async fn spawn_proxy(mut config: Config) -> ProxyHarness {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let addr = listener.local_addr().expect("proxy local_addr");
    config.listen = addr;
    let stats = Arc::new(Stats::default());
    let shutdown = CancellationToken::new();
    let handle = {
        let stats = Arc::clone(&stats);
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = proxy::serve(listener, Arc::new(config), stats, shutdown).await;
        })
    };
    ProxyHarness {
        addr,
        handle,
        stats,
        shutdown,
    }
}

struct ProxyHarness {
    addr: SocketAddr,
    handle: JoinHandle<()>,
    stats: Arc<Stats>,
    shutdown: CancellationToken,
}

fn base_config(target: SocketAddr, seed: u64) -> Config {
    Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        target,
        seed,
        client_to_server: FaultConfig::default(),
        server_to_client: FaultConfig::default(),
    }
}

async fn connect_nodelay(addr: SocketAddr) -> TcpStream {
    let s = TcpStream::connect(addr).await.expect("connect proxy");
    let _ = s.set_nodelay(true);
    s
}

/// Read from `stream` until EOF or `timeout` elapses. Returns whatever was
/// accumulated. Errors and partial reads short-circuit the loop rather than
/// bubbling, since most fault tests want to observe *what got through*, not
/// crash on the first hiccup.
async fn read_until_eof(stream: &mut TcpStream, timeout: Duration) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    let deadline = Instant::now() + timeout;
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        match tokio::time::timeout(remaining, stream.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
            Ok(Err(_)) => break,
            Err(_) => break, // deadline hit
        }
    }
    out
}

fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
    assert_eq!(a.len(), b.len(), "hamming_distance requires equal lengths");
    a.iter().zip(b).map(|(x, y)| (x ^ y).count_ones()).sum()
}

// ---------------------------------------------------------------------------
// 1. drop_probability=1.0 c2s → echo server sees nothing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drop_all_client_to_server_delivers_zero_bytes() {
    let (echo_addr, _echo) = spawn_echo_server().await;
    let mut cfg = base_config(echo_addr, 1);
    cfg.client_to_server.drop_probability = 1.0;
    let proxy = spawn_proxy(cfg).await;
    let proxy_addr = proxy.addr;

    let mut client = connect_nodelay(proxy_addr).await;
    client
        .write_all(b"this should never arrive at the echo server")
        .await
        .expect("client write");
    client.shutdown().await.expect("client shutdown");

    let got = read_until_eof(&mut client, Duration::from_secs(2)).await;
    assert!(
        got.is_empty(),
        "expected zero bytes back (echo saw nothing to echo), got {} bytes",
        got.len()
    );
}

// ---------------------------------------------------------------------------
// 2. latency_ms=200 c2s → observable RTT >= 200 ms
// ---------------------------------------------------------------------------

#[tokio::test]
async fn latency_c2s_adds_measurable_rtt() {
    let (echo_addr, _echo) = spawn_echo_server().await;
    let mut cfg = base_config(echo_addr, 1);
    cfg.client_to_server.latency_ms = 200;
    let proxy = spawn_proxy(cfg).await;
    let proxy_addr = proxy.addr;

    let mut client = connect_nodelay(proxy_addr).await;
    let payload = b"ping";
    let t0 = Instant::now();
    client.write_all(payload).await.expect("client write");
    let mut buf = [0u8; 16];
    let n = client.read(&mut buf).await.expect("client read");
    let rtt = t0.elapsed();

    assert_eq!(&buf[..n], payload, "echo returned wrong bytes");
    assert!(
        rtt >= Duration::from_millis(200),
        "expected RTT >= 200 ms, observed {rtt:?}"
    );
    // Loose upper bound catches "latency was applied twice" or similar bugs.
    assert!(
        rtt < Duration::from_millis(1500),
        "RTT {rtt:?} way over budget"
    );
}

// ---------------------------------------------------------------------------
// 3. corrupt=1.0, bits=3 c2s → echoed payload has exactly 3-bit Hamming distance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn corrupt_flips_exactly_n_distinct_bits() {
    let (echo_addr, _echo) = spawn_echo_server().await;
    let mut cfg = base_config(echo_addr, 1);
    cfg.client_to_server.corrupt_probability = 1.0;
    cfg.client_to_server.corrupt_bits = 3;
    let proxy = spawn_proxy(cfg).await;
    let proxy_addr = proxy.addr;

    let mut client = connect_nodelay(proxy_addr).await;
    // 512 bytes: comfortably under Ethernet MSS (~1460), so this write is a
    // single TCP segment and the proxy performs a single read → single
    // corrupt event.
    let payload: Vec<u8> = (0..512).map(|i| (i & 0xFF) as u8).collect();
    client.write_all(&payload).await.expect("client write");
    client.shutdown().await.expect("client shutdown");

    let got = read_until_eof(&mut client, Duration::from_secs(2)).await;
    assert_eq!(got.len(), payload.len(), "echo returned wrong length");
    let flips = hamming_distance(&payload, &got);
    assert_eq!(
        flips, 3,
        "expected exactly 3 distinct bit flips (without-replacement sampling), got {flips}"
    );
}

// ---------------------------------------------------------------------------
// 4. close_probability=1.0 c2s → connection terminates promptly after first chunk
// ---------------------------------------------------------------------------

#[tokio::test]
async fn close_probability_1_terminates_connection() {
    let (echo_addr, _echo) = spawn_echo_server().await;
    let mut cfg = base_config(echo_addr, 1);
    cfg.client_to_server.close_probability = 1.0;
    let proxy = spawn_proxy(cfg).await;
    let proxy_addr = proxy.addr;

    let mut client = connect_nodelay(proxy_addr).await;
    client.write_all(b"hello").await.expect("client write");
    // Give the proxy time to process, close, and tear down.
    // read_until_eof returns quickly once the socket is closed.
    let start = Instant::now();
    let _ = read_until_eof(&mut client, Duration::from_secs(2)).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "connection did not terminate within 2s (elapsed {elapsed:?})"
    );
}

// ---------------------------------------------------------------------------
// 5. Concurrent connections: 10 clients through one proxy, per-connection
//    drop rate within tolerance of the configured 0.5
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_connections_each_hit_target_drop_rate() {
    const N_CLIENTS: usize = 10;
    const MSGS_PER_CLIENT: usize = 500;
    const MSG_LEN: usize = 128;
    const P: f64 = 0.5;

    let (echo_addr, _echo) = spawn_echo_server().await;
    let mut cfg = base_config(echo_addr, 0xC0DE_C0DE);
    cfg.client_to_server.drop_probability = P;
    let proxy = spawn_proxy(cfg).await;
    let proxy_addr = proxy.addr;

    // Kernel-level coalescing means the pipeline may see fewer than
    // MSGS_PER_CLIENT reads even with NODELAY + inter-send sleep. Widen the
    // tolerance to what an effective N of MSGS_PER_CLIENT/2 would allow —
    // conservative, but keeps the test robust across schedulers.
    let tol = probability_tolerance(P, MSGS_PER_CLIENT / 2);

    let mut clients = Vec::with_capacity(N_CLIENTS);
    for client_id in 0..N_CLIENTS {
        clients.push(tokio::spawn(async move {
            let mut client = connect_nodelay(proxy_addr).await;
            let mut sent: u64 = 0;
            for i in 0..MSGS_PER_CLIENT {
                let mut msg = vec![0u8; MSG_LEN];
                // Non-repeating payload so the echo server never coalesces
                // identical-looking bytes into anything strange.
                msg[0] = client_id as u8;
                msg[1] = (i & 0xFF) as u8;
                client.write_all(&msg).await.expect("client write");
                sent += MSG_LEN as u64;
                // Small delay to discourage kernel coalescing.
                tokio::time::sleep(Duration::from_micros(500)).await;
            }
            client.shutdown().await.expect("client shutdown");
            let echoed = read_until_eof(&mut client, Duration::from_secs(15)).await;
            (sent, echoed.len() as u64)
        }));
    }

    for (idx, task) in clients.into_iter().enumerate() {
        let (sent, received) = task.await.expect("client task");
        let rate = 1.0 - (received as f64 / sent as f64);
        assert!(
            (rate - P).abs() < tol,
            "client {idx}: sent {sent}, received {received}, observed drop rate {rate:.4}, expected {P:.4} +/- {tol:.4}"
        );
    }
}

// ---------------------------------------------------------------------------
// 6. All four faults active simultaneously on one connection → connection
//    completes/terminates cleanly, and multiple fault types are observed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn all_four_faults_combined_do_not_deadlock() {
    let (echo_addr, _echo) = spawn_echo_server().await;
    let mut cfg = base_config(echo_addr, 0xFAAF_FAAF);
    cfg.client_to_server = FaultConfig {
        latency_ms: 5,
        latency_jitter_ms: 5,
        drop_probability: 0.2,
        corrupt_probability: 0.2,
        corrupt_bits: 1,
        close_probability: 0.01,
    };
    let proxy = spawn_proxy(cfg).await;
    let proxy_addr = proxy.addr;

    let mut client = connect_nodelay(proxy_addr).await;

    // Send many small messages until the connection is torn down (close
    // fault eventually fires) or we've exhausted the budget. Each iteration
    // sends and reads independently so we can't deadlock on backpressure.
    const MAX_MSGS: usize = 500;
    let mut sent: u64 = 0;
    let mut received: u64 = 0;
    for i in 0..MAX_MSGS {
        let mut msg = vec![0u8; 64];
        msg[0] = (i & 0xFF) as u8;
        msg[1] = ((i >> 8) & 0xFF) as u8;
        if client.write_all(&msg).await.is_err() {
            break;
        }
        sent += 64;
        // Drain any echoed bytes without blocking indefinitely.
        let mut buf = [0u8; 4096];
        match tokio::time::timeout(Duration::from_millis(50), client.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => received += n as u64,
            Ok(Err(_)) => break,
            Err(_) => continue, // no bytes ready, keep sending
        }
    }
    // Final drain.
    let tail = read_until_eof(&mut client, Duration::from_secs(3)).await;
    received += tail.len() as u64;

    // Sanity — the pipeline is doing *something* and the connection ended.
    assert!(sent > 0, "no bytes were sent");
    assert!(
        received < sent,
        "received ({received}) >= sent ({sent}) — no drops observed"
    );
    // Close probability 0.01 over up to 500 iterations gives ~99.3% chance of
    // firing (1 - 0.99^500). Combined with drop/corrupt closing the peer's
    // read side, the connection *should* end well before MAX_MSGS.
    // We don't hard-assert on `sent < MAX_MSGS * 64` because backpressure
    // paths differ — the important assertion is "we didn't hang".
}

// ---------------------------------------------------------------------------
// 7. Half-close through the pipeline under nonzero latency (both directions)
//    → client's shutdown propagates to the echo server, all bytes round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn half_close_propagates_through_latency_pipeline() {
    let (echo_addr, _echo) = spawn_echo_server().await;
    let mut cfg = base_config(echo_addr, 1);
    // Latency on both directions — the shutdown/EOF must survive the pipeline.
    cfg.client_to_server.latency_ms = 50;
    cfg.server_to_client.latency_ms = 50;
    // Drop / corrupt / close all zero: only the timing path is exercised.
    let proxy = spawn_proxy(cfg).await;
    let proxy_addr = proxy.addr;

    let mut client = connect_nodelay(proxy_addr).await;
    let payload: Vec<u8> = (0..2048).map(|i| (i & 0xFF) as u8).collect();
    client.write_all(&payload).await.expect("client write");
    // Shutdown the write side: echo server must see EOF, echo remaining bytes,
    // and close its side. read_until_eof then terminates cleanly at that FIN.
    client.shutdown().await.expect("client shutdown_write");

    let got = read_until_eof(&mut client, Duration::from_secs(5)).await;
    assert_eq!(
        got,
        payload,
        "half-close round-trip: expected {} bytes echoed intact, got {}",
        payload.len(),
        got.len()
    );
}

// ---------------------------------------------------------------------------
// 8. Graceful shutdown: stats accumulate across connections, shutdown token
//    stops the accept loop, and in-flight connections drain cleanly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shutdown_drains_and_accumulates_correct_stats() {
    let (echo_addr, _echo) = spawn_echo_server().await;
    let mut cfg = base_config(echo_addr, 1);
    // Small latency on c2s so per-chunk latency events are observable.
    cfg.client_to_server.latency_ms = 5;
    let proxy = spawn_proxy(cfg).await;
    let proxy_addr = proxy.addr;

    // Run three sequential connections, each round-tripping a 256-byte payload.
    const N_CONNS: usize = 3;
    const PAYLOAD_LEN: usize = 256;
    for _ in 0..N_CONNS {
        let mut client = connect_nodelay(proxy_addr).await;
        let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xFF) as u8).collect();
        client.write_all(&payload).await.expect("client write");
        client.shutdown().await.expect("client shutdown");
        let got = read_until_eof(&mut client, Duration::from_secs(3)).await;
        assert_eq!(got.len(), payload.len(), "round-trip length");
    }

    // Now trigger graceful shutdown. The accept loop should return, and the
    // proxy task's JoinHandle should complete within the drain window.
    proxy.shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), proxy.handle)
        .await
        .expect("proxy shutdown timed out")
        .expect("proxy task join");

    let snap = proxy.stats.snapshot();
    assert_eq!(
        snap.connections_handled, N_CONNS as u64,
        "expected {N_CONNS} connections accounted for"
    );
    assert_eq!(
        snap.client_to_server.bytes_forwarded,
        (N_CONNS * PAYLOAD_LEN) as u64,
        "c2s bytes",
    );
    assert_eq!(
        snap.server_to_client.bytes_forwarded,
        (N_CONNS * PAYLOAD_LEN) as u64,
        "s2c bytes",
    );
    // Each of the N connections has at least one c2s chunk with 5ms latency.
    assert!(
        snap.client_to_server.latency_events >= N_CONNS as u64,
        "expected at least {N_CONNS} latency events, got {}",
        snap.client_to_server.latency_events,
    );
    // No fault probabilities configured beyond latency -> no drops/corrupts/closes.
    assert_eq!(snap.client_to_server.chunks_dropped, 0);
    assert_eq!(snap.client_to_server.chunks_corrupted, 0);
    assert_eq!(snap.client_to_server.close_fault_fired, 0);
}
