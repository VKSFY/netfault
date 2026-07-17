//! Global fault-injection counters, updated live as chunks flow through the
//! proxy. Rendered as a summary on shutdown so a run can be checked against
//! the configured probabilities.

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-direction counters. All fields are `AtomicU64` so per-chunk updates
/// from the two forwarding tasks of a connection (running on potentially
/// different worker threads) don't need locking.
#[derive(Debug, Default)]
pub struct DirectionStats {
    /// Bytes actually forwarded to the peer after all faults were applied.
    /// Dropped bytes are *not* counted here — they never went out.
    pub bytes_forwarded: AtomicU64,
    pub latency_events: AtomicU64,
    pub chunks_dropped: AtomicU64,
    pub chunks_corrupted: AtomicU64,
    pub close_fault_fired: AtomicU64,
}

impl DirectionStats {
    pub fn snapshot(&self) -> DirectionSnapshot {
        DirectionSnapshot {
            bytes_forwarded: self.bytes_forwarded.load(Ordering::Relaxed),
            latency_events: self.latency_events.load(Ordering::Relaxed),
            chunks_dropped: self.chunks_dropped.load(Ordering::Relaxed),
            chunks_corrupted: self.chunks_corrupted.load(Ordering::Relaxed),
            close_fault_fired: self.close_fault_fired.load(Ordering::Relaxed),
        }
    }
}

/// Top-level counters shared across all connections handled by one proxy run.
#[derive(Debug, Default)]
pub struct Stats {
    pub connections_handled: AtomicU64,
    pub client_to_server: DirectionStats,
    pub server_to_client: DirectionStats,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            connections_handled: self.connections_handled.load(Ordering::Relaxed),
            client_to_server: self.client_to_server.snapshot(),
            server_to_client: self.server_to_client.snapshot(),
        }
    }
}

/// A point-in-time copy of `Stats` with plain integers, cheap to pass around
/// and print without touching atomics again.
#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub connections_handled: u64,
    pub client_to_server: DirectionSnapshot,
    pub server_to_client: DirectionSnapshot,
}

#[derive(Debug, Clone, Copy)]
pub struct DirectionSnapshot {
    pub bytes_forwarded: u64,
    pub latency_events: u64,
    pub chunks_dropped: u64,
    pub chunks_corrupted: u64,
    pub close_fault_fired: u64,
}
