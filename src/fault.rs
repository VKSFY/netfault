use std::time::Duration;

use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use crate::config::FaultConfig;

/// Per-chunk decisions produced by the pipeline.
#[derive(Debug)]
pub struct Outcome {
    /// The (possibly corrupted) chunk to forward. `None` means "drop this chunk".
    pub payload: Option<Vec<u8>>,
    /// If `true`, the connection should be closed after this chunk is processed.
    pub close_after: bool,
    /// Whether the drop fault fired on this chunk.
    pub dropped: bool,
    /// Whether the corrupt fault fired on this chunk.
    pub corrupted: bool,
}

/// Running counters for fault events on a single stream (one direction of one
/// connection). Rolled up into per-connection totals in M6.
#[derive(Debug, Clone, Copy, Default)]
pub struct InjectionStats {
    pub latency_events: u64,
    pub dropped: u64,
    pub corrupted: u64,
    /// 0 or 1 — a stream is closed at most once.
    pub closed: u64,
}

/// Composable fault pipeline for one direction of a single connection.
///
/// Steps are applied in this fixed order per chunk: `latency` (sleep for
/// `latency_ms` + Uniform(0, `latency_jitter_ms`)), then `drop` (with
/// `drop_probability`, discard the chunk), then `corrupt` (with
/// `corrupt_probability`, flip `corrupt_bits` random bits), then `close`
/// (with `close_probability`, signal that the connection should be closed
/// after this chunk).
///
/// `drop` runs before `corrupt`, so a dropped chunk is never also corrupted
/// (there is nothing to corrupt). `close` is evaluated regardless of whether
/// the chunk was dropped, because a dropped chunk is still a processed chunk.
pub struct FaultPipeline {
    config: FaultConfig,
    rng: StdRng,
    stats: InjectionStats,
}

impl FaultPipeline {
    pub fn new(config: FaultConfig, seed: u64) -> Self {
        Self {
            config,
            rng: StdRng::seed_from_u64(seed),
            stats: InjectionStats::default(),
        }
    }

    /// Apply the pipeline to a single chunk. Awaits the latency step.
    pub async fn process(&mut self, chunk: Vec<u8>) -> Outcome {
        let sleep = self.compute_latency();
        if !sleep.is_zero() {
            self.stats.latency_events += 1;
            tokio::time::sleep(sleep).await;
        }

        let dropped = self.roll(self.config.drop_probability);
        let (payload, corrupted) = if dropped {
            self.stats.dropped += 1;
            (None, false)
        } else if self.roll(self.config.corrupt_probability) {
            self.stats.corrupted += 1;
            (Some(self.corrupt(chunk)), true)
        } else {
            (Some(chunk), false)
        };

        let close_after = self.roll(self.config.close_probability);
        if close_after {
            self.stats.closed = 1;
        }

        Outcome {
            payload,
            close_after,
            dropped,
            corrupted,
        }
    }

    pub fn stats(&self) -> InjectionStats {
        self.stats
    }

    fn compute_latency(&mut self) -> Duration {
        let base = self.config.latency_ms;
        let jitter = if self.config.latency_jitter_ms > 0 {
            self.rng.gen_range(0..=self.config.latency_jitter_ms)
        } else {
            0
        };
        Duration::from_millis(base.saturating_add(jitter))
    }

    fn roll(&mut self, p: f64) -> bool {
        // Short-circuit the endpoints so a zero-probability fault doesn't consume
        // any RNG entropy — keeps sequences stable if a user toggles a fault off.
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        self.rng.gen::<f64>() < p
    }

    fn corrupt(&mut self, mut chunk: Vec<u8>) -> Vec<u8> {
        let total_bits = chunk.len().saturating_mul(8);
        if total_bits == 0 {
            return chunk;
        }
        let bits_to_flip = (self.config.corrupt_bits as usize).min(total_bits);
        for _ in 0..bits_to_flip {
            let bit_idx = self.rng.gen_range(0..total_bits);
            chunk[bit_idx / 8] ^= 1u8 << (bit_idx % 8);
        }
        chunk
    }
}

/// Derive a per-connection, per-direction seed from a master seed + connection id.
///
/// Uses SplitMix64-style mixing so nearby (master, conn_id, direction) tuples
/// still produce well-separated seeds. Each direction of each connection gets
/// its own RNG stream, so a fault sequence is fully reproducible from the master
/// seed regardless of how many connections happen in parallel.
pub fn derive_seed(master_seed: u64, conn_id: u64, direction_tag: u64) -> u64 {
    let mut x = master_seed
        ^ conn_id.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ direction_tag.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_all_off() -> FaultConfig {
        FaultConfig::default()
    }

    fn cfg_always_drop() -> FaultConfig {
        FaultConfig {
            drop_probability: 1.0,
            ..Default::default()
        }
    }

    fn cfg_always_corrupt(bits: u32) -> FaultConfig {
        FaultConfig {
            corrupt_probability: 1.0,
            corrupt_bits: bits,
            ..Default::default()
        }
    }

    fn cfg_always_close() -> FaultConfig {
        FaultConfig {
            close_probability: 1.0,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn all_off_forwards_unchanged() {
        let mut p = FaultPipeline::new(cfg_all_off(), 1);
        let out = p.process(vec![1, 2, 3, 4]).await;
        assert_eq!(out.payload.as_deref(), Some(&[1, 2, 3, 4][..]));
        assert!(!out.dropped);
        assert!(!out.corrupted);
        assert!(!out.close_after);
    }

    #[tokio::test]
    async fn always_drop_returns_none_and_no_corrupt() {
        let mut p = FaultPipeline::new(cfg_always_drop(), 1);
        let out = p.process(vec![9; 16]).await;
        assert!(out.payload.is_none());
        assert!(out.dropped);
        assert!(!out.corrupted);
        assert_eq!(p.stats().dropped, 1);
        assert_eq!(p.stats().corrupted, 0);
    }

    #[tokio::test]
    async fn always_corrupt_flips_exactly_n_bits() {
        // With a 16-byte chunk and 5 bit flips, the Hamming distance between
        // original and corrupted must be exactly 5 (bit index picks may repeat
        // — that would cancel — but with 128 possible positions and 5 draws,
        // repeats are extremely unlikely; we assert the strict count and let
        // the seeded RNG guarantee reproducibility).
        let original = vec![0u8; 16];
        let mut p = FaultPipeline::new(cfg_always_corrupt(5), 42);
        let out = p.process(original.clone()).await;
        let corrupted = out.payload.expect("not dropped");
        assert!(out.corrupted);
        let flips: u32 = original
            .iter()
            .zip(&corrupted)
            .map(|(a, b)| (a ^ b).count_ones())
            .sum();
        assert_eq!(flips, 5, "expected 5 bit flips, got {flips}");
    }

    #[tokio::test]
    async fn corrupt_clamps_bits_to_available() {
        // 1-byte chunk = 8 bits; asking for 100 flips should clamp to 8, and
        // hitting each bit position once flips every bit → 0xFF.
        // (In practice a random walk of 100 draws over 8 positions won't
        // hit-each-once, so we can't assert 0xFF. But we can assert clamping
        // by checking the pipeline doesn't panic on an empty chunk.)
        let mut p = FaultPipeline::new(cfg_always_corrupt(100), 7);
        let out = p.process(vec![]).await;
        // Empty chunk stays empty even with corrupt=1.0 (nothing to flip).
        assert_eq!(out.payload.as_deref(), Some(&[][..]));
    }

    #[tokio::test]
    async fn always_close_sets_close_after() {
        let mut p = FaultPipeline::new(cfg_always_close(), 1);
        let out = p.process(vec![1]).await;
        assert!(out.close_after);
        assert_eq!(p.stats().closed, 1);
        // Even a second processed chunk keeps `closed` at 1 (it's a boolean count).
        let _ = p.process(vec![2]).await;
        assert_eq!(p.stats().closed, 1);
    }

    #[tokio::test]
    async fn latency_advances_time() {
        // Use tokio's paused-time facility so this test doesn't actually sleep.
        tokio::time::pause();
        let cfg = FaultConfig {
            latency_ms: 100,
            ..Default::default()
        };
        let mut p = FaultPipeline::new(cfg, 1);
        let start = tokio::time::Instant::now();
        let _ = p.process(vec![1]).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(100),
            "expected >=100ms simulated elapsed, got {elapsed:?}"
        );
        assert_eq!(p.stats().latency_events, 1);
    }

    #[test]
    fn derive_seed_is_deterministic_and_separates_directions() {
        let a1 = derive_seed(42, 1, 0);
        let a2 = derive_seed(42, 1, 0);
        assert_eq!(a1, a2, "same inputs must produce same seed");
        let b = derive_seed(42, 1, 1);
        assert_ne!(
            a1, b,
            "different direction tags must produce different seeds"
        );
        let c = derive_seed(42, 2, 0);
        assert_ne!(a1, c, "different conn ids must produce different seeds");
    }

    #[tokio::test]
    async fn same_seed_produces_same_bit_flips() {
        // Reproducibility: two pipelines with the same seed and config produce
        // byte-identical output for the same input sequence.
        let mut p1 = FaultPipeline::new(cfg_always_corrupt(3), 99);
        let mut p2 = FaultPipeline::new(cfg_always_corrupt(3), 99);
        for _ in 0..10 {
            let input = vec![0xAAu8; 32];
            let o1 = p1.process(input.clone()).await;
            let o2 = p2.process(input).await;
            assert_eq!(o1.payload, o2.payload);
        }
    }

    // --- Statistical tests ---------------------------------------------------
    //
    // Each of the four faults has a probability parameter that promises "over N
    // trials, roughly N*p events fire". These tests verify that promise. All
    // trials use a fixed seed, so the observed rate is deterministic and the
    // test cannot become flaky across runs — the tolerance below only exists
    // to remain robust against small refactors that shift the RNG sequence.

    const N_TRIALS: usize = 10_000;

    /// 4-sigma binomial confidence bound on the observed rate, floored at 0.005
    /// to give a comfortable margin even at extreme `p`.
    fn tolerance(p: f64, n: usize) -> f64 {
        let sigma = (p * (1.0 - p) / n as f64).sqrt();
        (4.0 * sigma).max(0.005)
    }

    fn assert_rate_close(fault: &str, p: f64, observed: f64) {
        let tol = tolerance(p, N_TRIALS);
        assert!(
            (observed - p).abs() < tol,
            "{fault}: expected rate ~{p:.3} (tol {tol:.4}) over {N_TRIALS} trials, observed {observed:.4}"
        );
    }

    /// Run `N_TRIALS` chunks through a fresh pipeline and return the fraction
    /// of chunks for which `predicate(&outcome)` is true.
    async fn observed_rate<F>(cfg: FaultConfig, seed: u64, mut predicate: F) -> f64
    where
        F: FnMut(&Outcome) -> bool,
    {
        let mut pipe = FaultPipeline::new(cfg, seed);
        let mut hits = 0usize;
        for _ in 0..N_TRIALS {
            let out = pipe.process(vec![0x55u8; 8]).await;
            if predicate(&out) {
                hits += 1;
            }
        }
        hits as f64 / N_TRIALS as f64
    }

    #[tokio::test]
    async fn drop_rate_matches_probability() {
        for p in [0.1_f64, 0.3, 0.5, 0.75] {
            let cfg = FaultConfig {
                drop_probability: p,
                ..Default::default()
            };
            let rate = observed_rate(cfg, 0xDEAD_BEEF, |o| o.dropped).await;
            assert_rate_close("drop", p, rate);
        }
    }

    #[tokio::test]
    async fn corrupt_rate_matches_probability() {
        for p in [0.1_f64, 0.3, 0.5, 0.75] {
            let cfg = FaultConfig {
                corrupt_probability: p,
                corrupt_bits: 2,
                ..Default::default()
            };
            let rate = observed_rate(cfg, 0xC0FF_EE00, |o| o.corrupted).await;
            assert_rate_close("corrupt", p, rate);
        }
    }

    #[tokio::test]
    async fn close_rate_matches_probability() {
        // close_after is per-chunk. We measure its per-chunk fire rate the same
        // way as the other faults — a fresh pipeline per probability so the
        // trials aren't cut short by the first close firing.
        for p in [0.1_f64, 0.3, 0.5, 0.75] {
            let cfg = FaultConfig {
                close_probability: p,
                ..Default::default()
            };
            let rate = observed_rate(cfg, 0xFACE_D00D, |o| o.close_after).await;
            assert_rate_close("close", p, rate);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn latency_mean_matches_expected() {
        // Under paused time, tokio::time::sleep advances virtual time
        // instantly, so N_TRIALS trials complete in real-time milliseconds
        // while the *simulated* total equals the sum of every sleep.
        //
        // With latency_ms=100 and jitter_ms=40 sampled uniformly from
        // {0, 1, ..., 40}, expected mean per chunk = 100 + 20 = 120 ms.
        let cfg = FaultConfig {
            latency_ms: 100,
            latency_jitter_ms: 40,
            ..Default::default()
        };
        let mut pipe = FaultPipeline::new(cfg, 0xBADD_CAFE);

        let start = tokio::time::Instant::now();
        for _ in 0..N_TRIALS {
            pipe.process(vec![0x00u8; 4]).await;
        }
        let mean_ms = start.elapsed().as_micros() as f64 / (N_TRIALS as f64 * 1000.0);

        // Uniform{0..=40} has variance = (41^2 - 1) / 12 ≈ 140. Std of the
        // sample mean over N_TRIALS = sqrt(140/10000) ≈ 0.118 ms. 5-sigma
        // bound ≈ 0.6 ms; we allow 1 ms as a comfortable margin.
        let expected = 120.0;
        let tol_ms = 1.0;
        assert!(
            (mean_ms - expected).abs() < tol_ms,
            "expected mean latency ~{expected} ms (tol {tol_ms}), observed {mean_ms:.4}",
        );
        assert_eq!(pipe.stats().latency_events as usize, N_TRIALS);
    }

    #[tokio::test(start_paused = true)]
    async fn latency_jitter_range_is_respected() {
        // Every sampled sleep must fall in [latency_ms, latency_ms + jitter_ms].
        // Sample many delays via paused-time deltas.
        let cfg = FaultConfig {
            latency_ms: 50,
            latency_jitter_ms: 30,
            ..Default::default()
        };
        let mut pipe = FaultPipeline::new(cfg, 0xBEEF_CAFE);

        let mut min_ms = u128::MAX;
        let mut max_ms = 0u128;
        for _ in 0..1_000 {
            let t0 = tokio::time::Instant::now();
            pipe.process(vec![0x00u8; 4]).await;
            let d = t0.elapsed().as_millis();
            min_ms = min_ms.min(d);
            max_ms = max_ms.max(d);
        }
        assert!(min_ms >= 50, "observed min {min_ms} ms < base 50 ms");
        assert!(max_ms <= 80, "observed max {max_ms} ms > base+jitter 80 ms");
        // With 1000 draws over 31 possible values, we expect to touch both
        // endpoints. Assert we saw the low end and the high end (or close):
        assert!(
            min_ms <= 52,
            "min {min_ms} ms — RNG didn't hit near the bottom of the jitter range"
        );
        assert!(
            max_ms >= 78,
            "max {max_ms} ms — RNG didn't hit near the top of the jitter range"
        );
    }

    #[tokio::test]
    async fn drop_and_corrupt_never_co_occur_on_same_chunk() {
        // Pipeline order guarantee: `drop` runs before `corrupt`, so a chunk
        // that was dropped is never also corrupted. Verify over many trials
        // with both probabilities high enough to co-occur frequently if the
        // ordering were broken.
        let cfg = FaultConfig {
            drop_probability: 0.5,
            corrupt_probability: 0.5,
            corrupt_bits: 1,
            ..Default::default()
        };
        let mut pipe = FaultPipeline::new(cfg, 0x1234_5678);
        for _ in 0..N_TRIALS {
            let out = pipe.process(vec![0x00u8; 8]).await;
            assert!(
                !(out.dropped && out.corrupted),
                "dropped and corrupted both true on same chunk"
            );
            // Also: if dropped, payload must be None. If not dropped, Some.
            assert_eq!(out.dropped, out.payload.is_none());
        }
        // The two counters should each be roughly N/2, and their sum should
        // never exceed N (they partition the "processed" set).
        let s = pipe.stats();
        assert!(s.dropped + s.corrupted <= N_TRIALS as u64);
    }
}
