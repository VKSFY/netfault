use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Fault settings for one direction of traffic.
///
/// All fields default to a benign value: `0` for delays / bit-flips, `0.0` for
/// probabilities. A `FaultConfig::default()` is equivalent to "do nothing".
#[derive(Debug, Clone, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct FaultConfig {
    /// Fixed delay applied to every forwarded chunk, in milliseconds.
    pub latency_ms: u64,
    /// Additional random delay uniformly sampled from `[0, latency_jitter_ms]`.
    pub latency_jitter_ms: u64,
    /// Probability (0.0..=1.0) that any given chunk is silently dropped.
    pub drop_probability: f64,
    /// Probability (0.0..=1.0) that any given chunk has bits flipped before forwarding.
    pub corrupt_probability: f64,
    /// Number of bits to flip when corruption fires. Zero means "no bits flipped
    /// even if corrupt_probability is nonzero" (useful for smoke tests).
    pub corrupt_bits: u32,
    /// Probability (0.0..=1.0) that the connection is dropped after forwarding a chunk.
    pub close_probability: f64,
}

impl FaultConfig {
    fn validate(&self, name: &str) -> Result<()> {
        fn check_prob(section: &str, field: &str, v: f64) -> Result<()> {
            if v.is_nan() || !(0.0..=1.0).contains(&v) {
                anyhow::bail!("{section}.{field} must be in [0.0, 1.0], got {v}");
            }
            Ok(())
        }
        check_prob(name, "drop_probability", self.drop_probability)?;
        check_prob(name, "corrupt_probability", self.corrupt_probability)?;
        check_prob(name, "close_probability", self.close_probability)?;
        Ok(())
    }
}

/// Raw config as read from a TOML file. All fields are optional so the file can
/// be partial (CLI flags fill in the rest) or omitted entirely.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct FileConfig {
    pub listen: Option<SocketAddr>,
    pub target: Option<SocketAddr>,
    pub seed: Option<u64>,
    pub client_to_server: FaultConfig,
    pub server_to_client: FaultConfig,
}

impl FileConfig {
    /// Load and parse a TOML config file. Returns descriptive errors on I/O or
    /// parse failure.
    pub fn from_toml_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let cfg: FileConfig = toml::from_str(&text)
            .with_context(|| format!("failed to parse TOML config {}", path.display()))?;
        Ok(cfg)
    }
}

/// CLI-supplied overrides for top-level runtime settings.
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    pub listen: Option<SocketAddr>,
    pub target: Option<SocketAddr>,
    pub seed: Option<u64>,
}

/// Fully resolved runtime configuration. Constructed by merging a `FileConfig`
/// with `CliOverrides`; CLI values win.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub target: SocketAddr,
    pub seed: u64,
    pub client_to_server: FaultConfig,
    pub server_to_client: FaultConfig,
}

impl Config {
    /// Merge a parsed file config with CLI overrides. CLI wins on any conflict.
    /// If neither source specifies a seed, one is drawn from the OS entropy pool
    /// so it can still be logged and reproduced.
    pub fn resolve(file: FileConfig, cli: CliOverrides) -> Result<Self> {
        let listen = cli
            .listen
            .or(file.listen)
            .context("`listen` must be provided via --listen or in the config file")?;
        let target = cli
            .target
            .or(file.target)
            .context("`target` must be provided via --target or in the config file")?;
        let seed = cli.seed.or(file.seed).unwrap_or_else(rand::random);

        file.client_to_server.validate("client_to_server")?;
        file.server_to_client.validate("server_to_client")?;

        Ok(Self {
            listen,
            target,
            seed,
            client_to_server: file.client_to_server,
            server_to_client: file.server_to_client,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_benign() {
        let f = FaultConfig::default();
        assert_eq!(f.latency_ms, 0);
        assert_eq!(f.drop_probability, 0.0);
        assert_eq!(f.corrupt_probability, 0.0);
        assert_eq!(f.close_probability, 0.0);
        assert!(f.validate("x").is_ok());
    }

    #[test]
    fn rejects_out_of_range_probability() {
        let f = FaultConfig {
            drop_probability: 1.5,
            ..Default::default()
        };
        assert!(f.validate("c2s").is_err());
    }

    #[test]
    fn rejects_nan_probability() {
        let f = FaultConfig {
            corrupt_probability: f64::NAN,
            ..Default::default()
        };
        assert!(f.validate("c2s").is_err());
    }

    #[test]
    fn cli_overrides_win() {
        let file = FileConfig {
            listen: Some("127.0.0.1:1".parse().unwrap()),
            target: Some("127.0.0.1:2".parse().unwrap()),
            seed: Some(1),
            ..Default::default()
        };
        let cli = CliOverrides {
            listen: Some("127.0.0.1:9".parse().unwrap()),
            target: None,
            seed: Some(42),
        };
        let resolved = Config::resolve(file, cli).unwrap();
        assert_eq!(resolved.listen.port(), 9);
        assert_eq!(resolved.target.port(), 2);
        assert_eq!(resolved.seed, 42);
    }

    #[test]
    fn resolve_requires_listen_and_target() {
        let err = Config::resolve(FileConfig::default(), CliOverrides::default()).unwrap_err();
        assert!(err.to_string().contains("listen"));
    }

    #[test]
    fn parses_full_toml() {
        let toml_text = r#"
listen = "127.0.0.1:8080"
target = "127.0.0.1:9000"
seed = 12345

[client_to_server]
latency_ms = 100
latency_jitter_ms = 20
drop_probability = 0.05
corrupt_probability = 0.02
corrupt_bits = 3
close_probability = 0.001

[server_to_client]
latency_ms = 5
"#;
        let fc: FileConfig = toml::from_str(toml_text).unwrap();
        assert_eq!(fc.seed, Some(12345));
        assert_eq!(fc.client_to_server.latency_ms, 100);
        assert_eq!(fc.client_to_server.corrupt_bits, 3);
        assert_eq!(fc.server_to_client.latency_ms, 5);
        // Un-set fields default:
        assert_eq!(fc.server_to_client.drop_probability, 0.0);
    }

    #[test]
    fn rejects_unknown_fields() {
        let toml_text = r#"
listen = "127.0.0.1:8080"
target = "127.0.0.1:9000"
mystery = 42
"#;
        assert!(toml::from_str::<FileConfig>(toml_text).is_err());
    }
}
