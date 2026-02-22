use std::net::IpAddr;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

const DEFAULT_PORT: u16 = 4000;

fn parse_size(s: &str) -> Result<u64> {
    if s.is_empty() {
        bail!("empty size value");
    }
    let (num_str, multiplier) = match s.as_bytes().last() {
        Some(b'K') => (&s[..s.len() - 1], 1024u64),
        Some(b'M') => (&s[..s.len() - 1], 1024 * 1024),
        Some(b'G') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        Some(b'0'..=b'9') => (s, 1),
        _ => bail!("invalid size suffix in '{s}', expected K, M, or G"),
    };
    let n: u64 = num_str
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid number in '{s}'"))?;
    Ok(n * multiplier)
}

#[derive(Debug, Clone, PartialEq)]
pub struct BootstrapPeer {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub listen_addr: IpAddr,
    pub listen_port: u16,
    pub bootstrap_peers: Vec<BootstrapPeer>,
    pub data_dir: PathBuf,
    pub max_storage: u64,
    pub pow_difficulty: u8,
    pub mdns: bool,
    pub max_chunks_per_peer: u32,
    pub write_rate: u32,
    pub write_burst: u32,
    pub max_handlers: usize,
}

fn parse_bool(s: &str) -> Result<bool> {
    match s {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => bail!("expected 'yes' or 'no', got '{s}'"),
    }
}

fn unquote(s: &str) -> Result<String> {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        Ok(s[1..s.len() - 1].to_string())
    } else {
        bail!("expected quoted string, got '{s}'")
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "::".parse().unwrap(),
            listen_port: DEFAULT_PORT,
            bootstrap_peers: Vec::new(),
            data_dir: PathBuf::from("/var/lib/tesd"),
            max_storage: 1_073_741_824,
            pow_difficulty: 16,
            mdns: false,
            max_chunks_per_peer: 256,
            write_rate: 50,
            write_burst: 20,
            max_handlers: 256,
        }
    }
}

impl Config {
    /// Log changes between current and new config.
    pub fn log_reload_diff(&self, new: &Config) {
        if self.listen_addr != new.listen_addr
            || self.listen_port != new.listen_port
        {
            tracing::warn!("listen address changed, requires restart");
        }
        if self.bootstrap_peers != new.bootstrap_peers {
            tracing::warn!("bootstrap peers changed, requires restart");
        }
        if self.data_dir != new.data_dir {
            tracing::warn!("data-dir changed, requires restart");
        }
        if self.max_storage != new.max_storage {
            tracing::warn!("max-storage changed, requires restart");
        }
        if self.pow_difficulty != new.pow_difficulty {
            tracing::warn!("pow-difficulty changed, requires restart");
        }
        if self.mdns != new.mdns {
            tracing::warn!("mdns changed, requires restart");
        }
        if self.max_chunks_per_peer != new.max_chunks_per_peer {
            tracing::info!(
                "max-chunks-per-peer: {} -> {}",
                self.max_chunks_per_peer,
                new.max_chunks_per_peer
            );
        }
        if self.write_rate != new.write_rate {
            tracing::info!(
                "write-rate: {} -> {}",
                self.write_rate,
                new.write_rate
            );
        }
        if self.write_burst != new.write_burst {
            tracing::info!(
                "write-burst: {} -> {}",
                self.write_burst,
                new.write_burst
            );
        }
        if self.max_handlers != new.max_handlers {
            tracing::warn!(
                "max-handlers: {} -> {} (requires restart)",
                self.max_handlers,
                new.max_handlers
            );
        }
    }

    /// Build a NodeConfig with the reloadable tuning fields.
    pub fn to_node_config(&self) -> tesseras_dht::prelude::NodeConfig {
        let mut nc = tesseras_dht::prelude::NodeConfig::default();
        nc.max_chunks_per_peer = self.max_chunks_per_peer;
        nc.write_rate_per_second = self.write_rate;
        nc.write_rate_burst = self.write_burst;
        nc.max_concurrent_handlers = self.max_handlers;
        nc
    }

    /// Parse a config file from disk.
    pub fn parse(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        Self::parse_str(&content)
    }

    /// Parse config from a string (used by tests and parse).
    pub fn parse_str(input: &str) -> Result<Self> {
        let mut cfg = Config::default();
        let mut listen_set = false;

        for (lineno_0, raw_line) in input.lines().enumerate() {
            let lineno = lineno_0 + 1;
            let line = raw_line.trim();

            // Skip blank lines and comments.
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let tokens: Vec<&str> = line.split_whitespace().collect();
            if tokens.is_empty() {
                continue;
            }

            match tokens[0] {
                "listen" => {
                    if listen_set {
                        bail!("line {lineno}: duplicate 'listen' directive");
                    }
                    if tokens.get(1) != Some(&"on") {
                        bail!("line {lineno}: expected 'on' after 'listen'");
                    }
                    let addr_str = tokens.get(2).ok_or_else(|| {
                        anyhow::anyhow!(
                            "line {lineno}: missing address after 'listen on'"
                        )
                    })?;
                    cfg.listen_addr = addr_str.parse().map_err(|e| {
                        anyhow::anyhow!(
                            "line {lineno}: bad address '{addr_str}': {e}"
                        )
                    })?;
                    if tokens.get(3) == Some(&"port") {
                        let port_str = tokens.get(4).ok_or_else(|| {
                            anyhow::anyhow!(
                                "line {lineno}: missing port number"
                            )
                        })?;
                        cfg.listen_port = port_str.parse().map_err(|e| {
                            anyhow::anyhow!(
                                "line {lineno}: bad port '{port_str}': {e}"
                            )
                        })?;
                    }
                    listen_set = true;
                }
                "bootstrap" => {
                    let host_raw = tokens.get(1).ok_or_else(|| {
                        anyhow::anyhow!(
                            "line {lineno}: missing host after 'bootstrap'"
                        )
                    })?;
                    let host = if host_raw.starts_with('"') {
                        unquote(host_raw)
                            .map_err(|e| anyhow::anyhow!("line {lineno}: {e}"))?
                    } else {
                        host_raw.to_string()
                    };
                    let mut port = DEFAULT_PORT;
                    if tokens.get(2) == Some(&"port") {
                        let port_str = tokens.get(3).ok_or_else(|| {
                            anyhow::anyhow!(
                                "line {lineno}: missing port number"
                            )
                        })?;
                        port = port_str.parse().map_err(|e| {
                            anyhow::anyhow!(
                                "line {lineno}: bad port '{port_str}': {e}"
                            )
                        })?;
                    }
                    cfg.bootstrap_peers.push(BootstrapPeer { host, port });
                }
                "data-dir" => {
                    let val = tokens.get(1).ok_or_else(|| {
                        anyhow::anyhow!(
                            "line {lineno}: missing path after 'data-dir'"
                        )
                    })?;
                    cfg.data_dir = PathBuf::from(if val.starts_with('"') {
                        unquote(val)?
                    } else {
                        val.to_string()
                    });
                }
                "max-storage" => {
                    let val = tokens.get(1).ok_or_else(|| {
                        anyhow::anyhow!(
                            "line {lineno}: missing value after 'max-storage'"
                        )
                    })?;
                    cfg.max_storage = parse_size(val)
                        .map_err(|e| anyhow::anyhow!("line {lineno}: {e}"))?;
                }
                "pow-difficulty" => {
                    let val = tokens.get(1).ok_or_else(|| {
                        anyhow::anyhow!(
                            "line {lineno}: missing value after 'pow-difficulty'"
                        )
                    })?;
                    cfg.pow_difficulty = val.parse().map_err(|e| {
                        anyhow::anyhow!(
                            "line {lineno}: bad pow-difficulty '{val}': {e}"
                        )
                    })?;
                }
                "mdns" => {
                    let val = tokens.get(1).ok_or_else(|| {
                        anyhow::anyhow!(
                            "line {lineno}: missing value after 'mdns'"
                        )
                    })?;
                    cfg.mdns = parse_bool(val)
                        .map_err(|e| anyhow::anyhow!("line {lineno}: {e}"))?;
                }
                "max-chunks-per-peer" => {
                    let val = tokens.get(1).ok_or_else(|| {
                        anyhow::anyhow!("line {lineno}: missing value")
                    })?;
                    cfg.max_chunks_per_peer = val
                        .parse()
                        .map_err(|e| anyhow::anyhow!("line {lineno}: {e}"))?;
                }
                "write-rate" => {
                    let val = tokens.get(1).ok_or_else(|| {
                        anyhow::anyhow!("line {lineno}: missing value")
                    })?;
                    cfg.write_rate = val
                        .parse()
                        .map_err(|e| anyhow::anyhow!("line {lineno}: {e}"))?;
                }
                "write-burst" => {
                    let val = tokens.get(1).ok_or_else(|| {
                        anyhow::anyhow!("line {lineno}: missing value")
                    })?;
                    cfg.write_burst = val
                        .parse()
                        .map_err(|e| anyhow::anyhow!("line {lineno}: {e}"))?;
                }
                "max-handlers" => {
                    let val = tokens.get(1).ok_or_else(|| {
                        anyhow::anyhow!("line {lineno}: missing value")
                    })?;
                    cfg.max_handlers = val
                        .parse()
                        .map_err(|e| anyhow::anyhow!("line {lineno}: {e}"))?;
                }
                kw => bail!("line {lineno}: unknown keyword '{kw}'"),
            }
        }

        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_plain_number() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
    }

    #[test]
    fn parse_size_k_suffix() {
        assert_eq!(parse_size("4K").unwrap(), 4 * 1024);
    }

    #[test]
    fn parse_size_m_suffix() {
        assert_eq!(parse_size("10M").unwrap(), 10 * 1024 * 1024);
    }

    #[test]
    fn parse_size_g_suffix() {
        assert_eq!(parse_size("1G").unwrap(), 1_073_741_824);
    }

    #[test]
    fn parse_size_bad_input() {
        assert!(parse_size("abc").is_err());
        assert!(parse_size("").is_err());
        assert!(parse_size("1T").is_err());
    }

    #[test]
    fn parse_bool_yes_no() {
        assert!(parse_bool("yes").unwrap());
        assert!(!parse_bool("no").unwrap());
    }

    #[test]
    fn parse_bool_bad_value() {
        assert!(parse_bool("true").is_err());
        assert!(parse_bool("1").is_err());
    }

    #[test]
    fn unquote_removes_double_quotes() {
        assert_eq!(unquote("\"hello\"").unwrap(), "hello");
    }

    #[test]
    fn unquote_rejects_unquoted() {
        assert!(unquote("hello").is_err());
    }

    #[test]
    fn unquote_rejects_mismatched() {
        assert!(unquote("\"hello").is_err());
    }

    #[test]
    fn parse_minimal_config() {
        let input = r#"
# minimal config
listen on 0.0.0.0 port 4433
bootstrap "tesseras.net"
"#;
        let cfg = Config::parse_str(input).unwrap();
        assert_eq!(cfg.listen_addr, "0.0.0.0".parse::<IpAddr>().unwrap());
        assert_eq!(cfg.listen_port, 4433);
        assert_eq!(cfg.bootstrap_peers.len(), 1);
        assert_eq!(cfg.bootstrap_peers[0].host, "tesseras.net");
        assert_eq!(cfg.bootstrap_peers[0].port, 4000);
    }

    #[test]
    fn parse_full_config() {
        let input = r#"
listen on :: port 4000
bootstrap "tesseras.net"
bootstrap "157.90.160.207" port 4433
data-dir "/tmp/tesd-test"
max-storage 2G
pow-difficulty 20
mdns yes
max-chunks-per-peer 512
write-rate 100
write-burst 40
max-handlers 128
"#;
        let cfg = Config::parse_str(input).unwrap();
        assert_eq!(cfg.listen_addr, "::".parse::<IpAddr>().unwrap());
        assert_eq!(cfg.listen_port, 4000);
        assert_eq!(cfg.bootstrap_peers.len(), 2);
        assert_eq!(cfg.bootstrap_peers[1].host, "157.90.160.207");
        assert_eq!(cfg.bootstrap_peers[1].port, 4433);
        assert_eq!(cfg.data_dir, PathBuf::from("/tmp/tesd-test"));
        assert_eq!(cfg.max_storage, 2 * 1024 * 1024 * 1024);
        assert_eq!(cfg.pow_difficulty, 20);
        assert!(cfg.mdns);
        assert_eq!(cfg.max_chunks_per_peer, 512);
        assert_eq!(cfg.write_rate, 100);
        assert_eq!(cfg.write_burst, 40);
        assert_eq!(cfg.max_handlers, 128);
    }

    #[test]
    fn parse_empty_config_uses_defaults() {
        let input = "# empty\n";
        let cfg = Config::parse_str(input).unwrap();
        assert_eq!(cfg, Config::default());
    }

    #[test]
    fn parse_unknown_keyword_is_error() {
        let input = "foobar 42\n";
        let err = Config::parse_str(input).unwrap_err();
        assert!(err.to_string().contains("line 1"));
        assert!(err.to_string().contains("unknown keyword"));
    }

    #[test]
    fn parse_listen_missing_on_is_error() {
        let input = "listen 0.0.0.0\n";
        let err = Config::parse_str(input).unwrap_err();
        assert!(err.to_string().contains("expected 'on'"));
    }

    #[test]
    fn parse_duplicate_listen_is_error() {
        let input = "listen on :: port 4000\nlisten on 0.0.0.0 port 4001\n";
        let err = Config::parse_str(input).unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn parse_bootstrap_without_quotes() {
        // IP addresses don't require quotes
        let input = "bootstrap 10.0.0.1 port 4433\n";
        let cfg = Config::parse_str(input).unwrap();
        assert_eq!(cfg.bootstrap_peers[0].host, "10.0.0.1");
    }

    #[test]
    fn parse_listen_default_port() {
        let input = "listen on 0.0.0.0\n";
        let cfg = Config::parse_str(input).unwrap();
        assert_eq!(cfg.listen_port, 4000);
    }

    #[test]
    fn default_config_has_sane_values() {
        let cfg = Config::default();
        assert_eq!(cfg.listen_addr, "::".parse::<IpAddr>().unwrap());
        assert_eq!(cfg.listen_port, 4000);
        assert!(cfg.bootstrap_peers.is_empty());
        assert_eq!(cfg.data_dir, PathBuf::from("/var/lib/tesd"));
        assert_eq!(cfg.max_storage, 1_073_741_824); // 1G
        assert_eq!(cfg.pow_difficulty, 16);
        assert!(!cfg.mdns);
        assert_eq!(cfg.max_chunks_per_peer, 256);
        assert_eq!(cfg.write_rate, 50);
        assert_eq!(cfg.write_burst, 20);
        assert_eq!(cfg.max_handlers, 256);
    }
}
