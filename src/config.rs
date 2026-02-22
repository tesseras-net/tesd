use std::net::IpAddr;
use std::path::PathBuf;

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
