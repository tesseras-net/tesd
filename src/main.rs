mod config;

use std::env;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use tesseras_dht::prelude::*;
use tracing::Level;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const REBOOTSTRAP_INTERVAL: Duration = Duration::from_secs(30 * 60);

struct CliOpts {
    config_file: PathBuf,
    configtest: bool,
    foreground: bool,
    verbosity: u8,
}

fn parse_cli(args: &[&str]) -> Result<CliOpts> {
    let mut opts = CliOpts {
        config_file: PathBuf::from("/etc/tesd.conf"),
        configtest: false,
        foreground: false,
        verbosity: 0,
    };

    let mut i = 0;
    while i < args.len() {
        let arg = args[i];
        if !arg.starts_with('-') || arg == "-" {
            bail!("unexpected argument '{arg}'");
        }

        let chars: Vec<char> = arg[1..].chars().collect();
        let mut j = 0;
        while j < chars.len() {
            match chars[j] {
                'f' => {
                    let value = if j + 1 < chars.len() {
                        let rest: String = chars[j + 1..].iter().collect();
                        j = chars.len();
                        rest
                    } else {
                        i += 1;
                        args.get(i)
                            .ok_or_else(|| {
                                anyhow::anyhow!("-f requires an argument")
                            })?
                            .to_string()
                    };
                    opts.config_file = PathBuf::from(value);
                }
                'n' => opts.configtest = true,
                'd' => opts.foreground = true,
                'v' => opts.verbosity = opts.verbosity.saturating_add(1),
                'h' => {
                    eprint!(
                        "usage: tesd [-dnvVh] [-f file]\n\n\
                         Options:\n\
                         \x20   -f file     Configuration file (default: /etc/tesd.conf)\n\
                         \x20   -n          Check config and exit\n\
                         \x20   -d          Do not fork, stay in foreground\n\
                         \x20   -v          Verbose mode. Multiple -v increase verbosity\n\
                         \x20   -h          Print help and exit\n\
                         \x20   -V          Print version and exit\n"
                    );
                    process::exit(0);
                }
                'V' => {
                    eprintln!("tesd {VERSION}");
                    process::exit(0);
                }
                c => bail!("unknown option '-{c}'"),
            }
            j += 1;
        }
        i += 1;
    }

    Ok(opts)
}

#[cfg(unix)]
fn daemonize() -> Result<()> {
    use std::fs::File;
    use std::os::unix::io::AsRawFd;

    unsafe {
        libc::umask(0o027);
    }

    let pid = unsafe { libc::fork() };
    if pid < 0 {
        bail!("fork: {}", std::io::Error::last_os_error());
    }
    if pid > 0 {
        process::exit(0);
    }

    if unsafe { libc::setsid() } < 0 {
        bail!("setsid: {}", std::io::Error::last_os_error());
    }

    let pid = unsafe { libc::fork() };
    if pid < 0 {
        bail!("fork: {}", std::io::Error::last_os_error());
    }
    if pid > 0 {
        process::exit(0);
    }

    if unsafe { libc::chdir(c"/".as_ptr()) } < 0 {
        bail!("chdir: {}", std::io::Error::last_os_error());
    }

    let devnull = File::open("/dev/null")?;
    let fd = devnull.as_raw_fd();
    for target_fd in [0, 1, 2] {
        if unsafe { libc::dup2(fd, target_fd) } < 0 {
            bail!(
                "dup2({fd}, {target_fd}): {}",
                std::io::Error::last_os_error()
            );
        }
    }

    Ok(())
}

fn verbosity_to_level(v: u8) -> Level {
    match v {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        _ => Level::TRACE,
    }
}

fn setup_logging(foreground: bool, verbosity: u8) -> Result<()> {
    let level = verbosity_to_level(verbosity);

    if foreground {
        tracing_subscriber::fmt()
            .with_max_level(level)
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .without_time()
            .init();
    } else {
        use syslog::{Facility, Formatter3164};

        let formatter = Formatter3164 {
            facility: Facility::LOG_DAEMON,
            hostname: None,
            process: "tesd".into(),
            pid: std::process::id(),
        };

        let logger = syslog::unix(formatter)
            .map_err(|e| anyhow::anyhow!("syslog: {e}"))?;

        log::set_boxed_logger(Box::new(syslog::BasicLogger::new(logger)))
            .map_err(|e| anyhow::anyhow!("set logger: {e}"))?;

        log::set_max_level(match level {
            Level::ERROR => log::LevelFilter::Error,
            Level::WARN => log::LevelFilter::Warn,
            Level::INFO => log::LevelFilter::Info,
            Level::DEBUG => log::LevelFilter::Debug,
            Level::TRACE => log::LevelFilter::Trace,
        });

        tracing_log::LogTracer::init()
            .map_err(|e| anyhow::anyhow!("log tracer: {e}"))?;
    }

    Ok(())
}

fn ensure_data_dir(data_dir: &Path) -> Result<()> {
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                data_dir,
                std::fs::Permissions::from_mode(0o750),
            )?;
        }
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let mode = std::fs::metadata(data_dir)?.mode() & 0o777;
            if mode & 0o007 != 0 {
                tracing::warn!(
                    "data directory {} is world-accessible \
                     (mode {:04o})",
                    data_dir.display(),
                    mode
                );
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn write_pid_file(data_dir: &Path) -> Result<std::fs::File> {
    use std::os::unix::io::AsRawFd;

    let pid_path = data_dir.join("tesd.pid");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&pid_path)?;

    let ret =
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if ret < 0 {
        bail!(
            "another instance is running (cannot lock {})",
            pid_path.display()
        );
    }

    writeln!(file, "{}", process::id())?;
    Ok(file)
}

#[cfg(not(unix))]
fn write_pid_file(data_dir: &Path) -> Result<std::fs::File> {
    let pid_path = data_dir.join("tesd.pid");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&pid_path)?;
    writeln!(file, "{}", process::id())?;
    Ok(file)
}

fn remove_pid_file(data_dir: &Path) {
    let _ = std::fs::remove_file(data_dir.join("tesd.pid"));
}

/// Resolve a bootstrap peer host to socket addresses.
/// Retries DNS resolution up to 3 times with exponential backoff.
async fn resolve_peer(s: &str, port: u16) -> Result<Vec<SocketAddr>> {
    // Try as IP literal first (no retry needed)
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    // DNS resolution with retry
    let lookup = format!("{s}:{port}");
    let mut last_err = None;

    for attempt in 0..3u32 {
        match tokio::net::lookup_host(&lookup).await {
            Ok(addrs) => {
                let resolved: Vec<SocketAddr> = addrs.collect();
                if resolved.is_empty() {
                    bail!("no addresses found for '{s}'");
                }
                return Ok(resolved);
            }
            Err(e) => {
                last_err = Some(e);
                if attempt < 2 {
                    tracing::warn!(
                        peer = %s,
                        attempt = attempt + 1,
                        "DNS resolution failed, retrying"
                    );
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                }
            }
        }
    }

    bail!("resolve '{s}': {}", last_err.unwrap())
}

async fn run(
    cfg: config::Config,
    config_path: PathBuf,
) -> Result<()> {
    let mut cfg = cfg;

    #[cfg(unix)]
    let (mut sigterm, mut sigint, mut sighup) = {
        use tokio::signal::unix::{SignalKind, signal};
        (
            signal(SignalKind::terminate())?,
            signal(SignalKind::interrupt())?,
            signal(SignalKind::hangup())?,
        )
    };

    let bind_addr = SocketAddr::new(cfg.listen_addr, cfg.listen_port);

    let mut node_config = NodeConfig::default();
    node_config.max_chunks_per_peer = cfg.max_chunks_per_peer;
    node_config.write_rate_per_second = cfg.write_rate;
    node_config.write_rate_burst = cfg.write_burst;
    node_config.max_concurrent_handlers = cfg.max_handlers;

    let mut builder = NodeBuilder::new(&cfg.data_dir);
    builder = builder
        .bind(bind_addr)
        .max_storage(cfg.max_storage)
        .pow_difficulty(cfg.pow_difficulty)
        .mdns(cfg.mdns)
        .config(node_config);

    let first_run = !cfg.data_dir.join("metadata.db").exists();
    if first_run {
        tracing::info!("first run, generating identity (PoW may take a moment)");
    }

    let node = Arc::new(builder.spawn().await?);

    tracing::info!(
        node_id = %node.node_id(),
        listen = %node.local_addr(),
        "node started"
    );

    // Resolve and bootstrap peers
    let mut all_addrs = Vec::new();
    for peer in &cfg.bootstrap_peers {
        match resolve_peer(&peer.host, peer.port).await {
            Ok(addrs) => {
                tracing::info!(
                    peer = %peer.host,
                    count = addrs.len(),
                    "resolved bootstrap peer"
                );
                all_addrs.extend(addrs);
            }
            Err(e) => {
                tracing::warn!(
                    peer = %peer.host,
                    error = %e,
                    "failed to resolve bootstrap peer"
                );
            }
        }
    }

    let bootstrap_handle = if !all_addrs.is_empty() {
        let node_handle = Arc::clone(&node);
        Some(tokio::spawn(async move {
            if let Err(e) = node_handle.bootstrap(all_addrs).await {
                tracing::warn!(error = %e, "bootstrap failed");
            }
        }))
    } else {
        None
    };

    // Re-bootstrap task
    let rebootstrap_handle = if !cfg.bootstrap_peers.is_empty() {
        let node_handle = Arc::clone(&node);
        let peers = cfg.bootstrap_peers.clone();
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(REBOOTSTRAP_INTERVAL).await;
                let mut addrs = Vec::new();
                for peer in &peers {
                    match resolve_peer(&peer.host, peer.port).await {
                        Ok(a) => addrs.extend(a),
                        Err(e) => {
                            tracing::warn!(
                                peer = %peer.host,
                                error = %e,
                                "re-bootstrap: resolve failed"
                            );
                        }
                    }
                }
                if !addrs.is_empty() {
                    match node_handle.bootstrap(addrs).await {
                        Ok(()) => tracing::debug!("re-bootstrap completed"),
                        Err(e) => {
                            tracing::warn!(error = %e, "re-bootstrap failed")
                        }
                    }
                }
            }
        }))
    } else {
        None
    };

    // Signal loop (SIGHUP reload placeholder — Task 8)
    #[cfg(unix)]
    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM");
                break;
            }
            _ = sigint.recv() => {
                tracing::info!("received SIGINT");
                break;
            }
            _ = sighup.recv() => {
                tracing::info!("received SIGHUP, reloading configuration");
                match config::Config::parse(&config_path) {
                    Ok(new_cfg) => {
                        cfg.log_reload_diff(&new_cfg);
                        cfg = new_cfg;
                        tracing::info!("configuration reloaded");
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "failed to reload configuration, keeping current"
                        );
                    }
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        tracing::info!("received Ctrl+C");
    }

    if let Some(handle) = bootstrap_handle {
        handle.abort();
    }
    if let Some(handle) = rebootstrap_handle {
        handle.abort();
    }

    tracing::info!("shutting down");
    node.shutdown().await;
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    let cli = match parse_cli(&arg_refs) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("tesd: {e}");
            process::exit(1);
        }
    };

    let cfg = match config::Config::parse(&cli.config_file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("tesd: {e}");
            process::exit(1);
        }
    };

    if cli.configtest {
        eprintln!("configuration OK");
        process::exit(0);
    }

    let data_dir = cfg.data_dir.clone();

    #[cfg(unix)]
    if !cli.foreground
        && let Err(e) = daemonize()
    {
        eprintln!("tesd: {e}");
        process::exit(1);
    }

    if let Err(e) = setup_logging(cli.foreground, cli.verbosity) {
        eprintln!("tesd: {e}");
        process::exit(1);
    }

    if let Err(e) = ensure_data_dir(&data_dir) {
        tracing::error!(error = %e, "data directory");
        process::exit(1);
    }

    let _pid_guard = match write_pid_file(&data_dir) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, "pid file");
            process::exit(1);
        }
    };

    let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
    if let Err(e) = rt.block_on(run(cfg, cli.config_file.clone())) {
        tracing::error!(error = %e, "fatal error");
        remove_pid_file(&data_dir);
        process::exit(1);
    }

    remove_pid_file(&data_dir);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cli_defaults() {
        let cli = parse_cli(&[]).unwrap();
        assert_eq!(cli.config_file, PathBuf::from("/etc/tesd.conf"));
        assert!(!cli.configtest);
        assert!(!cli.foreground);
        assert_eq!(cli.verbosity, 0);
    }

    #[test]
    fn parse_cli_custom_config() {
        let cli = parse_cli(&["-f", "/tmp/test.conf"]).unwrap();
        assert_eq!(cli.config_file, PathBuf::from("/tmp/test.conf"));
    }

    #[test]
    fn parse_cli_verbose_stacks() {
        let cli = parse_cli(&["-vvv"]).unwrap();
        assert_eq!(cli.verbosity, 3);
    }

    #[test]
    fn parse_cli_configtest() {
        let cli = parse_cli(&["-n"]).unwrap();
        assert!(cli.configtest);
    }

    #[test]
    fn parse_cli_foreground() {
        let cli = parse_cli(&["-d"]).unwrap();
        assert!(cli.foreground);
    }

    #[test]
    fn parse_cli_combined() {
        let cli = parse_cli(&["-dnvv", "-f", "/tmp/t.conf"]).unwrap();
        assert!(cli.foreground);
        assert!(cli.configtest);
        assert_eq!(cli.verbosity, 2);
        assert_eq!(cli.config_file, PathBuf::from("/tmp/t.conf"));
    }

    #[test]
    fn parse_cli_unknown_flag_is_error() {
        assert!(parse_cli(&["-x"]).is_err());
    }

    #[test]
    fn verbosity_levels() {
        assert_eq!(verbosity_to_level(0), Level::ERROR);
        assert_eq!(verbosity_to_level(1), Level::WARN);
        assert_eq!(verbosity_to_level(2), Level::INFO);
        assert_eq!(verbosity_to_level(3), Level::DEBUG);
        assert_eq!(verbosity_to_level(4), Level::TRACE);
        assert_eq!(verbosity_to_level(255), Level::TRACE);
    }
}
