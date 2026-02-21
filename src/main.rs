use std::env;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use tesseras_dht::prelude::*;
use tracing::Level;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_PORT: u16 = 4000;
const REBOOTSTRAP_INTERVAL: Duration = Duration::from_secs(30 * 60);

struct Opts {
    ipv4_only: bool,
    ipv6_only: bool,
    listen_addr: Option<SocketAddr>,
    bootstrap_peers: Vec<String>,
    foreground: bool,
    data_dir: Option<String>,
    verbosity: u8,
}

/// Parse `-a ip[@port]` into a SocketAddr. Default port is 4000.
fn parse_listen_addr(s: &str) -> Result<SocketAddr> {
    if let Some((ip_str, port_str)) = s.rsplit_once('@') {
        let ip: IpAddr = ip_str
            .parse()
            .map_err(|e| anyhow::anyhow!("bad IP '{ip_str}': {e}"))?;
        let port: u16 = port_str
            .parse()
            .map_err(|e| {
                anyhow::anyhow!("bad port '{port_str}': {e}")
            })?;
        Ok(SocketAddr::new(ip, port))
    } else {
        let ip: IpAddr = s
            .parse()
            .map_err(|e| anyhow::anyhow!("bad IP '{s}': {e}"))?;
        Ok(SocketAddr::new(ip, DEFAULT_PORT))
    }
}

fn parse_args() -> Result<Opts> {
    let args: Vec<String> = env::args().collect();

    let mut opts = getopts::Options::new();
    opts.optflag("4", "", "Only listen to IPv4 connections");
    opts.optflag("6", "", "Only listen to IPv6 connections");
    opts.optopt("a", "", "Listen address ip[@port]", "ADDR");
    opts.optmulti("b", "", "Bootstrap peer host[@port]", "PEER");
    opts.optflag("d", "", "Do not fork, stay in foreground");
    opts.optopt("D", "", "Data directory", "DIR");
    opts.optopt("V", "", "Verbosity level (0-4)", "LEVEL");
    opts.optflag("h", "", "Print help and exit");
    opts.optflag("v", "", "Print version and exit");

    let usage_line = "usage: tesd [-46dhv] \
        [-a ip[@port]] [-b host[@port]] \
        [-D datadir] [-V level]";

    let matches =
        opts.parse(&args[1..]).map_err(|e| anyhow::anyhow!("{e}"))?;

    if matches.opt_present("h") {
        eprint!("{}", opts.usage(usage_line));
        process::exit(0);
    }

    if matches.opt_present("v") {
        eprintln!("tesd {VERSION}");
        process::exit(0);
    }

    if !matches.free.is_empty() {
        eprint!("{}", opts.usage(usage_line));
        process::exit(1);
    }

    let ipv4_only = matches.opt_present("4");
    let ipv6_only = matches.opt_present("6");

    if ipv4_only && ipv6_only {
        bail!("cannot use both -4 and -6");
    }

    let listen_addr = match matches.opt_str("a") {
        Some(s) => {
            let addr = parse_listen_addr(&s)?;
            if ipv4_only && addr.is_ipv6() {
                bail!("IPv6 address {addr} not allowed with -4");
            }
            if ipv6_only && addr.is_ipv4() {
                bail!("IPv4 address {addr} not allowed with -6");
            }
            Some(addr)
        }
        None => None,
    };

    let bootstrap_peers = matches.opt_strs("b");
    let foreground = matches.opt_present("d");
    let data_dir = matches.opt_str("D");

    let verbosity: u8 = match matches.opt_str("V") {
        Some(s) => s
            .parse()
            .map_err(|_| anyhow::anyhow!("bad verbosity '{s}'"))?,
        None => 0,
    };

    if verbosity > 4 {
        bail!("verbosity must be 0-4");
    }

    Ok(Opts {
        ipv4_only,
        ipv6_only,
        listen_addr,
        bootstrap_peers,
        foreground,
        data_dir,
        verbosity,
    })
}

#[cfg(unix)]
fn daemonize() -> Result<()> {
    use std::fs::File;
    use std::os::unix::io::AsRawFd;

    // Set restrictive file creation mask
    unsafe {
        libc::umask(0o027);
    }

    // First fork
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        bail!("fork: {}", std::io::Error::last_os_error());
    }
    if pid > 0 {
        process::exit(0);
    }

    // Create new session
    if unsafe { libc::setsid() } < 0 {
        bail!("setsid: {}", std::io::Error::last_os_error());
    }

    // Second fork (prevent acquiring a controlling terminal)
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        bail!("fork: {}", std::io::Error::last_os_error());
    }
    if pid > 0 {
        process::exit(0);
    }

    // Change to root directory to avoid blocking unmounts
    if unsafe { libc::chdir(c"/".as_ptr()) } < 0 {
        bail!("chdir: {}", std::io::Error::last_os_error());
    }

    // Redirect stdin/stdout/stderr to /dev/null
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

        log::set_boxed_logger(Box::new(
            syslog::BasicLogger::new(logger),
        ))
        .map_err(|e| anyhow::anyhow!("set logger: {e}"))?;

        log::set_max_level(match level {
            Level::ERROR => log::LevelFilter::Error,
            Level::WARN => log::LevelFilter::Warn,
            Level::INFO => log::LevelFilter::Info,
            Level::DEBUG => log::LevelFilter::Debug,
            Level::TRACE => log::LevelFilter::Trace,
        });

        // Bridge tracing events to log crate
        tracing_log::LogTracer::init()
            .map_err(|e| anyhow::anyhow!("log tracer: {e}"))?;
    }

    Ok(())
}

fn default_data_dir() -> Result<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        let base = env::var("LOCALAPPDATA")
            .map_err(|_| anyhow::anyhow!("LOCALAPPDATA not set"))?;
        Ok(PathBuf::from(base).join("tesd"))
    }
    #[cfg(target_os = "macos")]
    {
        Ok(PathBuf::from("/usr/local/var/lib/tesd"))
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        // Linux, FreeBSD, OpenBSD, NetBSD
        Ok(PathBuf::from("/var/lib/tesd"))
    }
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

    // Exclusive lock — fails if another instance holds it
    let ret = unsafe {
        libc::flock(
            file.as_raw_fd(),
            libc::LOCK_EX | libc::LOCK_NB,
        )
    };
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

/// Resolve a `-b host[@port]` string to socket addresses.
/// Retries DNS resolution up to 3 times with exponential backoff.
async fn resolve_peer(
    s: &str,
    ipv4_only: bool,
    ipv6_only: bool,
) -> Result<Vec<SocketAddr>> {
    let (host, port) = if let Some((h, p)) = s.rsplit_once('@') {
        let port: u16 = p
            .parse()
            .map_err(|_| anyhow::anyhow!("bad port '{p}'"))?;
        (h.to_string(), port)
    } else {
        (s.to_string(), DEFAULT_PORT)
    };

    // Try as IP literal first (no retry needed)
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    // DNS resolution with retry
    let lookup = format!("{host}:{port}");
    let mut last_err = None;

    for attempt in 0..3u32 {
        match tokio::net::lookup_host(&lookup).await {
            Ok(addrs) => {
                let filtered: Vec<SocketAddr> = addrs
                    .filter(|addr| {
                        if ipv4_only {
                            addr.is_ipv4()
                        } else if ipv6_only {
                            addr.is_ipv6()
                        } else {
                            true
                        }
                    })
                    .collect();

                if filtered.is_empty() {
                    bail!(
                        "no addresses found for '{host}'"
                    );
                }
                return Ok(filtered);
            }
            Err(e) => {
                last_err = Some(e);
                if attempt < 2 {
                    tracing::warn!(
                        peer = %host,
                        attempt = attempt + 1,
                        "DNS resolution failed, retrying"
                    );
                    tokio::time::sleep(
                        Duration::from_secs(1 << attempt),
                    )
                    .await;
                }
            }
        }
    }

    bail!("resolve '{host}': {}", last_err.unwrap())
}

async fn run(opts: Opts, data_dir: &Path) -> Result<()> {
    // Register signal handlers before spawning node to
    // avoid a race where signals arrive before handlers
    // are installed.
    #[cfg(unix)]
    let (mut sigterm, mut sigint, mut sighup) = {
        use tokio::signal::unix::{SignalKind, signal};
        (
            signal(SignalKind::terminate())?,
            signal(SignalKind::interrupt())?,
            signal(SignalKind::hangup())?,
        )
    };

    // Build node
    let mut builder = NodeBuilder::new(data_dir);
    builder = builder.mdns(false);

    let bind_addr = if let Some(addr) = opts.listen_addr {
        addr
    } else if opts.ipv4_only {
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            DEFAULT_PORT,
        )
    } else {
        SocketAddr::new(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            DEFAULT_PORT,
        )
    };
    builder = builder.bind(bind_addr);

    let first_run = !data_dir.join("metadata.db").exists();
    if first_run {
        tracing::info!(
            "first run, generating identity \
             (PoW may take a moment)"
        );
    }

    let node = Arc::new(builder.spawn().await?);

    tracing::info!(
        node_id = %node.node_id(),
        listen = %node.local_addr(),
        "node started"
    );

    // Resolve and bootstrap peers
    let mut all_addrs = Vec::new();
    for peer in &opts.bootstrap_peers {
        match resolve_peer(
            peer,
            opts.ipv4_only,
            opts.ipv6_only,
        )
        .await
        {
            Ok(addrs) => {
                tracing::info!(
                    peer = %peer,
                    count = addrs.len(),
                    "resolved bootstrap peer"
                );
                all_addrs.extend(addrs);
            }
            Err(e) => {
                tracing::warn!(
                    peer = %peer,
                    error = %e,
                    "failed to resolve bootstrap peer"
                );
            }
        }
    }

    // Spawn bootstrap as a background task so we can
    // handle signals immediately (bootstrap may block for
    // a long time if peers are unreachable).
    let bootstrap_handle = if !all_addrs.is_empty() {
        let node_handle = Arc::clone(&node);
        Some(tokio::spawn(async move {
            if let Err(e) =
                node_handle.bootstrap(all_addrs).await
            {
                tracing::warn!(
                    error = %e,
                    "bootstrap failed"
                );
            }
        }))
    } else {
        None
    };

    // Spawn periodic re-bootstrap task
    let rebootstrap_handle =
        if !opts.bootstrap_peers.is_empty() {
            let node_handle = Arc::clone(&node);
            let peers = opts.bootstrap_peers.clone();
            let ipv4_only = opts.ipv4_only;
            let ipv6_only = opts.ipv6_only;

            Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(REBOOTSTRAP_INTERVAL)
                        .await;

                    let mut addrs = Vec::new();
                    for peer in &peers {
                        match resolve_peer(
                            peer, ipv4_only, ipv6_only,
                        )
                        .await
                        {
                            Ok(a) => addrs.extend(a),
                            Err(e) => {
                                tracing::warn!(
                                    peer = %peer,
                                    error = %e,
                                    "re-bootstrap: resolve \
                                     failed"
                                );
                            }
                        }
                    }

                    if !addrs.is_empty() {
                        match node_handle
                            .bootstrap(addrs)
                            .await
                        {
                            Ok(()) => {
                                tracing::debug!(
                                    "re-bootstrap completed"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "re-bootstrap failed"
                                );
                            }
                        }
                    }
                }
            }))
        } else {
            None
        };

    // Wait for shutdown signal
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
                tracing::info!("received SIGHUP, ignoring");
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        tracing::info!("received Ctrl+C");
    }

    // Cleanup
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
    let opts = match parse_args() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("tesd: {e}");
            process::exit(1);
        }
    };

    let data_dir = match &opts.data_dir {
        Some(d) => PathBuf::from(d),
        None => match default_data_dir() {
            Ok(d) => d,
            Err(e) => {
                eprintln!("tesd: {e}");
                process::exit(1);
            }
        },
    };

    // Fork must happen before tokio runtime starts
    #[cfg(unix)]
    if !opts.foreground
        && let Err(e) = daemonize()
    {
        eprintln!("tesd: {e}");
        process::exit(1);
    }

    // Logging must be set up after fork (syslog connects
    // from child)
    if let Err(e) = setup_logging(opts.foreground, opts.verbosity)
    {
        eprintln!("tesd: {e}");
        process::exit(1);
    }

    // Ensure data directory exists with correct permissions
    if let Err(e) = ensure_data_dir(&data_dir) {
        tracing::error!(error = %e, "data directory");
        process::exit(1);
    }

    // Write PID file (holds flock to prevent multiple
    // instances)
    let _pid_guard = match write_pid_file(&data_dir) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, "pid file");
            process::exit(1);
        }
    };

    // Build and run tokio runtime
    let rt = tokio::runtime::Runtime::new()
        .expect("failed to create runtime");
    if let Err(e) = rt.block_on(run(opts, &data_dir)) {
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
    fn parse_listen_addr_ipv4_with_port() {
        let addr = parse_listen_addr("127.0.0.1@8080").unwrap();
        assert_eq!(
            addr.ip(),
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
        );
        assert_eq!(addr.port(), 8080);
    }

    #[test]
    fn parse_listen_addr_ipv4_default_port() {
        let addr = parse_listen_addr("0.0.0.0").unwrap();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), DEFAULT_PORT);
    }

    #[test]
    fn parse_listen_addr_ipv6_with_port() {
        let addr = parse_listen_addr("::1@9000").unwrap();
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addr.port(), 9000);
    }

    #[test]
    fn parse_listen_addr_ipv6_default_port() {
        let addr = parse_listen_addr("::").unwrap();
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), DEFAULT_PORT);
    }

    #[test]
    fn parse_listen_addr_bad_ip() {
        assert!(parse_listen_addr("not-an-ip").is_err());
    }

    #[test]
    fn parse_listen_addr_bad_port() {
        assert!(parse_listen_addr("127.0.0.1@99999").is_err());
    }

    #[test]
    fn parse_listen_addr_empty_port() {
        assert!(parse_listen_addr("127.0.0.1@").is_err());
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

    #[test]
    fn default_data_dir_returns_path() {
        let dir = default_data_dir().unwrap();
        assert!(!dir.as_os_str().is_empty());
    }
}
