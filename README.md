# tesd

Tesseras DHT daemon for running bootstrap nodes on the
[Tesseras](https://tesseras.net) P2P network.

## About

`tesd` is a lightweight daemon that runs a
[Tesseras DHT](https://git.sr.ht/~ijanc/tesseras-dht) node in the background. It
is designed to operate as a long-running bootstrap node that other peers can
connect to when joining the network.

Features:

- Classic Unix daemonization with double fork
- Syslog logging in background mode, stderr in foreground
- DNS and IP literal resolution for bootstrap peers
- Dual-stack IPv4/IPv6 support
- Graceful shutdown on SIGTERM/SIGINT
- Platform-specific default data directories

## Usage

```
usage: tesd [-46dhv] [-a ip[@port]] [-b host[@port]] [-D datadir] [-V level]

Options:
    -4          Only listen to IPv4 connections
    -6          Only listen to IPv6 connections
    -a ADDR     Listen address ip[@port] (default: [::]:4000)
    -b PEER     Bootstrap peer host[@port]
    -d          Do not fork, stay in foreground
    -D DIR      Data directory
    -V LEVEL    Verbosity level (0=error, 1=warn, 2=info, 3=debug, 4=trace)
    -h          Print help and exit
    -v          Print version and exit
```

### Examples

Run in foreground with info-level logging:

```sh
tesd -d -V 2
```

Listen on a specific address and bootstrap from an existing node:

```sh
tesd -d -a 127.0.0.1@4001 -b bootstrap.tesseras.net
```

Run as a background daemon with a custom data directory:

```sh
tesd -D /srv/tesd -a 0.0.0.0@4000 -b 192.0.2.1@4000
```

### Default data directory

| Platform    | Path                      |
| ----------- | ------------------------- |
| Linux, *BSD | `/var/lib/tesd`           |
| macOS       | `/usr/local/var/lib/tesd` |
| Windows     | `%LOCALAPPDATA%\tesd`     |

## Links

- [Website](https://tesseras.net)
- [Documentation](https://tesseras.net/book/en/)
- [Source code](https://git.sr.ht/~ijanc/tesd) (primary)
- [GitHub mirror](https://github.com/tesseras-net/tesd)
- [Ticket tracker](https://todo.sr.ht/~ijanc/tesseras)
- [Mailing lists](https://tesseras.net/subscriptions/)

## License

ISC — see [LICENSE](LICENSE).
