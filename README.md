<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Rust implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Async/await and streaming values are supported out of the box.

# Installation

```bash
cargo add channel-rs
```

# Dependencies

Uses crates from the Rust ecosystem: `tokio` (async runtime), `tokio-tungstenite` and `http` (WebSocket),
`serde`, `serde_json` and `simd-json` (JSON), plus `futures`, `async-stream`, `dashmap`, `parking_lot`, `bytes`,
`url`, `sha1`, `base64`, `getrandom`, `thiserror` and `anyhow`.

# Usage

The API is synchronous: a call blocks and drives the servers and clients of its thread until it completes.

```rust
use channel_rs::{CallResult, Channel, ChannelError, ConnectOptions, ListenOptions, PostRequest};

#[derive(Default)]
struct State;

fn main() -> Result<(), ChannelError> {
    // Server
    let mut server_channel = Channel::<State>::new();
    server_channel.post_value("hello", |_request: PostRequest<'_, State, (), ()>| Ok("world"))?;
    let mut server = server_channel.listen(2049_u16, ListenOptions::default())?;

    // Client (calls block and drive this thread's server and client until the reply arrives)
    let mut client = Channel::<State>::new().connect(2049_u16, ConnectOptions::default())?;
    if let CallResult::Ok(Some(value)) = client.send::<String, _>("hello", ())? {
        println!("{value}"); // world
    }

    // Process transport (in-memory)
    let mut process_channel = Channel::<State>::new();
    process_channel.post_value("hello", |_request: PostRequest<'_, State, (), ()>| Ok("world"))?;
    let mut process = process_channel.process(State)?;
    if let CallResult::Ok(Some(value)) = process.send::<String, _>("hello", ())? {
        println!("{value}"); // world
    }

    client.stop();
    server.stop();
    Ok(())
}
```

# QUIC transport (experimental)

QUIC is optional: it sits behind the `quic` feature, so a WebSocket-only build compiles none of its crates.
The feature adds `quinn` (QUIC), `rustls` with `ring` (TLS 1.3) and `rustls-native-certs` (the system's roots).

```bash
cargo add channel-rs --features quic
```

The same `Channel`, handlers, state, streams and events work over it; only the address and the certificate change.

```rust
use channel_rs::{CallResult, Channel, ChannelError, ConnectOptions, PostRequest, QuicListenOptions, QuicOptions};

#[derive(Default)]
struct State;

fn main() -> Result<(), ChannelError> {
    let cert = std::fs::read("cert.pem")?;
    let key = std::fs::read("key.pem")?;

    // Server: UDP 4433. QuicListenOptions::options holds the usual ListenOptions (state, on_connect, ...);
    // the state callback gets the client's headers, names lowercased.
    let mut server_channel = Channel::<State>::new();
    server_channel.post_value("hello", |_request: PostRequest<'_, State, (), ()>| Ok("world"))?;
    let mut server = server_channel.listen_quic("127.0.0.1:4433", QuicListenOptions::new(cert.clone(), key))?;

    // Client: a quic:// address
    let options = ConnectOptions {
        quic: QuicOptions { ca: Some(cert), ..QuicOptions::default() },
        ..ConnectOptions::default()
    };
    let mut client = Channel::<State>::new().connect("quic://127.0.0.1:4433", options)?;
    if let CallResult::Ok(Some(value)) = client.send::<String, _>("hello", ())? {
        println!("{value}"); // world
    }

    client.stop();
    server.stop();
    Ok(())
}
```

Certificates: QUIC is always encrypted, so the server needs a PEM certificate and key, and the client checks them
(`QuicOptions`):
- nothing set: the certificate must be valid for the address host under the system's root certificates;
- `ca`: trusts these PEM certificates instead, a CA or the server's own self-signed certificate (the address must be
  in its subject alternative names, as a DNS name or an IP address);
- `fingerprint`: accepts only the certificate with this SHA-256 (hex, colons optional, as
  `openssl x509 -in cert.pem -noout -fingerprint -sha256` prints it) and skips the other checks;
- `server_name`: the name sent and checked instead of the address host.

Each client connection dials from its own UDP socket, never a shared one (Network.framework servers take one
connection per client address and port). The wire format (one stream per connection, varint-length frames, a hello
with the headers, a keepalive every 15 s, 30 s idle timeout) is in the TS reference's `channel/docs/quic.md`.
Languages with QUIC: TypeScript, Go, Swift, C#, Rust, Python, Elixir, Java, Kotlin, C and Zig; Dart, Ruby and PHP stay on WebSocket. quinn runs on a small tokio runtime of its own (two threads) next to this
port's synchronous pump. Like the WebSocket client, the QUIC client doesn't reconnect.

# Workspace layout
- `crates/channel-rs-core`
- `crates/channel-rs-client`
- `crates/channel-rs-server`
- `crates/channel-rs-process`
- `crates/channel-rs`
- `crates/channel-rs-bench`

# Transports
- client (`channel-rs-client`)
- server (`channel-rs-server`)
- process/in-memory transport (`channel-rs-process`)
- unified facade crate (`channel-rs`)

# Scripts
- `./scripts/test`
- `./scripts/benchmark`
- `./scripts/coverage`

Coverage output:
- `coverage/lcov.info`

# Interop target
Integration tests use:
- `../channel/tests/server.ts`
- WebSocket endpoint: `127.0.0.1:2049`
- QUIC tests (`--features quic`): the same fixture on `CHANNEL_TEST_PORT` (default 2101), `quic://127.0.0.1:<port>`
  with `../channel/tests/quic/cert.pem`, and the TS client `crates/channel-rs-core/tests/quic/client.ts`
