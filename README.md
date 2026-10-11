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
