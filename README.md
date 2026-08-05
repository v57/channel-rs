<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Rust implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Async/await and streaming values are supported out of the box.

# Installation

```bash
cargo add channel-rs
```

# Usage

```rust
use channel_rs::{
    Channel, ChannelClientExt, ChannelProcessExt, ChannelServerExt, ClientOptions, ListenOptions,
};

type State = std::collections::HashMap<String, serde_json::Value>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Server
    let server = Channel::<State>::new()
        .post_value("hello", |_| "world")
        .listen_port(2049, ListenOptions::default())?;

    // Client
    let client = Channel::<State>::new().connect_port(2049, ClientOptions::default());
    let value: String = client.send_empty("hello").await?;
    println!("{value:?}");

    // Process transport (in-memory)
    let process_sender = Channel::<State>::new()
        .post_value("hello", |_| "world")
        .process();

    let value: String = process_sender.send_empty("hello").await?;
    println!("{value:?}");

    client.stop();
    server.stop().await;
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
