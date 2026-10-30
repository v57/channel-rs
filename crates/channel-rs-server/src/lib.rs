pub use channel_rs_core::{Channel, ChannelError, IntoListenTarget, ListenOptions, Server};

#[cfg(feature = "quic")]
pub use channel_rs_core::{QuicListenOptions, QuicServer};

pub type ChannelServer<State> = Server<State>;
