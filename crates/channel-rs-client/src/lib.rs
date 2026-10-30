pub use channel_rs_core::{
    CallResult, Channel, ChannelError, Client, ConnectOptions, IntoConnectTarget, Sender,
    StreamNext, SubscribeResult, SubscriptionHandle, Values,
};

#[cfg(feature = "quic")]
pub use channel_rs_core::QuicOptions;

pub type ClientSender<State> = Sender<State>;
pub type WebSocketClient<State> = Client<State>;
