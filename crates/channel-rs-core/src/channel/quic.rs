//! QUIC transport (experimental, the `quic` feature; `channel/docs/quic.md` in the TS reference).
//!
//! One bidirectional stream per connection, opened by the client, carries the same JSON messages as a
//! WebSocket, each one prefixed with its length as a QUIC variable-length integer. The first client frame is
//! `{"hello": {header: value}}`; an empty frame is a keepalive.
//!
//! quinn is async, this port is not: quinn runs on a small tokio runtime of its own, and each connection
//! talks to the thread's pump through two bounded channels (frames in, frames out), so handlers, blocking
//! calls and back-pressure work as with the WebSocket transport.

use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    sync::{Arc, OnceLock, mpsc as std_mpsc},
    time::Duration,
};

use quinn::{
    Endpoint, IdleTimeout, RecvStream, SendStream, TransportConfig, VarInt,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use rustls::{
    DigitallySignedStruct, RootCertStore, SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject},
};
use serde_json::{Map, Value};
use tokio::sync::mpsc::{
    self,
    error::{TryRecvError, TrySendError},
};

use super::{
    Channel, ChannelError, Client, ClientLink, ConnectOptions, ConnectTarget, IntoListenTarget, ListenOptions,
    Peer, RuntimeChannel, Sender, TransportMode, TransportVTable, peer_stop, pump_all, register_pump_entry,
    sleep_brief, unregister_pump_entry,
};

/// The QUIC application protocol of a Channel connection.
pub const QUIC_ALPN: &[u8] = b"channel/1";
/// Connections silent this long are closed (QUIC `max_idle_timeout`, both sides).
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// The client sends an empty frame this often to keep the connection open.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
/// Frames buffered between the pump and a connection's tasks, each way.
const QUEUED_FRAMES: usize = 1024;
const READ_CHUNK: usize = 64 * 1024;
/// How long a closing side waits for the peer to read what it still sent.
const CLOSE_GRACE: Duration = Duration::from_secs(1);
const CLOSE_OK: u32 = 0;
const CLOSE_PROTOCOL: u32 = 1;

/// Certificate checks for `quic://` addresses ([`ConnectOptions::quic`]). With none set, the server
/// certificate must be valid for the address host under the system's root certificates.
#[derive(Debug, Clone, Default)]
pub struct QuicOptions {
    /// PEM certificates to trust instead of the system roots: a CA, or a self-signed server certificate.
    pub ca: Option<Vec<u8>>,
    /// SHA-256 fingerprint of the server certificate (hex, colons optional). It replaces the other checks.
    pub fingerprint: Option<String>,
    /// The name sent (SNI) and checked, instead of the address host.
    pub server_name: Option<String>,
}

/// [`Channel::listen_quic`]'s options: the WebSocket server's [`ListenOptions`] plus the certificate.
pub struct QuicListenOptions<State> {
    pub options: ListenOptions<State>,
    /// PEM certificate chain.
    pub cert: Vec<u8>,
    /// PEM private key.
    pub key: Vec<u8>,
}

impl<State> QuicListenOptions<State> {
    pub fn new(cert: impl Into<Vec<u8>>, key: impl Into<Vec<u8>>) -> Self {
        Self {
            options: ListenOptions::default(),
            cert: cert.into(),
            key: key.into(),
        }
    }
}

/// Appends one frame: the RFC 9000 variable-length integer length of `payload`, then `payload`.
pub fn encode_quic_frame(out: &mut Vec<u8>, payload: &[u8]) {
    let length = payload.len() as u64;
    if length < 1 << 6 {
        out.push(length as u8);
    } else if length < 1 << 14 {
        out.extend_from_slice(&(length as u16 | 0x4000).to_be_bytes());
    } else if length < 1 << 30 {
        out.extend_from_slice(&(length as u32 | 0x8000_0000).to_be_bytes());
    } else {
        out.extend_from_slice(&(length | 0xc000_0000_0000_0000).to_be_bytes());
    }
    out.extend_from_slice(payload);
}

/// Splits a stream back into frames, skipping keepalives. A declared length over `max_payload` is
/// refused from the length alone.
pub(super) struct FrameDecoder {
    buffer: Vec<u8>,
    pos: usize,
    max_payload: usize,
}

impl FrameDecoder {
    pub(super) fn new(max_payload: usize) -> Self {
        Self {
            buffer: Vec::new(),
            pos: 0,
            max_payload,
        }
    }

    pub(super) fn push(&mut self, data: &[u8]) {
        if self.pos > 0 && (self.pos == self.buffer.len() || self.pos >= 64 * 1024) {
            self.buffer.drain(..self.pos);
            self.pos = 0;
        }
        self.buffer.extend_from_slice(data);
    }

    pub(super) fn next_frame(&mut self) -> Result<Option<Vec<u8>>, ChannelError> {
        loop {
            let data = &self.buffer[self.pos..];
            let Some(&first) = data.first() else {
                return Ok(None);
            };
            let size = 1_usize << (first >> 6);
            if data.len() < size {
                return Ok(None);
            }
            let mut length = u64::from(first & 0x3f);
            for &byte in &data[1..size] {
                length = length << 8 | u64::from(byte);
            }
            if length > self.max_payload as u64 {
                return Err(ChannelError::new("quic frame too large"));
            }
            // In range now: length <= max_payload, which fits in usize.
            let length = length as usize;
            if data.len() - size < length {
                return Ok(None);
            }
            let start = self.pos + size;
            self.pos = start + length;
            if length == 0 {
                continue;
            }
            return Ok(Some(self.buffer[start..start + length].to_vec()));
        }
    }
}

/// The hello frame's payload: the raw `name: value` header lines as `{"hello": {...}}`.
pub(super) fn hello_payload(headers: Option<&str>) -> Result<Vec<u8>, ChannelError> {
    let mut map = Map::new();
    for line in headers.unwrap_or_default().split('\n') {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            if !name.is_empty() {
                map.insert(name.to_string(), Value::String(value.trim().to_string()));
            }
        }
    }
    let mut hello = Map::new();
    hello.insert("hello".to_string(), Value::Object(map));
    Ok(serde_json::to_vec(&Value::Object(hello))?)
}

/// The headers of a hello frame, names lowercased and non-string values dropped. `None` when the frame
/// isn't a hello.
pub(super) fn parse_hello(payload: &[u8]) -> Option<HashMap<String, String>> {
    let Ok(Value::Object(mut first)) = serde_json::from_slice::<Value>(payload) else {
        return None;
    };
    let Some(Value::Object(hello)) = first.remove("hello") else {
        return None;
    };
    Some(
        hello
            .into_iter()
            .filter_map(|(name, value)| match value {
                Value::String(value) => Some((name.to_lowercase(), value)),
                _ => None,
            })
            .collect(),
    )
}

/// The tokio runtime quinn runs on, shared by every QUIC client and server of the process.
pub(super) fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("channel-quic")
            .enable_all()
            .build()
            .expect("channel quic runtime")
    })
}

fn error(error: impl std::fmt::Display) -> ChannelError {
    ChannelError::new(error.to_string())
}

// ---------------------------------------------------------------------------------------------------------
// TLS

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn transport_config(client: bool) -> Result<Arc<TransportConfig>, ChannelError> {
    let mut transport = TransportConfig::default();
    transport.max_idle_timeout(Some(IdleTimeout::try_from(IDLE_TIMEOUT).map_err(error)?));
    if client {
        // The server never opens streams
        transport.max_concurrent_bidi_streams(VarInt::from_u32(0));
    } else {
        // One stream per connection: the client's first; it may not open more or any unidirectional ones
        transport.max_concurrent_bidi_streams(VarInt::from_u32(1));
    }
    transport.max_concurrent_uni_streams(VarInt::from_u32(0));
    Ok(Arc::new(transport))
}

fn parse_fingerprint(text: &str) -> Result<[u8; 32], ChannelError> {
    let hex = text.replace(':', "").to_ascii_lowercase();
    let invalid = || ChannelError::new(format!("quic fingerprint isn't a SHA-256 in hex: {text:?}"));
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid());
    }
    let mut out = [0_u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).map_err(|_| invalid())?;
    }
    Ok(out)
}

fn read_certificates(pem: &[u8], what: &str) -> Result<Vec<CertificateDer<'static>>, ChannelError> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ChannelError::new(format!("quic {what}: {error}")))?;
    if certificates.is_empty() {
        return Err(ChannelError::new(format!("quic {what} has no PEM certificate")));
    }
    Ok(certificates)
}

fn client_config(options: &QuicOptions) -> Result<quinn::ClientConfig, ChannelError> {
    let provider = provider();
    let verifier: Arc<dyn ServerCertVerifier> = match &options.fingerprint {
        Some(fingerprint) => Arc::new(PinVerifier {
            sha256: parse_fingerprint(fingerprint)?,
            provider: provider.clone(),
        }),
        None => {
            let (roots, direct) = match &options.ca {
                Some(ca) => {
                    let certificates = read_certificates(ca, "ca")?;
                    let mut roots = RootCertStore::empty();
                    for certificate in &certificates {
                        // A self-signed server certificate marked as a CA isn't a valid root for webpki;
                        // it's trusted as itself below
                        let _ = roots.add(certificate.clone());
                    }
                    (roots, certificates)
                }
                None => {
                    let mut roots = RootCertStore::empty();
                    for certificate in rustls_native_certs::load_native_certs().certs {
                        let _ = roots.add(certificate);
                    }
                    (roots, Vec::new())
                }
            };
            let webpki = if roots.is_empty() {
                None
            } else {
                Some(
                    WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                        .build()
                        .map_err(error)?,
                )
            };
            Arc::new(TrustVerifier {
                webpki,
                direct,
                provider: provider.clone(),
            })
        }
    };
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(error)?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    tls.alpn_protocols = vec![QUIC_ALPN.to_vec()];
    // A replayed 0-RTT request could run twice
    tls.enable_early_data = false;
    let crypto = QuicClientConfig::try_from(tls).map_err(error)?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(transport_config(true)?);
    Ok(config)
}

fn server_config(cert: &[u8], key: &[u8]) -> Result<quinn::ServerConfig, ChannelError> {
    let certificates = read_certificates(cert, "cert")?;
    let key = PrivateKeyDer::from_pem_slice(key).map_err(|error| ChannelError::new(format!("quic key: {error}")))?;
    let mut tls = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(error)?
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(error)?;
    tls.alpn_protocols = vec![QUIC_ALPN.to_vec()];
    tls.max_early_data_size = 0;
    let crypto = QuicServerConfig::try_from(tls).map_err(error)?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(transport_config(false)?);
    Ok(config)
}

fn verify_signature(
    provider: &CryptoProvider,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
) -> Result<HandshakeSignatureValid, rustls::Error> {
    rustls::crypto::verify_tls13_signature(message, cert, dss, &provider.signature_verification_algorithms)
}

/// Verifies against roots (the system's or the `ca` option), and also trusts a server certificate that is
/// itself one of the `ca` certificates (a self-signed one, as Node and Go do) once its names match.
#[derive(Debug)]
struct TrustVerifier {
    webpki: Option<Arc<WebPkiServerVerifier>>,
    direct: Vec<CertificateDer<'static>>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for TrustVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self.direct.iter().any(|certificate| certificate.as_ref() == end_entity.as_ref()) {
            let parsed = rustls::server::ParsedCertificate::try_from(end_entity)?;
            rustls::client::verify_server_name(&parsed, server_name)?;
            return Ok(ServerCertVerified::assertion());
        }
        match &self.webpki {
            Some(webpki) => webpki.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now),
            None => Err(rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer)),
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("QUIC uses TLS 1.3".to_string()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_signature(&self.provider, message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

/// Accepts only the certificate with this SHA-256 (names, dates and issuer aren't checked, as in the TS
/// client). The handshake signature is still verified, so the server must hold the certificate's key.
#[derive(Debug)]
struct PinVerifier {
    sha256: [u8; 32],
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let digest = ring::digest::digest(&ring::digest::SHA256, end_entity.as_ref());
        if digest.as_ref() == self.sha256 {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("certificate fingerprint mismatch".to_string()))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("QUIC uses TLS 1.3".to_string()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_signature(&self.provider, message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

// ---------------------------------------------------------------------------------------------------------
// Connection tasks (on the runtime)

enum LinkEvent {
    Frame(Vec<u8>),
    /// The connection ended: `None` when it closed or was lost, the reason when the peer broke the wire rules.
    Closed(Option<String>),
}

/// The pump's ends of a connection's tasks.
struct LinkParts {
    connection: quinn::Connection,
    events: mpsc::Receiver<LinkEvent>,
    out: mpsc::Sender<Vec<u8>>,
}

/// Starts the reader and writer of an open stream. `endpoint` is the client's own one, kept until the
/// connection is gone.
fn spawn_link(
    connection: quinn::Connection,
    send: SendStream,
    recv: RecvStream,
    decoder: FrameDecoder,
    endpoint: Option<Endpoint>,
    keepalive: bool,
) -> LinkParts {
    let (event_tx, events) = mpsc::channel(QUEUED_FRAMES);
    let (out, out_rx) = mpsc::channel(QUEUED_FRAMES);
    tokio::spawn(read_loop(connection.clone(), recv, decoder, event_tx));
    tokio::spawn(write_loop(connection.clone(), send, out_rx, endpoint, keepalive));
    LinkParts {
        connection,
        events,
        out,
    }
}

/// The next frame; `Ok(None)` once the stream or the connection ended.
async fn read_frame(recv: &mut RecvStream, decoder: &mut FrameDecoder) -> Result<Option<Vec<u8>>, ChannelError> {
    loop {
        if let Some(frame) = decoder.next_frame()? {
            return Ok(Some(frame));
        }
        match recv.read_chunk(READ_CHUNK, true).await {
            Ok(Some(chunk)) => decoder.push(&chunk.bytes),
            Ok(None) | Err(_) => return Ok(None),
        }
    }
}

async fn read_loop(
    connection: quinn::Connection,
    mut recv: RecvStream,
    mut decoder: FrameDecoder,
    events: mpsc::Sender<LinkEvent>,
) {
    let reason = loop {
        match read_frame(&mut recv, &mut decoder).await {
            Ok(Some(frame)) => {
                if events.send(LinkEvent::Frame(frame)).await.is_err() {
                    break None;
                }
            }
            Ok(None) => break None,
            Err(error) => break Some(error.to_string()),
        }
    };
    match &reason {
        Some(reason) => connection.close(VarInt::from_u32(CLOSE_PROTOCOL), reason.as_bytes()),
        None => connection.close(VarInt::from_u32(CLOSE_OK), b""),
    }
    let _ = events.send(LinkEvent::Closed(reason)).await;
}

async fn write_loop(
    connection: quinn::Connection,
    mut send: SendStream,
    mut out: mpsc::Receiver<Vec<u8>>,
    endpoint: Option<Endpoint>,
    keepalive: bool,
) {
    let mut batch = Vec::new();
    let mut bytes = Vec::new();
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + KEEPALIVE_INTERVAL, KEEPALIVE_INTERVAL);
    let closed_here = loop {
        tokio::select! {
            count = out.recv_many(&mut batch, 256) => {
                if count == 0 {
                    break true;
                }
                // Frames queued meanwhile go out in one write
                bytes.clear();
                for frame in batch.drain(..) {
                    bytes.extend_from_slice(&frame);
                }
                if send.write_all(&bytes).await.is_err() {
                    break false;
                }
            }
            _ = ticker.tick(), if keepalive => {
                if send.write_all(&[0]).await.is_err() {
                    break false;
                }
            }
            _ = connection.closed() => break false,
        }
    };
    if closed_here && send.finish().is_ok() {
        // Let what was written arrive: the peer closes the connection once it reads the end of the stream
        let _ = tokio::time::timeout(CLOSE_GRACE, send.stopped()).await;
    }
    connection.close(VarInt::from_u32(CLOSE_OK), b"");
    if let Some(endpoint) = endpoint {
        let _ = tokio::time::timeout(Duration::from_secs(3), endpoint.wait_idle()).await;
    }
}

// ---------------------------------------------------------------------------------------------------------
// The pump's side of a connection

/// Frames to and from a connection's tasks. Outgoing frames queue in `outbox` while the channel is full,
/// so a handler that sends while an earlier frame waits (the wait pumps) appends behind it.
struct Link {
    connection: quinn::Connection,
    events: mpsc::Receiver<LinkEvent>,
    out: Option<mpsc::Sender<Vec<u8>>>,
    outbox: VecDeque<Vec<u8>>,
    flushing: bool,
}

impl Link {
    fn new(parts: LinkParts) -> Self {
        Self {
            connection: parts.connection,
            events: parts.events,
            out: Some(parts.out),
            outbox: VecDeque::new(),
            flushing: false,
        }
    }

    /// Queues one frame and blocks (pumping) while the connection applies back-pressure. A raw pointer:
    /// the wait pumps, which may re-enter this link.
    unsafe fn send(this: *mut Self, payload: &[u8]) -> Result<(), ChannelError> {
        unsafe {
            let mut frame = Vec::with_capacity(payload.len() + 4);
            encode_quic_frame(&mut frame, payload);
            (*this).outbox.push_back(frame);
            if (*this).flushing {
                return Ok(());
            }
            (*this).flushing = true;
            let result = Self::flush(this);
            (*this).flushing = false;
            result
        }
    }

    unsafe fn flush(this: *mut Self) -> Result<(), ChannelError> {
        unsafe {
            loop {
                let Some(frame) = (*this).outbox.pop_front() else {
                    return Ok(());
                };
                let Some(out) = (*this).out.as_ref() else {
                    (*this).outbox.clear();
                    return Err(ChannelError::new("not connected"));
                };
                match out.try_send(frame) {
                    Ok(()) => {}
                    Err(TrySendError::Full(frame)) => {
                        (*this).outbox.push_front(frame);
                        if !pump_all() {
                            sleep_brief();
                        }
                    }
                    Err(TrySendError::Closed(_)) => {
                        (*this).outbox.clear();
                        return Err(ChannelError::new("connection closed"));
                    }
                }
            }
        }
    }

    /// Moves queued frames on without blocking; true when any moved.
    fn try_flush(&mut self) -> bool {
        if self.flushing {
            return false;
        }
        let Some(out) = self.out.as_ref() else {
            return false;
        };
        let mut progressed = false;
        while let Some(frame) = self.outbox.pop_front() {
            match out.try_send(frame) {
                Ok(()) => progressed = true,
                Err(TrySendError::Full(frame)) => {
                    self.outbox.push_front(frame);
                    break;
                }
                Err(TrySendError::Closed(_)) => {
                    self.outbox.clear();
                    break;
                }
            }
        }
        progressed
    }

    fn next_event(&mut self) -> Option<LinkEvent> {
        match self.events.try_recv() {
            Ok(event) => Some(event),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some(LinkEvent::Closed(None)),
        }
    }

    /// Closes after the frames already handed to the writer (it finishes the stream). If the writer is
    /// stuck on flow control, the connection closes anyway a moment later.
    fn close(&mut self) {
        self.outbox.clear();
        if self.out.take().is_some() {
            let connection = self.connection.clone();
            runtime().spawn(async move {
                tokio::time::sleep(CLOSE_GRACE * 2).await;
                connection.close(VarInt::from_u32(CLOSE_OK), b"");
            });
        }
    }

    /// Closes at once, for a peer that broke the wire rules.
    fn abort(&mut self, reason: &str) {
        self.outbox.clear();
        self.out = None;
        self.connection.close(VarInt::from_u32(CLOSE_PROTOCOL), reason.as_bytes());
    }
}

// ---------------------------------------------------------------------------------------------------------
// Client

pub(super) struct QuicClientTransport<State> {
    link: Link,
    connected: bool,
    closed: bool,
    pump_token: Option<usize>,
    peer: *mut Peer<State>,
}

/// Dials from a UDP socket of its own (Network.framework servers take one connection per client address
/// and port), opens the stream and sends the hello.
async fn dial(
    config: quinn::ClientConfig,
    remote: SocketAddr,
    server_name: String,
    hello: Vec<u8>,
    max_payload: usize,
) -> Result<LinkParts, ChannelError> {
    let bind: SocketAddr = if remote.is_ipv6() {
        (Ipv6Addr::UNSPECIFIED, 0).into()
    } else {
        (Ipv4Addr::UNSPECIFIED, 0).into()
    };
    let endpoint = Endpoint::client(bind)?;
    let connection = endpoint.connect_with(config, remote, &server_name).map_err(error)?.await.map_err(error)?;
    let (mut send, recv) = connection.open_bi().await.map_err(error)?;
    let mut frame = Vec::with_capacity(hello.len() + 4);
    encode_quic_frame(&mut frame, &hello);
    send.write_all(&frame).await.map_err(error)?;
    Ok(spawn_link(
        connection,
        send,
        recv,
        FrameDecoder::new(max_payload),
        Some(endpoint),
        true,
    ))
}

/// `Channel::connect` for `quic://host:port`. Blocks (pumping this thread's other transports) until the
/// handshake is done and the hello is sent, or `handshake_timeout` passes.
pub(super) fn connect<State: Default + 'static>(
    channel: &Channel<State>,
    target: &ConnectTarget,
    options: ConnectOptions<State>,
) -> Result<Client<State>, ChannelError> {
    let config = client_config(&options.quic)?;
    let server_name = options.quic.server_name.clone().unwrap_or_else(|| target.host.clone());
    let remote = (target.host.as_str(), target.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| ChannelError::new("invalid address"))?;
    let hello = hello_payload(options.headers.as_deref())?;
    let timeout = options.handshake_timeout;
    let max_payload = options.max_payload;
    let (ready_tx, ready_rx) = std_mpsc::channel();
    runtime().spawn(async move {
        let result = tokio::time::timeout(timeout, dial(config, remote, server_name, hello, max_payload))
            .await
            .unwrap_or_else(|_| Err(ChannelError::new("handshake timeout")));
        let _ = ready_tx.send(result);
    });
    let parts = loop {
        match ready_rx.try_recv() {
            Ok(result) => break result?,
            Err(std_mpsc::TryRecvError::Empty) => {
                if !pump_all() {
                    sleep_brief();
                }
            }
            Err(std_mpsc::TryRecvError::Disconnected) => return Err(ChannelError::new("quic connect failed")),
        }
    };

    let mut transport = Box::new(QuicClientTransport {
        link: Link::new(parts),
        connected: true,
        closed: false,
        pump_token: None,
        peer: std::ptr::null_mut(),
    });
    let state = options.state.unwrap_or_default();
    let mut peer = Box::new(Peer::new_transport(
        channel.runtime(),
        state,
        TransportMode::Quic,
        transport.as_mut() as *mut QuicClientTransport<State> as *mut (),
        TransportVTable {
            send_frame: client_send_frame::<State>,
            close: client_close::<State>,
            is_connected: client_is_connected::<State>,
        },
    ));
    peer.timeout = options.timeout;
    peer.attach_publishers();
    transport.peer = peer.as_mut() as *mut _;
    let token = register_pump_entry(
        transport.as_mut() as *mut QuicClientTransport<State> as *mut (),
        client_poll::<State>,
    );
    transport.pump_token = Some(token);
    Ok(Client {
        peer,
        transport: ClientLink::Quic(transport),
        stop_fn: peer_stop::<State>,
    })
}

impl<State: 'static> QuicClientTransport<State> {
    pub(super) fn poll(&mut self) -> Result<bool, ChannelError> {
        unsafe { Self::poll_raw(self) }
    }

    /// Through a raw pointer: dispatching runs handlers, which may pump (and so re-enter this transport).
    /// A broken wire rule or invalid JSON stops the peer and settles its pending calls with the reason.
    unsafe fn poll_raw(this: *mut Self) -> Result<bool, ChannelError> {
        unsafe {
            if !(*this).connected {
                return Ok(false);
            }
            match Self::poll_inner(this) {
                Ok(progressed) => Ok(progressed),
                Err(error) => {
                    // Unless a handler already stopped the client (then it closes as asked)
                    if (*this).connected {
                        (*this).link.abort(&error.to_string());
                    }
                    (*this).connected = false;
                    if let Some(peer) = (*this).peer.as_mut() {
                        peer.fail(&error.to_string());
                    }
                    Err(error)
                }
            }
        }
    }

    unsafe fn poll_inner(this: *mut Self) -> Result<bool, ChannelError> {
        unsafe {
            let mut progressed = (*this).link.try_flush();
            loop {
                if !(*this).connected {
                    return Ok(true);
                }
                let Some(event) = (*this).link.next_event() else {
                    return Ok(progressed);
                };
                progressed = true;
                match event {
                    LinkEvent::Frame(payload) => {
                        if let Some(peer) = (*this).peer.as_mut() {
                            peer.handle_incoming_frame(&payload)?;
                        }
                    }
                    LinkEvent::Closed(Some(reason)) => return Err(ChannelError::new(reason)),
                    LinkEvent::Closed(None) => {
                        (*this).connected = false;
                        if let Some(peer) = (*this).peer.as_mut() {
                            peer.stop();
                        }
                        return Ok(true);
                    }
                }
            }
        }
    }
}

unsafe fn client_send_frame<State: 'static>(raw: *mut (), payload: &[u8]) -> Result<(), ChannelError> {
    let transport = raw.cast::<QuicClientTransport<State>>();
    unsafe {
        if transport.is_null() {
            return Err(ChannelError::new("client transport missing"));
        }
        if !(*transport).connected {
            return Err(ChannelError::new("not connected"));
        }
        Link::send(&mut (*transport).link, payload)
    }
}

unsafe fn client_close<State: 'static>(raw: *mut ()) {
    if let Some(transport) = unsafe { raw.cast::<QuicClientTransport<State>>().as_mut() } {
        if let Some(token) = transport.pump_token.take() {
            unregister_pump_entry(token);
        }
        if transport.closed {
            return;
        }
        transport.closed = true;
        transport.connected = false;
        transport.link.close();
    }
}

unsafe fn client_is_connected<State: 'static>(raw: *mut ()) -> bool {
    unsafe { raw.cast::<QuicClientTransport<State>>().as_ref() }
        .map(|transport| transport.connected)
        .unwrap_or(false)
}

unsafe fn client_poll<State: 'static>(raw: *mut ()) -> Result<bool, ChannelError> {
    let transport = raw.cast::<QuicClientTransport<State>>();
    if transport.is_null() {
        return Err(ChannelError::new("client transport missing"));
    }
    unsafe { QuicClientTransport::poll_raw(transport) }
}

// ---------------------------------------------------------------------------------------------------------
// Server

/// A connection whose hello arrived, handed from the runtime to the pump.
struct Accepted {
    headers: HashMap<String, String>,
    link: LinkParts,
}

async fn accept_loop(endpoint: Endpoint, accepted: mpsc::UnboundedSender<Accepted>, max_payload: usize) {
    while let Some(incoming) = endpoint.accept().await {
        if accepted.is_closed() {
            incoming.refuse();
            break;
        }
        tokio::spawn(serve(incoming, accepted.clone(), max_payload));
    }
}

/// Handshake, the client's stream and its hello. A first frame that isn't a hello closes the connection.
async fn serve(incoming: quinn::Incoming, accepted: mpsc::UnboundedSender<Accepted>, max_payload: usize) {
    let Ok(connection) = incoming.await else {
        return;
    };
    let Ok((send, mut recv)) = connection.accept_bi().await else {
        return;
    };
    let mut decoder = FrameDecoder::new(max_payload);
    let headers = match read_frame(&mut recv, &mut decoder).await {
        Ok(Some(first)) => parse_hello(&first),
        Ok(None) => return,
        Err(error) => {
            connection.close(VarInt::from_u32(CLOSE_PROTOCOL), error.to_string().as_bytes());
            return;
        }
    };
    let Some(headers) = headers else {
        connection.close(VarInt::from_u32(CLOSE_PROTOCOL), b"the first frame isn't a hello");
        return;
    };
    let link = spawn_link(connection.clone(), send, recv, decoder, None, false);
    if accepted.send(Accepted { headers, link }).is_err() {
        connection.close(VarInt::from_u32(CLOSE_OK), b"server stopped");
    }
}

/// A running QUIC server ([`Channel::listen_quic`]). It is driven by the thread's pump, like [`super::Server`].
pub struct QuicServer<State> {
    transport: Box<QuicServerTransport<State>>,
    stop_fn: unsafe fn(*mut ()),
}

impl<State: 'static> QuicServer<State> {
    pub fn poll(&mut self) -> Result<bool, ChannelError>
    where
        State: Default,
    {
        self.transport.poll()
    }

    /// Closes every connection and the endpoint.
    pub fn stop(&mut self)
    where
        State: Default,
    {
        self.transport.stop();
    }

    /// The UDP port, useful after listening on port 0.
    pub fn port(&self) -> u16 {
        self.transport.port
    }
}

impl<State> Drop for QuicServer<State> {
    fn drop(&mut self) {
        unsafe { (self.stop_fn)(self.transport.as_mut() as *mut QuicServerTransport<State> as *mut ()) };
    }
}

unsafe fn server_stop<State: Default + 'static>(raw: *mut ()) {
    if let Some(transport) = unsafe { raw.cast::<QuicServerTransport<State>>().as_mut() } {
        transport.stop();
    }
}

impl<State: 'static> Channel<State> {
    /// Serves this channel over QUIC on UDP (experimental, the `quic` feature). The options are the
    /// WebSocket server's plus the certificate; the state callback gets the hello's headers.
    pub fn listen_quic<T>(&self, target: T, options: QuicListenOptions<State>) -> Result<QuicServer<State>, ChannelError>
    where
        T: IntoListenTarget,
        State: Default,
    {
        let target = target.into_listen_target()?;
        let config = server_config(&options.cert, &options.key)?;
        let address = (target.host.as_str(), target.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| ChannelError::new("invalid address"))?;
        let runtime = runtime();
        let endpoint = {
            let _context = runtime.enter();
            Endpoint::server(config, address)?
        };
        let port = endpoint.local_addr()?.port();
        let (accepted_tx, accepted) = mpsc::unbounded_channel();
        runtime.spawn(accept_loop(endpoint.clone(), accepted_tx, options.options.max_payload));
        let mut transport = Box::new(QuicServerTransport {
            channel: self.runtime(),
            options: options.options,
            endpoint,
            accepted,
            port,
            stopped: false,
            poll_depth: 0,
            pump_token: None,
            connections: Vec::new(),
            _marker: PhantomData,
        });
        let token = register_pump_entry(
            transport.as_mut() as *mut QuicServerTransport<State> as *mut (),
            server_poll::<State>,
        );
        transport.pump_token = Some(token);
        Ok(QuicServer {
            transport,
            stop_fn: server_stop::<State>,
        })
    }
}

struct QuicServerConnection<State> {
    link: Link,
    peer: Option<Box<Peer<State>>>,
    closed: bool,
    removed: bool,
}

impl<State: 'static> QuicServerConnection<State> {
    unsafe fn poll(this: *mut Self) -> Result<bool, ChannelError> {
        unsafe {
            if (*this).closed {
                return Ok(false);
            }
            let mut progressed = (*this).link.try_flush();
            loop {
                if (*this).closed {
                    return Ok(true);
                }
                let Some(event) = (*this).link.next_event() else {
                    return Ok(progressed);
                };
                progressed = true;
                match event {
                    LinkEvent::Frame(payload) => {
                        let peer = (*this).peer.as_mut().map(|peer| peer.as_mut() as *mut Peer<State>);
                        if let Some(peer) = peer {
                            // Invalid JSON closes the connection, as in TS
                            (*peer).handle_incoming_frame(&payload)?;
                        }
                    }
                    LinkEvent::Closed(_) => {
                        (*this).closed = true;
                        (*this).removed = true;
                        return Ok(true);
                    }
                }
            }
        }
    }

    fn destroy(&mut self, options: &ListenOptions<State>) {
        if self.removed {
            if let Some(peer) = self.peer.as_mut() {
                peer.stop();
                if let Some(callback) = &options.on_disconnect {
                    let sender = Sender::new(peer.as_mut() as *mut _);
                    callback(&mut peer.state, sender);
                }
            }
            self.peer = None;
        }
        self.link.close();
    }
}

struct QuicServerTransport<State> {
    channel: RuntimeChannel<State>,
    options: ListenOptions<State>,
    endpoint: Endpoint,
    accepted: mpsc::UnboundedReceiver<Accepted>,
    port: u16,
    stopped: bool,
    /// How many `poll` calls for this server are on the stack; connections are only freed at zero, as an
    /// outer poll may be running a handler of the connection being closed.
    poll_depth: usize,
    pump_token: Option<usize>,
    connections: Vec<Box<QuicServerConnection<State>>>,
    _marker: PhantomData<State>,
}

impl<State> QuicServerTransport<State>
where
    State: Default + 'static,
{
    fn poll(&mut self) -> Result<bool, ChannelError> {
        unsafe { Self::poll_raw(self) }
    }

    unsafe fn poll_raw(this: *mut Self) -> Result<bool, ChannelError> {
        unsafe {
            if (*this).stopped {
                return Ok(false);
            }
            (*this).poll_depth += 1;
            let progressed = Self::poll_connections(this);
            (*this).poll_depth -= 1;
            if (*this).poll_depth == 0 {
                Self::reap(this);
            }
            Ok(progressed)
        }
    }

    unsafe fn poll_connections(this: *mut Self) -> bool {
        unsafe {
            let mut progressed = false;
            while !(*this).stopped {
                let Ok(accepted) = (*this).accepted.try_recv() else {
                    break;
                };
                progressed = true;
                Self::open(this, accepted);
            }

            let mut index = 0_usize;
            while index < (*this).connections.len() && !(*this).stopped {
                let connection = (&mut (*this).connections)[index].as_mut() as *mut QuicServerConnection<State>;
                match QuicServerConnection::poll(connection) {
                    Ok(value) => progressed = value || progressed,
                    Err(error) => {
                        // Invalid JSON: drop just this connection (unless a handler already closed it)
                        if !(*connection).closed {
                            (*connection).link.abort(&error.to_string());
                        }
                        (*connection).closed = true;
                        (*connection).removed = true;
                        progressed = true;
                    }
                }
                index += 1;
            }
            progressed
        }
    }

    /// Builds the connection's state from its hello headers (an error closes it) and runs `on_connect`.
    unsafe fn open(this: *mut Self, accepted: Accepted) {
        unsafe {
            let mut link = Link::new(accepted.link);
            let state = match &(*this).options.state {
                Some(factory) => match factory(&accepted.headers) {
                    Ok(state) => state,
                    Err(error) => {
                        link.abort(&error.to_string());
                        return;
                    }
                },
                None => State::default(),
            };
            let mut connection = Box::new(QuicServerConnection {
                link,
                peer: None,
                closed: false,
                removed: false,
            });
            let mut peer = Box::new(Peer::new_transport(
                (*this).channel.clone(),
                state,
                TransportMode::Quic,
                connection.as_mut() as *mut QuicServerConnection<State> as *mut (),
                TransportVTable {
                    send_frame: server_connection_send_frame::<State>,
                    close: server_connection_close::<State>,
                    is_connected: server_connection_is_connected::<State>,
                },
            ));
            peer.timeout = (*this).options.timeout;
            peer.attach_publishers();
            let peer_ptr = peer.as_mut() as *mut Peer<State>;
            connection.peer = Some(peer);
            (*this).connections.push(connection);
            if let Some(callback) = (*this).options.on_connect.clone() {
                if let Some(peer) = peer_ptr.as_mut() {
                    callback(&mut peer.state, Sender::new(peer_ptr));
                }
            }
        }
    }

    /// Frees closed connections, moved out of the list first so the callbacks may poll this server again.
    unsafe fn reap(this: *mut Self) {
        unsafe {
            let stopped = (*this).stopped;
            let connections = &mut (*this).connections;
            let mut dead = Vec::new();
            let mut index = 0;
            while index < connections.len() {
                if stopped || connections[index].closed || connections[index].removed {
                    dead.push(connections.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            for mut connection in dead {
                connection.removed = true;
                connection.destroy(&(*this).options);
            }
        }
    }

    fn stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        if let Some(token) = self.pump_token.take() {
            unregister_pump_entry(token);
        }
        self.accepted.close();
        self.endpoint.close(VarInt::from_u32(CLOSE_OK), b"server stopped");
        if self.poll_depth == 0 {
            unsafe { Self::reap(self) };
        } else {
            for connection in &mut self.connections {
                connection.closed = true;
                connection.removed = true;
            }
        }
    }
}

unsafe fn server_poll<State: Default + 'static>(raw: *mut ()) -> Result<bool, ChannelError> {
    let transport = raw.cast::<QuicServerTransport<State>>();
    if transport.is_null() {
        return Err(ChannelError::new("server transport missing"));
    }
    unsafe { QuicServerTransport::poll_raw(transport) }
}

unsafe fn server_connection_send_frame<State: 'static>(raw: *mut (), payload: &[u8]) -> Result<(), ChannelError> {
    let connection = raw.cast::<QuicServerConnection<State>>();
    unsafe {
        if connection.is_null() {
            return Err(ChannelError::new("server connection missing"));
        }
        if (*connection).closed {
            return Err(ChannelError::new("not connected"));
        }
        Link::send(&mut (*connection).link, payload)
    }
}

unsafe fn server_connection_close<State: 'static>(raw: *mut ()) {
    if let Some(connection) = unsafe { raw.cast::<QuicServerConnection<State>>().as_mut() } {
        connection.closed = true;
        connection.removed = true;
        connection.link.close();
    }
}

unsafe fn server_connection_is_connected<State: 'static>(raw: *mut ()) -> bool {
    unsafe { raw.cast::<QuicServerConnection<State>>().as_ref() }
        .map(|connection| !connection.closed)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests;
