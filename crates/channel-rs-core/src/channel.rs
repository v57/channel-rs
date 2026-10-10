use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap, HashSet},
    io::{ErrorKind, Read, Write},
    marker::PhantomData,
    net::{TcpListener, TcpStream},
    rc::Rc,
    thread,
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde::{Serialize, de::DeserializeOwned};
use sha1::{Digest, Sha1};
use url::Url;

use crate::{
    CallResult, ChannelError, PostRequest, Sender, SendingRequest, StreamEmitter, StreamRequest,
    SubscribeResult, SubscriptionHandle, Values, json_codec,
    subscription::{ErasedSubscriptionBinding, IntoErasedSubscription},
    wire::{OutgoingMessage, ParsedMessage, WireId, encode_message, parse_messages},
};

const SLEEP_MICROS: u64 = 100;
const OPCODE_CONTINUATION: u8 = 0x0;
const OPCODE_TEXT: u8 = 0x1;
const OPCODE_BINARY: u8 = 0x2;
const OPCODE_CLOSE: u8 = 0x8;
const OPCODE_PING: u8 = 0x9;
const OPCODE_PONG: u8 = 0xA;

/// Default cap on one WebSocket message (a frame, or a fragmented message
/// after reassembly). Matches Bun's default `maxPayloadLength` used by the
/// TS reference server.
pub const DEFAULT_MAX_PAYLOAD: usize = 16 * 1024 * 1024;
/// Cap on the HTTP upgrade request/response head.
const MAX_HANDSHAKE_BYTES: usize = 64 * 1024;
/// Default time `connect` waits for the HTTP upgrade response.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// How deep blocking waits may nest inside handlers on one thread (a handler
/// that blocks on a call to a peer whose handler blocks on a call...). Past
/// this, a wait stops pumping and only sleeps, so it then needs another
/// thread (or a timeout) to make progress instead of overflowing the stack.
const MAX_PUMP_DEPTH: usize = 32;

thread_local! {
    static PUMP_ENTRIES: RefCell<Vec<Option<PumpEntry>>> = const { RefCell::new(Vec::new()) };
    static PUMP_DEPTH: Cell<usize> = const { Cell::new(0) };
    static RANDOM_POOL: RefCell<([u8; 256], usize)> = const { RefCell::new(([0; 256], 256)) };
}

struct PumpEntry {
    ptr: *mut (),
    poll_fn: unsafe fn(*mut ()) -> Result<bool, ChannelError>,
}

fn register_pump_entry(
    ptr: *mut (),
    poll_fn: unsafe fn(*mut ()) -> Result<bool, ChannelError>,
) -> usize {
    PUMP_ENTRIES.with(|entries| {
        let mut entries = entries.borrow_mut();
        for (index, entry) in entries.iter_mut().enumerate() {
            if entry.is_none() {
                *entry = Some(PumpEntry { ptr, poll_fn });
                return index;
            }
        }
        entries.push(Some(PumpEntry { ptr, poll_fn }));
        entries.len() - 1
    })
}

fn unregister_pump_entry(token: usize) {
    PUMP_ENTRIES.with(|entries| {
        if let Some(entry) = entries.borrow_mut().get_mut(token) {
            *entry = None;
        }
    });
}

/// Resets the pump depth even when a poll function unwinds.
struct PumpDepthGuard;

impl PumpDepthGuard {
    fn enter() -> Option<Self> {
        PUMP_DEPTH.with(|depth| {
            if depth.get() >= MAX_PUMP_DEPTH {
                return None;
            }
            depth.set(depth.get() + 1);
            Some(PumpDepthGuard)
        })
    }
}

impl Drop for PumpDepthGuard {
    fn drop(&mut self) {
        PUMP_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Polls every transport registered on this thread once. Blocking waits
/// (`send`, `subscribe`, stream `next`, a full socket buffer) call this, and
/// it is re-entrant: a handler that runs inside a pump and then blocks on its
/// own call pumps again, so the reply can still be read (the `mirror`
/// pattern). Each transport contains its own errors (it fails its pending
/// calls and stops), so one bad connection never aborts the pump for the
/// others.
fn pump_all() -> bool {
    let Some(_guard) = PumpDepthGuard::enter() else {
        return false;
    };
    // Entries are re-read by index on every step instead of snapshotted: a
    // handler may stop (unregister) a transport while this loop runs.
    let count = PUMP_ENTRIES.with(|entries| entries.borrow().len());
    let mut progressed = false;
    for index in 0..count {
        let entry = PUMP_ENTRIES.with(|entries| {
            entries
                .borrow()
                .get(index)
                .and_then(|entry| entry.as_ref().map(|entry| (entry.ptr, entry.poll_fn)))
        });
        let Some((ptr, poll_fn)) = entry else {
            continue;
        };
        match unsafe { poll_fn(ptr) } {
            Ok(value) => progressed = value || progressed,
            Err(_) => progressed = true,
        }
    }
    progressed
}

/// The opportunistic pump after a send. It only runs at the top level: inside
/// a pump it would recurse once per buffered message (a 2048-request batch
/// would mean 2048 nested polls).
fn pump_after_send() {
    if PUMP_DEPTH.with(Cell::get) == 0 {
        pump_all();
    }
}

fn random_bytes<const N: usize>() -> [u8; N] {
    RANDOM_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        let (buffer, used) = &mut *pool;
        if *used + N > buffer.len() {
            if getrandom::fill(buffer).is_err() {
                // No OS randomness: fall back to the std hasher's random keys.
                use std::hash::{BuildHasher, Hasher};
                for chunk in buffer.chunks_mut(8) {
                    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
                    hasher.write_usize(chunk.as_ptr() as usize);
                    let value = hasher.finish().to_ne_bytes();
                    chunk.copy_from_slice(&value[..chunk.len()]);
                }
            }
            *used = 0;
        }
        let mut out = [0_u8; N];
        out.copy_from_slice(&buffer[*used..*used + N]);
        *used += N;
        out
    })
}

fn sleep_brief() {
    thread::sleep(Duration::from_micros(SLEEP_MICROS));
}

/// An API endpoint registration, built by [`post_spec`] / [`stream_spec`].
#[derive(Clone)]
pub struct ApiLeaf<State> {
    register_fn: Rc<dyn Fn(&mut Channel<State>, &str) -> Result<(), ChannelError>>,
}

pub enum ApiNode<State> {
    Post(ApiLeaf<State>),
    Stream(ApiLeaf<State>),
    Group(BTreeMap<String, ApiNode<State>>),
}

pub enum EventNode {
    Subscription(ErasedSubscriptionBinding),
    Group(BTreeMap<String, EventNode>),
}

pub fn post_spec<State: 'static, Req, Resp, Ctx, F>(handler: F) -> ApiNode<State>
where
    Req: DeserializeOwned + Serialize + 'static,
    Resp: Serialize + 'static,
    Ctx: DeserializeOwned + Serialize + 'static,
    F: Fn(PostRequest<'_, State, Req, Ctx>) -> Result<Option<Resp>, ChannelError> + 'static,
{
    let handler = Rc::new(handler);
    ApiNode::Post(ApiLeaf {
        register_fn: Rc::new(move |channel, path| {
            let handler = handler.clone();
            channel.post(path.to_string(), move |request| handler(request))?;
            Ok(())
        }),
    })
}

pub fn stream_spec<State: 'static, Req, Item, Ctx, F>(handler: F) -> ApiNode<State>
where
    Req: DeserializeOwned + Serialize + 'static,
    Item: Serialize + 'static,
    Ctx: DeserializeOwned + Serialize + 'static,
    F: Fn(StreamRequest<'_, State, Req, Ctx, Item>) -> Result<(), ChannelError> + 'static,
{
    let handler = Rc::new(handler);
    ApiNode::Stream(ApiLeaf {
        register_fn: Rc::new(move |channel, path| {
            let handler = handler.clone();
            channel.stream(path.to_string(), move |request| handler(request))?;
            Ok(())
        }),
    })
}

type PathMatcher = Rc<dyn Fn(&str) -> bool>;
type DisconnectCallback<State> = Rc<dyn Fn(&mut State, Sender<State>)>;

type PostInvoke<State> =
    dyn Fn(&mut Peer<State>, &str, Option<&str>, Option<&str>, Option<&WireId>) -> Result<(), ChannelError>;
type StreamInvoke<State> =
    dyn Fn(&mut Peer<State>, &str, Option<&str>, Option<&str>, &WireId) -> Result<(), ChannelError>;

struct ErasedPostHandler<State> {
    invoke: Rc<PostInvoke<State>>,
}

struct ErasedStreamHandler<State> {
    invoke: Rc<StreamInvoke<State>>,
}

struct RuntimeChannel<State> {
    posts: HashMap<String, ErasedPostHandler<State>>,
    streams: HashMap<String, ErasedStreamHandler<State>>,
    subscriptions: HashMap<String, ErasedSubscriptionBinding>,
    other_posts: Vec<(PathMatcher, ErasedPostHandler<State>)>,
    other_streams: Vec<(PathMatcher, ErasedStreamHandler<State>)>,
    on_disconnect: Option<DisconnectCallback<State>>,
}

impl<State> Clone for ErasedPostHandler<State> {
    fn clone(&self) -> Self {
        Self {
            invoke: self.invoke.clone(),
        }
    }
}

impl<State> Clone for ErasedStreamHandler<State> {
    fn clone(&self) -> Self {
        Self {
            invoke: self.invoke.clone(),
        }
    }
}

impl<State> Clone for RuntimeChannel<State> {
    fn clone(&self) -> Self {
        Self {
            posts: self.posts.clone(),
            streams: self.streams.clone(),
            subscriptions: self.subscriptions.clone(),
            other_posts: self.other_posts.clone(),
            other_streams: self.other_streams.clone(),
            on_disconnect: self.on_disconnect.clone(),
        }
    }
}

pub struct Channel<State> {
    posts: HashMap<String, ErasedPostHandler<State>>,
    streams: HashMap<String, ErasedStreamHandler<State>>,
    subscriptions: HashMap<String, ErasedSubscriptionBinding>,
    other_posts: Vec<(PathMatcher, ErasedPostHandler<State>)>,
    other_streams: Vec<(PathMatcher, ErasedStreamHandler<State>)>,
    on_disconnect: Option<DisconnectCallback<State>>,
}

impl<State: 'static> Default for Channel<State> {
    fn default() -> Self {
        Self::new()
    }
}

impl<State: 'static> Channel<State> {
    pub fn new() -> Self {
        Self {
            posts: HashMap::new(),
            streams: HashMap::new(),
            subscriptions: HashMap::new(),
            other_posts: Vec::new(),
            other_streams: Vec::new(),
            on_disconnect: None,
        }
    }

    pub fn post<Req, Resp, Ctx, F>(
        &mut self,
        path: impl Into<String>,
        handler: F,
    ) -> Result<&mut Self, ChannelError>
    where
        Req: DeserializeOwned + Serialize + 'static,
        Resp: Serialize + 'static,
        Ctx: DeserializeOwned + Serialize + 'static,
        F: Fn(PostRequest<'_, State, Req, Ctx>) -> Result<Option<Resp>, ChannelError> + 'static,
    {
        let handler = Rc::new(handler);
        let invoke_handler = handler.clone();
        self.posts.insert(
            path.into(),
            ErasedPostHandler {
                invoke: Rc::new(move |peer, path, body_json, context_json, id| {
                    // A body or context of the wrong type is the caller's
                    // error, answered like a throwing handler in TS; it must
                    // not take the connection down.
                    let decoded = json_codec::decode_field::<Req>(body_json, "body").and_then(|body| {
                        json_codec::decode_context::<Ctx>(context_json).map(|context| (body, context))
                    });
                    let result = match decoded {
                        Ok((body, context)) => {
                            let sender = Sender::new(peer as *mut _);
                            let request = PostRequest {
                                body,
                                context,
                                sender,
                                state: &mut peer.state,
                                path: path.to_string(),
                            };
                            invoke_handler(request)
                                .and_then(|response| json_codec::encode_response(response.as_ref()))
                        }
                        Err(error) => Err(error),
                    };
                    let Some(id) = id else {
                        return Ok(());
                    };
                    match result {
                        Ok(body_json) => peer.send_message(OutgoingMessage {
                            body_json: body_json.as_deref(),
                            ..OutgoingMessage::reply(id)
                        }),
                        Err(error) => peer.send_message(OutgoingMessage {
                            error_text: Some(&error.to_string()),
                            ..OutgoingMessage::reply(id)
                        }),
                    }
                }),
            },
        );
        Ok(self)
    }

    pub fn post_value<Req, Resp, Ctx, F>(
        &mut self,
        path: impl Into<String>,
        handler: F,
    ) -> Result<&mut Self, ChannelError>
    where
        Req: DeserializeOwned + Serialize + 'static,
        Resp: Serialize + 'static,
        Ctx: DeserializeOwned + Serialize + 'static,
        F: Fn(PostRequest<'_, State, Req, Ctx>) -> Result<Resp, ChannelError> + 'static,
    {
        self.post(path, move |request| handler(request).map(Some))
    }

    pub fn post_other<Req, Resp, Ctx, P, F>(
        &mut self,
        matcher: P,
        handler: F,
    ) -> Result<&mut Self, ChannelError>
    where
        Req: DeserializeOwned + Serialize + 'static,
        Resp: Serialize + 'static,
        Ctx: DeserializeOwned + Serialize + 'static,
        P: Fn(&str) -> bool + 'static,
        F: Fn(PostRequest<'_, State, Req, Ctx>) -> Result<Option<Resp>, ChannelError> + 'static,
    {
        let mut nested = Channel::new();
        nested.post("_", handler)?;
        let (_, handler) = nested.posts.into_iter().next().expect("post handler");
        self.other_posts.push((Rc::new(matcher), handler));
        Ok(self)
    }

    pub fn stream<Req, Item, Ctx, F>(
        &mut self,
        path: impl Into<String>,
        handler: F,
    ) -> Result<&mut Self, ChannelError>
    where
        Req: DeserializeOwned + Serialize + 'static,
        Item: Serialize + 'static,
        Ctx: DeserializeOwned + Serialize + 'static,
        F: Fn(StreamRequest<'_, State, Req, Ctx, Item>) -> Result<(), ChannelError> + 'static,
    {
        let handler = Rc::new(handler);
        self.streams.insert(
            path.into(),
            ErasedStreamHandler {
                invoke: Rc::new(move |peer, path, body_json, context_json, id| {
                    let decoded = json_codec::decode_field::<Req>(body_json, "body").and_then(|body| {
                        json_codec::decode_context::<Ctx>(context_json).map(|context| (body, context))
                    });
                    let (body, context) = match decoded {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            return peer.send_message(OutgoingMessage {
                                error_text: Some(&error.to_string()),
                                ..OutgoingMessage::reply(id)
                            });
                        }
                    };
                    let owner = peer as *mut _;
                    // Boxed: the task's address is handed to the emitter and
                    // must survive the map growing while the handler runs.
                    let task = peer.incoming_streams.entry(id.clone()).or_insert_with(|| {
                        Box::new(IncomingStreamTask {
                            owner,
                            id: id.clone(),
                            cancelled: false,
                        })
                    });
                    let emitter = StreamEmitter::new(
                        task.as_mut() as *mut IncomingStreamTask<State> as *mut (),
                        emit_stream_item::<State, Item>,
                        stream_item_is_cancelled::<State>,
                    );
                    let sender = Sender::new(peer as *mut _);
                    let request = StreamRequest {
                        body,
                        context,
                        sender,
                        state: &mut peer.state,
                        path: path.to_string(),
                        emitter,
                    };

                    let result = handler(request);
                    let cancelled = peer
                        .incoming_streams
                        .get(id)
                        .map(|task| task.cancelled)
                        .unwrap_or(false);
                    peer.incoming_streams.remove(id);
                    if cancelled {
                        return Ok(());
                    }
                    match result {
                        Ok(()) => peer.send_message(OutgoingMessage {
                            done: Some(true),
                            ..OutgoingMessage::reply(id)
                        }),
                        Err(error) => peer.send_message(OutgoingMessage {
                            error_text: Some(&error.to_string()),
                            ..OutgoingMessage::reply(id)
                        }),
                    }
                }),
            },
        );
        Ok(self)
    }

    pub fn stream_other<Req, Item, Ctx, P, F>(
        &mut self,
        matcher: P,
        handler: F,
    ) -> Result<&mut Self, ChannelError>
    where
        Req: DeserializeOwned + Serialize + 'static,
        Item: Serialize + 'static,
        Ctx: DeserializeOwned + Serialize + 'static,
        P: Fn(&str) -> bool + 'static,
        F: Fn(StreamRequest<'_, State, Req, Ctx, Item>) -> Result<(), ChannelError> + 'static,
    {
        let mut nested = Channel::new();
        nested.stream("_", handler)?;
        let (_, handler) = nested.streams.into_iter().next().expect("stream handler");
        self.other_streams.push((Rc::new(matcher), handler));
        Ok(self)
    }

    pub fn api(&mut self, api: BTreeMap<String, ApiNode<State>>) -> Result<&mut Self, ChannelError> {
        self.api_walk(api, String::new())?;
        Ok(self)
    }

    pub fn events<I, K, E>(&mut self, events: I, prefix: &str) -> Result<&mut Self, ChannelError>
    where
        I: IntoIterator<Item = (K, E)>,
        K: Into<String>,
        E: IntoErasedSubscription,
    {
        let normalized_prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        for (key, value) in events {
            let key = key.into();
            let path = format!("{normalized_prefix}{key}");
            let binding = value.into_erased_subscription();
            binding.set_prefix(path.clone());
            self.subscriptions.insert(path, binding);
        }
        Ok(self)
    }

    pub fn events_tree(
        &mut self,
        events: BTreeMap<String, EventNode>,
        prefix: &str,
    ) -> Result<&mut Self, ChannelError> {
        self.events_walk(events, prefix.to_string())?;
        Ok(self)
    }

    pub fn merge(&mut self, other: &Channel<State>, prefix: &str) -> Result<&mut Self, ChannelError> {
        let normalized_prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };

        for (path, handler) in &other.posts {
            self.posts
                .insert(format!("{normalized_prefix}{path}"), handler.clone());
        }
        for (path, handler) in &other.streams {
            self.streams
                .insert(format!("{normalized_prefix}{path}"), handler.clone());
        }
        for (path, binding) in &other.subscriptions {
            let merged = format!("{normalized_prefix}{path}");
            let binding = binding.clone();
            binding.set_prefix(merged.clone());
            self.subscriptions.insert(merged, binding);
        }
        Ok(self)
    }

    pub fn on_disconnect<F>(&mut self, callback: F) -> &mut Self
    where
        F: Fn(&mut State, Sender<State>) + 'static,
    {
        self.on_disconnect = Some(Rc::new(callback));
        self
    }

    pub fn process(&self, state: State) -> Result<Process<State>, ChannelError> {
        let mut peer = Box::new(Peer::new_process(self.runtime(), state));
        peer.attach_publishers();
        Ok(Process {
            peer,
            stop_fn: peer_stop::<State>,
        })
    }

    pub fn connect<T>(&self, target: T, options: ConnectOptions<State>) -> Result<Client<State>, ChannelError>
    where
        T: IntoConnectTarget,
        State: Default,
    {
        let target = target.into_connect_target()?;
        if target.tls {
            return Err(ChannelError::new("tls is unsupported"));
        }
        let mut stream = connect_tcp(&target, options.handshake_timeout)?;
        stream.set_nodelay(true).ok();
        stream.set_nonblocking(true)?;
        let overread = perform_client_handshake(
            &mut stream,
            &target,
            options.headers.as_deref(),
            options.handshake_timeout,
        )?;

        let mut reader = FrameReader::new(options.max_payload);
        reader.buffer.extend_from_slice(&overread);
        let mut transport = Box::new(ClientTransport {
            stream,
            reader,
            writer: FrameWriter::default(),
            connected: true,
            closed: false,
            pump_token: None,
            peer: std::ptr::null_mut(),
        });

        let state = options.state.unwrap_or_default();
        let mut peer = Box::new(Peer::new_transport(
            self.runtime(),
            state,
            TransportMode::WebsocketClient,
            transport.as_mut() as *mut ClientTransport<State> as *mut (),
            TransportVTable {
                send_frame: client_send_frame::<State>,
                close: client_close::<State>,
                is_connected: client_is_connected::<State>,
            },
        ));
        peer.timeout = options.timeout;
        peer.attach_publishers();
        transport.peer = peer.as_mut() as *mut _;
        let token = register_pump_entry(transport.as_mut() as *mut ClientTransport<State> as *mut (), client_poll::<State>);
        transport.pump_token = Some(token);

        Ok(Client {
            peer,
            transport,
            stop_fn: peer_stop::<State>,
        })
    }

    pub fn listen<T>(&self, target: T, options: ListenOptions<State>) -> Result<Server<State>, ChannelError>
    where
        T: IntoListenTarget,
        State: Default,
    {
        let target = target.into_listen_target()?;
        let listener = TcpListener::bind((target.host.as_str(), target.port))?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let mut transport = Box::new(ServerTransport {
            channel: self.runtime(),
            options,
            listener,
            port_value: port,
            stopped: false,
            poll_depth: 0,
            pump_token: None,
            connections: Vec::new(),
            _marker: PhantomData,
        });
        let token = register_pump_entry(
            transport.as_mut() as *mut ServerTransport<State> as *mut (),
            server_poll::<State>,
        );
        transport.pump_token = Some(token);
        Ok(Server {
            transport,
            stop_fn: server_stop::<State>,
        })
    }

    fn runtime(&self) -> RuntimeChannel<State> {
        RuntimeChannel {
            posts: self.posts.clone(),
            streams: self.streams.clone(),
            subscriptions: self.subscriptions.clone(),
            other_posts: self.other_posts.clone(),
            other_streams: self.other_streams.clone(),
            on_disconnect: self.on_disconnect.clone(),
        }
    }

    fn api_walk(
        &mut self,
        api: BTreeMap<String, ApiNode<State>>,
        prefix: String,
    ) -> Result<(), ChannelError> {
        for (key, value) in api {
            let path = if key == "_" {
                prefix.clone()
            } else if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}/{key}")
            };
            match value {
                ApiNode::Post(node) | ApiNode::Stream(node) => {
                    (node.register_fn)(self, &path)?;
                }
                ApiNode::Group(group) => self.api_walk(group, path)?,
            }
        }
        Ok(())
    }

    fn events_walk(
        &mut self,
        events: BTreeMap<String, EventNode>,
        prefix: String,
    ) -> Result<(), ChannelError> {
        for (key, value) in events {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}/{key}")
            };
            match value {
                EventNode::Subscription(binding) => {
                    binding.set_prefix(path.clone());
                    self.subscriptions.insert(path, binding);
                }
                EventNode::Group(group) => self.events_walk(group, path)?,
            }
        }
        Ok(())
    }
}

pub struct ConnectOptions<State> {
    pub state: Option<State>,
    /// Extra raw header lines for the HTTP upgrade request
    /// (`"name: value\r\n..."`). Evaluated once per `connect`; this port has
    /// no auto-reconnect, so callers that reconnect rebuild it each time.
    pub headers: Option<String>,
    /// Largest accepted WebSocket message, after reassembling fragments.
    /// Bigger frames close the connection. Default [`DEFAULT_MAX_PAYLOAD`].
    pub max_payload: usize,
    /// Limit for the TCP connect plus the HTTP upgrade. Default
    /// [`DEFAULT_HANDSHAKE_TIMEOUT`].
    pub handshake_timeout: Duration,
    /// Limit for each blocking `send` / `subscribe` wait. `None` (the
    /// default, like TS) waits until the reply arrives or the connection
    /// drops; on expiry the call settles as `RemoteErr("timeout")`.
    pub timeout: Option<Duration>,
}

impl<State> Default for ConnectOptions<State> {
    fn default() -> Self {
        Self {
            state: None,
            headers: None,
            max_payload: DEFAULT_MAX_PAYLOAD,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            timeout: None,
        }
    }
}

type StateFactory<State> = Rc<dyn Fn(&HashMap<String, String>) -> Result<State, ChannelError>>;
type ConnectCallback<State> = Rc<dyn Fn(&mut State, Sender<State>)>;

pub struct ListenOptions<State> {
    /// Builds a connection's state from the upgrade request's query
    /// parameters and headers (header names lowercased; headers win over
    /// query parameters, as in TS). An error rejects the connection.
    pub state: Option<StateFactory<State>>,
    pub on_connect: Option<ConnectCallback<State>>,
    pub on_disconnect: Option<ConnectCallback<State>>,
    /// Largest accepted WebSocket message, after reassembling fragments.
    /// Bigger frames close that connection. Default [`DEFAULT_MAX_PAYLOAD`].
    pub max_payload: usize,
    /// Limit for blocking calls a handler makes back to its client. `None`
    /// (the default) waits until the reply arrives or the connection drops.
    pub timeout: Option<Duration>,
}

impl<State> Default for ListenOptions<State> {
    fn default() -> Self {
        Self {
            state: None,
            on_connect: None,
            on_disconnect: None,
            max_payload: DEFAULT_MAX_PAYLOAD,
            timeout: None,
        }
    }
}

enum PendingEntry<State> {
    Call(Box<PendingCall>),
    Stream(*mut PendingStream<State>),
    Subscribe(Box<PendingSubscribe>),
}

struct PendingCall {
    done: bool,
    body_json: Option<String>,
    error_text: Option<String>,
}

struct PendingSubscribe {
    done: bool,
    topic: Option<String>,
    error_text: Option<String>,
}

struct RawStreamEvent {
    body_json: Option<String>,
    error_text: Option<String>,
    done: bool,
}

pub(crate) enum RawStreamEventOwned {
    Body(String),
    RemoteErr(String),
    Done,
}

struct PendingStream<State> {
    peer: *mut Peer<State>,
    id: i64,
    closed: bool,
    finished: bool,
    queue: Vec<RawStreamEvent>,
}

impl<State: 'static> PendingStream<State> {
    /// Blocks for the next event. Takes a raw pointer, not `&mut self`: the
    /// pump fills `queue` through another pointer, and with a `&mut` receiver
    /// the optimizer may assume it can't change (release builds then spun
    /// forever on a stale `queue.is_empty()`).
    unsafe fn next_raw(this: *mut Self) -> Result<RawStreamEventOwned, ChannelError> {
        unsafe {
            while (*this).queue.is_empty() && !(*this).finished {
                if !pump_all() {
                    let peer = (*this).peer.as_ref().ok_or_else(|| ChannelError::new("peer missing"))?;
                    if peer.stopped {
                        break;
                    }
                    sleep_brief();
                }
            }
        }

        let stream = unsafe { &mut *this };
        if stream.queue.is_empty() {
            return Ok(RawStreamEventOwned::Done);
        }
        let event = stream.queue.remove(0);
        if let Some(error) = event.error_text {
            return Ok(RawStreamEventOwned::RemoteErr(error));
        }
        if event.done {
            return Ok(RawStreamEventOwned::Done);
        }
        Ok(RawStreamEventOwned::Body(
            event.body_json.unwrap_or_else(|| "null".to_string()),
        ))
    }

    fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.finished = true;
        unsafe {
            if let Some(peer) = self.peer.as_mut() {
                peer.pending.remove(&self.id);
                let _ = peer.send_message(OutgoingMessage {
                    cancel: Some(self.id),
                    ..OutgoingMessage::default()
                });
            }
        }
    }
}

struct TopicCallbackEntry<State> {
    peer: *mut Peer<State>,
    topic: String,
    cancelled: bool,
    invoke: Box<dyn FnMut(Option<&str>)>,
}

struct IncomingStreamTask<State> {
    owner: *mut Peer<State>,
    id: WireId,
    cancelled: bool,
}

enum TransportMode {
    Process,
    WebsocketClient,
    WebsocketServer,
}

#[derive(Clone, Copy)]
struct TransportVTable {
    send_frame: unsafe fn(*mut (), &[u8]) -> Result<(), ChannelError>,
    close: unsafe fn(*mut ()),
    is_connected: unsafe fn(*mut ()) -> bool,
}

pub(crate) struct Peer<State> {
    channel: RuntimeChannel<State>,
    state: State,
    mode: TransportMode,
    transport_ptr: *mut (),
    transport_vtable: TransportVTable,
    pending: HashMap<i64, PendingEntry<State>>,
    next_id: i64,
    stream_entries: Vec<Box<PendingStream<State>>>,
    local_topics: HashMap<String, Vec<*mut TopicCallbackEntry<State>>>,
    callback_entries: Vec<Box<TopicCallbackEntry<State>>>,
    remote_topics: HashSet<String>,
    incoming_streams: HashMap<WireId, Box<IncomingStreamTask<State>>>,
    publishers: Vec<(ErasedSubscriptionBinding, usize)>,
    stopped: bool,
    /// Limit for blocking `send` / `subscribe` waits (`None` = no limit).
    timeout: Option<Duration>,
}

impl<State: 'static> Peer<State> {
    fn new_process(channel: RuntimeChannel<State>, state: State) -> Self {
        Self {
            channel,
            state,
            mode: TransportMode::Process,
            transport_ptr: std::ptr::null_mut(),
            transport_vtable: TransportVTable {
                send_frame: process_send_frame::<State>,
                close: process_close::<State>,
                is_connected: process_is_connected::<State>,
            },
            pending: HashMap::new(),
            next_id: 1,
            stream_entries: Vec::new(),
            local_topics: HashMap::new(),
            callback_entries: Vec::new(),
            remote_topics: HashSet::new(),
            incoming_streams: HashMap::new(),
            publishers: Vec::new(),
            stopped: false,
            timeout: None,
        }
    }

    fn new_transport(
        channel: RuntimeChannel<State>,
        state: State,
        mode: TransportMode,
        transport_ptr: *mut (),
        transport_vtable: TransportVTable,
    ) -> Self {
        Self {
            channel,
            state,
            mode,
            transport_ptr,
            transport_vtable,
            pending: HashMap::new(),
            next_id: 1,
            stream_entries: Vec::new(),
            local_topics: HashMap::new(),
            callback_entries: Vec::new(),
            remote_topics: HashSet::new(),
            incoming_streams: HashMap::new(),
            publishers: Vec::new(),
            stopped: false,
            timeout: None,
        }
    }

    fn attach_publishers(&mut self) {
        let bindings = self
            .channel
            .subscriptions
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for binding in bindings {
            let peer = self as *mut Self;
            let id = binding.add_publisher(Rc::new(move |topic, body_json| unsafe {
                if let Some(peer) = peer.as_mut() {
                    if !peer.remote_topics.contains(topic) {
                        return;
                    }
                    let _ = peer.send_message(OutgoingMessage {
                        topic: Some(topic),
                        body_json,
                        ..OutgoingMessage::default()
                    });
                }
            }));
            self.publishers.push((binding.clone(), id));
        }
    }

    fn detach_publishers(&mut self) {
        for (binding, id) in self.publishers.drain(..) {
            binding.remove_publisher(id);
        }
    }

    /// Stops the peer because its transport failed, settling every pending
    /// call with `reason` instead of the generic "disconnected".
    fn fail(&mut self, reason: &str) {
        if self.stopped {
            return;
        }
        for entry in self.pending.values_mut() {
            match entry {
                PendingEntry::Call(call) if call.error_text.is_none() => {
                    call.error_text = Some(reason.to_string());
                }
                PendingEntry::Subscribe(sub) if sub.error_text.is_none() => {
                    sub.error_text = Some(reason.to_string());
                }
                PendingEntry::Stream(ptr) => unsafe {
                    if let Some(stream) = ptr.as_mut() {
                        if !stream.finished {
                            stream.queue.push(RawStreamEvent {
                                body_json: None,
                                error_text: Some(reason.to_string()),
                                done: false,
                            });
                            stream.finished = true;
                        }
                    }
                },
                _ => {}
            }
        }
        self.stop();
    }

    pub(crate) fn stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        unsafe { (self.transport_vtable.close)(self.transport_ptr) };
        if let Some(callback) = self.channel.on_disconnect.clone() {
            let sender = Sender::new(self as *mut _);
            callback(&mut self.state, sender);
        }
        self.detach_publishers();
        for task in self.incoming_streams.values_mut() {
            task.cancelled = true;
        }
        self.remote_topics.clear();
        self.local_topics.clear();
        self.fail_all_pending();
    }

    fn fail_all_pending(&mut self) {
        let keys = self.pending.keys().copied().collect::<Vec<_>>();
        for id in keys {
            if let Some(entry) = self.pending.get_mut(&id) {
                match entry {
                    PendingEntry::Call(call) => {
                        call.done = true;
                        if call.error_text.is_none() {
                            call.error_text = Some("disconnected".to_string());
                        }
                    }
                    PendingEntry::Stream(ptr) => unsafe {
                        if let Some(stream) = ptr.as_mut() {
                            if !stream.finished {
                                stream.queue.push(RawStreamEvent {
                                    body_json: None,
                                    error_text: Some("disconnected".to_string()),
                                    done: false,
                                });
                                stream.finished = true;
                            }
                        }
                    },
                    PendingEntry::Subscribe(sub) => {
                        sub.done = true;
                        if sub.error_text.is_none() {
                            sub.error_text = Some("disconnected".to_string());
                        }
                    }
                }
            }
        }
        // Settled calls and subscribes stay until their waiter collects them,
        // so it sees the failure reason rather than a generic "disconnected".
        self.pending
            .retain(|_, entry| !matches!(entry, PendingEntry::Stream(_)));
    }

    fn is_connected(&self) -> bool {
        if self.stopped {
            return false;
        }
        unsafe { (self.transport_vtable.is_connected)(self.transport_ptr) }
    }

    pub(crate) fn is_remote_subscribed(&self, topic: &str) -> bool {
        self.remote_topics.contains(topic)
    }

    pub(crate) unsafe fn send_typed<Req, Resp, Ctx>(
        this: *mut Self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<CallResult<Resp>, ChannelError>
    where
        Req: Serialize + 'static,
        Resp: DeserializeOwned + 'static,
        Ctx: Serialize + 'static,
    {
        unsafe {
            let id = (*this).enqueue_typed(path, body, context)?;
            Self::resolve_call(this, id)
        }
    }

    /// Whether the pending entry `id` has settled (or is gone).
    unsafe fn is_settled(this: *const Self, id: i64) -> bool {
        let peer = unsafe { &*this };
        peer.stopped
            || peer
                .pending
                .get(&id)
                .map(|entry| match entry {
                    PendingEntry::Call(call) => call.done,
                    PendingEntry::Subscribe(sub) => sub.done,
                    PendingEntry::Stream(_) => true,
                })
                .unwrap_or(true)
    }

    /// Pumps until `id` settles, the peer stops, or the peer's timeout
    /// passes (returns false). A raw pointer, not `&mut self`: the pump
    /// settles the entry through another pointer, so the state must be
    /// re-read after every pump.
    unsafe fn wait_settled(this: *mut Self, id: i64) -> bool {
        let deadline = unsafe { (*this).timeout }.map(|timeout| Instant::now() + timeout);
        while !unsafe { Self::is_settled(this, id) } {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return false;
            }
            if !pump_all() {
                sleep_brief();
            }
        }
        true
    }

    /// Enqueues a request (id + pending entry + wire encode + send) and returns
    /// its id WITHOUT blocking. The caller can issue many requests this way and
    /// resolve them later — this is what enables pipelining (mirrors TS
    /// `Sender.request`). The peer must be pumped externally (or via
    /// `resolve_call`/`SendingRequest::response`) for responses to arrive.
    pub(crate) fn enqueue_typed<Req, Ctx>(
        &mut self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<i64, ChannelError>
    where
        Req: Serialize + 'static,
        Ctx: Serialize + 'static,
    {
        let id = self.next_id();
        self.pending.insert(
            id,
            PendingEntry::Call(Box::new(PendingCall {
                done: false,
                body_json: None,
                error_text: None,
            })),
        );
        let body_json = json_codec::encode_optional(&body)?;
        let context_json = json_codec::encode_optional(&context)?;
        self.send_message(OutgoingMessage {
            id: Some(id),
            path: Some(path),
            body_json: body_json.as_deref(),
            context_json: context_json.as_deref(),
            ..OutgoingMessage::default()
        })?;
        Ok(id)
    }

    /// Blocks until the call with `id` completes, then decodes the response.
    /// Pumps the registered transports while waiting (the same loop the old
    /// inline `send_typed` used).
    pub(crate) unsafe fn resolve_call<Resp>(
        this: *mut Self,
        id: i64,
    ) -> Result<CallResult<Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
    {
        let settled = unsafe { Self::wait_settled(this, id) };
        let peer = unsafe { &mut *this };
        if !settled {
            peer.cancel_call(id);
            return Ok(CallResult::RemoteErr("timeout".to_string()));
        }

        let Some(PendingEntry::Call(call)) = peer.pending.remove(&id) else {
            return Ok(CallResult::RemoteErr("disconnected".to_string()));
        };

        if let Some(error) = call.error_text {
            return Ok(CallResult::RemoteErr(error));
        }
        if json_codec::is_unit::<Resp>() || call.body_json.is_none() {
            return Ok(CallResult::Ok(None));
        }
        let value = json_codec::decode_field::<Resp>(call.body_json.as_deref(), "response body")?;
        Ok(CallResult::Ok(Some(value)))
    }

    /// Non-blocking probe: has the call with `id` completed (or vanished)?
    pub(crate) fn is_call_ready(&self, id: i64) -> bool {
        self.pending
            .get(&id)
            .and_then(|entry| match entry {
                PendingEntry::Call(call) => Some(call.done),
                _ => None,
            })
            .unwrap_or(true)
    }

    /// Cancels an in-flight call: removes the pending entry and sends a cancel
    /// frame (mirrors TS `SendingRequest.cancel`).
    pub(crate) fn cancel_call(&mut self, id: i64) {
        self.pending.remove(&id);
        let _ = self.send_message(OutgoingMessage {
            cancel: Some(id),
            ..OutgoingMessage::default()
        });
    }

    pub(crate) fn values_typed<Req, Item, Ctx>(
        &mut self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<Values<Item>, ChannelError>
    where
        Req: Serialize + 'static,
        Item: DeserializeOwned + 'static,
        Ctx: Serialize + 'static,
    {
        let id = self.next_id();
        let mut stream = Box::new(PendingStream {
            peer: self as *mut _,
            id,
            closed: false,
            finished: false,
            queue: Vec::new(),
        });
        let ptr = stream.as_mut() as *mut PendingStream<State>;
        self.stream_entries.push(stream);
        self.pending.insert(id, PendingEntry::Stream(ptr));

        let body_json = json_codec::encode_optional(&body)?;
        let context_json = json_codec::encode_optional(&context)?;
        self.send_message(OutgoingMessage {
            id: Some(id),
            stream: Some(path),
            body_json: body_json.as_deref(),
            context_json: context_json.as_deref(),
            ..OutgoingMessage::default()
        })?;

        Ok(Values::new(
            ptr as *mut (),
            pending_stream_next::<State>,
            pending_stream_close::<State>,
        ))
    }

    pub(crate) unsafe fn subscribe_typed<Req, Event, Ctx, F>(
        this: *mut Self,
        path: &str,
        body: Req,
        context: Ctx,
        mut on_event: F,
    ) -> Result<SubscribeResult, ChannelError>
    where
        Req: Serialize + 'static,
        Event: DeserializeOwned + 'static,
        Ctx: Serialize + 'static,
        F: FnMut(Event) + 'static,
    {
        let id = {
            let peer = unsafe { &mut *this };
            let id = peer.next_id();
            peer.pending.insert(
                id,
                PendingEntry::Subscribe(Box::new(PendingSubscribe {
                    done: false,
                    topic: None,
                    error_text: None,
                })),
            );

            let body_json = json_codec::encode_optional(&body)?;
            let context_json = json_codec::encode_optional(&context)?;
            peer.send_message(OutgoingMessage {
                id: Some(id),
                sub: Some(path),
                body_json: body_json.as_deref(),
                context_json: context_json.as_deref(),
                ..OutgoingMessage::default()
            })?;
            id
        };

        let settled = unsafe { Self::wait_settled(this, id) };
        let peer = unsafe { &mut *this };
        if !settled {
            peer.pending.remove(&id);
            return Ok(SubscribeResult::RemoteErr("timeout".to_string()));
        }

        let Some(PendingEntry::Subscribe(sub)) = peer.pending.remove(&id) else {
            return Ok(SubscribeResult::RemoteErr("disconnected".to_string()));
        };

        if let Some(error) = sub.error_text {
            return Ok(SubscribeResult::RemoteErr(error));
        }
        let Some(topic) = sub.topic else {
            return Ok(SubscribeResult::RemoteErr(
                "subscription missing topic".to_string(),
            ));
        };

        let entry = Box::new(TopicCallbackEntry {
            peer: this,
            topic: topic.clone(),
            cancelled: false,
            invoke: Box::new(move |body_json| {
                if let Ok(value) = json_codec::decode_field::<Event>(body_json, "event") {
                    on_event(value);
                }
            }),
        });
        let ptr = peer.register_local_callback(entry);
        Ok(SubscribeResult::Ok(SubscriptionHandle::new(
            ptr as *mut (),
            cancel_subscription::<State>,
        )))
    }

    fn next_id(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn register_local_callback(
        &mut self,
        mut entry: Box<TopicCallbackEntry<State>>,
    ) -> *mut TopicCallbackEntry<State> {
        let ptr = entry.as_mut() as *mut _;
        self.local_topics
            .entry(entry.topic.clone())
            .or_default()
            .push(ptr);
        self.callback_entries.push(entry);
        ptr
    }

    fn remove_local_callback(&mut self, entry_ptr: *mut TopicCallbackEntry<State>) {
        let topic = unsafe { entry_ptr.as_ref().map(|entry| entry.topic.clone()) };
        let Some(topic) = topic else {
            return;
        };

        let mut should_unsub = false;
        if let Some(entries) = self.local_topics.get_mut(&topic) {
            entries.retain(|ptr| *ptr != entry_ptr);
            should_unsub = entries.is_empty();
        }

        if should_unsub {
            self.local_topics.remove(&topic);
            let _ = self.send_message(OutgoingMessage {
                unsub: Some(&topic),
                ..OutgoingMessage::default()
            });
        }
    }

    fn send_message(&mut self, message: OutgoingMessage<'_>) -> Result<(), ChannelError> {
        if matches!(self.mode, TransportMode::Process) {
            let parsed = ParsedMessage {
                id: message
                    .id
                    .map(WireId::Int)
                    .or_else(|| message.id_raw.map(|raw| WireId::Raw(raw.to_string()))),
                path: message.path.map(str::to_string),
                stream: message.stream.map(str::to_string),
                sub: message.sub.map(str::to_string),
                unsub: message.unsub.map(str::to_string),
                cancel: message.cancel.map(WireId::Int),
                topic: message.topic.map(str::to_string),
                body_json: message.body_json.map(str::to_string),
                context_json: message.context_json.map(str::to_string),
                error_text: message.error_text.map(str::to_string),
                done: message.done,
            };
            return self.dispatch_incoming(parsed);
        }

        if self.stopped {
            return Err(ChannelError::new("not connected"));
        }

        let payload = encode_message(message)?;
        unsafe { (self.transport_vtable.send_frame)(self.transport_ptr, &payload)? };
        pump_after_send();
        Ok(())
    }

    fn dispatch_incoming(&mut self, message: ParsedMessage) -> Result<(), ChannelError> {
        if let Some(path) = message.path.as_deref() {
            let handler = self.channel.posts.get(path).cloned().or_else(|| {
                self.channel
                    .other_posts
                    .iter()
                    .find(|(matcher, _)| matcher(path))
                    .map(|(_, handler)| handler.clone())
            });
            if let Some(handler) = handler {
                return (handler.invoke)(
                    self,
                    path,
                    message.body_json.as_deref(),
                    message.context_json.as_deref(),
                    message.id.as_ref(),
                );
            }
            if let Some(id) = &message.id {
                self.send_message(OutgoingMessage {
                    error_text: Some("api not found"),
                    ..OutgoingMessage::reply(id)
                })?;
            }
            return Ok(());
        }

        if let Some(path) = message.stream.as_deref() {
            let Some(id) = &message.id else {
                return Ok(());
            };
            let handler = self.channel.streams.get(path).cloned().or_else(|| {
                self.channel
                    .other_streams
                    .iter()
                    .find(|(matcher, _)| matcher(path))
                    .map(|(_, handler)| handler.clone())
            });
            if let Some(handler) = handler {
                return (handler.invoke)(
                    self,
                    path,
                    message.body_json.as_deref(),
                    message.context_json.as_deref(),
                    id,
                );
            }
            self.send_message(OutgoingMessage {
                error_text: Some("api not found"),
                ..OutgoingMessage::reply(id)
            })?;
            return Ok(());
        }

        if let Some(cancel) = &message.cancel {
            if let Some(task) = self.incoming_streams.get_mut(cancel) {
                task.cancelled = true;
            }
            return Ok(());
        }

        if let Some(name) = message.sub.as_deref() {
            let Some(id) = &message.id else {
                return Ok(());
            };
            let Some(binding) = self.channel.subscriptions.get(name).cloned() else {
                self.send_message(OutgoingMessage {
                    error_text: Some("subscription not found"),
                    ..OutgoingMessage::reply(id)
                })?;
                return Ok(());
            };
            let ack = match binding.subscribe(message.body_json.as_deref()) {
                Ok(ack) => ack,
                Err(error) => {
                    return self.send_message(OutgoingMessage {
                        error_text: Some(&error.to_string()),
                        ..OutgoingMessage::reply(id)
                    });
                }
            };
            self.remote_topics.insert(ack.topic.clone());
            self.send_message(OutgoingMessage {
                topic: Some(&ack.topic),
                body_json: ack.body_json.as_deref(),
                ..OutgoingMessage::reply(id)
            })?;
            return Ok(());
        }

        if let Some(topic) = message.unsub.as_deref() {
            self.remote_topics.remove(topic);
            return Ok(());
        }

        if let Some(topic) = message.topic.as_deref() {
            if let Some(id) = message.id.as_ref().and_then(WireId::as_int) {
                self.complete_response(id, &message);
            }
            self.dispatch_local_event(topic, message.body_json.as_deref());
            return Ok(());
        }

        if let Some(id) = message.id.as_ref().and_then(WireId::as_int) {
            self.complete_response(id, &message);
        }
        Ok(())
    }

    fn complete_response(&mut self, id: i64, message: &ParsedMessage) {
        if let Some(entry) = self.pending.get_mut(&id) {
            match entry {
                PendingEntry::Call(call) => {
                    call.body_json = message.body_json.clone();
                    call.error_text = message.error_text.clone();
                    call.done = true;
                }
                PendingEntry::Stream(ptr) => unsafe {
                    if let Some(stream) = ptr.as_mut() {
                        stream.queue.push(RawStreamEvent {
                            body_json: message.body_json.clone(),
                            error_text: message.error_text.clone(),
                            done: message.done.unwrap_or(false),
                        });
                        if message.done == Some(true) || message.error_text.is_some() {
                            stream.finished = true;
                            self.pending.remove(&id);
                        }
                    }
                },
                PendingEntry::Subscribe(sub) => {
                    sub.topic = message.topic.clone();
                    sub.error_text = message.error_text.clone();
                    sub.done = true;
                }
            }
        }
    }

    fn dispatch_local_event(&mut self, topic: &str, body_json: Option<&str>) {
        let Some(entries) = self.local_topics.get(topic) else {
            return;
        };
        let callbacks = entries.clone();
        for entry_ptr in callbacks {
            unsafe {
                if let Some(entry) = entry_ptr.as_mut() {
                    if !entry.cancelled {
                        (entry.invoke)(body_json);
                    }
                }
            }
        }
    }

    fn handle_incoming_frame(&mut self, payload: &[u8]) -> Result<(), ChannelError> {
        for message in parse_messages(payload)? {
            self.dispatch_incoming(message)?;
        }
        Ok(())
    }
}

pub struct Process<State> {
    peer: Box<Peer<State>>,
    stop_fn: unsafe fn(*mut ()),
}

unsafe fn peer_stop<State: 'static>(raw: *mut ()) {
    if let Some(peer) = unsafe { raw.cast::<Peer<State>>().as_mut() } {
        peer.stop();
    }
}

unsafe fn server_stop<State: Default + 'static>(raw: *mut ()) {
    if let Some(transport) = unsafe { raw.cast::<ServerTransport<State>>().as_mut() } {
        transport.stop();
    }
}

// Dropping a process, client or server stops it: it leaves the thread's pump
// list and its subscriptions' publisher lists, which would otherwise keep
// pointers to the freed peer/transport.
impl<State> Drop for Process<State> {
    fn drop(&mut self) {
        unsafe { (self.stop_fn)(self.peer.as_mut() as *mut Peer<State> as *mut ()) };
    }
}

impl<State> Drop for Client<State> {
    fn drop(&mut self) {
        unsafe { (self.stop_fn)(self.peer.as_mut() as *mut Peer<State> as *mut ()) };
    }
}

impl<State> Drop for Server<State> {
    fn drop(&mut self) {
        unsafe { (self.stop_fn)(self.transport.as_mut() as *mut ServerTransport<State> as *mut ()) };
    }
}

impl<State: 'static> Process<State> {
    pub fn sender(&mut self) -> Sender<State> {
        Sender::new(self.peer.as_mut() as *mut _)
    }

    pub fn send<Resp, Req>(&mut self, path: &str, body: Req) -> Result<CallResult<Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
    {
        self.sender().send(path, body)
    }

    pub fn send_with_context<Resp, Req, Ctx>(
        &mut self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<CallResult<Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
        Ctx: Serialize + 'static,
    {
        self.sender().send_with_context(path, body, context)
    }

    /// Issues a non-blocking request (mirrors TS `Sender.request`). See
    /// [`Sender::request`] and [`SendingRequest`].
    pub fn request<Resp, Req>(
        &mut self,
        path: &str,
        body: Req,
    ) -> Result<SendingRequest<State, Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
    {
        self.sender().request(path, body)
    }

    pub fn values<Req, Item, Ctx>(
        &mut self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<Values<Item>, ChannelError>
    where
        Req: Serialize + 'static,
        Item: DeserializeOwned + 'static,
        Ctx: Serialize + 'static,
    {
        self.sender().values(path, body, context)
    }

    pub fn subscribe<Req, Event, Ctx, F>(
        &mut self,
        path: &str,
        body: Req,
        context: Ctx,
        on_event: F,
    ) -> Result<SubscribeResult, ChannelError>
    where
        Req: Serialize + 'static,
        Event: DeserializeOwned + 'static,
        Ctx: Serialize + 'static,
        F: FnMut(Event) + 'static,
    {
        self.sender().subscribe(path, body, context, on_event)
    }

    pub fn stop(&mut self) {
        self.peer.stop();
    }

    pub fn is_connected(&self) -> bool {
        self.peer.is_connected()
    }

    pub fn is_remote_subscribed(&self, topic: &str) -> bool {
        self.peer.is_remote_subscribed(topic)
    }
}

pub struct Client<State> {
    peer: Box<Peer<State>>,
    transport: Box<ClientTransport<State>>,
    stop_fn: unsafe fn(*mut ()),
}

impl<State: 'static> Client<State> {
    pub fn sender(&mut self) -> Sender<State> {
        Sender::new(self.peer.as_mut() as *mut _)
    }

    pub fn send<Resp, Req>(&mut self, path: &str, body: Req) -> Result<CallResult<Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
    {
        self.sender().send(path, body)
    }

    pub fn send_with_context<Resp, Req, Ctx>(
        &mut self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<CallResult<Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
        Ctx: Serialize + 'static,
    {
        self.sender().send_with_context(path, body, context)
    }

    /// Issues a non-blocking request (mirrors TS `Sender.request`). See
    /// [`Sender::request`] and [`SendingRequest`]. The transport must be
    /// pumped (via [`Client::poll`] or [`SendingRequest::response`]) for
    /// responses to arrive.
    pub fn request<Resp, Req>(
        &mut self,
        path: &str,
        body: Req,
    ) -> Result<SendingRequest<State, Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
    {
        self.sender().request(path, body)
    }

    pub fn values<Req, Item, Ctx>(
        &mut self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<Values<Item>, ChannelError>
    where
        Req: Serialize + 'static,
        Item: DeserializeOwned + 'static,
        Ctx: Serialize + 'static,
    {
        self.sender().values(path, body, context)
    }

    pub fn subscribe<Req, Event, Ctx, F>(
        &mut self,
        path: &str,
        body: Req,
        context: Ctx,
        on_event: F,
    ) -> Result<SubscribeResult, ChannelError>
    where
        Req: Serialize + 'static,
        Event: DeserializeOwned + 'static,
        Ctx: Serialize + 'static,
        F: FnMut(Event) + 'static,
    {
        self.sender().subscribe(path, body, context, on_event)
    }

    pub fn poll(&mut self) -> Result<bool, ChannelError> {
        self.transport.poll()
    }

    pub fn stop(&mut self) {
        self.peer.stop();
    }

    pub fn is_connected(&self) -> bool {
        self.peer.is_connected()
    }

    pub fn is_remote_subscribed(&self, topic: &str) -> bool {
        self.peer.is_remote_subscribed(topic)
    }
}

pub struct Server<State> {
    transport: Box<ServerTransport<State>>,
    stop_fn: unsafe fn(*mut ()),
}

impl<State: 'static> Server<State> {
    pub fn poll(&mut self) -> Result<bool, ChannelError>
    where
        State: Default,
    {
        self.transport.poll()
    }

    pub fn stop(&mut self)
    where
        State: Default,
    {
        self.transport.stop();
    }

    pub fn port(&self) -> u16 {
        self.transport.port_value
    }
}

pub trait IntoConnectTarget {
    fn into_connect_target(self) -> Result<ConnectTarget, ChannelError>;
}

pub trait IntoListenTarget {
    fn into_listen_target(self) -> Result<ListenTarget, ChannelError>;
}

impl IntoConnectTarget for u16 {
    fn into_connect_target(self) -> Result<ConnectTarget, ChannelError> {
        Ok(ConnectTarget {
            host: "127.0.0.1".to_string(),
            path: "/".to_string(),
            port: self,
            tls: false,
        })
    }
}

impl IntoConnectTarget for usize {
    fn into_connect_target(self) -> Result<ConnectTarget, ChannelError> {
        (self as u16).into_connect_target()
    }
}

impl IntoConnectTarget for &str {
    fn into_connect_target(self) -> Result<ConnectTarget, ChannelError> {
        parse_connect_target(self)
    }
}

impl IntoConnectTarget for String {
    fn into_connect_target(self) -> Result<ConnectTarget, ChannelError> {
        parse_connect_target(&self)
    }
}

impl IntoListenTarget for u16 {
    fn into_listen_target(self) -> Result<ListenTarget, ChannelError> {
        Ok(ListenTarget {
            host: "127.0.0.1".to_string(),
            port: self,
        })
    }
}

impl IntoListenTarget for usize {
    fn into_listen_target(self) -> Result<ListenTarget, ChannelError> {
        (self as u16).into_listen_target()
    }
}

impl IntoListenTarget for &str {
    fn into_listen_target(self) -> Result<ListenTarget, ChannelError> {
        parse_listen_target(self)
    }
}

impl IntoListenTarget for String {
    fn into_listen_target(self) -> Result<ListenTarget, ChannelError> {
        parse_listen_target(&self)
    }
}

/// A parsed `connect` address (see [`IntoConnectTarget`]).
pub struct ConnectTarget {
    host: String,
    path: String,
    port: u16,
    tls: bool,
}

/// A parsed `listen` address (see [`IntoListenTarget`]).
pub struct ListenTarget {
    host: String,
    port: u16,
}

fn parse_connect_target(text: &str) -> Result<ConnectTarget, ChannelError> {
    if text.starts_with("ws://") || text.starts_with("wss://") {
        let url =
            Url::parse(text).map_err(|error: url::ParseError| ChannelError::new(error.to_string()))?;
        let host = url
            .host_str()
            .ok_or_else(|| ChannelError::new("invalid address"))?
            .to_string();
        let path = if url.path().is_empty() {
            "/".to_string()
        } else if let Some(query) = url.query() {
            format!("{}?{query}", url.path())
        } else {
            url.path().to_string()
        };
        return Ok(ConnectTarget {
            host,
            path,
            port: url.port().unwrap_or(if url.scheme() == "wss" { 443 } else { 80 }),
            tls: url.scheme() == "wss",
        });
    }

    let (host, port) = text
        .rsplit_once(':')
        .ok_or_else(|| ChannelError::new("invalid address"))?;
    Ok(ConnectTarget {
        host: host.to_string(),
        path: "/".to_string(),
        port: port
            .parse()
            .map_err(|error| ChannelError::new(format!("invalid port: {error}")))?,
        tls: false,
    })
}

fn parse_listen_target(text: &str) -> Result<ListenTarget, ChannelError> {
    let (host, port) = text
        .rsplit_once(':')
        .ok_or_else(|| ChannelError::new("invalid address"))?;
    Ok(ListenTarget {
        host: host.to_string(),
        port: port
            .parse()
            .map_err(|error| ChannelError::new(format!("invalid port: {error}")))?,
    })
}

unsafe fn pending_stream_next<State: 'static>(
    raw: *mut (),
) -> Result<RawStreamEventOwned, ChannelError> {
    let stream = raw.cast::<PendingStream<State>>();
    if stream.is_null() {
        return Err(ChannelError::new("stream missing"));
    }
    unsafe { PendingStream::next_raw(stream) }
}

unsafe fn pending_stream_close<State: 'static>(raw: *mut ()) {
    if let Some(stream) = unsafe { raw.cast::<PendingStream<State>>().as_mut() } {
        stream.close();
    }
}

unsafe fn cancel_subscription<State: 'static>(raw: *mut ()) {
    if let Some(entry) = unsafe { raw.cast::<TopicCallbackEntry<State>>().as_mut() } {
        if entry.cancelled {
            return;
        }
        entry.cancelled = true;
        if let Some(peer) = unsafe { entry.peer.as_mut() } {
            peer.remove_local_callback(entry as *mut _);
        }
    }
}

unsafe fn emit_stream_item<State: 'static, Item: Serialize + 'static>(
    raw: *mut (),
    item_any: Box<dyn Any>,
) -> Result<(), ChannelError> {
    let task = unsafe { raw.cast::<IncomingStreamTask<State>>().as_mut() }
        .ok_or_else(|| ChannelError::new("stream task missing"))?;
    if task.cancelled {
        return Err(ChannelError::new("cancelled"));
    }
    let typed = *item_any
        .downcast::<Item>()
        .map_err(|_| ChannelError::new("stream item type mismatch"))?;
    let body_json = json_codec::encode_optional(&typed)?;
    if let Some(owner) = unsafe { task.owner.as_mut() } {
        owner.send_message(OutgoingMessage {
            body_json: body_json.as_deref(),
            ..OutgoingMessage::reply(&task.id)
        })?;
    }
    Ok(())
}

unsafe fn stream_item_is_cancelled<State>(raw: *mut ()) -> bool {
    unsafe { raw.cast::<IncomingStreamTask<State>>().as_ref() }
        .map(|task| task.cancelled)
        .unwrap_or(true)
}

unsafe fn process_send_frame<State: 'static>(raw: *mut (), payload: &[u8]) -> Result<(), ChannelError> {
    let peer = unsafe { raw.cast::<Peer<State>>().as_mut() }
        .ok_or_else(|| ChannelError::new("peer missing"))?;
    peer.handle_incoming_frame(payload)
}

unsafe fn process_close<State>(_raw: *mut ()) {}

unsafe fn process_is_connected<State: 'static>(raw: *mut ()) -> bool {
    unsafe { raw.cast::<Peer<State>>().as_ref() }
        .map(|peer| !peer.stopped)
        .unwrap_or(false)
}

/// Incremental WebSocket frame parser shared by the client and the server.
///
/// Hostile input is bounded: a declared length over `max_payload` (including
/// 2^64-1, which used to wrap the bounds arithmetic) is rejected from the
/// header alone, fragmented messages are reassembled up to the same cap, and
/// reading stops once one maximal frame is buffered.
struct FrameReader {
    buffer: Vec<u8>,
    pos: usize,
    max_payload: usize,
    fragments: Option<(u8, Vec<u8>)>,
}

/// What one `read_from` call saw.
struct ReadStatus {
    progressed: bool,
    eof: bool,
}

impl FrameReader {
    fn new(max_payload: usize) -> Self {
        Self {
            buffer: Vec::new(),
            pos: 0,
            max_payload,
            fragments: None,
        }
    }

    fn buffered(&self) -> usize {
        self.buffer.len() - self.pos
    }

    fn read_from(&mut self, stream: &mut TcpStream) -> Result<ReadStatus, ChannelError> {
        if self.pos > 0 && (self.pos == self.buffer.len() || self.pos >= 64 * 1024) {
            self.buffer.drain(..self.pos);
            self.pos = 0;
        }
        let mut status = ReadStatus {
            progressed: false,
            eof: false,
        };
        let mut buf = [0_u8; 16 * 1024];
        // Back-pressure: a full frame of the largest allowed size plus its
        // header is enough to make progress; parse that before reading more.
        while self.buffered() <= self.max_payload.saturating_add(14) {
            match stream.read(&mut buf) {
                Ok(0) => {
                    status.eof = true;
                    break;
                }
                Ok(n) => {
                    status.progressed = true;
                    self.buffer.extend_from_slice(&buf[..n]);
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(ChannelError::new(error.to_string())),
            }
        }
        Ok(status)
    }

    /// Returns the next complete message (data frames reassembled from their
    /// fragments) or control frame.
    fn next_frame(&mut self) -> Result<Option<Frame>, ChannelError> {
        loop {
            let data = &self.buffer[self.pos..];
            if data.len() < 2 {
                return Ok(None);
            }
            let first = data[0];
            let second = data[1];
            if first & 0x70 != 0 {
                return Err(ChannelError::new("websocket reserved bits set"));
            }
            let fin = first & 0x80 != 0;
            let opcode = first & 0x0F;
            let masked = second & 0x80 != 0;
            let (header_len, declared) = match second & 0x7F {
                126 => {
                    if data.len() < 4 {
                        return Ok(None);
                    }
                    (4_usize, u16::from_be_bytes([data[2], data[3]]) as u64)
                }
                127 => {
                    if data.len() < 10 {
                        return Ok(None);
                    }
                    let mut bytes = [0_u8; 8];
                    bytes.copy_from_slice(&data[2..10]);
                    (10_usize, u64::from_be_bytes(bytes))
                }
                n => (2_usize, n as u64),
            };

            let is_control = opcode & 0x8 != 0;
            if is_control && (!fin || declared > 125) {
                return Err(ChannelError::new("invalid websocket control frame"));
            }
            let assembled = if is_control {
                0
            } else {
                self.fragments.as_ref().map_or(0, |(_, bytes)| bytes.len()) as u64
            };
            if declared > self.max_payload as u64
                || assembled.saturating_add(declared) > self.max_payload as u64
            {
                return Err(ChannelError::new("websocket message too large"));
            }
            // In range now: declared <= max_payload, which fits in usize.
            let payload_len = declared as usize;
            let mask_len = if masked { 4 } else { 0 };
            let Some(total) = (header_len + mask_len).checked_add(payload_len) else {
                return Err(ChannelError::new("websocket message too large"));
            };
            if data.len() < total {
                return Ok(None);
            }

            let start = header_len + mask_len;
            let mut payload = data[start..total].to_vec();
            if masked {
                let mask = [
                    data[header_len],
                    data[header_len + 1],
                    data[header_len + 2],
                    data[header_len + 3],
                ];
                for (index, byte) in payload.iter_mut().enumerate() {
                    *byte ^= mask[index & 3];
                }
            }
            self.pos += total;

            match opcode {
                OPCODE_CONTINUATION => {
                    let Some((message_opcode, mut bytes)) = self.fragments.take() else {
                        return Err(ChannelError::new("unexpected websocket continuation frame"));
                    };
                    bytes.extend_from_slice(&payload);
                    if fin {
                        return Ok(Some(Frame {
                            opcode: message_opcode,
                            payload: bytes,
                        }));
                    }
                    self.fragments = Some((message_opcode, bytes));
                }
                OPCODE_TEXT | OPCODE_BINARY => {
                    if self.fragments.is_some() {
                        return Err(ChannelError::new("expected websocket continuation frame"));
                    }
                    if fin {
                        return Ok(Some(Frame { opcode, payload }));
                    }
                    self.fragments = Some((opcode, payload));
                }
                OPCODE_CLOSE | OPCODE_PING | OPCODE_PONG => {
                    return Ok(Some(Frame { opcode, payload }));
                }
                _ => return Err(ChannelError::new("unsupported websocket opcode")),
            }
        }
    }
}

struct Frame {
    opcode: u8,
    payload: Vec<u8>,
}

/// Outgoing bytes for one socket. Frames are encoded whole into `buffer`
/// before any byte is written, so a handler that sends while an earlier
/// frame is still being flushed (the flush pumps on a full socket buffer)
/// appends behind it instead of interleaving bytes inside it.
#[derive(Default)]
struct FrameWriter {
    buffer: Vec<u8>,
    pos: usize,
    flushing: bool,
}

fn encode_frame(out: &mut Vec<u8>, opcode: u8, payload: &[u8], masked: bool) {
    let mask_bit = if masked { 0x80 } else { 0x00 };
    out.push(0x80 | opcode);
    if payload.len() <= 125 {
        out.push(mask_bit | payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        out.push(mask_bit | 126);
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        out.push(mask_bit | 127);
        out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    if masked {
        // RFC 6455 5.3: client masks must be unpredictable.
        let mask = random_bytes::<4>();
        out.extend_from_slice(&mask);
        let start = out.len();
        out.extend_from_slice(payload);
        for (index, byte) in out[start..].iter_mut().enumerate() {
            *byte ^= mask[index & 3];
        }
    } else {
        out.extend_from_slice(payload);
    }
}

/// Queues raw bytes and flushes them (see [`FrameWriter`]). Raw pointers on
/// purpose: the flush pumps other transports, which may re-enter this writer.
unsafe fn write_bytes(
    stream: *mut TcpStream,
    writer: *mut FrameWriter,
    encode: impl FnOnce(&mut Vec<u8>),
) -> Result<(), ChannelError> {
    unsafe {
        encode(&mut (*writer).buffer);
        if (*writer).flushing {
            return Ok(());
        }
        (*writer).flushing = true;
        let result = flush_writer(stream, writer);
        (*writer).flushing = false;
        if result.is_err() {
            (*writer).buffer.clear();
            (*writer).pos = 0;
        }
        result
    }
}

unsafe fn flush_writer(stream: *mut TcpStream, writer: *mut FrameWriter) -> Result<(), ChannelError> {
    unsafe {
        loop {
            let pos = (*writer).pos;
            if pos >= (*writer).buffer.len() {
                (*writer).buffer.clear();
                (*writer).pos = 0;
                if (*writer).buffer.capacity() > 1024 * 1024 {
                    (*writer).buffer.shrink_to(64 * 1024);
                }
                return Ok(());
            }
            let written = (*stream).write(&(&(*writer).buffer)[pos..]);
            match written {
                Ok(0) => return Err(ChannelError::new("connection closed")),
                Ok(n) => (*writer).pos += n,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if !pump_all() {
                        sleep_brief();
                    }
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(ChannelError::new(error.to_string())),
            }
        }
    }
}

unsafe fn write_frame(
    stream: *mut TcpStream,
    writer: *mut FrameWriter,
    opcode: u8,
    payload: &[u8],
    masked: bool,
) -> Result<(), ChannelError> {
    unsafe { write_bytes(stream, writer, |out| encode_frame(out, opcode, payload, masked)) }
}

fn connect_tcp(target: &ConnectTarget, timeout: Duration) -> Result<TcpStream, ChannelError> {
    use std::net::ToSocketAddrs;
    let mut last_error = None;
    for address in (target.host.as_str(), target.port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error
        .map(ChannelError::from)
        .unwrap_or_else(|| ChannelError::new("invalid address")))
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

/// Sends the upgrade request and validates the response. Returns any bytes
/// the server sent after the response head (the first frames).
fn perform_client_handshake(
    stream: &mut TcpStream,
    target: &ConnectTarget,
    extra_headers: Option<&str>,
    timeout: Duration,
) -> Result<Vec<u8>, ChannelError> {
    let deadline = Instant::now() + timeout;
    let wait = || -> Result<(), ChannelError> {
        if Instant::now() >= deadline {
            return Err(ChannelError::new("handshake timeout"));
        }
        if !pump_all() {
            sleep_brief();
        }
        Ok(())
    };

    let key = BASE64.encode(random_bytes::<16>());
    let mut request = format!(
        "GET {} HTTP/1.1\r\ncontent-length: 0\r\nupgrade: websocket\r\nsec-websocket-version: 13\r\nconnection: upgrade\r\nsec-websocket-key: {}\r\nhost: {}:{}\r\n",
        target.path, key, target.host, target.port
    );
    if let Some(extra) = extra_headers {
        request.push_str(extra);
        if !extra.ends_with("\r\n") {
            request.push_str("\r\n");
        }
    }
    request.push_str("\r\n");

    let mut data = request.as_bytes();
    while !data.is_empty() {
        match stream.write(data) {
            Ok(0) => return Err(ChannelError::new("connection closed")),
            Ok(n) => data = &data[n..],
            Err(error) if error.kind() == ErrorKind::WouldBlock => wait()?,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(ChannelError::new(error.to_string())),
        }
    }

    let mut buffer = Vec::new();
    let header_end = loop {
        let mut temp = [0_u8; 4096];
        match stream.read(&mut temp) {
            Ok(0) => return Err(ChannelError::new("connection closed")),
            Ok(n) => {
                buffer.extend_from_slice(&temp[..n]);
                if let Some(end) = find_header_end(&buffer) {
                    break end;
                }
                if buffer.len() > MAX_HANDSHAKE_BYTES {
                    return Err(ChannelError::new("handshake response too large"));
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => wait()?,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(ChannelError::new(error.to_string())),
        }
    };

    let text = std::str::from_utf8(&buffer[..header_end])
        .map_err(|_| ChannelError::new("invalid handshake response"))?;
    let mut lines = text.split("\r\n");
    let status = lines
        .next()
        .ok_or_else(|| ChannelError::new("invalid handshake response"))?;
    if !status.starts_with("HTTP/1.1 101 ") && status != "HTTP/1.1 101" {
        return Err(ChannelError::new("invalid handshake response"));
    }

    let mut upgrade = false;
    let mut connection = false;
    let mut accept_ok = false;
    let mut sha = Sha1::new();
    sha.update(key.as_bytes());
    sha.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let expected = BASE64.encode(sha.finalize());

    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let value = value.trim();
            match name.trim().to_ascii_lowercase().as_str() {
                "upgrade" => upgrade = value.eq_ignore_ascii_case("websocket"),
                "connection" => {
                    connection = value
                        .split(',')
                        .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
                }
                "sec-websocket-accept" => accept_ok = value == expected,
                _ => {}
            }
        }
    }

    if !upgrade || !connection || !accept_ok {
        return Err(ChannelError::new("invalid handshake response"));
    }

    Ok(buffer[header_end..].to_vec())
}

struct ClientTransport<State> {
    stream: TcpStream,
    reader: FrameReader,
    writer: FrameWriter,
    connected: bool,
    closed: bool,
    pump_token: Option<usize>,
    peer: *mut Peer<State>,
}

impl<State: 'static> ClientTransport<State> {
    fn poll(&mut self) -> Result<bool, ChannelError> {
        unsafe { Self::poll_raw(self) }
    }

    /// Polls through a raw pointer: dispatching a message runs handlers,
    /// which may pump (and so re-enter this transport) while it is polled.
    /// Any failure (I/O, oversized or malformed frame, invalid JSON) stops the
    /// peer and settles its pending calls with the reason.
    unsafe fn poll_raw(this: *mut Self) -> Result<bool, ChannelError> {
        unsafe {
            if !(*this).connected {
                return Ok(false);
            }
            match Self::poll_inner(this) {
                Ok(progressed) => Ok(progressed),
                Err(error) => {
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
            let status = (*this).reader.read_from(&mut (*this).stream)?;
            let mut progressed = status.progressed;
            loop {
                if !(*this).connected {
                    return Ok(true);
                }
                let Some(frame) = (*this).reader.next_frame()? else {
                    break;
                };
                progressed = true;
                match frame.opcode {
                    OPCODE_TEXT | OPCODE_BINARY => {
                        if let Some(peer) = (*this).peer.as_mut() {
                            peer.handle_incoming_frame(&frame.payload)?;
                        }
                    }
                    OPCODE_PING => {
                        write_frame(
                            &mut (*this).stream,
                            &mut (*this).writer,
                            OPCODE_PONG,
                            &frame.payload,
                            true,
                        )?;
                    }
                    OPCODE_CLOSE => {
                        let _ = write_frame(
                            &mut (*this).stream,
                            &mut (*this).writer,
                            OPCODE_CLOSE,
                            &frame.payload[..frame.payload.len().min(2)],
                            true,
                        );
                        Self::disconnected(this);
                        return Ok(true);
                    }
                    _ => {}
                }
            }
            if status.eof {
                Self::disconnected(this);
                return Ok(true);
            }
            Ok(progressed)
        }
    }

    unsafe fn disconnected(this: *mut Self) {
        unsafe {
            (*this).connected = false;
            if let Some(peer) = (*this).peer.as_mut() {
                peer.stop();
            }
        }
    }
}

unsafe fn client_send_frame<State: 'static>(raw: *mut (), payload: &[u8]) -> Result<(), ChannelError> {
    let transport = raw.cast::<ClientTransport<State>>();
    unsafe {
        if transport.is_null() {
            return Err(ChannelError::new("client transport missing"));
        }
        if !(*transport).connected {
            return Err(ChannelError::new("not connected"));
        }
        write_frame(
            &mut (*transport).stream,
            &mut (*transport).writer,
            OPCODE_TEXT,
            payload,
            true,
        )
    }
}

unsafe fn client_close<State: 'static>(raw: *mut ()) {
    if let Some(transport) = unsafe { raw.cast::<ClientTransport<State>>().as_mut() } {
        if let Some(token) = transport.pump_token.take() {
            unregister_pump_entry(token);
        }
        if transport.closed {
            return;
        }
        transport.closed = true;
        transport.connected = false;
        let _ = transport.stream.shutdown(std::net::Shutdown::Both);
    }
}

unsafe fn client_is_connected<State: 'static>(raw: *mut ()) -> bool {
    unsafe { raw.cast::<ClientTransport<State>>().as_ref() }
        .map(|transport| transport.connected)
        .unwrap_or(false)
}

unsafe fn client_poll<State: 'static>(raw: *mut ()) -> Result<bool, ChannelError> {
    let transport = raw.cast::<ClientTransport<State>>();
    if transport.is_null() {
        return Err(ChannelError::new("client transport missing"));
    }
    unsafe { ClientTransport::poll_raw(transport) }
}

struct ServerConnection<State> {
    stream: TcpStream,
    handshake_buf: Vec<u8>,
    reader: FrameReader,
    writer: FrameWriter,
    peer: Option<Box<Peer<State>>>,
    closed: bool,
    removed: bool,
}

impl<State: 'static> ServerConnection<State> {
    unsafe fn poll(this: *mut Self, server: *mut ServerTransport<State>) -> Result<bool, ChannelError>
    where
        State: Default,
    {
        unsafe {
            if (*this).closed {
                return Ok(false);
            }
            if (*this).peer.is_none() {
                return Self::poll_handshake(this, server);
            }
            Self::poll_active(this)
        }
    }

    unsafe fn poll_handshake(
        this: *mut Self,
        server: *mut ServerTransport<State>,
    ) -> Result<bool, ChannelError>
    where
        State: Default,
    {
        unsafe {
            let mut temp = [0_u8; 4096];
            match (*this).stream.read(&mut temp) {
                Ok(0) => {
                    (*this).closed = true;
                    (*this).removed = true;
                    return Ok(true);
                }
                Ok(n) => (*this).handshake_buf.extend_from_slice(&temp[..n]),
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(false),
                Err(error) if error.kind() == ErrorKind::Interrupted => return Ok(false),
                Err(error) => return Err(ChannelError::new(error.to_string())),
            }

            let Some(header_end) = find_header_end(&(*this).handshake_buf) else {
                if (*this).handshake_buf.len() > MAX_HANDSHAKE_BYTES {
                    reject_handshake(&mut (*this).stream, "431 Request Header Fields Too Large");
                    return Err(ChannelError::new("handshake request too large"));
                }
                return Ok(true);
            };

            let head = std::mem::take(&mut (*this).handshake_buf);
            let Ok(request) = std::str::from_utf8(&head[..header_end]) else {
                reject_handshake(&mut (*this).stream, "400 Bad Request");
                return Err(ChannelError::new("invalid handshake request"));
            };
            let (_path, headers, key) = match parse_server_request(request) {
                Ok(parsed) => parsed,
                Err(error) => {
                    reject_handshake(&mut (*this).stream, "400 Bad Request");
                    return Err(error);
                }
            };
            let state = match &(*server).options.state {
                Some(factory) => match factory(&headers) {
                    Ok(state) => state,
                    Err(error) => {
                        reject_handshake(&mut (*this).stream, "500 Internal Server Error");
                        return Err(error);
                    }
                },
                None => State::default(),
            };

            let mut sha = Sha1::new();
            sha.update(key.as_bytes());
            sha.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
            let accept = BASE64.encode(sha.finalize());
            let response = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            );
            (*this).writer.buffer.extend_from_slice(response.as_bytes());
            (*this).reader.buffer.extend_from_slice(&head[header_end..]);

            let mut peer = Box::new(Peer::new_transport(
                (*server).channel.clone(),
                state,
                TransportMode::WebsocketServer,
                this as *mut (),
                TransportVTable {
                    send_frame: server_connection_send_frame::<State>,
                    close: server_connection_close::<State>,
                    is_connected: server_connection_is_connected::<State>,
                },
            ));
            peer.timeout = (*server).options.timeout;
            peer.attach_publishers();
            let peer_ptr = peer.as_mut() as *mut Peer<State>;
            (*this).peer = Some(peer);
            // The 101 goes out before anything `on_connect` sends.
            write_bytes(&mut (*this).stream, &mut (*this).writer, |_| {})?;
            if let Some(callback) = (*server).options.on_connect.clone() {
                if let Some(peer) = peer_ptr.as_mut() {
                    let sender = Sender::new(peer_ptr);
                    callback(&mut peer.state, sender);
                }
            }
            Ok(true)
        }
    }

    unsafe fn poll_active(this: *mut Self) -> Result<bool, ChannelError> {
        unsafe {
            let status = (*this).reader.read_from(&mut (*this).stream)?;
            let mut progressed = status.progressed;
            loop {
                if (*this).closed {
                    return Ok(true);
                }
                let Some(frame) = (*this).reader.next_frame()? else {
                    break;
                };
                progressed = true;
                match frame.opcode {
                    OPCODE_TEXT | OPCODE_BINARY => {
                        let peer = (*this)
                            .peer
                            .as_mut()
                            .map(|peer| peer.as_mut() as *mut Peer<State>);
                        if let Some(peer) = peer {
                            // Invalid JSON closes the connection, as in TS.
                            (*peer).handle_incoming_frame(&frame.payload)?;
                        }
                    }
                    OPCODE_PING => write_frame(
                        &mut (*this).stream,
                        &mut (*this).writer,
                        OPCODE_PONG,
                        &frame.payload,
                        false,
                    )?,
                    OPCODE_CLOSE => {
                        let _ = write_frame(
                            &mut (*this).stream,
                            &mut (*this).writer,
                            OPCODE_CLOSE,
                            &frame.payload[..frame.payload.len().min(2)],
                            false,
                        );
                        (*this).removed = true;
                        (*this).closed = true;
                        return Ok(true);
                    }
                    _ => {}
                }
            }
            if status.eof {
                (*this).removed = true;
                (*this).closed = true;
                return Ok(true);
            }
            Ok(progressed)
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
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}

/// Best-effort HTTP error for a rejected upgrade (one non-blocking write).
fn reject_handshake(stream: &mut TcpStream, status: &str) {
    let response = format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
    let _ = stream.write(response.as_bytes());
}

struct ServerTransport<State> {
    channel: RuntimeChannel<State>,
    options: ListenOptions<State>,
    listener: TcpListener,
    port_value: u16,
    stopped: bool,
    /// How many `poll` calls for this server are on the stack. Connections
    /// are only removed (and freed) when it is back to zero, because an
    /// outer poll may be running a handler of the connection being closed.
    poll_depth: usize,
    pump_token: Option<usize>,
    connections: Vec<Box<ServerConnection<State>>>,
    _marker: PhantomData<State>,
}

impl<State> ServerTransport<State>
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
            loop {
                match (*this).listener.accept() {
                    Ok((stream, _)) => {
                        progressed = true;
                        if stream.set_nonblocking(true).is_err() {
                            continue;
                        }
                        stream.set_nodelay(true).ok();
                        let max_payload = (*this).options.max_payload;
                        (*this).connections.push(Box::new(ServerConnection {
                            stream,
                            handshake_buf: Vec::new(),
                            reader: FrameReader::new(max_payload),
                            writer: FrameWriter::default(),
                            peer: None,
                            closed: false,
                            removed: false,
                        }));
                    }
                    Err(error) if error.kind() == ErrorKind::Interrupted => {}
                    // WouldBlock, or a transient accept failure such as
                    // EMFILE: serve the existing connections and retry later.
                    Err(_) => break,
                }
            }

            let mut index = 0_usize;
            while index < (*this).connections.len() && !(*this).stopped {
                let connection = (&mut (*this).connections)[index].as_mut() as *mut ServerConnection<State>;
                match ServerConnection::poll(connection, this) {
                    Ok(value) => progressed = value || progressed,
                    Err(_) => {
                        // Per-connection failure (reset, oversized frame,
                        // invalid JSON, rejected handshake): drop just this
                        // connection and keep the server alive.
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

    /// Frees closed connections. The closed ones are moved out of the list
    /// first, so callbacks run by `destroy` may poll this server again.
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

unsafe fn server_poll<State: 'static>(raw: *mut ()) -> Result<bool, ChannelError>
where
    State: Default,
{
    let transport = raw.cast::<ServerTransport<State>>();
    if transport.is_null() {
        return Err(ChannelError::new("server transport missing"));
    }
    unsafe { ServerTransport::poll_raw(transport) }
}

unsafe fn server_connection_send_frame<State: 'static>(
    raw: *mut (),
    payload: &[u8],
) -> Result<(), ChannelError> {
    let connection = raw.cast::<ServerConnection<State>>();
    unsafe {
        if connection.is_null() {
            return Err(ChannelError::new("server connection missing"));
        }
        if (*connection).closed {
            return Err(ChannelError::new("not connected"));
        }
        write_frame(
            &mut (*connection).stream,
            &mut (*connection).writer,
            OPCODE_TEXT,
            payload,
            false,
        )
    }
}

unsafe fn server_connection_close<State: 'static>(raw: *mut ()) {
    if let Some(connection) = unsafe { raw.cast::<ServerConnection<State>>().as_mut() } {
        connection.closed = true;
        connection.removed = true;
        let _ = connection.stream.shutdown(std::net::Shutdown::Both);
    }
}

unsafe fn server_connection_is_connected<State: 'static>(raw: *mut ()) -> bool {
    unsafe { raw.cast::<ServerConnection<State>>().as_ref() }
        .map(|connection| !connection.closed)
        .unwrap_or(false)
}

fn parse_server_request(
    request: &str,
) -> Result<(String, HashMap<String, String>, String), ChannelError> {
    let mut lines = request.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| ChannelError::new("invalid handshake request"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or("/");
    if method != "GET" {
        return Err(ChannelError::new("invalid handshake request"));
    }

    let mut headers = HashMap::new();
    let mut ws_key = None;
    if let Some((_, query)) = path.split_once('?') {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            headers.insert(key.into_owned(), value.into_owned());
        }
    }

    for line in lines {
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        if name == "sec-websocket-key" {
            ws_key = Some(value.clone());
        }
        headers.insert(name, value);
    }

    let key = ws_key.ok_or_else(|| ChannelError::new("missing sec-websocket-key"))?;
    Ok((path.to_string(), headers, key))
}
