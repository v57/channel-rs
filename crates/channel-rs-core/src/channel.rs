use std::{
    any::{Any, TypeId},
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap, HashSet},
    io::{ErrorKind, Read, Write},
    marker::PhantomData,
    net::{TcpListener, TcpStream},
    rc::Rc,
    thread,
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde::{Serialize, de::DeserializeOwned};
use sha1::{Digest, Sha1};
use url::Url;

use crate::{
    CallResult, ChannelError, PostRequest, Sender, SendingRequest, StreamEmitter, StreamRequest,
    SubscribeResult, SubscriptionHandle, Values, json_codec,
    subscription::{ErasedSubscriptionBinding, IntoErasedSubscription},
    wire::{OutgoingMessage, ParsedMessage, encode_message, parse_messages},
};

const SLEEP_MICROS: u64 = 100;
const OPCODE_TEXT: u8 = 0x1;
const OPCODE_BINARY: u8 = 0x2;
const OPCODE_CLOSE: u8 = 0x8;
const OPCODE_PING: u8 = 0x9;
const OPCODE_PONG: u8 = 0xA;

thread_local! {
    static PUMP_ENTRIES: RefCell<Vec<Option<PumpEntry>>> = const { RefCell::new(Vec::new()) };
    static IS_PUMPING: Cell<bool> = const { Cell::new(false) };
    static CLIENT_MASK_COUNTER: Cell<u32> = const { Cell::new(1) };
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

fn pump_all() -> Result<bool, ChannelError> {
    if IS_PUMPING.with(Cell::get) {
        return Ok(false);
    }

    IS_PUMPING.with(|cell| cell.set(true));
    let entries = PUMP_ENTRIES.with(|entries| {
        entries
            .borrow()
            .iter()
            .filter_map(|entry| {
                entry.as_ref().map(|entry| PumpEntry {
                    ptr: entry.ptr,
                    poll_fn: entry.poll_fn,
                })
            })
            .collect::<Vec<_>>()
    });

    let mut progressed = false;
    for entry in entries {
        progressed = unsafe { (entry.poll_fn)(entry.ptr)? } || progressed;
    }

    IS_PUMPING.with(|cell| cell.set(false));
    Ok(progressed)
}

fn sleep_brief() {
    thread::sleep(Duration::from_micros(SLEEP_MICROS));
}

fn is_connection_closed(error: &ChannelError) -> bool {
    error == &ChannelError::new("connection closed")
}

#[derive(Clone)]
struct ApiLeaf<State> {
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

struct ErasedPostHandler<State> {
    invoke: Rc<dyn Fn(&mut Peer<State>, &str, Option<&str>, Option<&str>, Option<i64>) -> Result<(), ChannelError>>,
}

struct ErasedStreamHandler<State> {
    invoke: Rc<dyn Fn(&mut Peer<State>, &str, Option<&str>, Option<&str>, i64) -> Result<(), ChannelError>>,
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
                    let body = json_codec::decode_required::<Req>(body_json)?;
                    let context = json_codec::decode_required::<Ctx>(context_json)?;
                    let sender = Sender::new(peer as *mut _);
                    let request = PostRequest {
                        body,
                        context,
                        sender,
                        state: &mut peer.state,
                        path: path.to_string(),
                    };
                    match invoke_handler(request) {
                        Ok(response) => {
                            if let Some(id) = id {
                                let body_json = json_codec::encode_response(response.as_ref())?;
                                peer.send_message(OutgoingMessage {
                                    id: Some(id),
                                    body_json: body_json.as_deref(),
                                    ..OutgoingMessage::default()
                                })?;
                            }
                        }
                        Err(error) => {
                            if let Some(id) = id {
                                peer.send_message(OutgoingMessage {
                                    id: Some(id),
                                    error_text: Some(&error.to_string()),
                                    ..OutgoingMessage::default()
                                })?;
                            }
                        }
                    }
                    Ok(())
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
                    let body = json_codec::decode_required::<Req>(body_json)?;
                    let context = json_codec::decode_required::<Ctx>(context_json)?;
                    let owner = peer as *mut _;
                    let task = peer.incoming_streams.entry(id).or_insert(IncomingStreamTask {
                        owner: owner,
                        id,
                        cancelled: false,
                    });
                    let emitter = StreamEmitter::new(
                        task as *mut IncomingStreamTask<State> as *mut (),
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
                        .get(&id)
                        .map(|task| task.cancelled)
                        .unwrap_or(false);
                    peer.incoming_streams.remove(&id);
                    match result {
                        Ok(()) => {
                            if !cancelled {
                                peer.send_message(OutgoingMessage {
                                    id: Some(id),
                                    done: Some(true),
                                    ..OutgoingMessage::default()
                                })?;
                            }
                        }
                        Err(error) => {
                            if !cancelled {
                                peer.send_message(OutgoingMessage {
                                    id: Some(id),
                                    error_text: Some(&error.to_string()),
                                    ..OutgoingMessage::default()
                                })?;
                            }
                        }
                    }
                    Ok(())
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
        Ok(Process { peer })
    }

    pub fn connect<T>(&self, target: T, options: ConnectOptions<State>) -> Result<Client<State>, ChannelError>
    where
        T: IntoConnectTarget,
        State: Default,
    {
        let target = target.into_connect_target()?;
        let mut stream = TcpStream::connect((target.host.as_str(), target.port))?;
        stream.set_nodelay(true).ok();
        stream.set_nonblocking(true)?;
        perform_client_handshake(&mut stream, &target, options.headers.as_deref())?;

        let mut transport = Box::new(ClientTransport {
            stream,
            reader: FrameReader::default(),
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
        peer.attach_publishers();
        transport.peer = peer.as_mut() as *mut _;
        let token = register_pump_entry(transport.as_mut() as *mut ClientTransport<State> as *mut (), client_poll::<State>);
        transport.pump_token = Some(token);

        Ok(Client { peer, transport })
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
            pump_token: None,
            connections: Vec::new(),
            _marker: PhantomData,
        });
        let token = register_pump_entry(
            transport.as_mut() as *mut ServerTransport<State> as *mut (),
            server_poll::<State>,
        );
        transport.pump_token = Some(token);
        Ok(Server { transport })
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
    pub headers: Option<String>,
}

impl<State> Default for ConnectOptions<State> {
    fn default() -> Self {
        Self {
            state: None,
            headers: None,
        }
    }
}

type StateFactory<State> = Rc<dyn Fn(&HashMap<String, String>) -> Result<State, ChannelError>>;
type ConnectCallback<State> = Rc<dyn Fn(&mut State, Sender<State>)>;

pub struct ListenOptions<State> {
    pub state: Option<StateFactory<State>>,
    pub on_connect: Option<ConnectCallback<State>>,
    pub on_disconnect: Option<ConnectCallback<State>>,
}

impl<State> Default for ListenOptions<State> {
    fn default() -> Self {
        Self {
            state: None,
            on_connect: None,
            on_disconnect: None,
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
    fn next_raw(&mut self) -> Result<RawStreamEventOwned, ChannelError> {
        while self.queue.is_empty() && !self.finished {
            if !pump_all()? {
                let peer = unsafe { self.peer.as_ref() }.ok_or_else(|| ChannelError::new("peer missing"))?;
                if peer.stopped {
                    break;
                }
                sleep_brief();
            }
        }

        if self.queue.is_empty() {
            return Ok(RawStreamEventOwned::Done);
        }
        let event = self.queue.remove(0);
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
    id: i64,
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
    incoming_streams: HashMap<i64, IncomingStreamTask<State>>,
    publishers: Vec<(ErasedSubscriptionBinding, usize)>,
    stopped: bool,
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
        self.pending.clear();
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

    pub(crate) fn send_typed<Req, Resp, Ctx>(
        &mut self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<CallResult<Resp>, ChannelError>
    where
        Req: Serialize + 'static,
        Resp: DeserializeOwned + 'static,
        Ctx: Serialize + 'static,
    {
        let id = self.enqueue_typed(path, body, context)?;
        self.resolve_call(id)
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
    pub(crate) fn resolve_call<Resp>(
        &mut self,
        id: i64,
    ) -> Result<CallResult<Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
    {
        while !self
            .pending
            .get(&id)
            .and_then(|entry| match entry {
                PendingEntry::Call(call) => Some(call.done),
                _ => None,
            })
            .unwrap_or(true)
            && !self.stopped
        {
            if !pump_all()? {
                sleep_brief();
            }
        }

        let Some(PendingEntry::Call(call)) = self.pending.remove(&id) else {
            return Ok(CallResult::RemoteErr("disconnected".to_string()));
        };

        if let Some(error) = call.error_text {
            return Ok(CallResult::RemoteErr(error));
        }
        if json_codec::is_unit::<Resp>() || call.body_json.is_none() {
            return Ok(CallResult::Ok(None));
        }
        let value = json_codec::decode_required::<Resp>(call.body_json.as_deref())?;
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

    pub(crate) fn subscribe_typed<Req, Event, Ctx, F>(
        &mut self,
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
        let id = self.next_id();
        self.pending.insert(
            id,
            PendingEntry::Subscribe(Box::new(PendingSubscribe {
                done: false,
                topic: None,
                error_text: None,
            })),
        );

        let body_json = json_codec::encode_optional(&body)?;
        let context_json = json_codec::encode_optional(&context)?;
        self.send_message(OutgoingMessage {
            id: Some(id),
            sub: Some(path),
            body_json: body_json.as_deref(),
            context_json: context_json.as_deref(),
            ..OutgoingMessage::default()
        })?;

        while !self
            .pending
            .get(&id)
            .and_then(|entry| match entry {
                PendingEntry::Subscribe(sub) => Some(sub.done),
                _ => None,
            })
            .unwrap_or(true)
            && !self.stopped
        {
            if !pump_all()? {
                sleep_brief();
            }
        }

        let Some(PendingEntry::Subscribe(sub)) = self.pending.remove(&id) else {
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
            peer: self as *mut _,
            topic: topic.clone(),
            cancelled: false,
            invoke: Box::new(move |body_json| {
                if json_codec::is_unit::<Event>() {
                    let value = json_codec::decode_required::<Event>(Some("null"));
                    if let Ok(value) = value {
                        on_event(value);
                    }
                    return;
                }
                if let Ok(value) = json_codec::decode_required::<Event>(body_json) {
                    on_event(value);
                }
            }),
        });
        let ptr = self.register_local_callback(entry);
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

    fn lookup_post_handler(&self, path: &str) -> Option<ErasedPostHandler<State>> {
        self.channel.posts.get(path).cloned().or_else(|| {
            self.channel
                .other_posts
                .iter()
                .find(|(matcher, _)| matcher(path))
                .map(|(_, handler)| handler.clone())
        })
    }

    fn send_message(&mut self, message: OutgoingMessage<'_>) -> Result<(), ChannelError> {
        if matches!(self.mode, TransportMode::Process) {
            let parsed = ParsedMessage {
                id: message.id,
                path: message.path.map(str::to_string),
                stream: message.stream.map(str::to_string),
                sub: message.sub.map(str::to_string),
                unsub: message.unsub.map(str::to_string),
                cancel: message.cancel,
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
        let _ = pump_all();
        Ok(())
    }

    fn dispatch_incoming(&mut self, message: ParsedMessage) -> Result<(), ChannelError> {
        if let Some(path) = message.path.as_deref() {
            if let Some(handler) = self.channel.posts.get(path).cloned() {
                return (handler.invoke)(
                    self,
                    path,
                    message.body_json.as_deref(),
                    message.context_json.as_deref(),
                    message.id,
                );
            }
            if let Some(handler) = self
                .channel
                .other_posts
                .iter()
                .find(|(matcher, _)| matcher(path))
                .map(|(_, handler)| handler.clone())
            {
                return (handler.invoke)(
                    self,
                    path,
                    message.body_json.as_deref(),
                    message.context_json.as_deref(),
                    message.id,
                );
            }
            if let Some(id) = message.id {
                self.send_message(OutgoingMessage {
                    id: Some(id),
                    error_text: Some("api not found"),
                    ..OutgoingMessage::default()
                })?;
            }
            return Ok(());
        }

        if let Some(path) = message.stream.as_deref() {
            let Some(id) = message.id else {
                return Ok(());
            };
            if let Some(handler) = self.channel.streams.get(path).cloned() {
                return (handler.invoke)(
                    self,
                    path,
                    message.body_json.as_deref(),
                    message.context_json.as_deref(),
                    id,
                );
            }
            if let Some(handler) = self
                .channel
                .other_streams
                .iter()
                .find(|(matcher, _)| matcher(path))
                .map(|(_, handler)| handler.clone())
            {
                return (handler.invoke)(
                    self,
                    path,
                    message.body_json.as_deref(),
                    message.context_json.as_deref(),
                    id,
                );
            }
            self.send_message(OutgoingMessage {
                id: Some(id),
                error_text: Some("api not found"),
                ..OutgoingMessage::default()
            })?;
            return Ok(());
        }

        if let Some(cancel) = message.cancel {
            if let Some(task) = self.incoming_streams.get_mut(&cancel) {
                task.cancelled = true;
            }
            return Ok(());
        }

        if let Some(name) = message.sub.as_deref() {
            let Some(id) = message.id else {
                return Ok(());
            };
            let Some(binding) = self.channel.subscriptions.get(name).cloned() else {
                self.send_message(OutgoingMessage {
                    id: Some(id),
                    error_text: Some("subscription not found"),
                    ..OutgoingMessage::default()
                })?;
                return Ok(());
            };
            let ack = binding.subscribe(message.body_json.as_deref())?;
            self.remote_topics.insert(ack.topic.clone());
            self.send_message(OutgoingMessage {
                id: Some(id),
                topic: Some(&ack.topic),
                body_json: ack.body_json.as_deref(),
                ..OutgoingMessage::default()
            })?;
            return Ok(());
        }

        if let Some(topic) = message.unsub.as_deref() {
            self.remote_topics.remove(topic);
            return Ok(());
        }

        if let Some(topic) = message.topic.as_deref() {
            if let Some(id) = message.id {
                self.complete_response(id, &message);
            }
            self.dispatch_local_event(topic, message.body_json.as_deref());
            return Ok(());
        }

        if let Some(id) = message.id {
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

struct ConnectTarget {
    host: String,
    path: String,
    port: u16,
    tls: bool,
}

struct ListenTarget {
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
    raw.cast::<PendingStream<State>>()
        .as_mut()
        .ok_or_else(|| ChannelError::new("stream missing"))?
        .next_raw()
}

unsafe fn pending_stream_close<State: 'static>(raw: *mut ()) {
    if let Some(stream) = raw.cast::<PendingStream<State>>().as_mut() {
        stream.close();
    }
}

unsafe fn cancel_subscription<State: 'static>(raw: *mut ()) {
    if let Some(entry) = raw.cast::<TopicCallbackEntry<State>>().as_mut() {
        if entry.cancelled {
            return;
        }
        entry.cancelled = true;
        if let Some(peer) = entry.peer.as_mut() {
            peer.remove_local_callback(entry as *mut _);
        }
    }
}

unsafe fn emit_stream_item<State: 'static, Item: Serialize + 'static>(
    raw: *mut (),
    item_any: Box<dyn Any>,
) -> Result<(), ChannelError> {
    let task = raw
        .cast::<IncomingStreamTask<State>>()
        .as_mut()
        .ok_or_else(|| ChannelError::new("stream task missing"))?;
    if task.cancelled {
        return Err(ChannelError::new("cancelled"));
    }
    let typed = *item_any
        .downcast::<Item>()
        .map_err(|_| ChannelError::new("stream item type mismatch"))?;
    let body_json = json_codec::encode_optional(&typed)?;
    if let Some(owner) = task.owner.as_mut() {
        owner.send_message(OutgoingMessage {
            id: Some(task.id),
            body_json: body_json.as_deref(),
            ..OutgoingMessage::default()
        })?;
    }
    Ok(())
}

unsafe fn stream_item_is_cancelled<State>(raw: *mut ()) -> bool {
    raw.cast::<IncomingStreamTask<State>>()
        .as_ref()
        .map(|task| task.cancelled)
        .unwrap_or(true)
}

unsafe fn process_send_frame<State: 'static>(raw: *mut (), payload: &[u8]) -> Result<(), ChannelError> {
    let peer = raw
        .cast::<Peer<State>>()
        .as_mut()
        .ok_or_else(|| ChannelError::new("peer missing"))?;
    peer.handle_incoming_frame(payload)
}

unsafe fn process_close<State>(_raw: *mut ()) {}

unsafe fn process_is_connected<State: 'static>(raw: *mut ()) -> bool {
    raw.cast::<Peer<State>>()
        .as_ref()
        .map(|peer| !peer.stopped)
        .unwrap_or(false)
}

#[derive(Default)]
struct FrameReader {
    buffer: Vec<u8>,
}

impl FrameReader {
    fn read_from(&mut self, stream: &mut TcpStream) -> Result<bool, ChannelError> {
        let mut progressed = false;
        let mut buf = [0_u8; 4096];
        loop {
            match stream.read(&mut buf) {
                Ok(0) => return Err(ChannelError::new("connection closed")),
                Ok(n) => {
                    progressed = true;
                    self.buffer.extend_from_slice(&buf[..n]);
                    if n < buf.len() {
                        return Ok(progressed);
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(progressed),
                Err(error) => return Err(ChannelError::new(error.to_string())),
            }
        }
    }

    fn next_frame(&mut self) -> Result<Option<Frame>, ChannelError> {
        if self.buffer.len() < 2 {
            return Ok(None);
        }

        let first = self.buffer[0];
        if first & 0x80 == 0 {
            return Err(ChannelError::new("fragmented websocket frames are not supported"));
        }
        let opcode = first & 0x0F;
        let masked = self.buffer[1] & 0x80 != 0;
        let mut offset = 2_usize;
        let payload_len = match self.buffer[1] & 0x7F {
            n @ 0..=125 => n as usize,
            126 => {
                if self.buffer.len() < offset + 2 {
                    return Ok(None);
                }
                let value = u16::from_be_bytes([self.buffer[offset], self.buffer[offset + 1]]);
                offset += 2;
                value as usize
            }
            127 => {
                if self.buffer.len() < offset + 8 {
                    return Ok(None);
                }
                let value = u64::from_be_bytes([
                    self.buffer[offset],
                    self.buffer[offset + 1],
                    self.buffer[offset + 2],
                    self.buffer[offset + 3],
                    self.buffer[offset + 4],
                    self.buffer[offset + 5],
                    self.buffer[offset + 6],
                    self.buffer[offset + 7],
                ]);
                offset += 8;
                value as usize
            }
            _ => unreachable!(),
        };

        let mask = if masked {
            if self.buffer.len() < offset + 4 {
                return Ok(None);
            }
            let mask = [
                self.buffer[offset],
                self.buffer[offset + 1],
                self.buffer[offset + 2],
                self.buffer[offset + 3],
            ];
            offset += 4;
            Some(mask)
        } else {
            None
        };

        if self.buffer.len() < offset + payload_len {
            return Ok(None);
        }

        let mut payload = self.buffer[offset..offset + payload_len].to_vec();
        if let Some(mask) = mask {
            for (index, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[index & 3];
            }
        }
        self.buffer.drain(..offset + payload_len);
        Ok(Some(Frame { opcode, payload }))
    }
}

struct Frame {
    opcode: u8,
    payload: Vec<u8>,
}

fn next_client_mask() -> [u8; 4] {
    CLIENT_MASK_COUNTER.with(|counter| {
        let value = counter.get();
        counter.set(value.wrapping_add(1));
        value.to_be_bytes()
    })
}

fn next_websocket_key() -> [u8; 16] {
    CLIENT_MASK_COUNTER.with(|counter| {
        let start = counter.get();
        counter.set(start.wrapping_add(4));

        let mut key = [0_u8; 16];
        for (index, chunk) in key.chunks_exact_mut(4).enumerate() {
            chunk.copy_from_slice(&start.wrapping_add(index as u32).to_be_bytes());
        }
        key
    })
}

fn write_socket_all(stream: &mut TcpStream, mut data: &[u8]) -> Result<(), ChannelError> {
    while !data.is_empty() {
        match stream.write(data) {
            Ok(0) => return Err(ChannelError::new("connection closed")),
            Ok(n) => data = &data[n..],
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if !pump_all()? {
                    sleep_brief();
                }
            }
            Err(error) => return Err(ChannelError::new(error.to_string())),
        }
    }
    Ok(())
}

fn write_frame(
    stream: &mut TcpStream,
    opcode: u8,
    payload: &[u8],
    masked: bool,
) -> Result<(), ChannelError> {
    let mut header = Vec::with_capacity(14);
    header.push(0x80 | opcode);
    if payload.len() <= 125 {
        header.push((if masked { 0x80 } else { 0x00 }) | payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        header.push((if masked { 0x80 } else { 0x00 }) | 126);
        header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        header.push((if masked { 0x80 } else { 0x00 }) | 127);
        header.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }

    if masked {
        let mask = next_client_mask();
        header.extend_from_slice(&mask);
        write_socket_all(stream, &header)?;
        let mut encoded = payload.to_vec();
        for (index, byte) in encoded.iter_mut().enumerate() {
            *byte ^= mask[index & 3];
        }
        write_socket_all(stream, &encoded)?;
        return Ok(());
    }

    write_socket_all(stream, &header)?;
    write_socket_all(stream, payload)
}

fn perform_client_handshake(
    stream: &mut TcpStream,
    target: &ConnectTarget,
    extra_headers: Option<&str>,
) -> Result<(), ChannelError> {
    if target.tls {
        return Err(ChannelError::new("tls is unsupported"));
    }

    let raw_key = next_websocket_key();
    let key = BASE64.encode(raw_key);
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
    write_socket_all(stream, request.as_bytes())?;

    let mut buffer = Vec::new();
    loop {
        let mut temp = [0_u8; 4096];
        match stream.read(&mut temp) {
            Ok(0) => return Err(ChannelError::new("connection closed")),
            Ok(n) => {
                buffer.extend_from_slice(&temp[..n]);
                if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if !pump_all()? {
                    sleep_brief();
                }
            }
            Err(error) => return Err(ChannelError::new(error.to_string())),
        }
    }

    let text = String::from_utf8(buffer).map_err(|error| ChannelError::new(error.to_string()))?;
    let mut lines = text.split("\r\n");
    let status = lines
        .next()
        .ok_or_else(|| ChannelError::new("invalid handshake response"))?;
    if !status.starts_with("HTTP/1.1 101 ") {
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
            match name.to_ascii_lowercase().as_str() {
                "upgrade" => upgrade = value.eq_ignore_ascii_case("websocket"),
                "connection" => connection = value.eq_ignore_ascii_case("upgrade"),
                "sec-websocket-accept" => accept_ok = value == expected,
                _ => {}
            }
        }
    }

    if !upgrade || !connection || !accept_ok {
        return Err(ChannelError::new("invalid handshake response"));
    }

    Ok(())
}

struct ClientTransport<State> {
    stream: TcpStream,
    reader: FrameReader,
    connected: bool,
    closed: bool,
    pump_token: Option<usize>,
    peer: *mut Peer<State>,
}

impl<State: 'static> ClientTransport<State> {
    fn poll(&mut self) -> Result<bool, ChannelError> {
        if !self.connected {
            return Ok(false);
        }

        let mut progressed = match self.reader.read_from(&mut self.stream) {
            Ok(value) => value,
            Err(error) if is_connection_closed(&error) => {
                self.connected = false;
                unsafe {
                    if let Some(peer) = self.peer.as_mut() {
                        peer.stop();
                    }
                }
                return Ok(true);
            }
            Err(error) => return Err(error),
        };
        while let Some(frame) = self.reader.next_frame()? {
            progressed = true;
            match frame.opcode {
                OPCODE_TEXT | OPCODE_BINARY => unsafe {
                    if let Some(peer) = self.peer.as_mut() {
                        if let Err(error) = peer.handle_incoming_frame(&frame.payload) {
                            self.connected = false;
                            peer.stop();
                            return Err(error);
                        }
                    }
                },
                OPCODE_PING => {
                    write_frame(&mut self.stream, OPCODE_PONG, &frame.payload, true)?;
                }
                OPCODE_PONG => {}
                OPCODE_CLOSE => {
                    self.connected = false;
                    unsafe {
                        if let Some(peer) = self.peer.as_mut() {
                            peer.stop();
                        }
                    }
                    return Ok(true);
                }
                _ => {}
            }
        }
        Ok(progressed)
    }
}

unsafe fn client_send_frame<State: 'static>(raw: *mut (), payload: &[u8]) -> Result<(), ChannelError> {
    let transport = raw
        .cast::<ClientTransport<State>>()
        .as_mut()
        .ok_or_else(|| ChannelError::new("client transport missing"))?;
    if !transport.connected {
        return Err(ChannelError::new("not connected"));
    }
    write_frame(&mut transport.stream, OPCODE_TEXT, payload, true)
}

unsafe fn client_close<State: 'static>(raw: *mut ()) {
    if let Some(transport) = raw.cast::<ClientTransport<State>>().as_mut() {
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
    raw.cast::<ClientTransport<State>>()
        .as_ref()
        .map(|transport| transport.connected)
        .unwrap_or(false)
}

unsafe fn client_poll<State: 'static>(raw: *mut ()) -> Result<bool, ChannelError> {
    raw.cast::<ClientTransport<State>>()
        .as_mut()
        .ok_or_else(|| ChannelError::new("client transport missing"))?
        .poll()
}

struct ServerConnection<State> {
    stream: TcpStream,
    handshake_buf: Vec<u8>,
    reader: FrameReader,
    peer: Option<Box<Peer<State>>>,
    closed: bool,
    removed: bool,
}

impl<State: 'static> ServerConnection<State> {
    fn poll(&mut self, server: &mut ServerTransport<State>) -> Result<bool, ChannelError>
    where
        State: Default,
    {
        if self.closed {
            return Ok(false);
        }
        if self.peer.is_none() {
            return self.poll_handshake(server);
        }
        self.poll_active()
    }

    fn poll_handshake(&mut self, server: &mut ServerTransport<State>) -> Result<bool, ChannelError>
    where
        State: Default,
    {
        let mut temp = [0_u8; 4096];
        match self.stream.read(&mut temp) {
            Ok(0) => {
                self.closed = true;
                self.removed = true;
                return Ok(true);
            }
            Ok(n) => self.handshake_buf.extend_from_slice(&temp[..n]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(ChannelError::new(error.to_string())),
        }

        let Some(header_end) = self
            .handshake_buf
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|value| value + 4) else {
            return Ok(true);
        };

        let request = String::from_utf8(self.handshake_buf[..header_end].to_vec())
            .map_err(|error| ChannelError::new(error.to_string()))?;
        let overread = self.handshake_buf[header_end..].to_vec();
        self.handshake_buf.clear();

        let (path, headers, key) = parse_server_request(&request)?;
        let state = if let Some(factory) = &server.options.state {
            factory(&headers)?
        } else {
            State::default()
        };

        let mut sha = Sha1::new();
        sha.update(key.as_bytes());
        sha.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let accept = BASE64.encode(sha.finalize());
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        write_socket_all(&mut self.stream, response.as_bytes())?;
        self.reader.buffer.extend_from_slice(&overread);

        let mut peer = Box::new(Peer::new_transport(
            server.channel.clone(),
            state,
            TransportMode::WebsocketServer,
            self as *mut ServerConnection<State> as *mut (),
            TransportVTable {
                send_frame: server_connection_send_frame::<State>,
                close: server_connection_close::<State>,
                is_connected: server_connection_is_connected::<State>,
            },
        ));
        peer.attach_publishers();
        if let Some(callback) = &server.options.on_connect {
            let sender = Sender::new(peer.as_mut() as *mut _);
            callback(&mut peer.state, sender);
        }
        self.peer = Some(peer);
        let _ = path;
        Ok(true)
    }

    fn poll_active(&mut self) -> Result<bool, ChannelError> {
        let mut progressed = match self.reader.read_from(&mut self.stream) {
            Ok(progressed) => progressed,
            Err(error) if is_connection_closed(&error) => {
                self.removed = true;
                self.closed = true;
                return Ok(true);
            }
            Err(error) => return Err(error),
        };
        while let Some(frame) = self.reader.next_frame()? {
            progressed = true;
            match frame.opcode {
                OPCODE_TEXT | OPCODE_BINARY => {
                    if let Some(peer) = self.peer.as_mut() {
                        if peer.handle_incoming_frame(&frame.payload).is_err() {
                            self.removed = true;
                            self.closed = true;
                            return Ok(true);
                        }
                    }
                }
                OPCODE_PING => write_frame(&mut self.stream, OPCODE_PONG, &frame.payload, false)?,
                OPCODE_PONG => {}
                OPCODE_CLOSE => {
                    self.removed = true;
                    self.closed = true;
                    return Ok(true);
                }
                _ => {}
            }
        }
        Ok(progressed)
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

struct ServerTransport<State> {
    channel: RuntimeChannel<State>,
    options: ListenOptions<State>,
    listener: TcpListener,
    port_value: u16,
    stopped: bool,
    pump_token: Option<usize>,
    connections: Vec<Box<ServerConnection<State>>>,
    _marker: PhantomData<State>,
}

impl<State> ServerTransport<State>
where
    State: Default + 'static,
{
    fn poll(&mut self) -> Result<bool, ChannelError> {
        if self.stopped {
            return Ok(false);
        }

        let mut progressed = false;
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    stream.set_nodelay(true).ok();
                    self.connections.push(Box::new(ServerConnection {
                        stream,
                        handshake_buf: Vec::new(),
                        reader: FrameReader::default(),
                        peer: None,
                        closed: false,
                        removed: false,
                    }));
                    progressed = true;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => return Err(ChannelError::new(error.to_string())),
            }
        }

        let mut index = 0_usize;
        let server_ptr = self as *mut Self;
        while index < self.connections.len() {
            let connection_ptr = self.connections[index].as_mut() as *mut ServerConnection<State>;
            let poll_result = unsafe { (&mut *connection_ptr).poll(&mut *server_ptr) };
            match poll_result {
                Ok(connection_progressed) => progressed = connection_progressed || progressed,
                Err(_) => {
                    // Per-connection failure (e.g. peer reset while reading):
                    // drop just this connection and keep the server alive.
                    // Other clients must not be taken down by one aborted
                    // socket — the benchmark harness kills clients between
                    // steps, so a reset here is routine, not fatal.
                    unsafe {
                        (&mut *connection_ptr).closed = true;
                        (&mut *connection_ptr).removed = true;
                    }
                    progressed = true;
                }
            }
            if self.connections[index].removed || self.connections[index].closed {
                let mut connection = self.connections.remove(index);
                connection.removed = true;
                connection.destroy(&self.options);
            } else {
                index += 1;
            }
        }

        Ok(progressed)
    }

    fn stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        if let Some(token) = self.pump_token.take() {
            unregister_pump_entry(token);
        }
        for connection in &mut self.connections {
            connection.removed = true;
            connection.destroy(&self.options);
        }
        self.connections.clear();
    }
}

unsafe fn server_poll<State: 'static>(raw: *mut ()) -> Result<bool, ChannelError>
where
    State: Default,
{
    raw.cast::<ServerTransport<State>>()
        .as_mut()
        .ok_or_else(|| ChannelError::new("server transport missing"))?
        .poll()
}

unsafe fn server_connection_send_frame<State: 'static>(
    raw: *mut (),
    payload: &[u8],
) -> Result<(), ChannelError> {
    let connection = raw
        .cast::<ServerConnection<State>>()
        .as_mut()
        .ok_or_else(|| ChannelError::new("server connection missing"))?;
    write_frame(&mut connection.stream, OPCODE_TEXT, payload, false)
}

unsafe fn server_connection_close<State: 'static>(raw: *mut ()) {
    if let Some(connection) = raw.cast::<ServerConnection<State>>().as_mut() {
        connection.closed = true;
        connection.removed = true;
        let _ = connection.stream.shutdown(std::net::Shutdown::Both);
    }
}

unsafe fn server_connection_is_connected<State: 'static>(raw: *mut ()) -> bool {
    raw.cast::<ServerConnection<State>>()
        .as_ref()
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
