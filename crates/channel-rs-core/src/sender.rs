use std::{marker::PhantomData, ptr::NonNull};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    ChannelError,
    channel::{Peer, RawStreamEventOwned},
    json_codec,
};

pub enum CallResult<T> {
    Ok(Option<T>),
    RemoteErr(String),
}

/// A non-blocking in-flight request (mirrors TS `SendingRequest`).
///
/// Created via [`Sender::request`]. The request is written to the wire
/// immediately; the response is resolved when [`SendingRequest::response`]
/// is called (which pumps until this id completes), or probed without
/// blocking via [`SendingRequest::is_ready`] so callers can pipeline many
/// requests before draining them.
pub struct SendingRequest<State, Resp> {
    peer: NonNull<Peer<State>>,
    id: i64,
    _marker: PhantomData<Resp>,
}

impl<State: 'static, Resp: DeserializeOwned + 'static> SendingRequest<State, Resp> {
    pub(crate) fn new(peer: *mut Peer<State>, id: i64) -> Self {
        Self {
            peer: NonNull::new(peer).expect("peer pointer"),
            id,
            _marker: PhantomData,
        }
    }

    /// Blocks until this request's response arrives, then returns the
    /// decoded result. Pumps the registered transports while waiting.
    pub fn response(self) -> Result<CallResult<Resp>, ChannelError> {
        unsafe { Peer::resolve_call(self.peer.as_ptr(), self.id) }
    }

    /// Non-blocking probe: has the response for this request arrived?
    /// Returns true if the call is done (or has been dropped/disconnected).
    pub fn is_ready(&self) -> bool {
        unsafe { self.peer.as_ptr().as_ref().unwrap().is_call_ready(self.id) }
    }

    /// Cancels the in-flight request (mirrors TS `SendingRequest.cancel`).
    pub fn cancel(&self) {
        unsafe {
            self.peer.as_ptr().as_mut().unwrap().cancel_call(self.id);
        }
    }
}

pub enum StreamNext<T> {
    Item(T),
    Done,
    RemoteErr(String),
}

pub enum SubscribeResult {
    Ok(SubscriptionHandle),
    RemoteErr(String),
}

pub struct SubscriptionHandle {
    raw: *mut (),
    cancel_fn: unsafe fn(*mut ()),
}

impl SubscriptionHandle {
    pub(crate) fn new(raw: *mut (), cancel_fn: unsafe fn(*mut ())) -> Self {
        Self { raw, cancel_fn }
    }

    pub fn cancel(&mut self) {
        if self.raw.is_null() {
            return;
        }
        unsafe { (self.cancel_fn)(self.raw) };
        self.raw = std::ptr::null_mut();
    }
}

pub struct Values<T> {
    raw: *mut (),
    next_fn: unsafe fn(*mut ()) -> Result<RawStreamEventOwned, ChannelError>,
    close_fn: unsafe fn(*mut ()),
    _marker: PhantomData<T>,
}

impl<T> Values<T> {
    pub(crate) fn new(
        raw: *mut (),
        next_fn: unsafe fn(*mut ()) -> Result<RawStreamEventOwned, ChannelError>,
        close_fn: unsafe fn(*mut ()),
    ) -> Self {
        Self {
            raw,
            next_fn,
            close_fn,
            _marker: PhantomData,
        }
    }

    pub fn close(&mut self) {
        if self.raw.is_null() {
            return;
        }
        unsafe { (self.close_fn)(self.raw) };
    }
}

impl<T> Drop for Values<T> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T> Values<T>
where
    T: DeserializeOwned + 'static,
{
    pub fn next(&mut self) -> Result<StreamNext<T>, ChannelError> {
        let raw = unsafe { (self.next_fn)(self.raw)? };
        match raw {
            RawStreamEventOwned::Done => Ok(StreamNext::Done),
            RawStreamEventOwned::RemoteErr(error) => Ok(StreamNext::RemoteErr(error)),
            RawStreamEventOwned::Body(body_json) => {
                let value = json_codec::decode_field::<T>(Some(&body_json), "stream item")?;
                Ok(StreamNext::Item(value))
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct Sender<State> {
    peer: NonNull<Peer<State>>,
}

impl<State: 'static> Sender<State> {
    pub(crate) fn new(peer: *mut Peer<State>) -> Self {
        Self {
            peer: NonNull::new(peer).expect("peer pointer"),
        }
    }

    pub fn send<Resp, Req>(&self, path: &str, body: Req) -> Result<CallResult<Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
    {
        self.send_with_context(path, body, ())
    }

    pub fn send_with_context<Resp, Req, Ctx>(
        &self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<CallResult<Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
        Ctx: Serialize + 'static,
    {
        unsafe { Peer::send_typed(self.peer.as_ptr(), path, body, context) }
    }

    /// Issues a request without blocking for its response (mirrors TS
    /// `Sender.request`). The request is written to the wire immediately;
    /// the returned [`SendingRequest`] resolves it later. This is the
    /// pipelining primitive: issue many requests, pump, then drain ready
    /// ones. `send` is `request(...)?.response()` under the hood.
    pub fn request<Resp, Req>(
        &self,
        path: &str,
        body: Req,
    ) -> Result<SendingRequest<State, Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
    {
        self.request_with_context(path, body, ())
    }

    /// Like [`Sender::request`] but with an explicit context value.
    pub fn request_with_context<Resp, Req, Ctx>(
        &self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<SendingRequest<State, Resp>, ChannelError>
    where
        Resp: DeserializeOwned + 'static,
        Req: Serialize + 'static,
        Ctx: Serialize + 'static,
    {
        let id = unsafe {
            self.peer
                .as_ptr()
                .as_mut()
                .unwrap()
                .enqueue_typed(path, body, context)?
        };
        Ok(SendingRequest::new(self.peer.as_ptr(), id))
    }

    pub fn values<Req, Item, Ctx>(
        &self,
        path: &str,
        body: Req,
        context: Ctx,
    ) -> Result<Values<Item>, ChannelError>
    where
        Req: Serialize + 'static,
        Item: DeserializeOwned + 'static,
        Ctx: Serialize + 'static,
    {
        unsafe {
            self.peer
                .as_ptr()
                .as_mut()
                .unwrap()
                .values_typed(path, body, context)
        }
    }

    pub fn subscribe<Req, Event, Ctx, F>(
        &self,
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
        unsafe { Peer::subscribe_typed(self.peer.as_ptr(), path, body, context, on_event) }
    }

    pub fn stop(&self) {
        unsafe {
            self.peer.as_ptr().as_mut().unwrap().stop();
        }
    }
}
