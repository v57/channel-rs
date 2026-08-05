use std::{any::Any, marker::PhantomData};

use crate::{ChannelError, Sender};

pub struct PostRequest<'a, State, Req, Ctx> {
    pub body: Req,
    pub context: Ctx,
    pub sender: Sender<State>,
    pub state: &'a mut State,
    pub path: String,
}

pub struct StreamEmitter<Item> {
    raw: *mut (),
    emit_fn: unsafe fn(*mut (), Box<dyn Any>) -> Result<(), ChannelError>,
    is_cancelled_fn: unsafe fn(*mut ()) -> bool,
    _marker: PhantomData<Item>,
}

impl<Item> StreamEmitter<Item> {
    pub(crate) fn new(
        raw: *mut (),
        emit_fn: unsafe fn(*mut (), Box<dyn Any>) -> Result<(), ChannelError>,
        is_cancelled_fn: unsafe fn(*mut ()) -> bool,
    ) -> Self {
        Self {
            raw,
            emit_fn,
            is_cancelled_fn,
            _marker: PhantomData,
        }
    }
}

impl<Item: 'static> StreamEmitter<Item> {
    pub fn emit(&mut self, item: Item) -> Result<(), ChannelError> {
        unsafe { (self.emit_fn)(self.raw, Box::new(item)) }
    }

    pub fn is_cancelled(&self) -> bool {
        unsafe { (self.is_cancelled_fn)(self.raw) }
    }
}

pub struct StreamRequest<'a, State, Req, Ctx, Item> {
    pub body: Req,
    pub context: Ctx,
    pub sender: Sender<State>,
    pub state: &'a mut State,
    pub path: String,
    pub emitter: StreamEmitter<Item>,
}
