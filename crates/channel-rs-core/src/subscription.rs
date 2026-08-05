use std::{
    cell::{Cell, RefCell},
    marker::PhantomData,
    rc::Rc,
};

use serde::{Serialize, de::DeserializeOwned};

use crate::{ChannelError, json_codec};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionAck {
    pub topic: String,
    pub body_json: Option<String>,
}

#[derive(Clone)]
struct PublisherSlot {
    id: usize,
    publish: Rc<dyn Fn(&str, Option<&str>)>,
}

struct SubscriptionInner<Req, Event> {
    prefix: RefCell<String>,
    publishers: RefCell<Vec<PublisherSlot>>,
    next_publisher_id: Cell<usize>,
    _marker: PhantomData<fn(Req, Event)>,
}

pub struct Subscription<Req, Event> {
    inner: Rc<SubscriptionInner<Req, Event>>,
}

impl<Req, Event> Clone for Subscription<Req, Event> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub trait IntoErasedSubscription {
    fn into_erased_subscription(self) -> ErasedSubscriptionBinding;
}

pub struct ErasedSubscriptionBinding {
    subscribe_fn: Rc<dyn for<'a> Fn(Option<&'a str>) -> Result<SubscriptionAck, ChannelError>>,
    add_publisher_fn: Rc<dyn Fn(Rc<dyn Fn(&str, Option<&str>)>) -> usize>,
    remove_publisher_fn: Rc<dyn Fn(usize)>,
    set_prefix_fn: Rc<dyn Fn(String)>,
}

impl Clone for ErasedSubscriptionBinding {
    fn clone(&self) -> Self {
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
            add_publisher_fn: self.add_publisher_fn.clone(),
            remove_publisher_fn: self.remove_publisher_fn.clone(),
            set_prefix_fn: self.set_prefix_fn.clone(),
        }
    }
}

impl<Req, Event> Subscription<Req, Event>
where
    Req: Serialize + DeserializeOwned + 'static,
    Event: Serialize + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Rc::new(SubscriptionInner {
                prefix: RefCell::new(String::new()),
                publishers: RefCell::new(Vec::new()),
                next_publisher_id: Cell::new(1),
                _marker: PhantomData,
            }),
        }
    }

    pub fn send(&self, request: Req, event: Event) -> Result<(), ChannelError> {
        let local_topic = json_codec::topic_from_value(&request)?;
        let prefix = self.inner.prefix.borrow().clone();
        let topic = if prefix.is_empty() {
            local_topic
        } else {
            format!("{prefix}/{local_topic}")
        };

        let body_json = json_codec::encode_optional(&event)?;
        if json_codec::is_unit::<Event>() && body_json.is_none() {
            return Ok(());
        }

        let publishers = self.inner.publishers.borrow().clone();
        for publisher in publishers {
            (publisher.publish)(&topic, body_json.as_deref());
        }
        Ok(())
    }

    pub fn set_prefix(&self, prefix: impl Into<String>) {
        *self.inner.prefix.borrow_mut() = prefix.into();
    }

    pub fn prefix(&self) -> String {
        self.inner.prefix.borrow().clone()
    }

    fn subscribe_impl(&self, body_json: Option<&str>) -> Result<SubscriptionAck, ChannelError> {
        let request = json_codec::decode_required::<Req>(body_json)?;
        let local_topic = json_codec::topic_from_value(&request)?;
        let prefix = self.inner.prefix.borrow().clone();
        Ok(SubscriptionAck {
            topic: if prefix.is_empty() {
                local_topic
            } else {
                format!("{prefix}/{local_topic}")
            },
            body_json: None,
        })
    }

    fn add_publisher_impl(&self, publish: Rc<dyn Fn(&str, Option<&str>)>) -> usize {
        let id = self.inner.next_publisher_id.get();
        self.inner.next_publisher_id.set(id + 1);
        self.inner
            .publishers
            .borrow_mut()
            .push(PublisherSlot { id, publish });
        id
    }

    fn remove_publisher_impl(&self, id: usize) {
        self.inner
            .publishers
            .borrow_mut()
            .retain(|slot| slot.id != id);
    }
}

impl<Req, Event> Default for Subscription<Req, Event>
where
    Req: Serialize + DeserializeOwned + 'static,
    Event: Serialize + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Req, Event> IntoErasedSubscription for Subscription<Req, Event>
where
    Req: Serialize + DeserializeOwned + 'static,
    Event: Serialize + 'static,
{
    fn into_erased_subscription(self) -> ErasedSubscriptionBinding {
        let subscription = self.clone();
        let subscribe_fn: Rc<
            dyn for<'a> Fn(Option<&'a str>) -> Result<SubscriptionAck, ChannelError>,
        > = Rc::new(move |body_json| subscription.subscribe_impl(body_json));

        let subscription = self.clone();
        let add_publisher_fn =
            Rc::new(move |publish| subscription.add_publisher_impl(publish)) as Rc<_>;

        let subscription = self.clone();
        let remove_publisher_fn =
            Rc::new(move |id| subscription.remove_publisher_impl(id)) as Rc<_>;

        let subscription = self;
        let set_prefix_fn = Rc::new(move |prefix| subscription.set_prefix(prefix)) as Rc<_>;

        ErasedSubscriptionBinding {
            subscribe_fn,
            add_publisher_fn,
            remove_publisher_fn,
            set_prefix_fn,
        }
    }
}

impl<Req, Event> IntoErasedSubscription for &Subscription<Req, Event>
where
    Req: Serialize + DeserializeOwned + 'static,
    Event: Serialize + 'static,
{
    fn into_erased_subscription(self) -> ErasedSubscriptionBinding {
        self.clone().into_erased_subscription()
    }
}

impl ErasedSubscriptionBinding {
    pub(crate) fn subscribe(&self, body_json: Option<&str>) -> Result<SubscriptionAck, ChannelError> {
        (self.subscribe_fn)(body_json)
    }

    pub(crate) fn add_publisher(&self, publish: Rc<dyn Fn(&str, Option<&str>)>) -> usize {
        (self.add_publisher_fn)(publish)
    }

    pub(crate) fn remove_publisher(&self, id: usize) {
        (self.remove_publisher_fn)(id)
    }

    pub(crate) fn set_prefix(&self, prefix: impl Into<String>) {
        (self.set_prefix_fn)(prefix.into())
    }
}
