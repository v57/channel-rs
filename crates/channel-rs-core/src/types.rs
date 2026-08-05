use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::Mutex;
use serde_json::Value;

use crate::{ChannelError, ChannelMessage, sender::ChannelSender};

pub trait Cancellable {
    fn cancel(&self);
}

pub type EventCallback = Arc<dyn Fn(Value) + Send + Sync + 'static>;
pub type TopicCancel = Arc<dyn Fn() -> bool + Send + Sync + 'static>;

pub trait Connection: Send + Sync {
    fn send(&self, body: ChannelMessage) -> i64;
    fn send_with_immediate(&self, body: ChannelMessage) -> (i64, Option<ChannelMessage>) {
        (self.send(body), None)
    }
    fn send_direct(
        &self,
        _path: &str,
        _body: Option<&Value>,
        _context: Option<&Value>,
    ) -> Option<Pin<Box<dyn Future<Output = Result<Option<Value>, ChannelError>> + Send>>> {
        None
    }
    fn sent(&self, id: i64);
    fn cancel(&self, id: i64) -> bool;
    fn notify(&self, body: ChannelMessage);
    fn add_topic(&self, topic: String, on_event: EventCallback) -> TopicCancel;
    fn stop(&self);
}

type CancelAction = Box<dyn Fn() + Send + Sync + 'static>;

pub struct CancellableRequest {
    on_cancelled: Arc<dyn Fn() + Send + Sync + 'static>,
    is_cancelled: AtomicBool,
    actions: Mutex<Vec<CancelAction>>,
}

impl std::fmt::Debug for CancellableRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellableRequest")
            .field("is_cancelled", &self.is_cancelled.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl CancellableRequest {
    pub fn new(on_cancelled: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            on_cancelled: Arc::new(on_cancelled),
            is_cancelled: AtomicBool::new(false),
            actions: Mutex::new(Vec::new()),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::Acquire)
    }

    pub fn on_cancel(&self, action: impl Fn() + Send + Sync + 'static) {
        if self.is_cancelled() {
            action();
            return;
        }

        self.actions.lock().push(Box::new(action));
    }

    pub fn cancel_from_disconnect(&self) {
        if self
            .is_cancelled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let actions = std::mem::take(&mut *self.actions.lock());
        for action in actions {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action));
        }
    }
}

impl Cancellable for CancellableRequest {
    fn cancel(&self) {
        if self
            .is_cancelled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let actions = std::mem::take(&mut *self.actions.lock());
        for action in actions {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action));
        }
        (self.on_cancelled)();
    }
}

pub struct StateHandle<State>(Arc<tokio::sync::Mutex<State>>);

impl<State> Clone for StateHandle<State> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<State> std::fmt::Debug for StateHandle<State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StateHandle").finish()
    }
}

impl<State> StateHandle<State> {
    pub fn new(state: State) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(state)))
    }

    pub fn from_arc(state: Arc<tokio::sync::Mutex<State>>) -> Self {
        Self(state)
    }

    pub fn inner(&self) -> Arc<tokio::sync::Mutex<State>> {
        self.0.clone()
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, State> {
        self.0.lock().await
    }
}

pub type ResponseFn = Arc<dyn Fn(ChannelMessage) + Send + Sync + 'static>;
pub type SubscribeFn = Arc<dyn Fn(String) + Send + Sync + 'static>;
pub type EventFn = Arc<dyn Fn(String, Value) + Send + Sync + 'static>;

pub struct ChannelController<State: Send + Sync + 'static> {
    response: ResponseFn,
    subscribe: SubscribeFn,
    unsubscribe: SubscribeFn,
    event: EventFn,
    pub sender: ChannelSender<State>,
    pub state: StateHandle<State>,
}

impl<State: Send + Sync + 'static> Clone for ChannelController<State> {
    fn clone(&self) -> Self {
        Self {
            response: self.response.clone(),
            subscribe: self.subscribe.clone(),
            unsubscribe: self.unsubscribe.clone(),
            event: self.event.clone(),
            sender: self.sender.clone(),
            state: self.state.clone(),
        }
    }
}

impl<State: Send + Sync + 'static> ChannelController<State> {
    pub fn new(
        response: ResponseFn,
        subscribe: SubscribeFn,
        unsubscribe: SubscribeFn,
        event: EventFn,
        sender: ChannelSender<State>,
        state: StateHandle<State>,
    ) -> Self {
        Self {
            response,
            subscribe,
            unsubscribe,
            event,
            sender,
            state,
        }
    }

    pub fn response(&self, body: ChannelMessage) {
        (self.response)(body);
    }

    pub fn subscribe(&self, topic: String) {
        (self.subscribe)(topic);
    }

    pub fn unsubscribe(&self, topic: String) {
        (self.unsubscribe)(topic);
    }

    pub fn event(&self, topic: String, body: Value) {
        (self.event)(topic, body);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use parking_lot::Mutex;
    use serde_json::{Value, json};

    use crate::{
        Cancellable, CancellableRequest, Channel, ChannelController, ChannelMessage, Connection,
        EventCallback, StateHandle, TopicCancel,
    };

    #[derive(Default)]
    struct NoopConnection;

    impl Connection for NoopConnection {
        fn send(&self, _body: ChannelMessage) -> i64 {
            0
        }

        fn sent(&self, _id: i64) {}

        fn cancel(&self, _id: i64) -> bool {
            false
        }

        fn notify(&self, _body: ChannelMessage) {}

        fn add_topic(&self, _topic: String, _on_event: EventCallback) -> TopicCancel {
            Arc::new(|| false)
        }

        fn stop(&self) {}
    }

    #[test]
    fn cancellable_request_debug_and_cancel_paths() {
        let cancelled = Arc::new(AtomicUsize::new(0));
        let request = CancellableRequest::new({
            let cancelled = cancelled.clone();
            move || {
                cancelled.fetch_add(1, Ordering::Relaxed);
            }
        });

        let action_calls = Arc::new(AtomicUsize::new(0));
        request.on_cancel({
            let action_calls = action_calls.clone();
            move || {
                action_calls.fetch_add(1, Ordering::Relaxed);
            }
        });
        request.on_cancel(|| {
            panic!("panic in on_cancel action should be swallowed");
        });
        assert!(!request.is_cancelled());
        assert!(format!("{request:?}").contains("CancellableRequest"));
        request.cancel();
        request.cancel();

        assert!(request.is_cancelled());
        assert_eq!(action_calls.load(Ordering::Relaxed), 1);
        assert_eq!(cancelled.load(Ordering::Relaxed), 1);

        let after_cancel = Arc::new(AtomicBool::new(false));
        request.on_cancel({
            let after_cancel = after_cancel.clone();
            move || {
                after_cancel.store(true, Ordering::Release);
            }
        });
        assert!(after_cancel.load(Ordering::Acquire));
    }

    #[test]
    fn cancel_from_disconnect_runs_actions_once_without_on_cancelled() {
        let request = CancellableRequest::new(|| {});
        let action_calls = Arc::new(AtomicUsize::new(0));
        request.on_cancel({
            let action_calls = action_calls.clone();
            move || {
                action_calls.fetch_add(1, Ordering::Relaxed);
            }
        });

        request.cancel_from_disconnect();
        request.cancel_from_disconnect();
        assert!(request.is_cancelled());
        assert_eq!(action_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn state_handle_and_controller_clone_are_usable() {
        let shared = Arc::new(tokio::sync::Mutex::new(41_i64));
        let state = StateHandle::from_arc(shared.clone());
        assert!(format!("{state:?}").contains("StateHandle"));
        assert!(Arc::ptr_eq(&state.inner(), &shared));
        {
            let mut lock = state.lock().await;
            *lock += 1;
        }
        assert_eq!(*shared.lock().await, 42);

        let channel = Channel::<i64>::new();
        let connection = Arc::new(NoopConnection);
        assert_eq!(connection.send(ChannelMessage::default()), 0);
        connection.sent(10);
        assert!(!connection.cancel(11));
        connection.notify(ChannelMessage::default());
        let cancel = connection.add_topic("topic".to_string(), Arc::new(|_| {}));
        assert!(!cancel());
        connection.stop();

        let sender = channel.create_sender(connection);
        let responses = Arc::new(Mutex::new(Vec::<ChannelMessage>::new()));
        let subscriptions = Arc::new(Mutex::new(Vec::<String>::new()));
        let unsubs = Arc::new(Mutex::new(Vec::<String>::new()));
        let events = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));

        let controller = ChannelController::new(
            Arc::new({
                let responses = responses.clone();
                move |message| responses.lock().push(message)
            }),
            Arc::new({
                let subscriptions = subscriptions.clone();
                move |topic| subscriptions.lock().push(topic)
            }),
            Arc::new({
                let unsubs = unsubs.clone();
                move |topic| unsubs.lock().push(topic)
            }),
            Arc::new({
                let events = events.clone();
                move |topic, body| events.lock().push((topic, body))
            }),
            sender,
            StateHandle::new(0),
        );
        let cloned = controller.clone();

        controller.response(ChannelMessage {
            id: Some(1),
            ..Default::default()
        });
        controller.subscribe("a".to_string());
        controller.unsubscribe("a".to_string());
        cloned.event("a".to_string(), json!(7));

        assert_eq!(responses.lock().len(), 1);
        assert_eq!(subscriptions.lock().clone(), vec!["a".to_string()]);
        assert_eq!(unsubs.lock().clone(), vec!["a".to_string()]);
        assert_eq!(events.lock().clone(), vec![("a".to_string(), json!(7))]);
    }
}
