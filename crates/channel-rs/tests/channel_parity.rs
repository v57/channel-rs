use std::{
    cell::Cell,
    collections::BTreeMap,
    rc::Rc,
    thread,
    time::{Duration, Instant},
};

use channel_rs::{
    post_spec, stream_spec, ApiNode, CallResult, Channel, ChannelError, ConnectOptions,
    ListenOptions, PostRequest, StreamNext, StreamRequest, SubscribeResult, Subscription,
};

#[derive(Default)]
struct TestState;

#[derive(Default)]
struct ServerState {
    header_wins: bool,
}

fn expect_ok_some<T>(result: CallResult<T>, expected: T)
where
    T: PartialEq + std::fmt::Debug,
{
    match result {
        CallResult::Ok(Some(value)) => assert_eq!(value, expected),
        CallResult::Ok(None) => panic!("expected response body"),
        CallResult::RemoteErr(error) => panic!("unexpected remote error: {error}"),
    }
}

fn expect_remote_err<T>(result: CallResult<T>, expected: &str) {
    match result {
        CallResult::RemoteErr(error) => assert_eq!(error, expected),
        CallResult::Ok(_) => panic!("expected remote error"),
    }
}

fn wait_until(
    timeout: Duration,
    mut predicate: impl FnMut() -> Result<bool, ChannelError>,
) -> Result<(), ChannelError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate()? {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(1));
    }
    Err(ChannelError::new("timeout"))
}

#[test]
fn process_send_success_and_error() -> Result<(), ChannelError> {
    let mut channel = Channel::<TestState>::new();
    channel.post_value("hello", |_request: PostRequest<'_, TestState, (), ()>| {
        Ok("world".to_string())
    })?;
    channel.post_value("echo", |request: PostRequest<'_, TestState, i64, ()>| {
        Ok(request.body)
    })?;
    channel.post::<(), String, (), _>("fail", |_request: PostRequest<'_, TestState, (), ()>| {
        Err(ChannelError::new("failed"))
    })?;
    channel.stream("stream/values", |mut request: StreamRequest<'_, TestState, (), (), i64>| {
        for value in 0..3_i64 {
            request.emitter.emit(value)?;
            thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    })?;

    let mut process = channel.process(TestState)?;

    expect_ok_some(process.send::<String, _>("hello", ())?, "world".to_string());
    expect_ok_some(process.send::<i64, _>("echo", 42_i64)?, 42_i64);
    expect_remote_err(process.send::<String, _>("fail", ())?, "failed");

    let mut values = process.values::<(), i64, ()>("stream/values", (), ())?;
    let mut seen = Vec::new();
    loop {
        match values.next()? {
            StreamNext::Item(value) => seen.push(value),
            StreamNext::Done => break,
            StreamNext::RemoteErr(error) => panic!("unexpected stream error: {error}"),
        }
    }
    assert_eq!(seen, vec![0, 1, 2]);
    Ok(())
}

#[test]
fn process_subscription_cancel_and_stream_cancel() -> Result<(), ChannelError> {
    let events = Subscription::<String, String>::new();
    let event_count = Rc::new(Cell::new(0_usize));
    let stream_count = Rc::new(Cell::new(0_usize));

    let mut channel = Channel::<TestState>::new();
    channel.events([(String::from("hello"), events.clone())], "")?;
    channel.stream("stream/cancel", {
        let stream_count = stream_count.clone();
        move |mut request: StreamRequest<'_, TestState, (), (), i64>| {
            for value in 0..10_i64 {
                if request.emitter.is_cancelled() {
                    return Ok(());
                }
                request.emitter.emit(value)?;
                stream_count.set(stream_count.get() + 1);
                thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        }
    })?;

    let mut process = channel.process(TestState)?;
    let mut handle = match process.subscribe::<String, String, (), _>(
        "hello",
        "test".to_string(),
        (),
        {
            let event_count = event_count.clone();
            move |event| {
                if event.contains("event") {
                    event_count.set(event_count.get() + 1);
                }
            }
        },
    )? {
        SubscribeResult::Ok(handle) => handle,
        SubscribeResult::RemoteErr(error) => panic!("unexpected subscribe error: {error}"),
    };

    events.send("test".to_string(), "event 1".to_string())?;
    assert_eq!(event_count.get(), 1);

    handle.cancel();
    handle.cancel();
    events.send("test".to_string(), "event 2".to_string())?;
    assert_eq!(event_count.get(), 1);

    let mut values = process.values::<(), i64, ()>("stream/cancel", (), ())?;
    loop {
        match values.next()? {
            StreamNext::Item(value) if value == 2 => break,
            StreamNext::Item(_) => {}
            StreamNext::Done => break,
            StreamNext::RemoteErr(error) => panic!("unexpected stream error: {error}"),
        }
    }
    values.close();
    assert_eq!(stream_count.get(), 10);
    Ok(())
}

#[test]
fn websocket_server_and_client_typed_api() -> Result<(), ChannelError> {
    let disconnect_count = Rc::new(Cell::new(0_usize));
    let updates = Subscription::<String, String>::new();

    let mut merged = Channel::<ServerState>::new();
    merged.post_value("hello", |_request: PostRequest<'_, ServerState, (), ()>| {
        Ok("merged".to_string())
    })?;

    let mut server_channel = Channel::<ServerState>::new();
    server_channel.post_value(
        "state/check",
        |request: PostRequest<'_, ServerState, (), ()>| Ok(request.state.header_wins),
    )?;
    server_channel.post_other(
        |path| path.starts_with("other/post/"),
        |request: PostRequest<'_, ServerState, (), ()>| Ok(Some(request.path)),
    )?;
    server_channel.stream_other(
        |path| path.starts_with("other/stream/"),
        |mut request: StreamRequest<'_, ServerState, (), (), i64>| {
            request.emitter.emit(7)?;
            request.emitter.emit(8)?;
            Ok(())
        },
    )?;
    server_channel.api(BTreeMap::from([(
        String::from("api"),
        ApiNode::Group(BTreeMap::from([
            (
                String::from("hello"),
                post_spec::<ServerState, (), String, (), _>(
                    |_request: PostRequest<'_, ServerState, (), ()>| Ok(Some("api".to_string())),
                ),
            ),
            (
                String::from("numbers"),
                stream_spec::<ServerState, (), i64, (), _>(
                    |mut request: StreamRequest<'_, ServerState, (), (), i64>| {
                        request.emitter.emit(9)?;
                        Ok(())
                    },
                ),
            ),
        ])),
    )]))?;
    server_channel.events([(String::from("updates"), updates.clone())], "")?;
    server_channel.merge(&merged, "merged")?;
    server_channel.on_disconnect({
        let disconnect_count = disconnect_count.clone();
        move |_state, _sender| {
            disconnect_count.set(disconnect_count.get() + 1);
        }
    });

    let mut server = server_channel.listen(
        0_u16,
        ListenOptions {
            state: Some(Rc::new(|headers| {
                Ok(ServerState {
                    header_wins: headers.get("name").map(|value| value == "header").unwrap_or(false),
                })
            })),
            on_connect: None,
            on_disconnect: None,
            ..ListenOptions::default()
        },
    )?;
    assert_ne!(server.port(), 0);

    let mut client = Channel::<TestState>::new().connect(
        format!("ws://127.0.0.1:{}/?name=query", server.port()),
        ConnectOptions {
            state: None,
            headers: Some("name: header\r\n".to_string()),
            ..ConnectOptions::default()
        },
    )?;

    expect_ok_some(client.send::<bool, _>("state/check", ())?, true);
    expect_ok_some(
        client.send::<String, _>("other/post/demo", ())?,
        "other/post/demo".to_string(),
    );
    expect_ok_some(client.send::<String, _>("api/hello", ())?, "api".to_string());
    expect_ok_some(
        client.send::<String, _>("merged/hello", ())?,
        "merged".to_string(),
    );

    let mut values = client.values::<(), i64, ()>("other/stream/demo", (), ())?;
    assert!(matches!(values.next()?, StreamNext::Item(7)));
    assert!(matches!(values.next()?, StreamNext::Item(8)));
    assert!(matches!(values.next()?, StreamNext::Done));

    let mut values = client.values::<(), i64, ()>("api/numbers", (), ())?;
    assert!(matches!(values.next()?, StreamNext::Item(9)));
    assert!(matches!(values.next()?, StreamNext::Done));

    let event_count = Rc::new(Cell::new(0_usize));
    let mut subscription = match client.subscribe::<String, String, (), _>(
        "updates",
        "1".to_string(),
        (),
        {
            let event_count = event_count.clone();
            move |event| {
                if event == "hello" {
                    event_count.set(event_count.get() + 1);
                }
            }
        },
    )? {
        SubscribeResult::Ok(handle) => handle,
        SubscribeResult::RemoteErr(error) => panic!("unexpected subscribe error: {error}"),
    };

    updates.send("1".to_string(), "hello".to_string())?;
    wait_until(Duration::from_secs(2), || {
        let _ = server.poll()?;
        let _ = client.poll()?;
        Ok(event_count.get() == 1)
    })?;
    assert_eq!(event_count.get(), 1);

    subscription.cancel();
    client.stop();
    client.stop();
    wait_until(Duration::from_secs(2), || {
        let _ = server.poll()?;
        let _ = client.poll()?;
        Ok(disconnect_count.get() == 1)
    })?;
    assert_eq!(disconnect_count.get(), 1);

    server.stop();
    server.stop();
    Ok(())
}

#[test]
fn websocket_client_handles_serial_sends_without_writer_thread() -> Result<(), ChannelError> {
    let mut server_channel = Channel::<ServerState>::new();
    server_channel.post_value("echo", |request: PostRequest<'_, ServerState, i64, ()>| {
        Ok(request.body)
    })?;
    let mut server = server_channel.listen(0_u16, ListenOptions::default())?;

    let mut client = Channel::<TestState>::new().connect(
        format!("ws://127.0.0.1:{}/", server.port()),
        ConnectOptions::default(),
    )?;

    for expected in 0_i64..256_i64 {
        expect_ok_some(client.send::<i64, _>("echo", expected)?, expected);
    }

    client.stop();
    server.stop();
    Ok(())
}
