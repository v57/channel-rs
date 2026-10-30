use std::{
    cell::Cell,
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    rc::Rc,
    thread,
    time::{Duration, Instant},
};

use channel_rs::{
    CallResult, Channel, ChannelError, ConnectOptions, PostRequest, StreamNext, StreamRequest,
    SubscribeResult, Subscription,
};
use serde::Serialize;
use serde_json::Value;

#[derive(Default)]
struct TestState;

#[derive(Serialize)]
struct AuthRequest<'a> {
    name: &'a str,
}

/// The TS reference server on 127.0.0.1:2049. An already running one (e.g.
/// a shared fixture) is reused and left alone; otherwise one is spawned and
/// killed when the fixture drops.
struct TsServerFixture {
    child: Option<Child>,
}

impl TsServerFixture {
    fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let address: SocketAddr = "127.0.0.1:2049".parse()?;
        if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
            return Ok(Self { child: None });
        }
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("..")
            .join("channel");
        let mut child = Command::new("bun")
            .arg("tests/server.ts")
            // CHANNEL_TEST_PORT may name the QUIC tests' fixture; this one is 2049
            .env("CHANNEL_TEST_PORT", "2049")
            .current_dir(workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if TcpStream::connect_timeout(&address, Duration::from_millis(50)).is_ok() {
                return Ok(Self { child: Some(child) });
            }
            if let Some(status) = child.try_wait()? {
                return Err(format!("ts server exited early with status: {status}").into());
            }
            thread::sleep(Duration::from_millis(25));
        }

        let _ = child.kill();
        Err("timed out waiting for ts server on 127.0.0.1:2049".into())
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for TsServerFixture {
    fn drop(&mut self) {
        self.stop();
    }
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

fn expect_ok_none<T>(result: CallResult<T>) {
    match result {
        CallResult::Ok(None) => {}
        CallResult::Ok(Some(_)) => panic!("expected empty response"),
        CallResult::RemoteErr(error) => panic!("unexpected remote error: {error}"),
    }
}

fn expect_remote_err<T>(result: CallResult<T>, expected: &str) {
    match result {
        CallResult::RemoteErr(error) => assert_eq!(error, expected),
        CallResult::Ok(_) => panic!("expected remote error"),
    }
}

fn wait_remote_topic(
    client: &mut channel_rs::Client<TestState>,
    topic: &str,
    expected: bool,
    timeout: Duration,
) -> Result<(), ChannelError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if client.is_remote_subscribed(topic) == expected {
            return Ok(());
        }
        let _ = client.poll()?;
        thread::sleep(Duration::from_millis(1));
    }
    Err(ChannelError::new("timeout"))
}

#[test]
fn rust_client_interops_with_typescript_server() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = TsServerFixture::start()?;
    let client_values_sent = Rc::new(Cell::new(0_usize));
    let names = Subscription::<String, String>::new();

    let mut channel = Channel::<TestState>::new();
    channel.post_value("hello", |_request: PostRequest<'_, TestState, (), ()>| {
        Ok("client world".to_string())
    })?;
    channel.stream("stream/values", |mut request: StreamRequest<'_, TestState, (), (), i64>| {
        for value in 0..3_i64 {
            if request.emitter.is_cancelled() {
                return Ok(());
            }
            request.emitter.emit(value)?;
            thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    })?;
    channel.stream("stream/cancel", {
        let client_values_sent = client_values_sent.clone();
        move |mut request: StreamRequest<'_, TestState, (), (), i64>| {
            for value in 0..10_i64 {
                if request.emitter.is_cancelled() {
                    return Ok(());
                }
                request.emitter.emit(value)?;
                client_values_sent.set(client_values_sent.get() + 1);
                thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        }
    })?;
    channel.events([(String::from("names"), names.clone())], "")?;

    let mut client = channel.connect(2049_u16, ConnectOptions::default())?;

    expect_ok_some(client.send::<String, _>("hello", ())?, "world".to_string());
    expect_ok_some(client.send::<f64, _>("echo", 0.1337_f64)?, 0.1337_f64);
    expect_ok_none(client.send::<Value, _>("empty", ())?);
    expect_remote_err(client.send::<Value, _>("notfound", ())?, "api not found");

    match client.subscribe::<String, String, (), _>("notfound", "1".to_string(), (), |_| {})? {
        SubscribeResult::Ok(_) => panic!("expected subscription error"),
        SubscribeResult::RemoteErr(error) => assert_eq!(error, "subscription not found"),
    }

    let mut values = client.values::<(), i64, ()>("stream/values", (), ())?;
    let mut seen = Vec::new();
    loop {
        match values.next()? {
            StreamNext::Item(value) => seen.push(value),
            StreamNext::Done => break,
            StreamNext::RemoteErr(error) => panic!("unexpected stream error: {error}"),
        }
    }
    assert_eq!(seen, vec![0, 1, 2]);

    let mut values = client.values::<(), i64, ()>("mirror/stream/cancel", (), ())?;
    loop {
        match values.next()? {
            StreamNext::Item(value) if value == 2 => break,
            StreamNext::Item(_) => {}
            StreamNext::Done => break,
            StreamNext::RemoteErr(error) => panic!("unexpected stream error: {error}"),
        }
    }
    values.close();
    assert_eq!(client_values_sent.get(), 10);

    expect_ok_some(client.send::<String, _>("mirror", ())?, "client world".to_string());

    let mut values = client.values::<(), i64, ()>("mirror/stream", (), ())?;
    let mut seen = Vec::new();
    loop {
        match values.next()? {
            StreamNext::Item(value) => seen.push(value),
            StreamNext::Done => break,
            StreamNext::RemoteErr(error) => panic!("unexpected stream error: {error}"),
        }
    }
    assert_eq!(seen, vec![0, 1, 2]);

    expect_ok_none(client.send::<Value, _>("mirror/events", ())?);
    wait_remote_topic(&mut client, "names/1", true, Duration::from_secs(2))?;
    names.send("1".to_string(), "h".to_string())?;
    wait_remote_topic(&mut client, "names/1", false, Duration::from_secs(2))?;

    expect_ok_some(
        client.send::<String, _>("auth", AuthRequest { name: "tester" })?,
        "tester".to_string(),
    );
    expect_ok_some(client.send::<String, _>("auth/name", ())?, "tester".to_string());

    let mut second = Channel::<TestState>::new().connect(2049_u16, ConnectOptions::default())?;
    expect_remote_err(second.send::<String, _>("auth/name", ())?, "unauthorized");
    second.stop();

    let _ = client.send::<Value, _>("disconnect", ());
    let deadline = Instant::now() + Duration::from_secs(2);
    while client.is_connected() && Instant::now() < deadline {
        let _ = client.poll()?;
        thread::sleep(Duration::from_millis(1));
    }
    assert!(!client.is_connected());
    client.stop();

    fixture.stop();
    Ok(())
}
