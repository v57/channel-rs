//! QUIC transport tests: frames, a Rust client against a Rust server, the wire rules with a raw quinn client,
//! certificate trust, disconnects, and interop both ways with the TS reference (`../channel`).

use std::{
    cell::Cell,
    collections::HashMap,
    future::Future,
    io::Read,
    net::{Ipv4Addr, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    rc::Rc,
    sync::mpsc as std_mpsc,
    thread,
    time::{Duration, Instant},
};

use serde_json::{Value, json};

use super::*;
use crate::channel::{DEFAULT_MAX_PAYLOAD, parse_connect_target, pump_all};
use crate::{CallResult, PostRequest, StreamNext, StreamRequest, SubscribeResult, Subscription};

/// The TS reference's test certificate: localhost, 127.0.0.1 and ::1.
const FINGERPRINT: &str = "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B";

fn reference_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../channel")
}

fn read_pem(name: &str) -> Vec<u8> {
    let path = reference_dir().join("tests/quic").join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("the TS reference's {}: {error}", path.display()))
}

/// Pumps this thread's transports until `ready`, failing after 5 s.
fn wait_for(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out");
        if !pump_all() {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

fn pump_for(duration: Duration) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if !pump_all() {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Runs `future` on the QUIC runtime while this thread pumps (so a server on it answers).
fn drive<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) -> T {
    let (tx, rx) = std_mpsc::channel();
    runtime().spawn(async move {
        let _ = tx.send(future.await);
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(value) = rx.try_recv() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out");
        if !pump_all() {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

fn ok<T>(result: Result<CallResult<T>, ChannelError>) -> Option<T> {
    match result {
        Ok(CallResult::Ok(value)) => value,
        Ok(CallResult::RemoteErr(error)) => panic!("remote error: {error}"),
        Err(error) => panic!("error: {error}"),
    }
}

fn collect<T: serde::de::DeserializeOwned + 'static>(mut values: crate::Values<T>) -> Vec<T> {
    let mut seen = Vec::new();
    loop {
        match values.next().expect("next") {
            StreamNext::Item(value) => seen.push(value),
            StreamNext::Done => return seen,
            StreamNext::RemoteErr(error) => panic!("stream error: {error}"),
        }
    }
}

#[derive(Default)]
struct TestState {
    headers: HashMap<String, String>,
}

struct Fixture {
    server: QuicServer<TestState>,
    ticks: Subscription<String, String>,
    cert: Vec<u8>,
    connected: Rc<Cell<usize>>,
    disconnected: Rc<Cell<usize>>,
}

impl Fixture {
    fn ca(&self) -> QuicOptions {
        QuicOptions {
            ca: Some(self.cert.clone()),
            ..QuicOptions::default()
        }
    }
}

fn server_channel(ticks: &Subscription<String, String>) -> Channel<TestState> {
    let mut channel = Channel::new();
    channel
        .post_value("hello", |_: PostRequest<'_, TestState, (), ()>| Ok("world"))
        .unwrap()
        .post_value("echo", |request: PostRequest<'_, TestState, Value, ()>| Ok(request.body))
        .unwrap()
        .post_value("name", |request: PostRequest<'_, TestState, (), ()>| {
            Ok(request.state.headers.get("x-name").cloned())
        })
        .unwrap()
        .post("mirror", |request: PostRequest<'_, TestState, (), ()>| {
            match request.sender.send::<String, _>("hello", ())? {
                CallResult::Ok(value) => Ok(value),
                CallResult::RemoteErr(error) => Err(ChannelError::new(error)),
            }
        })
        .unwrap()
        .post("disconnect", |request: PostRequest<'_, TestState, (), ()>| {
            request.sender.stop();
            Ok(None::<()>)
        })
        .unwrap()
        .stream("count", |mut request: StreamRequest<'_, TestState, (), (), i64>| {
            for value in 0..3 {
                request.emitter.emit(value)?;
            }
            Ok(())
        })
        .unwrap();
    let tick = ticks.clone();
    channel
        .post("tick", move |_: PostRequest<'_, TestState, (), ()>| {
            tick.send("a".to_string(), "tick".to_string())?;
            Ok(None::<()>)
        })
        .unwrap()
        .events([(String::from("ticks"), ticks.clone())], "")
        .unwrap();
    channel
}

fn listen(address: &str) -> Fixture {
    let ticks = Subscription::new();
    let connected = Rc::new(Cell::new(0));
    let disconnected = Rc::new(Cell::new(0));
    let cert = read_pem("cert.pem");
    let mut options = QuicListenOptions::new(cert.clone(), read_pem("key.pem"));
    options.options.state = Some(Rc::new(|headers: &HashMap<String, String>| {
        Ok(TestState {
            headers: headers.clone(),
        })
    }));
    let count = connected.clone();
    options.options.on_connect = Some(Rc::new(move |_: &mut TestState, _| count.set(count.get() + 1)));
    let count = disconnected.clone();
    options.options.on_disconnect = Some(Rc::new(move |_: &mut TestState, _| count.set(count.get() + 1)));
    let server = server_channel(&ticks).listen_quic(address, options).expect("listen_quic");
    Fixture {
        server,
        ticks,
        cert,
        connected,
        disconnected,
    }
}

fn client_channel() -> Channel<TestState> {
    let mut channel = Channel::new();
    channel
        .post_value("hello", |_: PostRequest<'_, TestState, (), ()>| Ok("client world"))
        .unwrap();
    channel
}

fn connect_options(quic: QuicOptions, headers: Option<&str>) -> ConnectOptions<TestState> {
    ConnectOptions {
        headers: headers.map(str::to_string),
        handshake_timeout: Duration::from_secs(5),
        quic,
        ..ConnectOptions::default()
    }
}

fn connect(port: u16, quic: QuicOptions) -> Result<Client<TestState>, ChannelError> {
    client_channel().connect(format!("quic://127.0.0.1:{port}"), connect_options(quic, None))
}

#[test]
fn frames_and_varints() {
    for size in [0_usize, 1, 63, 64, 16383, 16384, 70000] {
        let payload = vec![b'x'; size];
        let mut frame = Vec::new();
        encode_quic_frame(&mut frame, &payload);
        let header = frame.len() - size;
        let want = if size < 64 {
            1
        } else if size < 16384 {
            2
        } else {
            4
        };
        assert_eq!(header, want, "size {size}");

        // A keepalive in front is skipped; byte by byte, the frames still come out whole
        let mut stream = vec![0_u8];
        stream.extend_from_slice(&frame);
        encode_quic_frame(&mut stream, b"next");
        let mut decoder = FrameDecoder::new(DEFAULT_MAX_PAYLOAD);
        let mut frames = Vec::new();
        for byte in &stream {
            decoder.push(std::slice::from_ref(byte));
            while let Some(frame) = decoder.next_frame().unwrap() {
                frames.push(frame);
            }
        }
        let mut want = Vec::new();
        if size > 0 {
            want.push(payload.clone());
        }
        want.push(b"next".to_vec());
        assert_eq!(frames, want, "size {size}");
    }

    // 8-byte lengths decode too, and anything above the limit is refused from the length alone
    let mut decoder = FrameDecoder::new(DEFAULT_MAX_PAYLOAD);
    decoder.push(&[0xc0, 0, 0, 0, 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o']);
    assert_eq!(decoder.next_frame().unwrap(), Some(b"hello".to_vec()));
    let mut big = Vec::new();
    encode_quic_frame(&mut big, &vec![b'x'; 1 << 30]);
    assert_eq!(&big[..8], &(((1_u64 << 30) | 0xc000_0000_0000_0000).to_be_bytes()));
    let mut decoder = FrameDecoder::new(10);
    let mut frame = Vec::new();
    encode_quic_frame(&mut frame, &[b'x'; 11]);
    decoder.push(&frame[..1]);
    assert!(decoder.next_frame().is_err());
    let mut decoder = FrameDecoder::new(DEFAULT_MAX_PAYLOAD);
    decoder.push(&[0x80 | 0x01, 0, 0, 1]);
    assert!(decoder.next_frame().is_err());
}

#[test]
fn hello_frames() {
    let payload = hello_payload(Some("X-Name: rust\r\nAuthorization: Bearer a:b\r\n")).unwrap();
    let value: Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(value, json!({"hello": {"X-Name": "rust", "Authorization": "Bearer a:b"}}));
    assert_eq!(hello_payload(None).unwrap(), br#"{"hello":{}}"#);

    let headers = parse_hello(br#"{"hello":{"X-Name":"raw","ignored":1}}"#).unwrap();
    assert_eq!(headers, HashMap::from([("x-name".to_string(), "raw".to_string())]));
    assert!(parse_hello(br#"{"id":1,"path":"hello"}"#).is_none());
    assert!(parse_hello(br#"{"hello":"x"}"#).is_none());
    assert!(parse_hello(b"{nope").is_none());
}

#[test]
fn addresses_and_fingerprints() {
    let target = parse_connect_target("quic://127.0.0.1:4433").unwrap();
    assert!(target.quic && target.host == "127.0.0.1" && target.port == 4433);
    let target = parse_connect_target("quic://[::1]:4433").unwrap();
    assert!(target.quic && target.host == "::1");
    assert!(parse_connect_target("quic://example.com").is_err());
    assert!(!parse_connect_target("ws://127.0.0.1:1").unwrap().quic);

    let pin = parse_fingerprint(FINGERPRINT).unwrap();
    assert_eq!(parse_fingerprint(&FINGERPRINT.replace(':', "").to_lowercase()).unwrap(), pin);
    assert_eq!(pin[0], 0xdd);
    assert!(parse_fingerprint("nope").is_err());
    assert!(parse_fingerprint(&"0".repeat(62)).is_err());
}

#[test]
fn client_and_server() {
    let mut fixture = listen("127.0.0.1:0");
    let mut client = client_channel()
        .connect(
            format!("quic://127.0.0.1:{}", fixture.server.port()),
            connect_options(fixture.ca(), Some("X-Name: rust")),
        )
        .expect("connect");
    assert!(client.is_connected());

    // post
    assert_eq!(ok(client.send::<String, _>("hello", ())), Some("world".to_string()));
    let body = json!({"list": [1, "two", null], "text": "é".repeat(20000)});
    assert_eq!(ok(client.send::<Value, _>("echo", body.clone())), Some(body));
    // hello headers become the state, names lowercased
    assert_eq!(ok(client.send::<String, _>("name", ())), Some("rust".to_string()));
    // the server calls the client back
    assert_eq!(ok(client.send::<String, _>("mirror", ())), Some("client world".to_string()));
    // stream
    assert_eq!(collect(client.values::<(), i64, ()>("count", (), ()).unwrap()), vec![0, 1, 2]);

    // events
    let count = Rc::new(Cell::new(0));
    let events = count.clone();
    let SubscribeResult::Ok(mut subscription) = client
        .subscribe::<String, String, (), _>("ticks", "a".to_string(), (), move |_| events.set(events.get() + 1))
        .unwrap()
    else {
        panic!("subscribe failed");
    };
    fixture.ticks.send("a".to_string(), "tick".to_string()).unwrap();
    fixture.ticks.send("b".to_string(), "not for us".to_string()).unwrap();
    fixture.ticks.send("a".to_string(), "tick".to_string()).unwrap();
    wait_for(|| count.get() == 2);
    subscription.cancel();
    pump_for(Duration::from_millis(50));
    fixture.ticks.send("a".to_string(), "after cancel".to_string()).unwrap();
    pump_for(Duration::from_millis(50));
    assert_eq!(count.get(), 2);

    // many in flight
    let requests = (0..2000_i64)
        .map(|index| client.request::<i64, _>("echo", index).unwrap())
        .collect::<Vec<_>>();
    for (index, request) in requests.into_iter().enumerate() {
        assert_eq!(ok(request.response()), Some(index as i64));
    }

    assert_eq!(fixture.connected.get(), 1);
    client.stop();
    assert!(!client.is_connected());
    wait_for(|| fixture.disconnected.get() == 1);
    fixture.server.stop();
}

#[test]
fn certificate_trust() {
    let mut fixture = listen("127.0.0.1:0");
    let port = fixture.server.port();

    let pin = FINGERPRINT.replace(':', "").to_lowercase();
    let mut pinned = connect(
        port,
        QuicOptions {
            fingerprint: Some(pin),
            ..QuicOptions::default()
        },
    )
    .expect("pinned");
    assert_eq!(ok(pinned.send::<String, _>("hello", ())), Some("world".to_string()));
    let mut by_name = connect(
        port,
        QuicOptions {
            server_name: Some("localhost".to_string()),
            ..fixture.ca()
        },
    )
    .expect("server name");
    assert_eq!(ok(by_name.send::<String, _>("hello", ())), Some("world".to_string()));

    for (name, options) in [
        ("system roots", QuicOptions::default()),
        (
            "wrong pin",
            QuicOptions {
                fingerprint: Some("00".repeat(32)),
                ..QuicOptions::default()
            },
        ),
        (
            "name not in the certificate",
            QuicOptions {
                server_name: Some("example.com".to_string()),
                ..fixture.ca()
            },
        ),
    ] {
        assert!(connect(port, options).is_err(), "{name}: connected");
    }
    let invalid = QuicOptions {
        fingerprint: Some("nope".to_string()),
        ..QuicOptions::default()
    };
    assert!(connect(port, invalid).is_err(), "an invalid fingerprint is accepted");
    let not_pem = QuicOptions {
        ca: Some(b"not a certificate".to_vec()),
        ..QuicOptions::default()
    };
    assert!(connect(port, not_pem).is_err(), "a CA without a certificate is accepted");
    let options = QuicListenOptions::<TestState>::new(b"nope".to_vec(), read_pem("key.pem"));
    assert!(Channel::<TestState>::new().listen_quic("127.0.0.1:0", options).is_err());

    pinned.stop();
    by_name.stop();
    fixture.server.stop();
}

async fn raw_session(port: u16, cert: Vec<u8>) -> (Endpoint, quinn::Connection, SendStream, RecvStream) {
    let config = client_config(&QuicOptions {
        ca: Some(cert),
        ..QuicOptions::default()
    })
    .unwrap();
    let endpoint = Endpoint::client((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let connection = endpoint
        .connect_with(config, (Ipv4Addr::LOCALHOST, port).into(), "localhost")
        .unwrap()
        .await
        .unwrap();
    let (send, recv) = connection.open_bi().await.unwrap();
    (endpoint, connection, send, recv)
}

fn frames(payloads: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for payload in payloads {
        encode_quic_frame(&mut out, payload);
    }
    out
}

#[test]
fn wire_rules() {
    let mut fixture = listen("127.0.0.1:0");
    let port = fixture.server.port();

    // Raw frames: the hello's headers, a keepalive, then a batch
    let cert = fixture.cert.clone();
    let responses = drive(async move {
        let (_endpoint, _connection, mut send, mut recv) = raw_session(port, cert).await;
        let mut data = frames(&[br#"{"hello":{"X-Name":"raw","ignored":1}}"#]);
        data.push(0);
        encode_quic_frame(&mut data, br#"[{"id":1,"path":"name"},{"id":2,"path":"hello"}]"#);
        send.write_all(&data).await.unwrap();
        let mut decoder = FrameDecoder::new(DEFAULT_MAX_PAYLOAD);
        let mut got = HashMap::new();
        while got.len() < 2 {
            let frame = read_frame(&mut recv, &mut decoder).await.unwrap().expect("a response");
            let messages = match serde_json::from_slice::<Value>(&frame).unwrap() {
                Value::Array(messages) => messages,
                message => vec![message],
            };
            for message in messages {
                got.insert(message["id"].as_i64().unwrap(), message["body"].clone());
            }
        }
        got
    });
    assert_eq!(responses[&1], json!("raw"));
    assert_eq!(responses[&2], json!("world"));

    // One stream per connection: no second one, no unidirectional ones
    let cert = fixture.cert.clone();
    let (second, unidirectional) = drive(async move {
        let (_endpoint, connection, mut send, _recv) = raw_session(port, cert).await;
        send.write_all(&frames(&[br#"{"hello":{}}"#])).await.unwrap();
        let wait = Duration::from_millis(300);
        let second = tokio::time::timeout(wait, connection.open_bi()).await;
        let unidirectional = tokio::time::timeout(wait, connection.open_uni()).await;
        (second.is_ok(), unidirectional.is_ok())
    });
    assert!(!second, "a second stream opened");
    assert!(!unidirectional, "a unidirectional stream opened");

    let mut oversize = frames(&[br#"{"hello":{}}"#]);
    oversize.extend_from_slice(&[0x80 | 0x01, 0, 0, 1]);
    for (name, first) in [
        ("a first frame that isn't a hello closes", frames(&[br#"{"id":1,"path":"hello"}"#])),
        ("invalid JSON closes", frames(&[br#"{"hello":{}}"#, b"{nope"])),
        ("a frame over the limit closes", oversize),
    ] {
        let cert = fixture.cert.clone();
        let closed = drive(async move {
            let (_endpoint, connection, mut send, _recv) = raw_session(port, cert).await;
            let _ = send.write_all(&first).await;
            tokio::time::timeout(Duration::from_secs(2), connection.closed()).await.is_ok()
        });
        assert!(closed, "{name}: still open");
    }
    fixture.server.stop();
}

#[test]
fn disconnects() {
    let mut fixture = listen("127.0.0.1:0");
    let port = fixture.server.port();

    // The server drops the connection: the call fails and the client is disconnected (this port's clients
    // don't reconnect, over WebSocket neither)
    let mut client = connect(port, fixture.ca()).expect("connect");
    assert_eq!(ok(client.send::<String, _>("hello", ())), Some("world".to_string()));
    assert!(matches!(client.send::<Value, _>("disconnect", ()), Ok(CallResult::RemoteErr(_))));
    wait_for(|| !client.is_connected());
    wait_for(|| fixture.disconnected.get() == 1);
    assert!(client.send::<String, _>("hello", ()).is_err(), "sent after the drop");

    // A stopped client fails what is sent after
    let mut client = connect(port, fixture.ca()).expect("connect");
    client.stop();
    assert!(client.send::<String, _>("hello", ()).is_err(), "sent after stop");
    wait_for(|| fixture.disconnected.get() == 2);
    fixture.server.stop();

    // Nothing listens there
    let started = Instant::now();
    let options = ConnectOptions {
        handshake_timeout: Duration::from_millis(500),
        quic: fixture.ca(),
        ..ConnectOptions::default()
    };
    assert!(client_channel().connect("quic://127.0.0.1:1", options).is_err());
    assert!(started.elapsed() < Duration::from_secs(3));
}

enum ServerCommand {
    Pause,
    Stop,
}

/// A request in flight when the server goes away fails. The server runs on its own thread, paused so the
/// request stays unanswered.
#[test]
fn server_stop_fails_requests_in_flight() {
    let (port_tx, port_rx) = std_mpsc::channel();
    let (commands, command_rx) = std_mpsc::channel();
    let server_thread = thread::spawn(move || {
        let mut fixture = listen("127.0.0.1:0");
        port_tx.send((fixture.server.port(), fixture.cert.clone())).unwrap();
        loop {
            match command_rx.try_recv() {
                Ok(ServerCommand::Pause) => {
                    while let Ok(command) = command_rx.recv() {
                        if matches!(command, ServerCommand::Stop) {
                            break;
                        }
                    }
                    fixture.server.stop();
                    return;
                }
                Ok(ServerCommand::Stop) | Err(std_mpsc::TryRecvError::Disconnected) => {
                    fixture.server.stop();
                    return;
                }
                Err(std_mpsc::TryRecvError::Empty) => {
                    if !fixture.server.poll().unwrap_or(false) {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }
    });
    let (port, cert) = port_rx.recv().unwrap();
    let mut client = connect(
        port,
        QuicOptions {
            ca: Some(cert),
            ..QuicOptions::default()
        },
    )
    .expect("connect");
    assert_eq!(ok(client.send::<String, _>("hello", ())), Some("world".to_string()));
    commands.send(ServerCommand::Pause).unwrap();
    thread::sleep(Duration::from_millis(20));
    let request = client.request::<String, _>("hello", ()).unwrap();
    pump_for(Duration::from_millis(50));
    assert!(!request.is_ready());
    commands.send(ServerCommand::Stop).unwrap();
    server_thread.join().unwrap();
    assert!(matches!(request.response(), Ok(CallResult::RemoteErr(_))));
    assert!(!client.is_connected());
}

/// The TS reference's fixture on `CHANNEL_TEST_PORT` (2101 by default): reused when it runs, else started
/// and stopped with the test. It serves QUIC on UDP next to its WebSocket on TCP.
struct TsFixture {
    port: u16,
    child: Option<Child>,
}

impl TsFixture {
    fn start() -> Self {
        let port = std::env::var("CHANNEL_TEST_PORT")
            .ok()
            .and_then(|port| port.parse().ok())
            .unwrap_or(2101);
        let address = (Ipv4Addr::LOCALHOST, port).into();
        if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
            return Self { port, child: None };
        }
        let mut child = Command::new("bun")
            .arg("tests/server.ts")
            .env("CHANNEL_TEST_PORT", port.to_string())
            .current_dir(reference_dir())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("bun tests/server.ts");
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect_timeout(&address, Duration::from_millis(50)).is_err() {
            if let Ok(Some(status)) = child.try_wait() {
                panic!("the TS fixture exited: {status}");
            }
            assert!(Instant::now() < deadline, "the TS fixture didn't start on {port}");
            thread::sleep(Duration::from_millis(25));
        }
        Self {
            port,
            child: Some(child),
        }
    }
}

impl Drop for TsFixture {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn interop_typescript_server() {
    let fixture = TsFixture::start();
    let names = Subscription::<String, String>::new();
    let mut channel = client_channel();
    channel
        .stream("stream/values", |mut request: StreamRequest<'_, TestState, (), (), i64>| {
            for value in 0..3 {
                request.emitter.emit(value)?;
            }
            Ok(())
        })
        .unwrap()
        .events([(String::from("names"), names.clone())], "")
        .unwrap();
    let address = format!("quic://127.0.0.1:{}", fixture.port);
    let ca = QuicOptions {
        ca: Some(read_pem("cert.pem")),
        ..QuicOptions::default()
    };
    // The fixture opens its TCP port just before its QUIC one
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut client = loop {
        match channel.connect(address.as_str(), connect_options(ca.clone(), Some("X-Port: rust"))) {
            Ok(client) => break client,
            Err(error) if Instant::now() > deadline => panic!("connect to the TS fixture: {error}"),
            Err(_) => thread::sleep(Duration::from_millis(100)),
        }
    };

    assert_eq!(ok(client.send::<String, _>("hello", ())), Some("world".to_string()));
    let body = json!({"a": [1, "b"]});
    assert_eq!(ok(client.send::<Value, _>("echo", body.clone())), Some(body));
    assert_eq!(ok(client.send::<String, _>("mirror", ())), Some("client world".to_string()));
    assert_eq!(ok(client.send::<String, _>("auth", json!({"name": "quic"}))), Some("quic".to_string()));
    assert_eq!(ok(client.send::<String, _>("auth/name", ())), Some("quic".to_string()));
    let headers = ok(client.send::<Value, _>("v2/headers", ())).expect("headers");
    assert_eq!(headers["x-port"], json!("rust"));
    assert_eq!(collect(client.values::<(), i64, ()>("mirror/stream", (), ()).unwrap()), vec![0, 1, 2]);

    // The fixture subscribes to this client's names/1 and cancels after the first event
    assert_eq!(ok(client.send::<Value, _>("mirror/events", ())), None);
    wait_for(|| client.is_remote_subscribed("names/1"));
    names.send("1".to_string(), "h".to_string()).unwrap();
    wait_for(|| !client.is_remote_subscribed("names/1"));

    assert!(matches!(client.send::<Value, _>("v2/error/code", ()), Ok(CallResult::RemoteErr(_))));

    // Pinned, the fingerprint as openssl prints it
    let pin = QuicOptions {
        fingerprint: Some(FINGERPRINT.to_string()),
        ..QuicOptions::default()
    };
    let mut pinned = client_channel().connect(address.as_str(), connect_options(pin, None)).expect("pinned");
    assert_eq!(ok(pinned.send::<String, _>("hello", ())), Some("world".to_string()));
    pinned.stop();

    // The fixture drops the connection
    let _ = client.send::<Value, _>("disconnect", ());
    wait_for(|| !client.is_connected());
    client.stop();
}

/// Serves QUIC from Rust and calls it with the TS reference's client (`tests/quic/client.ts`).
#[test]
fn interop_typescript_client() {
    let mut fixture = listen("127.0.0.1:0");
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/quic/client.ts");
    let cert = reference_dir().join("tests/quic/cert.pem");
    let mut child = match Command::new("bun")
        .arg(&script)
        .arg(fixture.server.port().to_string())
        .arg(&cert)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            eprintln!("skipped: bun isn't available: {error}");
            return;
        }
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("the TS client didn't finish");
        }
        if !fixture.server.poll().unwrap() {
            thread::sleep(Duration::from_millis(1));
        }
    };
    let mut output = String::new();
    let mut errors = String::new();
    child.stdout.take().unwrap().read_to_string(&mut output).unwrap();
    child.stderr.take().unwrap().read_to_string(&mut errors).unwrap();
    assert!(status.success(), "bun: {status}\n{errors}");
    let result: Value = serde_json::from_str(output.trim()).unwrap_or_else(|error| panic!("{output:?}: {error}"));
    assert_eq!(
        result,
        json!({
            "hello": "world",
            "echo": {"a": [1, "b"]},
            "name": "ts",
            "mirror": "ts world",
            "values": [0, 1, 2],
            "event": "tick",
        })
    );
    wait_for(|| fixture.connected.get() == 1 && fixture.disconnected.get() == 1);
    fixture.server.stop();
}
