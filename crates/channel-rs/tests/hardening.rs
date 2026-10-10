// Regression tests for the AUDIT.md / AUDIT2.md findings: hostile frames,
// size caps, fragmentation, id/body handling, random masks, nested calls,
// pump recovery, timeouts and Subscription formatters.
//
// Every test that opens a socket uses its own fixed port in 6200-6299.
use std::{
    cell::RefCell,
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use channel_rs::{
    CallResult, Channel, ChannelError, ConnectOptions, ListenOptions, PostRequest, SubscribeResult,
    Subscription,
};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};

#[derive(Default)]
struct S;

// ---------------------------------------------------------------- helpers

/// Runs a channel-rs server on its own thread, polling until the guard drops.
struct ServerThread {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Drop for ServerThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn spawn_server(port: u16) -> ServerThread {
    let stop = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::channel();
    let flag = stop.clone();
    let handle = thread::spawn(move || {
        let mut channel = Channel::<S>::new();
        channel
            .post_value("echo", |request: PostRequest<'_, S, i64, ()>| Ok(request.body))
            .unwrap();
        channel
            .post_value("any", |request: PostRequest<'_, S, Value, ()>| Ok(request.body))
            .unwrap();
        let mut server = channel.listen(port, ListenOptions::default()).unwrap();
        ready_tx.send(()).unwrap();
        while !flag.load(Ordering::SeqCst) {
            if !server.poll().unwrap_or(true) {
                thread::sleep(Duration::from_micros(200));
            }
        }
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    ServerThread {
        stop,
        handle: Some(handle),
    }
}

fn accept_key(key: &str) -> String {
    let mut sha = Sha1::new();
    sha.update(key.as_bytes());
    sha.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    BASE64.encode(sha.finalize())
}

fn read_head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(1) => head.push(byte[0]),
            _ => break,
        }
    }
    String::from_utf8_lossy(&head).into_owned()
}

/// A raw WebSocket client: handshake done, blocking with a read timeout.
fn raw_client(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream
        .write_all(
            b"GET / HTTP/1.1\r\nhost: 127.0.0.1\r\nupgrade: websocket\r\nconnection: upgrade\r\n\
sec-websocket-version: 13\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        )
        .unwrap();
    let head = read_head(&mut stream);
    assert!(head.starts_with("HTTP/1.1 101"), "handshake failed: {head:?}");
    stream
}

fn frame(fin: bool, opcode: u8, payload: &[u8], masked: bool) -> Vec<u8> {
    let mut out = vec![(if fin { 0x80 } else { 0 }) | opcode];
    let mask_bit = if masked { 0x80 } else { 0 };
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
        let mask = [1_u8, 2, 3, 4];
        out.extend_from_slice(&mask);
        out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i & 3]));
    } else {
        out.extend_from_slice(payload);
    }
    out
}

/// Reads one unmasked server frame; `None` when the connection is closed.
fn read_frame(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut header = [0_u8; 2];
    stream.read_exact(&mut header).ok()?;
    let len = match header[1] & 0x7F {
        126 => {
            let mut b = [0_u8; 2];
            stream.read_exact(&mut b).ok()?;
            u16::from_be_bytes(b) as usize
        }
        127 => {
            let mut b = [0_u8; 8];
            stream.read_exact(&mut b).ok()?;
            u64::from_be_bytes(b) as usize
        }
        n => n as usize,
    };
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).ok()?;
    Some((header[0] & 0x0F, payload))
}

fn read_json(stream: &mut TcpStream) -> Option<Value> {
    loop {
        let (opcode, payload) = read_frame(stream)?;
        match opcode {
            0x1 => return serde_json::from_slice(&payload).ok(),
            0x8 => return None,
            _ => {}
        }
    }
}

/// True when the peer closed the socket (EOF, reset or a close frame).
fn is_closed(stream: &mut TcpStream) -> bool {
    let mut buf = [0_u8; 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return true,
            // An HTTP error response or a close frame before EOF.
            Ok(_) => continue,
            Err(error) if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::TimedOut => {
                return false;
            }
            Err(_) => return true,
        }
    }
}

fn send_json(stream: &mut TcpStream, value: Value) {
    stream
        .write_all(&frame(true, 0x1, value.to_string().as_bytes(), true))
        .unwrap();
}

fn assert_server_alive(port: u16) {
    let mut stream = raw_client(port);
    send_json(&mut stream, json!({"id": 1, "path": "echo", "body": 5}));
    assert_eq!(read_json(&mut stream), Some(json!({"id": 1, "body": 5})));
}

/// A raw server that completes one handshake, then runs `after` on the socket.
fn raw_server<F>(port: u16, after: F) -> thread::JoinHandle<()>
where
    F: FnOnce(TcpStream) + Send + 'static,
{
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let head = read_head(&mut stream);
        let key = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("sec-websocket-key").then(|| value.trim().to_string())
            })
            .unwrap();
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
            accept_key(&key)
        );
        stream.write_all(response.as_bytes()).unwrap();
        after(stream);
    })
}

/// Reads one masked client frame; returns (mask, unmasked payload).
fn read_client_frame(stream: &mut TcpStream) -> ([u8; 4], Vec<u8>) {
    let mut header = [0_u8; 2];
    stream.read_exact(&mut header).unwrap();
    assert!(header[1] & 0x80 != 0, "client frames must be masked");
    let len = match header[1] & 0x7F {
        126 => {
            let mut b = [0_u8; 2];
            stream.read_exact(&mut b).unwrap();
            u16::from_be_bytes(b) as usize
        }
        127 => {
            let mut b = [0_u8; 8];
            stream.read_exact(&mut b).unwrap();
            u64::from_be_bytes(b) as usize
        }
        n => n as usize,
    };
    let mut mask = [0_u8; 4];
    stream.read_exact(&mut mask).unwrap();
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).unwrap();
    for (i, b) in payload.iter_mut().enumerate() {
        *b ^= mask[i & 3];
    }
    (mask, payload)
}

fn run_with_deadline<T: Send + 'static>(seconds: u64, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(seconds))
        .unwrap_or_else(|_| panic!("did not finish within {seconds}s (deadlock?)"))
}

// ------------------------------------------------------------ server side

#[test]
fn server_survives_frame_length_2_pow_64_minus_1() {
    let _server = spawn_server(6201);
    let mut stream = raw_client(6201);
    let mut bytes = vec![0x81, 0xFF];
    bytes.extend_from_slice(&u64::MAX.to_be_bytes());
    bytes.extend_from_slice(&[1, 2, 3, 4, b'x']);
    stream.write_all(&bytes).unwrap();
    assert!(is_closed(&mut stream), "hostile connection should be closed");
    assert_server_alive(6201);
}

#[test]
fn server_closes_oversized_declared_frame_without_buffering() {
    let _server = spawn_server(6202);
    let mut stream = raw_client(6202);
    let mut bytes = vec![0x81, 0xFF];
    bytes.extend_from_slice(&(1_u64 << 30).to_be_bytes());
    bytes.extend_from_slice(&[1, 2, 3, 4]);
    stream.write_all(&bytes).unwrap();
    let started = Instant::now();
    assert!(is_closed(&mut stream));
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_server_alive(6202);
}

#[test]
fn server_caps_handshake_size() {
    let _server = spawn_server(6203);
    let mut stream = TcpStream::connect(("127.0.0.1", 6203)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let _ = stream.write_all(b"GET / HTTP/1.1\r\n");
    let line = format!("x-filler: {}\r\n", "a".repeat(1000));
    for _ in 0..100 {
        if stream.write_all(line.as_bytes()).is_err() {
            break;
        }
    }
    assert!(is_closed(&mut stream), "a never-ending handshake must be cut off");
    assert_server_alive(6203);
}

#[test]
fn server_accepts_fragmented_messages() {
    let _server = spawn_server(6204);
    let mut stream = raw_client(6204);
    let text = br#"{"id":1,"path":"echo","body":7}"#;
    let mut bytes = frame(false, 0x1, &text[..10], true);
    bytes.extend(frame(true, 0x9, b"ping", true)); // control frames may interleave
    bytes.extend(frame(false, 0x0, &text[10..20], true));
    bytes.extend(frame(true, 0x0, &text[20..], true));
    stream.write_all(&bytes).unwrap();
    // The pong comes first, then the reply.
    assert_eq!(read_frame(&mut stream), Some((0xA, b"ping".to_vec())));
    assert_eq!(read_json(&mut stream), Some(json!({"id": 1, "body": 7})));
}

#[test]
fn server_echoes_non_numeric_ids() {
    let _server = spawn_server(6205);
    let mut stream = raw_client(6205);
    send_json(&mut stream, json!({"id": "abc", "path": "echo", "body": 1}));
    assert_eq!(read_json(&mut stream), Some(json!({"id": "abc", "body": 1})));
    send_json(&mut stream, json!({"id": 1.5, "path": "nope"}));
    assert_eq!(read_json(&mut stream), Some(json!({"id": 1.5, "error": "api not found"})));
    // Wrong-typed routing fields are lookups that fail, not fatal errors.
    send_json(&mut stream, json!({"id": 2, "path": 42}));
    assert_eq!(read_json(&mut stream), Some(json!({"id": 2, "error": "api not found"})));
    send_json(&mut stream, json!({"id": 3, "path": "echo", "body": 3}));
    assert_eq!(read_json(&mut stream), Some(json!({"id": 3, "body": 3})));
}

#[test]
fn server_answers_wrong_typed_body_with_error() {
    let _server = spawn_server(6206);
    let mut stream = raw_client(6206);
    send_json(&mut stream, json!({"id": 1, "path": "echo", "body": "not a number"}));
    assert_eq!(read_json(&mut stream), Some(json!({"id": 1, "error": "invalid body"})));
    // The connection is still usable.
    send_json(&mut stream, json!({"id": 2, "path": "echo", "body": 9}));
    assert_eq!(read_json(&mut stream), Some(json!({"id": 2, "body": 9})));
    // A missing body is `null`, which `Value` accepts.
    send_json(&mut stream, json!({"id": 3, "path": "any"}));
    assert_eq!(read_json(&mut stream), Some(json!({"id": 3, "body": null})));
}

#[test]
fn server_closes_connection_on_invalid_json_only() {
    let _server = spawn_server(6207);
    let mut stream = raw_client(6207);
    stream.write_all(&frame(true, 0x1, b"{nope", true)).unwrap();
    assert!(is_closed(&mut stream));
    assert_server_alive(6207);
}

// ------------------------------------------------------------ client side

#[test]
fn client_keys_and_masks_are_random() {
    let (tx, rx) = mpsc::channel();
    let server = raw_server(6210, move |mut stream| {
        let first = read_client_frame(&mut stream);
        let second = read_client_frame(&mut stream);
        tx.send((first, second)).unwrap();
    });
    let mut client = Channel::<S>::new().connect(6210_u16, ConnectOptions::default()).unwrap();
    let _a = client.request::<Value, _>("a", 1).unwrap();
    let _b = client.request::<Value, _>("b", 2).unwrap();
    let ((mask_a, body_a), (mask_b, body_b)) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    server.join().unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&body_a).unwrap()["path"], "a");
    assert_eq!(serde_json::from_slice::<Value>(&body_b).unwrap()["path"], "b");
    // The old counter produced consecutive masks (n, n+1).
    assert_ne!(mask_a, mask_b);
    let counter_a = u32::from_be_bytes(mask_a);
    assert_ne!(u32::from_be_bytes(mask_b), counter_a.wrapping_add(1));

    let keys = Arc::new(std::sync::Mutex::new(Vec::new()));
    let listener = TcpListener::bind(("127.0.0.1", 6211)).unwrap();
    let collected = keys.clone();
    let key_server = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let head = read_head(&mut stream);
            let key = head
                .lines()
                .find_map(|line| line.strip_prefix("sec-websocket-key: ").map(str::to_string))
                .unwrap();
            collected.lock().unwrap().push(key);
        }
    });
    for _ in 0..2 {
        let options = ConnectOptions {
            handshake_timeout: Duration::from_millis(500),
            ..ConnectOptions::default()
        };
        assert!(Channel::<S>::new().connect(6211_u16, options).is_err());
    }
    key_server.join().unwrap();
    let keys = keys.lock().unwrap();
    assert_ne!(keys[0], keys[1]);
    // Old keys were four consecutive counter values.
    let raw = BASE64.decode(&keys[0]).unwrap();
    let words: Vec<u32> = raw.chunks(4).map(|c| u32::from_be_bytes(c.try_into().unwrap())).collect();
    assert!(!(words[1] == words[0] + 1 && words[2] == words[1] + 1));
}

#[test]
fn client_survives_frame_length_2_pow_64_minus_1() {
    let server = raw_server(6212, |mut stream| {
        let _ = read_client_frame(&mut stream);
        let mut bytes = vec![0x81, 0x7F];
        bytes.extend_from_slice(&u64::MAX.to_be_bytes());
        bytes.push(b'x');
        let _ = stream.write_all(&bytes);
        thread::sleep(Duration::from_millis(300));
    });
    let mut client = Channel::<S>::new().connect(6212_u16, ConnectOptions::default()).unwrap();
    match client.send::<Value, _>("x", 1).unwrap() {
        CallResult::RemoteErr(error) => assert_eq!(error, "websocket message too large"),
        CallResult::Ok(_) => panic!("expected an error"),
    }
    assert!(!client.is_connected());
    server.join().unwrap();
}

#[test]
fn client_accepts_fragmented_replies() {
    let server = raw_server(6213, |mut stream| {
        let (_, payload) = read_client_frame(&mut stream);
        let id = serde_json::from_slice::<Value>(&payload).unwrap()["id"].clone();
        let reply = json!({"id": id, "body": "ok"}).to_string();
        let bytes = reply.as_bytes();
        let mut out = frame(false, 0x1, &bytes[..5], false);
        out.extend(frame(true, 0x0, &bytes[5..], false));
        stream.write_all(&out).unwrap();
        thread::sleep(Duration::from_millis(300));
    });
    let mut client = Channel::<S>::new().connect(6213_u16, ConnectOptions::default()).unwrap();
    match client.send::<String, _>("x", 1).unwrap() {
        CallResult::Ok(Some(body)) => assert_eq!(body, "ok"),
        _ => panic!("expected the reassembled reply"),
    }
    server.join().unwrap();
}

#[test]
fn client_settles_undecodable_reply_with_clean_error() {
    let server = raw_server(6214, |mut stream| {
        let (_, payload) = read_client_frame(&mut stream);
        let id = serde_json::from_slice::<Value>(&payload).unwrap()["id"].clone();
        let reply = json!({"id": id, "body": "not a number"}).to_string();
        stream.write_all(&frame(true, 0x1, reply.as_bytes(), false)).unwrap();
        // An error object (non-string) is stringified, not fatal.
        let (_, payload) = read_client_frame(&mut stream);
        let id = serde_json::from_slice::<Value>(&payload).unwrap()["id"].clone();
        let reply = json!({"id": id, "error": {"code": 500}}).to_string();
        stream.write_all(&frame(true, 0x1, reply.as_bytes(), false)).unwrap();
        thread::sleep(Duration::from_millis(300));
    });
    let mut client = Channel::<S>::new().connect(6214_u16, ConnectOptions::default()).unwrap();
    let error = client.send::<i64, _>("x", 1).err().expect("decode error");
    assert_eq!(error.to_string(), "invalid response body");
    match client.send::<i64, _>("y", 1).unwrap() {
        CallResult::RemoteErr(error) => assert_eq!(error, r#"{"code":500}"#),
        CallResult::Ok(_) => panic!("expected an error"),
    }
    server.join().unwrap();
}

#[test]
fn client_call_timeout_settles_silent_server() {
    let server = raw_server(6215, |mut stream| {
        let _ = read_client_frame(&mut stream);
        thread::sleep(Duration::from_millis(1500));
    });
    let options = ConnectOptions {
        timeout: Some(Duration::from_millis(300)),
        ..ConnectOptions::default()
    };
    let mut client = Channel::<S>::new().connect(6215_u16, options).unwrap();
    let started = Instant::now();
    match client.send::<Value, _>("x", 1).unwrap() {
        CallResult::RemoteErr(error) => assert_eq!(error, "timeout"),
        CallResult::Ok(_) => panic!("expected a timeout"),
    }
    assert!(started.elapsed() < Duration::from_secs(1));
    server.join().unwrap();
}

#[test]
fn handshake_timeout_on_silent_server() {
    let listener = TcpListener::bind(("127.0.0.1", 6216)).unwrap();
    let server = thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        thread::sleep(Duration::from_millis(1500));
    });
    let options = ConnectOptions {
        handshake_timeout: Duration::from_millis(300),
        ..ConnectOptions::default()
    };
    let started = Instant::now();
    let error = Channel::<S>::new().connect(6216_u16, options).err().expect("timeout");
    assert_eq!(error.to_string(), "handshake timeout");
    assert!(started.elapsed() < Duration::from_secs(1));
    server.join().unwrap();
}

// ------------------------------------------------------- pump / nesting

/// AUDIT §3.2 repro (audit/rs_nested_call_repro.rs): the server handler calls
/// back into the client while the client is blocked in `send`, client and
/// server on one thread. This deadlocked on the global `IS_PUMPING` guard.
#[test]
fn server_handler_nested_call_completes_same_thread() {
    let result = run_with_deadline(10, || -> Result<String, String> {
        let mut server_channel = Channel::<S>::new();
        server_channel
            .post::<(), String, (), _>("mirror", |request: PostRequest<'_, S, (), ()>| {
                match request.sender.send::<String, _>("hello", ())? {
                    CallResult::Ok(value) => Ok(value),
                    CallResult::RemoteErr(e) => Err(ChannelError::new(format!("nested failed: {e}"))),
                }
            })
            .map_err(|e| e.to_string())?;
        let mut server = server_channel
            .listen(6220_u16, ListenOptions::default())
            .map_err(|e| e.to_string())?;
        let mut client_channel = Channel::<S>::new();
        client_channel
            .post_value("hello", |_r: PostRequest<'_, S, (), ()>| Ok("client world".to_string()))
            .map_err(|e| e.to_string())?;
        let mut client = client_channel
            .connect(6220_u16, ConnectOptions::default())
            .map_err(|e| e.to_string())?;
        let result = client.send::<String, _>("mirror", ()).map_err(|e| e.to_string())?;
        client.stop();
        server.stop();
        match result {
            CallResult::Ok(value) => Ok(value.unwrap_or_default()),
            CallResult::RemoteErr(e) => Err(e),
        }
    });
    assert_eq!(result, Ok("client world".to_string()));
}

#[test]
fn server_handler_nested_call_completes_cross_thread() {
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut server_channel = Channel::<S>::new();
        server_channel
            .post::<(), String, (), _>("mirror", |request: PostRequest<'_, S, (), ()>| {
                match request.sender.send::<String, _>("hello", ())? {
                    CallResult::Ok(value) => Ok(value),
                    CallResult::RemoteErr(e) => Err(ChannelError::new(e)),
                }
            })
            .unwrap();
        let mut server = server_channel.listen(6221_u16, ListenOptions::default()).unwrap();
        ready_tx.send(()).unwrap();
        while !flag.load(Ordering::SeqCst) {
            if !server.poll().unwrap_or(false) {
                thread::sleep(Duration::from_micros(200));
            }
        }
    });
    ready_rx.recv().unwrap();
    let result = run_with_deadline(10, || {
        let mut client_channel = Channel::<S>::new();
        client_channel
            .post_value("hello", |_r: PostRequest<'_, S, (), ()>| Ok("client world".to_string()))
            .unwrap();
        let mut client = client_channel.connect(6221_u16, ConnectOptions::default()).unwrap();
        match client.send::<String, _>("mirror", ()).unwrap() {
            CallResult::Ok(value) => value,
            CallResult::RemoteErr(e) => panic!("{e}"),
        }
    });
    assert_eq!(result.as_deref(), Some("client world"));
    stop.store(true, Ordering::SeqCst);
    server.join().unwrap();
}

/// Two levels of nesting: client -> server handler -> client handler ->
/// server, all on one thread.
#[test]
fn doubly_nested_calls_complete_same_thread() {
    let result = run_with_deadline(10, || {
        let mut server_channel = Channel::<S>::new();
        server_channel
            .post_value("outer", |request: PostRequest<'_, S, (), ()>| {
                match request.sender.send::<i64, _>("middle", ())? {
                    CallResult::Ok(value) => Ok(value.unwrap_or_default() + 1),
                    CallResult::RemoteErr(e) => Err(ChannelError::new(e)),
                }
            })
            .unwrap();
        server_channel
            .post_value("inner", |_r: PostRequest<'_, S, (), ()>| Ok(40_i64))
            .unwrap();
        let mut server = server_channel.listen(6222_u16, ListenOptions::default()).unwrap();
        let mut client_channel = Channel::<S>::new();
        client_channel
            .post_value("middle", |request: PostRequest<'_, S, (), ()>| {
                match request.sender.send::<i64, _>("inner", ())? {
                    CallResult::Ok(value) => Ok(value.unwrap_or_default() + 1),
                    CallResult::RemoteErr(e) => Err(ChannelError::new(e)),
                }
            })
            .unwrap();
        let mut client = client_channel.connect(6222_u16, ConnectOptions::default()).unwrap();
        let result = match client.send::<i64, _>("outer", ()).unwrap() {
            CallResult::Ok(value) => value,
            CallResult::RemoteErr(e) => panic!("{e}"),
        };
        client.stop();
        server.stop();
        result
    });
    assert_eq!(result, Some(42));
}

/// AUDIT §3.2: `pump_all` used to return early via `?` and leave
/// `IS_PUMPING` set, so every later blocking call on the thread could only
/// sleep. A transport error must not wedge the other transports.
#[test]
fn transport_error_does_not_wedge_the_thread() {
    let hostile = raw_server(6223, |mut stream| {
        let _ = read_client_frame(&mut stream);
        let _ = stream.write_all(&frame(true, 0x1, b"{broken", false));
        thread::sleep(Duration::from_millis(500));
    });
    let result = run_with_deadline(10, || {
        let mut server_channel = Channel::<S>::new();
        server_channel
            .post_value("echo", |request: PostRequest<'_, S, i64, ()>| Ok(request.body))
            .unwrap();
        let mut server = server_channel.listen(6224_u16, ListenOptions::default()).unwrap();
        let mut good = Channel::<S>::new().connect(6224_u16, ConnectOptions::default()).unwrap();
        let mut bad = Channel::<S>::new().connect(6223_u16, ConnectOptions::default()).unwrap();
        let bad_result = match bad.send::<Value, _>("x", 1) {
            Ok(CallResult::RemoteErr(error)) => error,
            Ok(CallResult::Ok(_)) => "unexpected ok".to_string(),
            Err(error) => error.to_string(),
        };
        let good_result = match good.send::<i64, _>("echo", 11).unwrap() {
            CallResult::Ok(value) => value,
            CallResult::RemoteErr(e) => panic!("{e}"),
        };
        good.stop();
        server.stop();
        (bad_result, good_result)
    });
    assert_eq!(result, ("invalid json message".to_string(), Some(11)));
    hostile.join().unwrap();
}

/// Dropping a client or server without `stop()` must unregister it from the
/// thread's pump list (it used to leave a dangling pointer behind).
#[test]
fn dropping_without_stop_is_safe() {
    let result = run_with_deadline(10, || {
        let mut server_channel = Channel::<S>::new();
        server_channel
            .post_value("echo", |request: PostRequest<'_, S, i64, ()>| Ok(request.body))
            .unwrap();
        let server = server_channel.listen(6225_u16, ListenOptions::default()).unwrap();
        {
            let mut client = Channel::<S>::new().connect(6225_u16, ConnectOptions::default()).unwrap();
            let _ = client.send::<i64, _>("echo", 1).unwrap();
        }
        let mut client = Channel::<S>::new().connect(6225_u16, ConnectOptions::default()).unwrap();
        let value = match client.send::<i64, _>("echo", 2).unwrap() {
            CallResult::Ok(value) => value,
            CallResult::RemoteErr(e) => panic!("{e}"),
        };
        drop(server);
        // The server is gone; the next call settles instead of using freed memory.
        let after = match client.send::<i64, _>("echo", 3) {
            Ok(CallResult::RemoteErr(error)) => error,
            Ok(CallResult::Ok(_)) => "unexpected ok".to_string(),
            Err(error) => error.to_string(),
        };
        (value, after)
    });
    assert_eq!(result.0, Some(2));
    assert!(!result.1.is_empty());
}

// ------------------------------------------------------------ subscription

#[test]
fn subscription_topic_and_body_formatters_and_falsy_bodies() -> Result<(), ChannelError> {
    let events = Subscription::<String, i64>::new();
    events.topic(|request| format!("t-{request}"));
    events.body(|request| Some(request.len() as i64));

    let mut channel = Channel::<S>::new();
    channel.events([(String::from("counts"), events.clone())], "")?;
    let mut process = channel.process(S)?;

    let received = Rc::new(RefCell::new(Vec::new()));
    let sink = received.clone();
    let mut handle = match process.subscribe::<String, i64, (), _>("counts", "abc".to_string(), (), move |event| {
        sink.borrow_mut().push(event)
    })? {
        SubscribeResult::Ok(handle) => handle,
        SubscribeResult::RemoteErr(error) => panic!("subscribe failed: {error}"),
    };
    assert!(process.is_remote_subscribed("counts/t-abc"));

    events.send_request("abc".to_string())?; // body from the formatter: 3
    events.send("abc".to_string(), 7)?;
    events.send("abc".to_string(), 0)?; // falsy: dropped, as in TS
    events.send("other".to_string(), 9)?; // different topic
    events.publish("t-abc", 5)?;
    assert_eq!(*received.borrow(), vec![3, 7, 5]);

    handle.cancel();
    events.send("abc".to_string(), 8)?;
    assert_eq!(*received.borrow(), vec![3, 7, 5]);
    Ok(())
}

#[test]
fn subscription_falsy_bodies_are_not_published() -> Result<(), ChannelError> {
    let events = Subscription::<String, Value>::new();
    let mut channel = Channel::<S>::new();
    channel.events([(String::from("e"), events.clone())], "")?;
    let mut process = channel.process(S)?;
    let received = Rc::new(RefCell::new(Vec::new()));
    let sink = received.clone();
    let _handle = process.subscribe::<String, Value, (), _>("e", "t".to_string(), (), move |event| {
        sink.borrow_mut().push(event)
    })?;
    for body in [json!(null), json!(false), json!(0), json!(""), json!(0.0)] {
        events.send("t".to_string(), body)?;
    }
    for body in [json!(1), json!("x"), json!(true), json!([]), json!({})] {
        events.send("t".to_string(), body)?;
    }
    assert_eq!(
        *received.borrow(),
        vec![json!(1), json!("x"), json!(true), json!([]), json!({})]
    );
    Ok(())
}
