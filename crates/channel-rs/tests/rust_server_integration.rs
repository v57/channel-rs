use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    thread,
    time::{Duration, Instant},
};

use channel_rs::{
    Channel, ChannelError, ChannelMessage, ListenOptions, PostRequest, deserialize_messages,
    serialize_message,
};
use serde_json::json;

#[derive(Default)]
struct TestState;

#[derive(Default)]
struct FrameReader {
    buffer: Vec<u8>,
}

enum ClientRead {
    Frame { opcode: u8, payload: Vec<u8> },
    Closed,
}

impl FrameReader {
    fn read_from(&mut self, stream: &mut TcpStream) -> Result<bool, ChannelError> {
        let mut progressed = false;
        loop {
            let mut temp = [0_u8; 4096];
            match stream.read(&mut temp) {
                Ok(0) => return Ok(progressed),
                Ok(n) => {
                    progressed = true;
                    self.buffer.extend_from_slice(&temp[..n]);
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(progressed),
                Err(error) => return Err(ChannelError::new(error.to_string())),
            }
        }
    }

    fn next_frame(&mut self) -> Result<Option<ClientRead>, ChannelError> {
        if self.buffer.len() < 2 {
            return Ok(None);
        }

        let opcode = self.buffer[0] & 0x0f;
        let masked = (self.buffer[1] & 0x80) != 0;
        if masked {
            return Err(ChannelError::new("server frames must be unmasked"));
        }

        let mut index = 2_usize;
        let payload_len = match self.buffer[1] & 0x7f {
            len @ 0..=125 => len as usize,
            126 => {
                if self.buffer.len() < 4 {
                    return Ok(None);
                }
                index = 4;
                u16::from_be_bytes([self.buffer[2], self.buffer[3]]) as usize
            }
            127 => {
                if self.buffer.len() < 10 {
                    return Ok(None);
                }
                index = 10;
                u64::from_be_bytes([
                    self.buffer[2],
                    self.buffer[3],
                    self.buffer[4],
                    self.buffer[5],
                    self.buffer[6],
                    self.buffer[7],
                    self.buffer[8],
                    self.buffer[9],
                ]) as usize
            }
            _ => unreachable!(),
        };

        if self.buffer.len() < index + payload_len {
            return Ok(None);
        }

        let payload = self.buffer[index..index + payload_len].to_vec();
        self.buffer.drain(..index + payload_len);
        Ok(Some(ClientRead::Frame { opcode, payload }))
    }
}

fn perform_handshake(
    stream: &mut TcpStream,
    server: &mut channel_rs::Server<TestState>,
) -> Result<(), ChannelError> {
    let request = format!(
        "GET / HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        server.port()
    );
    stream.write_all(request.as_bytes())?;

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut response = Vec::new();
    while Instant::now() < deadline {
        let _ = server.poll()?;
        let mut temp = [0_u8; 4096];
        match stream.read(&mut temp) {
            Ok(0) => return Err(ChannelError::new("connection closed during handshake")),
            Ok(n) => {
                response.extend_from_slice(&temp[..n]);
                if response.windows(4).any(|window| window == b"\r\n\r\n") {
                    let text = String::from_utf8(response)
                        .map_err(|error| ChannelError::new(error.to_string()))?;
                    if text.starts_with("HTTP/1.1 101 ") {
                        return Ok(());
                    }
                    return Err(ChannelError::new("invalid handshake response"));
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(ChannelError::new(error.to_string())),
        }
    }
    Err(ChannelError::new("handshake timeout"))
}

fn write_client_frame(
    stream: &mut TcpStream,
    opcode: u8,
    payload: &[u8],
) -> Result<(), ChannelError> {
    let mut header = vec![0x80 | opcode];
    match payload.len() {
        0..=125 => header.push(0x80 | payload.len() as u8),
        126..=65535 => {
            header.push(0x80 | 126);
            header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        _ => {
            header.push(0x80 | 127);
            header.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
    }

    let mask = [0x12_u8, 0x34, 0x56, 0x78];
    header.extend_from_slice(&mask);
    let masked = payload
        .iter()
        .enumerate()
        .map(|(index, byte)| byte ^ mask[index % 4])
        .collect::<Vec<_>>();

    stream.write_all(&header)?;
    stream.write_all(&masked)?;
    Ok(())
}

fn wait_for_client_read(
    stream: &mut TcpStream,
    server: &mut channel_rs::Server<TestState>,
    reader: &mut FrameReader,
) -> Result<ClientRead, ChannelError> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let _ = server.poll()?;
        let progressed = reader.read_from(stream)?;
        if let Some(frame) = reader.next_frame()? {
            return Ok(frame);
        }
        let mut one = [0_u8; 1];
        match stream.read(&mut one) {
            Ok(0) => return Ok(ClientRead::Closed),
            Ok(n) => reader.buffer.extend_from_slice(&one[..n]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if !progressed {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            Err(error) => return Err(ChannelError::new(error.to_string())),
        }
    }
    Err(ChannelError::new("timeout"))
}

#[test]
fn rust_server_reads_binary_frames_and_closes_on_invalid_json() -> Result<(), ChannelError> {
    let mut channel = Channel::<TestState>::new();
    channel.post_value("hello", |_request: PostRequest<'_, TestState, (), ()>| {
        Ok("world".to_string())
    })?;
    let mut server = channel.listen(0_u16, ListenOptions::default())?;

    let mut stream = TcpStream::connect(("127.0.0.1", server.port()))
        .map_err(|error| ChannelError::new(error.to_string()))?;
    stream
        .set_nonblocking(true)
        .map_err(|error| ChannelError::new(error.to_string()))?;
    perform_handshake(&mut stream, &mut server)?;

    let request = ChannelMessage {
        id: Some(7),
        path: Some("hello".to_string()),
        ..Default::default()
    };
    let payload = serialize_message(&request)?;
    write_client_frame(&mut stream, 0x2, &payload)?;

    let mut reader = FrameReader::default();
    match wait_for_client_read(&mut stream, &mut server, &mut reader)? {
        ClientRead::Frame { opcode, payload } => {
            assert!(opcode == 0x1 || opcode == 0x2);
            let messages = deserialize_messages(&payload)?;
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, Some(7));
            assert_eq!(messages[0].body, Some(json!("world")));
        }
        ClientRead::Closed => panic!("server closed before sending response"),
    }

    write_client_frame(&mut stream, 0x1, b"not-json")?;
    match wait_for_client_read(&mut stream, &mut server, &mut reader)? {
        ClientRead::Frame { opcode, .. } => assert_eq!(opcode, 0x8),
        ClientRead::Closed => {}
    }

    server.stop();
    Ok(())
}
