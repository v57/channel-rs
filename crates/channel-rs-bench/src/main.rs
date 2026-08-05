use std::{
    error::Error,
    fs,
    io::{self, Write},
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

use channel_rs::{CallResult, Channel, ConnectOptions, ListenOptions};
use serde::{Deserialize, Serialize};

const REQUEST_PATH: &str = "text/count";
const STAGE_CLIENTS: [usize; 4] = [1, 10, 100, 1000];
const DEFAULT_STAGE_MS: u64 = 5_000;
const REQUEST_TEXT: &str = "hello";
const IDLE_POLL_SLEEP: Duration = Duration::from_micros(100);

type BenchResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Deserialize, Serialize)]
struct TextCountRequest {
    text: String,
}

#[derive(Clone, Copy)]
enum Mode {
    All,
    Process,
    Websocket,
    Server,
    Client,
}

#[derive(Serialize)]
struct BenchmarkOutput {
    language: &'static str,
    process: Vec<BenchmarkStageResult>,
    websocket: Vec<BenchmarkStageResult>,
}

#[derive(Serialize)]
struct BenchmarkStageResult {
    clients: usize,
    rps: f64,
}

#[derive(Serialize)]
struct ReadyMessage {
    #[serde(rename = "type")]
    kind: &'static str,
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    clients: Option<usize>,
}

#[derive(Serialize)]
struct ResultMessage {
    #[serde(rename = "type")]
    kind: &'static str,
    pid: u32,
    #[serde(rename = "elapsedSeconds")]
    elapsed_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed: Option<u64>,
    #[serde(rename = "requestsSent", skip_serializing_if = "Option::is_none")]
    requests_sent: Option<u64>,
    #[serde(rename = "responsesReceived", skip_serializing_if = "Option::is_none")]
    responses_received: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    errors: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requests: Option<u64>,
}

fn main() -> BenchResult<()> {
    match parse_mode()? {
        Mode::All => run_smoke(Mode::All),
        Mode::Websocket => run_smoke(Mode::Websocket),
        Mode::Process if std::env::var_os("BENCH_TOTAL_REQUESTS").is_some() => run_process_worker(),
        Mode::Process => run_smoke(Mode::Process),
        Mode::Server => run_server_worker(),
        Mode::Client => run_client_worker(),
    }
}

fn parse_mode() -> BenchResult<Mode> {
    match std::env::args().nth(1).as_deref() {
        None | Some("all") | Some("smoke") => Ok(Mode::All),
        Some("process") => Ok(Mode::Process),
        Some("websocket") => Ok(Mode::Websocket),
        Some("server") => Ok(Mode::Server),
        Some("client") => Ok(Mode::Client),
        Some(other) => Err(format!("unknown mode: {other}").into()),
    }
}

fn run_smoke(mode: Mode) -> BenchResult<()> {
    let stage_duration = stage_duration();
    let process = if matches!(mode, Mode::All | Mode::Process) {
        run_process_benchmark(stage_duration)?
    } else {
        Vec::new()
    };
    let websocket = if matches!(mode, Mode::All | Mode::Websocket) {
        run_websocket_benchmark(stage_duration)?
    } else {
        Vec::new()
    };

    let output = BenchmarkOutput {
        language: "rust",
        process,
        websocket,
    };
    let text = serde_json::to_string_pretty(&output)?;
    fs::write(output_path(), format!("{text}\n"))?;
    println!("{text}");
    Ok(())
}

fn run_server_worker() -> BenchResult<()> {
    let port = parse_positive_u16_env("BENCH_PORT")?;
    let channel = build_channel();
    let mut server = channel.listen(port, ListenOptions::default())?;

    loop {
        if !server.poll()? {
            thread::sleep(IDLE_POLL_SLEEP);
        }
    }
}

fn run_client_worker() -> BenchResult<()> {
    let host = std::env::var("BENCH_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = parse_positive_u16_env("BENCH_PORT")?;
    let clients = parse_positive_usize_env("BENCH_CLIENTS")?;
    let total_requests = parse_non_negative_u64_env("BENCH_TOTAL_REQUESTS")?;
    let request_mode = std::env::var("BENCH_REQUEST_MODE").unwrap_or_else(|_| "burst".to_string());
    if !request_mode.trim().eq_ignore_ascii_case("burst") {
        return Err("BENCH_REQUEST_MODE must be 'burst'".into());
    }

    let channel = Channel::<()>::new();
    let target = format!("ws://{host}:{port}");
    let mut sockets = (0..clients)
        .map(|_| channel.connect(target.as_str(), ConnectOptions::default()))
        .collect::<Result<Vec<_>, _>>()?;
    emit_json(&ReadyMessage {
        kind: "ready",
        pid: std::process::id(),
        port: None,
        clients: Some(clients),
    })?;

    let request = TextCountRequest {
        text: REQUEST_TEXT.to_string(),
    };

    // Pipelined burst: keep a window of in-flight requests per socket and
    // drain them in order as responses arrive. Mirrors the TS worker's
    // concurrency model instead of blocking one request at a time.
    const WINDOW: usize = 4096;
    let mut in_flight: std::collections::VecDeque<channel_rs::SendingRequest<(), usize>> =
        std::collections::VecDeque::new();
    let started = Instant::now();
    let mut completed = 0_u64;
    let mut errors = 0_u64;
    let mut index = 0_usize;

    while completed + errors < total_requests {
        while in_flight.len() < WINDOW
            && completed + errors + (in_flight.len() as u64) < total_requests
        {
            let req = sockets[index].request::<usize, _>(REQUEST_PATH, request.clone())?;
            in_flight.push_back(req);
            index += 1;
            if index == sockets.len() {
                index = 0;
            }
        }

        // Pump transports so responses land.
        let mut progressed = false;
        for socket in &mut sockets {
            progressed = socket.poll()? || progressed;
        }

        // Drain ready responses in order; verify each result.
        while let Some(front) = in_flight.front() {
            if !front.is_ready() {
                break;
            }
            let req = in_flight.pop_front().unwrap();
            match req.response()? {
                channel_rs::CallResult::Ok(Some(value)) if value == REQUEST_TEXT.len() => {
                    completed += 1;
                }
                _ => {
                    errors += 1;
                }
            }
        }

        if !progressed && !in_flight.is_empty() {
            thread::sleep(IDLE_POLL_SLEEP);
        }
    }

    for socket in &mut sockets {
        socket.stop();
    }

    emit_json(&ResultMessage {
        kind: "result",
        pid: std::process::id(),
        elapsed_seconds: started.elapsed().as_secs_f64(),
        completed: Some(completed),
        requests_sent: Some(completed + errors),
        responses_received: Some(completed),
        errors: Some(errors),
        requests: None,
    })
}

fn run_process_worker() -> BenchResult<()> {
    let total_requests = parse_non_negative_u64_env("BENCH_TOTAL_REQUESTS")?;
    let channel = build_channel();
    let mut process = channel.process(())?;
    emit_json(&ReadyMessage {
        kind: "ready",
        pid: std::process::id(),
        port: None,
        clients: None,
    })?;

    let request = TextCountRequest {
        text: REQUEST_TEXT.to_string(),
    };
    let started = Instant::now();
    let mut completed = 0_u64;
    while completed < total_requests {
        run_count(process.send::<usize, _>(REQUEST_PATH, request.clone()))?;
        completed += 1;
    }
    process.stop();

    emit_json(&ResultMessage {
        kind: "result",
        pid: std::process::id(),
        elapsed_seconds: started.elapsed().as_secs_f64(),
        completed: Some(completed),
        requests_sent: Some(completed),
        responses_received: Some(completed),
        errors: Some(0),
        requests: Some(completed),
    })
}

fn stage_duration() -> Duration {
    let millis = std::env::var("BENCH_STAGE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_STAGE_MS);
    Duration::from_millis(millis)
}

fn output_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("benchmark.json")
}

fn build_channel() -> Channel<()> {
    let mut channel = Channel::new();
    channel
        .post_value(REQUEST_PATH, |request: channel_rs::PostRequest<'_, (), TextCountRequest, ()>| {
            Ok(request.body.text.chars().count())
        })
        .expect("register text/count");
    channel
}

fn run_process_benchmark(stage_duration: Duration) -> BenchResult<Vec<BenchmarkStageResult>> {
    let mut results = Vec::new();
    for clients in STAGE_CLIENTS {
        let channel = build_channel();
        let mut processes = (0..clients)
            .map(|_| channel.process(()))
            .collect::<Result<Vec<_>, _>>()?;
        let requests = run_stage(stage_duration, clients, |index| {
            run_count(processes[index].send::<usize, _>(
                REQUEST_PATH,
                TextCountRequest {
                    text: REQUEST_TEXT.to_string(),
                },
            ))
        })?;
        for process in &mut processes {
            process.stop();
        }
        results.push(BenchmarkStageResult {
            clients,
            rps: requests as f64 / stage_duration.as_secs_f64(),
        });
    }
    Ok(results)
}

fn run_websocket_benchmark(stage_duration: Duration) -> BenchResult<Vec<BenchmarkStageResult>> {
    let mut results = Vec::new();
    for clients in STAGE_CLIENTS {
        let server_channel = build_channel();
        let mut server = server_channel.listen(0_u16, ListenOptions::default())?;
        let port = server.port();

        let client_channel = Channel::<()>::new();
        let mut sockets = (0..clients)
            .map(|_| client_channel.connect(port, ConnectOptions::default()))
            .collect::<Result<Vec<_>, _>>()?;

        let requests = run_stage(stage_duration, clients, |index| {
            run_count(sockets[index].send::<usize, _>(
                REQUEST_PATH,
                TextCountRequest {
                    text: REQUEST_TEXT.to_string(),
                },
            ))
        })?;

        for socket in &mut sockets {
            socket.stop();
        }
        server.stop();

        results.push(BenchmarkStageResult {
            clients,
            rps: requests as f64 / stage_duration.as_secs_f64(),
        });
    }
    Ok(results)
}

fn run_stage(
    stage_duration: Duration,
    clients: usize,
    mut request: impl FnMut(usize) -> BenchResult<()>,
) -> BenchResult<u64> {
    let start = Instant::now();
    let mut requests = 0_u64;
    let mut index = 0_usize;
    while start.elapsed() < stage_duration {
        request(index)?;
        requests += 1;
        index += 1;
        if index == clients {
            index = 0;
        }
    }
    Ok(requests)
}

fn run_count(result: Result<CallResult<usize>, channel_rs::ChannelError>) -> BenchResult<()> {
    match result? {
        CallResult::Ok(Some(value)) if value == REQUEST_TEXT.chars().count() => Ok(()),
        CallResult::Ok(Some(value)) => Err(format!("unexpected count: {value}").into()),
        CallResult::Ok(None) => Err("missing response body".into()),
        CallResult::RemoteErr(error) => Err(error.into()),
    }
}

fn emit_json<T: Serialize>(value: &T) -> BenchResult<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

fn parse_positive_u16_env(name: &str) -> BenchResult<u16> {
    let raw = std::env::var(name).map_err(|_| format!("{name} must be a valid positive integer"))?;
    raw.parse::<u16>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a valid positive integer").into())
}

fn parse_positive_usize_env(name: &str) -> BenchResult<usize> {
    let raw = std::env::var(name).map_err(|_| format!("{name} must be a valid positive integer"))?;
    raw.parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a valid positive integer").into())
}

fn parse_non_negative_u64_env(name: &str) -> BenchResult<u64> {
    let raw =
        std::env::var(name).map_err(|_| format!("{name} must be a valid non-negative integer"))?;
    raw.parse::<u64>()
        .map_err(|_| format!("{name} must be a valid non-negative integer").into())
}
