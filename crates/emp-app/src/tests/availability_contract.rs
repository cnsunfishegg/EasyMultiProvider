//! Real HTTP/SSE/WS boundaries, with fixture-only upstreams.
use super::*;
use std::sync::atomic::AtomicUsize;

struct Upstream {
    address: SocketAddr,
    count: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl Upstream {
    fn start(status: u16, payload: Value, sse: bool) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (seen, stopping) = (Arc::clone(&count), Arc::clone(&stop));
        let worker = thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                let Ok((mut stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                // A pooled client can open and abandon an idle connection.
                // Do not let it occupy this fixture's single accept worker.
                let Some(head) = crate::http::request::read_request_head_before(
                    &mut stream,
                    std::time::Instant::now() + Duration::from_millis(500),
                ) else {
                    continue;
                };
                let request = parse_request(&head.head).unwrap();
                let length = request
                    .header("Content-Length")
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                let mut body = head.body_prefix;
                while body.len() < length {
                    let mut bytes = [0; 4096];
                    let count = stream.read(&mut bytes).unwrap();
                    if count == 0 {
                        break;
                    }
                    body.extend_from_slice(&bytes[..count]);
                }
                seen.fetch_add(1, Ordering::AcqRel);
                let raw = if sse {
                    format!("data: {}\n\n", payload)
                } else {
                    payload.to_string()
                };
                let media = if sse {
                    "text/event-stream"
                } else {
                    "application/json"
                };
                write!(stream, "HTTP/1.1 {status} Test\r\nContent-Type: {media}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw}", raw.len()).unwrap();
            }
        });
        Self {
            address,
            count,
            stop,
            worker: Some(worker),
        }
    }
    fn url(&self) -> String {
        format!("http://{}/v1", self.address)
    }
}
impl Drop for Upstream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !thread::panicking() {
                result.unwrap();
            }
        }
    }
}

fn quota_error() -> Value {
    json!({"error":{"code":"insufficient_quota","message":"fixture private details"}})
}

#[test]
fn confirmed_exhaustion_stops_http_and_sse_before_another_dispatch() {
    for auth in ["api_key", "forward"] {
        let upstream = Upstream::start(429, quota_error(), false);
        let (_root, server) = configured_protocol_server(&upstream.url(), "responses", auth);
        let session = session_header(&server);
        let headers = [&session[..], "Authorization: Bearer fixture-caller"];
        let first = post(
            &server,
            "/v1/responses",
            br#"{"model":"demo/model","input":"first"}"#,
            &headers,
        );
        assert!(first.starts_with("HTTP/1.1 429"), "{first}");
        let second = post(
            &server,
            "/v1/responses",
            br#"{"model":"demo/model","input":"second","stream":true}"#,
            &headers,
        );
        assert!(second.contains("usage_limit_reached"), "{second}");
        assert!(!second.contains("fixture private"));
        assert_eq!(upstream.count.load(Ordering::Acquire), 1, "{auth}");
        server.shutdown().unwrap();
    }
}

#[test]
fn stream_failure_is_feedback_and_a_second_turn_on_the_same_socket_is_admitted_again() {
    let upstream = Upstream::start(
        200,
        json!({"type":"response.failed","response":{
        "id":"resp_quota","object":"response","status":"failed","output":[],
        "error":{"code":"usage_limit_reached","message":"fixture quota"}}}),
        true,
    );
    let (_root, server) = configured_protocol_server(&upstream.url(), "responses", "api_key");
    let headers = BTreeMap::from([("x-emp-session".to_owned(), server.session_token())]);
    let mut socket = emp_transport::ClientWebSocket::connect(
        &format!("ws://{}/v1/responses", server.local_addr()),
        &headers,
        Duration::from_secs(5),
    )
    .unwrap();
    for attempt in 0..2 {
        socket
            .send_json(&json!({"type":"response.create","model":"demo/model","input":"fixture"}))
            .unwrap();
        let mut terminal = None;
        for _ in 0..10 {
            let event = socket.receive_json().unwrap().unwrap();
            if matches!(event["type"].as_str(), Some("error" | "response.failed")) {
                terminal = Some(event);
                break;
            }
        }
        let terminal = terminal.expect("terminal quota response");
        if attempt == 1 {
            assert_eq!(
                terminal["error"]["code"], "usage_limit_reached",
                "{terminal}"
            );
        }
    }
    assert_eq!(upstream.count.load(Ordering::Acquire), 1);
    drop(socket);
    server.shutdown().unwrap();
}

#[test]
fn unstructured_quota_text_does_not_create_a_cross_request_block() {
    let upstream = Upstream::start(
        429,
        json!({"error":{"message":"quota might be low"}}),
        false,
    );
    let (_root, server) = configured_protocol_server(&upstream.url(), "responses", "api_key");
    let session = session_header(&server);
    for _ in 0..2 {
        let response = post(
            &server,
            "/v1/responses",
            br#"{"model":"demo/model","input":"fixture"}"#,
            &[&session],
        );
        assert!(response.starts_with("HTTP/1.1 429"), "{response}");
    }
    assert_eq!(upstream.count.load(Ordering::Acquire), 2);
    server.shutdown().unwrap();
}

#[test]
fn quota_block_is_model_scoped_and_credential_replacement_uses_a_new_source() {
    let upstream = Upstream::start(429, quota_error(), false);
    let (_root, server) = configured_protocol_server(&upstream.url(), "responses", "api_key");
    let session = session_header(&server);
    let call = || {
        post(
            &server,
            "/v1/responses",
            br#"{"model":"demo/model","input":"fixture"}"#,
            &[&session],
        )
    };
    assert!(call().starts_with("HTTP/1.1 429"));
    assert!(call().contains("usage_limit_reached"));
    {
        let mut config = server.state.backend.configuration.snapshot().unwrap();
        let mut other = config["models"][0].clone();
        other["id"] = json!("demo/other");
        other["upstream_id"] = json!("other-upstream");
        config["models"].as_array_mut().unwrap().push(other);
        assert!(crate::services::configuration::settings::update(&server.state, &config).is_ok());
    }
    let other = post(
        &server,
        "/v1/responses",
        br#"{"model":"demo/other","input":"fixture"}"#,
        &[&session],
    );
    assert!(other.starts_with("HTTP/1.1 429"));
    assert_eq!(upstream.count.load(Ordering::Acquire), 2);
    {
        let mut config = server.state.backend.configuration.snapshot().unwrap();
        config["providers"][0]["api_key"] = json!("replacement-fixture-key");
        assert!(crate::services::configuration::settings::update(&server.state, &config).is_ok());
    }
    assert!(call().starts_with("HTTP/1.1 429"));
    assert_eq!(upstream.count.load(Ordering::Acquire), 3);
    server.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
fn concurrent_alias_refreshes_share_one_real_helper_and_keep_their_own_ids() {
    use std::os::unix::fs::PermissionsExt;
    let executable_root = tempfile::Builder::new()
        .prefix("emp-quota-coalesce-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let binary = executable_root.path().join("codex");
    std::fs::write(
        &binary,
        r#"#!/usr/bin/env python3
import json, os, pathlib, sys, time
assert pathlib.Path(os.environ['HOME']) == pathlib.Path(os.environ['CODEX_HOME'])
assert 'plugins = false' in (pathlib.Path(os.environ['CODEX_HOME']) / 'config.toml').read_text()
counter = pathlib.Path(sys.argv[0]).with_suffix('.calls')
with counter.open('a') as f: f.write('query\n')
for line in sys.stdin:
    r = json.loads(line)
    if 'id' not in r: continue
    if r['method'] == 'account/read': value = {'account': {'planType':'pro'}}
    elif r['method'] == 'account/rateLimits/read':
        time.sleep(0.4)
        value = {'rateLimits': {'limitId':'codex', 'primary': {'usedPercent':7}}}
    else: value = {}
    print(json.dumps({'id':r['id'], 'result':value}), flush=True)
"#,
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let root = canonical_root(&directory);
    let config = root.join("config.json");
    std::fs::write(&config, b"{}").unwrap();
    let server = ServerHandle::start_with_config_options(
        Ipv4Addr::LOCALHOST.into(),
        0,
        &config,
        binary.to_str().unwrap(),
        root.join("native/auth.json"),
    )
    .unwrap();
    for id in ["a", "b"] {
        crate::services::accounts::import_account_state(&server.state, &json!({"id":id,"prefix":id,
            "auth_json":{"tokens":{"access_token":format!("fixture-{id}"),"account_id":"same-owner"}}})).unwrap();
    }
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers = ["a", "b"].map(|id| {
        let (state, ready) = (Arc::clone(&server.state), Arc::clone(&barrier));
        thread::spawn(move || {
            ready.wait();
            crate::services::quota::refresh_account_serialized(&state, id).unwrap()
        })
    });
    barrier.wait();
    for (id, worker) in ["a", "b"].into_iter().zip(workers) {
        let result = worker.join().unwrap();
        assert_eq!(result["id"], id);
        assert_eq!(result["quota"]["rate_limits"]["primary"]["usedPercent"], 7);
    }
    assert_eq!(
        std::fs::read_to_string(binary.with_extension("calls"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    server.shutdown().unwrap();
}
