//! End-to-end MCP adapter tests. Credentials are injected, never installed into
//! process-global environment variables, so permission tests can run concurrently.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use rmcp::{
    ServiceExt as _,
    model::CallToolRequestParams,
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use skerrykv::{config::Config, coordinator::Coordinator, mcp::Credentials, server};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

struct Fixture {
    coordinator: Arc<Coordinator>,
    router: axum::Router,
    cancellation: CancellationToken,
    // Drop after all handles so Windows can remove the directory.
    _root: tempfile::TempDir,
}
impl Fixture {
    async fn new(configure: impl FnOnce(&mut Config), credentials: Credentials) -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        config.cache.payload_mib = 0;
        config.writes.batch_max_delay_ms = 1;
        configure(&mut config);
        let coordinator = Coordinator::open(config).await.unwrap();
        let cancellation = CancellationToken::new();
        let router = server::router_with_mcp_credentials(
            coordinator.clone(),
            cancellation.clone(),
            credentials,
        )
        .unwrap();
        Self {
            _root: root,
            coordinator,
            router,
            cancellation,
        }
    }
    async fn request(
        &self,
        method: &str,
        params: Value,
        token: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut request = Request::post("/mcp")
            .header("host", "127.0.0.1:7788")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-11-25");
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let response = self
            .router
            .clone()
            .oneshot(
                request
                    .body(Body::from(
                        json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap();
        (status, decode_rpc(&body))
    }
    async fn call(&self, name: &str, arguments: Value, token: Option<&str>) -> Value {
        let (status, result) = self
            .request(
                "tools/call",
                json!({"name":name,"arguments":arguments}),
                token,
            )
            .await;
        assert!(status.is_success(), "{name}: {status}: {result}");
        result
    }
    async fn close(self) {
        self.cancellation.cancel();
        drop(self.router);
        self.coordinator.shutdown().await.unwrap();
    }
}
fn decode_rpc(body: &[u8]) -> Value {
    if body.is_empty() {
        return Value::Null;
    }
    if let Ok(value) = serde_json::from_slice(body) {
        return value;
    }
    let text = String::from_utf8_lossy(body);
    text.lines()
        .filter_map(|line| {
            line.strip_prefix("data: ")
                .or_else(|| line.strip_prefix("data:"))
        })
        .find_map(|data| serde_json::from_str(data).ok())
        .unwrap_or_else(|| json!({"raw":text}))
}
fn payload(result: &Value) -> Value {
    assert!(result.get("error").is_none(), "RPC error: {result}");
    assert_ne!(result["result"]["isError"], true, "tool error: {result}");
    if let Some(value) = result["result"].get("structuredContent") {
        return value.clone();
    }
    let text = result["result"]["content"][0]["text"]
        .as_str()
        .expect("tool text or structured content");
    serde_json::from_str(text).unwrap_or_else(|_| json!({"text":text}))
}
fn is_error(result: &Value) -> bool {
    result.get("error").is_some() || result["result"]["isError"] == true
}
fn credentials() -> Credentials {
    Credentials {
        service: None,
        read: Some("read-test".into()),
        write: Some("write-test".into()),
        maintenance: Some("maintenance-test".into()),
        gateway: None,
    }
}

#[tokio::test]
async fn initialize_discovery_annotations_and_no_eager_object_enumeration() {
    let fixture = Fixture::new(|_| {}, Credentials::default()).await;
    let (status, initialize) = fixture.request("initialize", json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"skerry-tests","version":"1"}}), None).await;
    assert!(status.is_success(), "{initialize}");
    assert_eq!(initialize["result"]["protocolVersion"], "2025-11-25");
    let (_, listing) = fixture.request("tools/list", json!({}), None).await;
    let tools = listing["result"]["tools"].as_array().unwrap();
    let names: BTreeSet<_> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for name in [
        "store_status",
        "list_segments",
        "segment_info",
        "object_info",
        "get_object",
        "put_object",
        "verify_segment",
    ] {
        assert!(names.contains(name), "missing {name}");
    }
    assert!(
        !names.contains("shutdown")
            && !names.contains("rebuild_index")
            && !names.contains("search")
    );
    for tool in tools {
        assert_eq!(tool["inputSchema"]["type"], "object");
        assert!(tool["description"].as_str().is_some_and(|s| !s.is_empty()));
        assert_eq!(tool["annotations"]["openWorldHint"], false);
        if tool["name"] == "verify_segment" {
            assert_eq!(tool["annotations"]["readOnlyHint"], false);
            assert_eq!(tool["annotations"]["destructiveHint"], false);
        } else if tool["name"] == "put_object" {
            assert_eq!(tool["annotations"]["destructiveHint"], false);
            assert_eq!(tool["annotations"]["idempotentHint"], true);
        } else {
            assert_eq!(tool["annotations"]["readOnlyHint"], true);
        }
    }
    let (_, templates) = fixture
        .request("resources/templates/list", json!({}), None)
        .await;
    assert_eq!(
        templates["result"]["resourceTemplates"][0]["uriTemplate"],
        "skerry://objects/{hash}"
    );
    let (_, resources) = fixture.request("resources/list", json!({}), None).await;
    assert!(
        resources["result"]["resources"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let status = payload(&fixture.call("store_status", json!({}), None).await);
    assert_eq!(status["storage"]["objects"], 0);
    fixture.close().await;
}

#[tokio::test]
async fn authenticated_put_deduplicates_and_binary_resources_preserve_bytes() {
    let fixture = Fixture::new(|c| c.mcp.read_only = false, credentials()).await;
    let bytes = Bytes::from_static(b"\0\xffimmutable MCP binary\0");
    let expected = blake3::hash(&bytes).to_hex().to_string();
    let arguments = json!({"data_base64":STANDARD.encode(&bytes)});
    let first = payload(
        &fixture
            .call("put_object", arguments.clone(), Some("write-test"))
            .await,
    );
    let second = payload(
        &fixture
            .call("put_object", arguments, Some("write-test"))
            .await,
    );
    assert_eq!(first["hash"], expected);
    assert_eq!(second["hash"], expected);
    assert_eq!(first["duplicate"], false);
    assert_eq!(second["duplicate"], true);
    let result = payload(
        &fixture
            .call("get_object", json!({"hash":expected}), Some("read-test"))
            .await,
    );
    assert_eq!(
        STANDARD
            .decode(result["data_base64"].as_str().unwrap())
            .unwrap(),
        bytes
    );
    let result = payload(
        &fixture
            .call("object_info", json!({"hash":expected}), Some("read-test"))
            .await,
    );
    assert_eq!(result["exists"], true);
    assert_eq!(result["payload_length"], bytes.len());
    assert!(result.get("path").is_none());
    let (status, result) = fixture
        .request(
            "resources/read",
            json!({"uri":format!("skerry://objects/{expected}")}),
            Some("read-test"),
        )
        .await;
    assert!(status.is_success(), "{result}");
    assert_eq!(
        result["result"]["contents"][0]["mimeType"],
        "application/octet-stream"
    );
    assert_eq!(
        STANDARD
            .decode(result["result"]["contents"][0]["blob"].as_str().unwrap())
            .unwrap(),
        bytes
    );
    assert_eq!(
        fixture.coordinator.status().await.unwrap().storage.objects,
        1
    );
    assert_eq!(
        fixture
            .coordinator
            .get(*blake3::hash(&bytes).as_bytes())
            .await
            .unwrap()
            .unwrap(),
        bytes
    );
    fixture.close().await;
}

#[tokio::test]
async fn default_read_only_and_least_privilege_permissions_are_enforced() {
    let fixture = Fixture::new(|_| {}, credentials()).await;
    let (status, _) = fixture.request("tools/list", json!({}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = fixture
        .request("tools/list", json!({}), Some("unknown-token"))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let argument = json!({"data_base64":STANDARD.encode(b"forbidden")});
    assert!(is_error(
        &fixture
            .call("put_object", argument.clone(), Some("write-test"))
            .await
    ));
    assert!(is_error(
        &fixture
            .call("put_object", argument.clone(), Some("read-test"))
            .await
    ));
    assert!(is_error(
        &fixture
            .call(
                "verify_segment",
                json!({"segment_id":"00112233-4455-4677-8899-aabbccddeeff"}),
                Some("read-test")
            )
            .await
    ));
    assert_eq!(
        fixture.coordinator.status().await.unwrap().storage.objects,
        0
    );
    fixture.close().await;
    let fixture = Fixture::new(|c| c.mcp.read_only = false, credentials()).await;
    assert!(is_error(
        &fixture
            .call("put_object", argument.clone(), Some("read-test"))
            .await
    ));
    assert!(is_error(
        &fixture
            .call("put_object", argument, Some("maintenance-test"))
            .await
    ));
    fixture.close().await;
}

#[tokio::test]
async fn invalid_hash_missing_objects_bad_base64_and_oversize_are_explicit() {
    let fixture = Fixture::new(|c| c.mcp.read_only = false, credentials()).await;
    for name in ["get_object", "object_info"] {
        assert!(is_error(
            &fixture
                .call(name, json!({"hash":"not-a-hash"}), Some("read-test"))
                .await
        ));
    }
    let missing = "00".repeat(32);
    let info = payload(
        &fixture
            .call("object_info", json!({"hash":missing}), Some("read-test"))
            .await,
    );
    assert_eq!(info["exists"], false);
    let missing_object = payload(
        &fixture
            .call("get_object", json!({"hash":missing}), Some("read-test"))
            .await,
    );
    assert_eq!(missing_object["exists"], false);
    assert_eq!(missing_object["inline"], false);
    assert!(is_error(
        &fixture
            .call(
                "put_object",
                json!({"data_base64":"%%%not-base64"}),
                Some("write-test")
            )
            .await
    ));
    let big = vec![42; 129 * 1024];
    assert!(is_error(
        &fixture
            .call(
                "put_object",
                json!({"data_base64":STANDARD.encode(&big)}),
                Some("write-test")
            )
            .await
    ));
    let hash = fixture
        .coordinator
        .put(Bytes::from(big.clone()))
        .await
        .unwrap()
        .hash;
    let result = payload(
        &fixture
            .call(
                "get_object",
                json!({"hash":hex::encode(hash)}),
                Some("read-test"),
            )
            .await,
    );
    assert!(result.get("data_base64").is_none() || result["data_base64"].is_null());
    assert!(result.to_string().contains("HTTP") || result.to_string().contains("http"));
    assert!(fixture.coordinator.get(hash).await.unwrap().is_some());
    let (_, resource) = fixture
        .request(
            "resources/read",
            json!({"uri":format!("skerry://objects/{}",hex::encode(hash))}),
            Some("read-test"),
        )
        .await;
    assert!(resource.get("error").is_some());
    fixture.close().await;
}

#[tokio::test]
async fn segment_pagination_rejects_invalid_cursors_and_discloses_no_paths() {
    let fixture = Fixture::new(
        |c| {
            c.resources.max_object_mib = 1;
            c.segments.max_size_bytes = 1024 * 1024 + 1024;
        },
        Credentials::default(),
    )
    .await;
    for byte in 0..3 {
        fixture
            .coordinator
            .put(Bytes::from(vec![byte; 700 * 1024]))
            .await
            .unwrap();
    }
    let mut cursor = None;
    let mut ids = BTreeSet::new();
    loop {
        let mut args = json!({"limit":1});
        if let Some(cursor) = cursor {
            args["cursor"] = cursor;
        }
        let page = payload(&fixture.call("list_segments", args, None).await);
        let segments = page["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 1);
        let id = segments[0]["segment_id"].as_str().unwrap();
        assert!(ids.insert(id.to_owned()));
        assert!(
            !segments[0].to_string().contains(".seg") && !segments[0].to_string().contains("path")
        );
        let info = payload(
            &fixture
                .call("segment_info", json!({"segment_id":id}), None)
                .await,
        );
        assert_eq!(info["segment"]["segment_id"], id);
        cursor = page.get("next_cursor").filter(|v| !v.is_null()).cloned();
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(ids.len(), 3);
    for args in [
        json!({"cursor":"../../etc/passwd"}),
        json!({"cursor":"unknown-version:00112233"}),
        json!({"limit":0}),
        json!({"limit":101}),
    ] {
        assert!(is_error(&fixture.call("list_segments", args, None).await));
    }
    fixture.close().await;
}

#[tokio::test]
async fn origin_request_body_and_disabled_endpoint_limits_are_enforced() {
    let fixture = Fixture::new(|_| {}, Credentials::default()).await;
    let response = fixture
        .router
        .clone()
        .oneshot(
            Request::post("/mcp")
                .header("host", "127.0.0.1:7788")
                .header("origin", "https://evil.example")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = fixture
        .router
        .clone()
        .oneshot(
            Request::post("/mcp")
                .header("host", "127.0.0.1:7788")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(vec![b' '; 257 * 1024]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    fixture.close().await;
    let fixture = Fixture::new(|c| c.mcp.enabled = false, Credentials::default()).await;
    let (status, _) = fixture.request("tools/list", json!({}), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    fixture.close().await;
}

#[tokio::test]
async fn official_sdk_client_uses_streamable_http_during_ordinary_ingestion() {
    let fixture = Fixture::new(|_| {}, Credentials::default()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cancellation = fixture.cancellation.clone();
    let app = fixture.router.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(cancellation.cancelled_owned())
            .await
            .unwrap();
    });
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
    );
    let client = ().serve(transport).await.unwrap();
    let tools = client.list_all_tools().await.unwrap();
    assert!(tools.iter().any(|t| t.name == "store_status"));
    let coordinator = fixture.coordinator.clone();
    let writer = tokio::spawn(async move {
        for n in 0..16 {
            coordinator
                .put(Bytes::from(vec![n; 5 * 1024]))
                .await
                .unwrap();
        }
    });
    for _ in 0..4 {
        let result = client
            .call_tool(CallToolRequestParams::new("store_status"))
            .await
            .unwrap();
        assert_ne!(result.is_error, Some(true));
    }
    writer.await.unwrap();
    let http = reqwest::Client::new();
    let response = http
        .put(format!("http://{address}/v1/objects"))
        .header("content-type", "application/octet-stream")
        .body("native concurrent payload")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let result = client
        .call_tool(
            CallToolRequestParams::new("get_object").with_arguments(
                json!({"hash":response["hash"]})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_ne!(result.is_error, Some(true));
    let result = serde_json::to_value(result).unwrap();
    let content = result.get("structuredContent").cloned().unwrap_or_else(|| {
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap()
    });
    assert_eq!(
        STANDARD
            .decode(content["data_base64"].as_str().unwrap())
            .unwrap(),
        b"native concurrent payload"
    );
    assert_eq!(
        fixture.coordinator.status().await.unwrap().storage.objects,
        17
    );
    client.cancel().await.unwrap();
    fixture.cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn mcp_reads_share_native_payload_memory_and_release_capacity() {
    let fixture = Fixture::new(
        |c| {
            c.resources.max_object_mib = 1;
            c.resources.max_inflight_read_mib = 1;
        },
        Credentials::default(),
    )
    .await;
    let large = fixture
        .coordinator
        .put(Bytes::from(vec![7; 1024 * 1024]))
        .await
        .unwrap()
        .hash;
    let small = fixture
        .coordinator
        .put(Bytes::from_static(b"small MCP object"))
        .await
        .unwrap()
        .hash;
    let held = fixture
        .router
        .clone()
        .oneshot(
            Request::get(format!("/v1/objects/{}", hex::encode(large)))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(held.status(), StatusCode::OK);
    assert!(is_error(
        &fixture
            .call("get_object", json!({"hash":hex::encode(small)}), None)
            .await
    ));
    drop(held);
    let object = payload(
        &fixture
            .call("get_object", json!({"hash":hex::encode(small)}), None)
            .await,
    );
    assert_eq!(
        STANDARD
            .decode(object["data_base64"].as_str().unwrap())
            .unwrap(),
        b"small MCP object"
    );
    fixture.close().await;
}

#[tokio::test]
async fn client_rate_limit_is_bounded_and_token_specific() {
    let fixture = Fixture::new(|c| c.mcp.requests_per_minute = 2, credentials()).await;
    for _ in 0..2 {
        let (status, result) = fixture
            .request("tools/list", json!({}), Some("read-test"))
            .await;
        assert!(status.is_success(), "{result}");
    }
    let (status, _) = fixture
        .request("tools/list", json!({}), Some("read-test"))
        .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let (status, result) = fixture
        .request("tools/list", json!({}), Some("write-test"))
        .await;
    assert!(
        status.is_success(),
        "separate token must have its own rate budget: {result}"
    );
    fixture.close().await;
}

#[tokio::test]
async fn maintenance_verification_jobs_are_bounded_readable_and_owner_bound() {
    let fixture = Fixture::new(|c| c.mcp.allow_verification = true, credentials()).await;
    fixture
        .coordinator
        .put(Bytes::from_static(b"verified MCP job payload"))
        .await
        .unwrap();
    let page = payload(
        &fixture
            .call("list_segments", json!({}), Some("read-test"))
            .await,
    );
    let id = page["segments"][0]["segment_id"].as_str().unwrap();
    let job = payload(
        &fixture
            .call(
                "verify_segment",
                json!({"segment_id":id}),
                Some("maintenance-test"),
            )
            .await,
    );
    let job_id = job["job_id"].as_str().unwrap();
    assert!(
        is_error(
            &fixture
                .call(
                    "verification_status",
                    json!({"job_id":job_id}),
                    Some("read-test")
                )
                .await
        ),
        "another principal must not inspect private verification jobs"
    );
    let mut completed = None;
    for _ in 0..100 {
        let state = payload(
            &fixture
                .call(
                    "verification_status",
                    json!({"job_id":job_id}),
                    Some("maintenance-test"),
                )
                .await,
        );
        match state["state"].as_str().unwrap() {
            "completed" => {
                completed = Some(state);
                break;
            }
            "running" => tokio::time::sleep(Duration::from_millis(50)).await,
            other => panic!("unexpected verification state {other}: {state}"),
        }
    }
    let state = completed.expect("verification job must complete");
    assert!(state["verified_bytes"].as_u64().unwrap() > 0);
    assert_eq!(state["report"]["segment_id"], id);
    assert!(is_error(
        &fixture
            .call(
                "verification_status",
                json!({"job_id":"not-a-job"}),
                Some("maintenance-test")
            )
            .await
    ));
    assert!(is_error(
        &fixture
            .call(
                "verify_segment",
                json!({"segment_id":"not-a-uuid"}),
                Some("maintenance-test")
            )
            .await
    ));
    let status = fixture.coordinator.status().await.unwrap();
    assert_eq!(status.storage.objects, 1);
    fixture.close().await;
}

#[tokio::test]
async fn outstanding_encoded_responses_have_bounded_lifetime_memory() {
    let fixture = Fixture::new(
        |c| c.mcp.max_response_memory_mib = 1,
        Credentials::default(),
    )
    .await;
    let hash = fixture
        .coordinator
        .put(Bytes::from(vec![19; 128 * 1024]))
        .await
        .unwrap()
        .hash;
    let mut held = Vec::new();
    let mut exhausted = false;
    for id in 0..8 {
        let response=fixture.router.clone().oneshot(Request::post("/mcp").header("host","127.0.0.1:7788").header("content-type","application/json").header("accept","application/json, text/event-stream").header("mcp-protocol-version","2025-11-25").body(Body::from(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"get_object","arguments":{"hash":hex::encode(hash)}}}).to_string())).unwrap()).await.unwrap();
        if response.status() == StatusCode::SERVICE_UNAVAILABLE {
            exhausted = true;
            break;
        }
        assert!(response.status().is_success());
        held.push(response);
    }
    assert!(
        exhausted,
        "MCP retained response allocations must exhaust the configured budget"
    );
    drop(held);
    let object = payload(
        &fixture
            .call("get_object", json!({"hash":hex::encode(hash)}), None)
            .await,
    );
    assert_eq!(
        STANDARD
            .decode(object["data_base64"].as_str().unwrap())
            .unwrap()
            .len(),
        128 * 1024
    );
    fixture.close().await;
}

#[tokio::test]
async fn gateway_credentials_and_attested_scopes_cannot_be_forged_by_clients() {
    let mut tokens = credentials();
    tokens.gateway = Some("private-gateway-secret".into());
    let fixture = Fixture::new(
        |c| {
            c.mcp.auth_gateway = true;
            c.mcp.external_url = Some("https://mcp.example/mcp".into());
            c.mcp.allowed_origins = vec!["https://agent.example".into()];
            c.mcp.read_only = false;
        },
        tokens,
    )
    .await;
    let call = |token: &str, scopes: &str, origin: &str, method: &str, args: Value| {
        Request::post("/mcp").header("host","mcp.example").header("content-type","application/json").header("accept","application/json, text/event-stream").header("mcp-protocol-version","2025-11-25").header("authorization",format!("Bearer {token}")).header("x-skerry-mcp-subject","agent-A").header("x-skerry-mcp-scopes",scopes).header("origin",origin).body(Body::from(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":method,"arguments":args}}).to_string())).unwrap()
    };
    let response = fixture
        .router
        .clone()
        .oneshot(call(
            "write-test",
            "read write",
            "https://agent.example",
            "put_object",
            json!({"data_base64":STANDARD.encode(b"forged")}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = fixture
        .router
        .clone()
        .oneshot(call(
            "private-gateway-secret",
            "read",
            "https://agent.example",
            "put_object",
            json!({"data_base64":STANDARD.encode(b"forbidden")}),
        ))
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert!(is_error(&decode_rpc(
        &to_bytes(response.into_body(), 1024 * 1024).await.unwrap()
    )));
    let response = fixture
        .router
        .clone()
        .oneshot(call(
            "private-gateway-secret",
            "read write",
            "https://agent.example",
            "put_object",
            json!({"data_base64":STANDARD.encode(b"authorized gateway")}),
        ))
        .await
        .unwrap();
    assert!(response.status().is_success());
    let result = decode_rpc(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap());
    assert_eq!(
        payload(&result)["hash"],
        blake3::hash(b"authorized gateway").to_hex().to_string()
    );
    let response = fixture
        .router
        .clone()
        .oneshot(call(
            "private-gateway-secret",
            "read",
            "http://agent.example",
            "store_status",
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        fixture.coordinator.status().await.unwrap().storage.objects,
        1
    );
    fixture.close().await;
}

#[tokio::test]
async fn non_loopback_mcp_requires_explicit_https_gateway_configuration() {
    let root = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.root = root.path().into();
    config.server.listen = "0.0.0.0:7788".into();
    let coordinator = Coordinator::open(config).await.unwrap();
    let tokens = Credentials {
        service: Some("native-service-token".into()),
        ..Default::default()
    };
    let error =
        server::router_with_mcp_credentials(coordinator.clone(), CancellationToken::new(), tokens)
            .expect_err("static bearer alone must not expose remote MCP");
    assert!(error.to_string().contains("gateway") || error.to_string().contains("MCP"));
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupted_payloads_fail_integrity_checks_without_disclosing_paths_or_bytes() {
    use std::io::{Seek as _, SeekFrom, Write as _};
    let fixture = Fixture::new(|c| c.mcp.allow_verification = true, credentials()).await;
    let stored = fixture
        .coordinator
        .put(Bytes::from_static(b"verified immutable bytes"))
        .await
        .unwrap();
    let page = payload(
        &fixture
            .call("list_segments", json!({}), Some("read-test"))
            .await,
    );
    let segment = page["segments"][0]["segment_id"].as_str().unwrap();
    let path = fixture
        ._root
        .path()
        .join("segments")
        .join(&segment.replace('-', "")[..2])
        .join(format!("{segment}.seg"));
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(128)).unwrap();
    file.write_all(b"X").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let result = fixture
        .call(
            "get_object",
            json!({"hash":hex::encode(stored.hash)}),
            Some("read-test"),
        )
        .await;
    assert!(is_error(&result));
    assert!(!result.to_string().contains("data_base64"));
    assert!(
        !result
            .to_string()
            .contains(fixture._root.path().to_string_lossy().as_ref())
    );
    let job = payload(
        &fixture
            .call(
                "verify_segment",
                json!({"segment_id":segment}),
                Some("maintenance-test"),
            )
            .await,
    );
    let job_id = job["job_id"].as_str().unwrap();
    let mut failed = None;
    for _ in 0..100 {
        let status = payload(
            &fixture
                .call(
                    "verification_status",
                    json!({"job_id":job_id}),
                    Some("maintenance-test"),
                )
                .await,
        );
        if status["state"] == "failed" {
            failed = Some(status);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let failed = failed.expect("corrupt segment verification must fail");
    assert!(!failed.to_string().contains(path.to_string_lossy().as_ref()));
    for uri in ["file:///etc/passwd", "https://example.com/secret"] {
        let (_, result) = fixture
            .request("resources/read", json!({"uri":uri}), Some("read-test"))
            .await;
        assert!(result.get("error").is_some());
    }
    fixture.close().await;
}

#[tokio::test]
async fn trace_diagnostics_never_include_mcp_payloads_or_credentials() {
    use std::process::{Command, Stdio};
    struct Process(std::process::Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let root = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.root = root.path().join("store");
    config.mcp.read_only = false;
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    config.server.listen = socket.local_addr().unwrap().to_string();
    drop(socket);
    let url = format!("http://{}", config.server.listen);
    let path = root.path().join("config.toml");
    config.save(&path).unwrap();
    let log_path = root.path().join("diagnostics.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_skerry"));
    command
        .args(["serve", "--config"])
        .arg(path)
        .env("RUST_LOG", "trace,rmcp::service=trace")
        .env("SKERRY_MCP_WRITE_TOKEN", "diagnostic-write-secret")
        .env("SKERRY_TOKEN", "diagnostic-admin-secret")
        .env_remove("SKERRY_MCP_READ_TOKEN")
        .env_remove("SKERRY_MCP_MAINTENANCE_TOKEN")
        .env_remove("SKERRY_MCP_GATEWAY_TOKEN")
        .env_remove("SKERRY_ROOT")
        .env_remove("SKERRY_LISTEN")
        .env_remove("SKERRY_MCP_ENABLED")
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    let mut process = Process(command.spawn().unwrap());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let mut ready = false;
    for _ in 0..200 {
        if process.0.try_wait().unwrap().is_some() {
            panic!("diagnostic test service exited early");
        }
        if client
            .get(format!("{url}/v1/health"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(ready);
    let secret = b"PRIVATE-SKERRY-MCP-BYTES-DO-NOT-LOG";
    let encoded = STANDARD.encode(secret);
    let response=client.post(format!("{url}/mcp")).bearer_auth("diagnostic-write-secret").header("accept","application/json, text/event-stream").header("mcp-protocol-version","2025-11-25").json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"put_object","arguments":{"data_base64":encoded}}})).send().await.unwrap();
    assert!(response.status().is_success());
    let result = decode_rpc(&response.bytes().await.unwrap());
    let hash = payload(&result)["hash"].as_str().unwrap().to_owned();
    let response=client.post(format!("{url}/mcp")).bearer_auth("diagnostic-write-secret").header("accept","application/json, text/event-stream").header("mcp-protocol-version","2025-11-25").json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_object","arguments":{"hash":hash}}})).send().await.unwrap();
    assert!(response.status().is_success());
    assert_eq!(
        payload(&decode_rpc(&response.bytes().await.unwrap()))["data_base64"],
        encoded
    );
    let _=client.post(format!("{url}/mcp")).bearer_auth("diagnostic-write-secret").header("accept","application/json, text/event-stream").header("mcp-protocol-version","2025-11-25").json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":999,"reason":"PRIVATE-CANCELLATION-REASON-DO-NOT-LOG"}})).send().await.unwrap();
    client
        .post(format!("{url}/v1/admin/shutdown"))
        .bearer_auth("diagnostic-admin-secret")
        .send()
        .await
        .unwrap();
    for _ in 0..200 {
        if process.0.try_wait().unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(process.0.try_wait().unwrap().is_some());
    let diagnostics = std::fs::read_to_string(log_path).unwrap();
    assert!(
        !diagnostics.contains(&encoded),
        "MCP payload leaked through trace diagnostics"
    );
    assert!(
        !diagnostics.contains(std::str::from_utf8(secret).unwrap()),
        "plaintext payload leaked through diagnostics"
    );
    assert!(
        !diagnostics.contains("diagnostic-write-secret")
            && !diagnostics.contains("diagnostic-admin-secret"),
        "MCP credential leaked through diagnostics"
    );
    assert!(
        !diagnostics.contains("PRIVATE-CANCELLATION-REASON-DO-NOT-LOG"),
        "peer notification contents leaked through diagnostics"
    );
    assert!(
        !diagnostics.contains(config.storage.root.to_string_lossy().as_ref()),
        "local store path leaked through diagnostics"
    );
}

#[tokio::test]
async fn native_and_mcp_requests_share_the_global_http_admission_limit() {
    let fixture = Fixture::new(|c| c.resources.max_requests = 1, Credentials::default()).await;
    let held = fixture
        .router
        .clone()
        .oneshot(Request::get("/v1/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(held.status(), StatusCode::OK);
    let (status, _) = fixture.request("tools/list", json!({}), None).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "MCP must use native global HTTP admission"
    );
    drop(held);
    let (status, result) = fixture.request("tools/list", json!({}), None).await;
    assert!(status.is_success(), "{result}");
    fixture.close().await;
}

#[tokio::test]
async fn scoped_clients_can_fetch_large_native_objects_without_admin_privileges() {
    let fixture = Fixture::new(|_| {}, credentials()).await;
    let bytes = Bytes::from(vec![99; 129 * 1024]);
    let hash = fixture.coordinator.put(bytes.clone()).await.unwrap().hash;
    let description = payload(
        &fixture
            .call(
                "get_object",
                json!({"hash":hex::encode(hash)}),
                Some("read-test"),
            )
            .await,
    );
    assert_eq!(description["inline"], false);
    let path = format!("/v1/objects/{}", hex::encode(hash));
    let unauthorized = fixture
        .router
        .clone()
        .oneshot(Request::get(&path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let fetched = fixture
        .router
        .clone()
        .oneshot(
            Request::get(&path)
                .header("authorization", "Bearer read-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fetched.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(fetched.into_body(), 1024 * 1024).await.unwrap(),
        bytes
    );
    let head = fixture
        .router
        .clone()
        .oneshot(
            Request::head(&path)
                .header("authorization", "Bearer read-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    for route in ["/v1/admin/shutdown", "/v1/admin/cache-clear", "/v1/objects"] {
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::post(route)
                    .header("authorization", "Bearer read-test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "read scope cannot access {route}"
        );
    }
    let response = fixture
        .router
        .clone()
        .oneshot(
            Request::put("/v1/objects")
                .header("authorization", "Bearer read-test")
                .header("content-type", "application/octet-stream")
                .body(Body::from("forbidden native write"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = fixture
        .router
        .clone()
        .oneshot(
            Request::get("/v1/status")
                .header("authorization", "Bearer read-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "raw event/error logs are administrative"
    );
    let response = fixture
        .router
        .clone()
        .oneshot(
            Request::get("/v1/mcp/status")
                .header("authorization", "Bearer read-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!fixture.cancellation.is_cancelled());
    assert_eq!(
        fixture.coordinator.status().await.unwrap().storage.objects,
        1
    );
    fixture.close().await;
}

#[tokio::test]
async fn acknowledged_mcp_put_survives_forced_service_process_termination() {
    use std::process::{Child, Command, Stdio};
    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let root = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.root = root.path().join("store");
    config.mcp.read_only = false;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    config.server.listen = listener.local_addr().unwrap().to_string();
    drop(listener);
    let path = root.path().join("config.toml");
    config.save(&path).unwrap();
    let url = format!("http://{}", config.server.listen);
    let start = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_skerry"));
        command
            .args(["serve", "--config"])
            .arg(&path)
            .env("SKERRY_MCP_WRITE_TOKEN", "durable-mcp-write-test")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for variable in [
            "SKERRY_TOKEN",
            "SKERRY_ROOT",
            "SKERRY_LISTEN",
            "SKERRY_MCP_ENABLED",
            "SKERRY_MCP_READ_TOKEN",
            "SKERRY_MCP_MAINTENANCE_TOKEN",
            "SKERRY_MCP_GATEWAY_TOKEN",
            "SKERRY_FAILPOINT",
        ] {
            command.env_remove(variable);
        }
        Process(command.spawn().unwrap())
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let ready = |url: String| {
        let client = client.clone();
        async move {
            for _ in 0..200 {
                if client
                    .get(format!("{url}/v1/health"))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            panic!("MCP process did not become ready");
        }
    };
    let mut process = start();
    ready(url.clone()).await;
    let bytes: Vec<u8> = (0..20 * 1024).map(|n| (n % 251) as u8).collect();
    let response=client.post(format!("{url}/mcp")).bearer_auth("durable-mcp-write-test").header("accept","application/json, text/event-stream").header("mcp-protocol-version","2025-11-25").json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"put_object","arguments":{"data_base64":STANDARD.encode(&bytes)}}})).send().await.unwrap().error_for_status().unwrap();
    let result = payload(&decode_rpc(&response.bytes().await.unwrap()));
    let hash = result["hash"].as_str().unwrap().to_owned();
    assert_eq!(hash, blake3::hash(&bytes).to_hex().as_str());
    assert_eq!(result["duplicate"], false);
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    drop(process);
    let _process = start();
    ready(url.clone()).await;
    let recovered=client.post(format!("{url}/mcp")).bearer_auth("durable-mcp-write-test").header("accept","application/json, text/event-stream").header("mcp-protocol-version","2025-11-25").json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_object","arguments":{"hash":hash}}})).send().await.unwrap().error_for_status().unwrap();
    let recovered = payload(&decode_rpc(&recovered.bytes().await.unwrap()));
    assert_eq!(
        STANDARD
            .decode(recovered["data_base64"].as_str().unwrap())
            .unwrap(),
        bytes
    );
}
