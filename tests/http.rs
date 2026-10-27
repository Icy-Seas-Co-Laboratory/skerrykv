use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use bytes::Bytes;
use skerrykv::{
    config::Config,
    coordinator::Coordinator,
    server::{self, BATCH_MEDIA_TYPE, Frame, encode_frame},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

async fn setup() -> (
    tempfile::TempDir,
    std::sync::Arc<Coordinator>,
    axum::Router,
    CancellationToken,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.root = dir.path().to_path_buf();
    let coordinator = Coordinator::open(config).await.unwrap();
    let cancellation = CancellationToken::new();
    let app = server::router(coordinator.clone(), cancellation.clone()).unwrap();
    (dir, coordinator, app, cancellation)
}

#[tokio::test]
async fn raw_roundtrip_head_metrics_and_graceful_request() {
    let (_dir, coordinator, app, cancellation) = setup().await;
    let payload = Bytes::from_static(b"\0binary\xff data");
    let expected = blake3::hash(&payload).to_hex().to_string();
    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/objects")
                .header("content-type", "application/octet-stream")
                .body(Body::from(payload.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-skerry-hash"], expected);
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["hash"], expected);
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/v1/objects/{expected}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(to_bytes(response.into_body(), 4096).await.unwrap(), payload);
    let response = app
        .clone()
        .oneshot(
            Request::head(format!("/v1/objects/{expected}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        to_bytes(response.into_body(), 4096)
            .await
            .unwrap()
            .is_empty()
    );
    let response = app
        .clone()
        .oneshot(Request::get("/v1/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        !to_bytes(response.into_body(), 16384)
            .await
            .unwrap()
            .is_empty()
    );
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/admin/shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(cancellation.is_cancelled());
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_hash_missing_object_and_media_type() {
    let (_dir, coordinator, app, _) = setup().await;
    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/objects/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/v1/objects/{}", "00".repeat(32)))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = app
        .oneshot(
            Request::put("/v1/objects")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    coordinator.shutdown().await.unwrap();
}

fn parse_frames(mut data: &[u8]) -> Vec<Frame> {
    let mut result = Vec::new();
    while !data.is_empty() {
        let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        assert!(len >= 56 && data.len() >= len + 4);
        assert_eq!(&data[4..8], b"SKB1");
        result.push(Frame {
            kind: data[8],
            status: data[9],
            ordinal: u64::from_le_bytes(data[12..20].try_into().unwrap()),
            hash: data[20..52].try_into().unwrap(),
            payload: Bytes::copy_from_slice(&data[60..4 + len]),
        });
        data = &data[4 + len..];
    }
    result
}

#[tokio::test]
async fn streaming_batch_put_get_ordering_and_partial_failure() {
    let (_dir, coordinator, app, _) = setup().await;
    let mut input = Vec::new();
    for ordinal in 0..3 {
        input.extend_from_slice(&encode_frame(&Frame {
            kind: 1,
            status: 0,
            ordinal,
            hash: [0; 32],
            payload: Bytes::from(vec![ordinal as u8; 1024]),
        }));
    }
    // Complete earlier records remain committed despite the malformed suffix.
    input.extend_from_slice(&[1, 0, 0, 0]);
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/objects/batch-put")
                .header("content-type", BATCH_MEDIA_TYPE)
                .body(Body::from(input))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let frames = parse_frames(&to_bytes(response.into_body(), 65536).await.unwrap());
    assert_eq!(frames.len(), 4);
    assert_eq!(frames[3].kind, 255);
    assert_eq!(frames[3].status, 3);
    let hashes: Vec<_> = frames[..3]
        .iter()
        .map(|frame| {
            assert_eq!(frame.status, 0);
            frame.hash
        })
        .collect();
    for ordered in [true, false] {
        let mut input = Vec::new();
        for (ordinal, hash) in hashes.iter().enumerate() {
            input.extend_from_slice(&encode_frame(&Frame {
                kind: 2,
                status: 0,
                ordinal: ordinal as u64,
                hash: *hash,
                payload: Bytes::new(),
            }));
        }
        input.extend_from_slice(&encode_frame(&Frame {
            kind: 2,
            status: 0,
            ordinal: 3,
            hash: [0; 32],
            payload: Bytes::new(),
        }));
        let response = app
            .clone()
            .oneshot(
                Request::post(format!(
                    "/v1/objects/batch-get?ordered={ordered}&priority=bulk"
                ))
                .header("content-type", BATCH_MEDIA_TYPE)
                .body(Body::from(input))
                .unwrap(),
            )
            .await
            .unwrap();
        let mut results = parse_frames(&to_bytes(response.into_body(), 65536).await.unwrap());
        assert_eq!(results.len(), 4);
        if ordered {
            assert_eq!(
                results.iter().map(|r| r.ordinal).collect::<Vec<_>>(),
                vec![0, 1, 2, 3]
            );
        }
        results.sort_by_key(|frame| frame.ordinal);
        for frame in &results[..3] {
            assert_eq!(frame.status, 0);
            assert_eq!(frame.payload.len(), 1024);
        }
        assert_eq!(results[3].status, 1);
    }
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn non_loopback_requires_auth_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.root = dir.path().to_path_buf();
    config.server.listen = "0.0.0.0:7788".into();
    let coordinator = Coordinator::open(config).await.unwrap();
    if std::env::var("SKERRY_TOKEN").is_err() {
        assert!(server::router(coordinator.clone(), CancellationToken::new()).is_err());
    }
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn completion_batch_returns_before_input_eof() {
    use futures_util::StreamExt;
    use std::convert::Infallible;
    let (_dir, coordinator, app, _) = setup().await;
    let stored = coordinator
        .put(Bytes::from_static(b"progressive response"))
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(1);
    let stream = async_stream::stream! {
        while let Some(bytes) = rx.recv().await { yield Ok::<_,Infallible>(bytes); }
    };
    let response = app
        .oneshot(
            Request::post("/v1/objects/batch-get?ordered=false")
                .header("content-type", BATCH_MEDIA_TYPE)
                .body(Body::from_stream(stream))
                .unwrap(),
        )
        .await
        .unwrap();
    tx.send(encode_frame(&Frame {
        kind: 2,
        status: 0,
        ordinal: 0,
        hash: stored.hash,
        payload: Bytes::new(),
    }))
    .await
    .unwrap();
    let mut output = response.into_body().into_data_stream();
    let header = tokio::time::timeout(std::time::Duration::from_secs(2), output.next())
        .await
        .expect("first result must not wait for more input frames")
        .unwrap()
        .unwrap();
    assert_eq!(&header[4..8], b"SKB1");
    assert_eq!(header[9], 0);
    let payload = output.next().await.unwrap().unwrap();
    assert_eq!(payload, "progressive response");
    drop(tx);
    assert!(output.next().await.is_none());
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn request_capacity_is_retained_until_body_drops() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.root = dir.path().to_path_buf();
    config.resources.max_requests = 1;
    let coordinator = Coordinator::open(config).await.unwrap();
    let app = server::router(coordinator.clone(), CancellationToken::new()).unwrap();
    let retained = app
        .clone()
        .oneshot(Request::get("/v1/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let exhausted = app
        .clone()
        .oneshot(Request::get("/v1/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(exhausted.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(retained);
    let recovered = app
        .oneshot(Request::get("/v1/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(recovered.status(), StatusCode::OK);
    drop(recovered);
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn ingress_capacity_releases_and_oversized_raw_body_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.root = dir.path().to_path_buf();
    config.resources.max_object_mib = 1;
    config.writes.max_pending_mib = 1;
    let coordinator = Coordinator::open(config).await.unwrap();
    let app = server::router(coordinator.clone(), CancellationToken::new()).unwrap();
    let retained = app
        .clone()
        .oneshot(
            Request::post("/v1/objects/batch-put")
                .header("content-type", BATCH_MEDIA_TYPE)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let exhausted = app
        .clone()
        .oneshot(
            Request::put("/v1/objects")
                .header("content-type", "application/octet-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(exhausted.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(retained);
    let oversized = app
        .clone()
        .oneshot(
            Request::put("/v1/objects")
                .header("content-type", "application/octet-stream")
                .body(Body::from(vec![0; 1024 * 1024 + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    drop(oversized);
    let recovered = app
        .oneshot(
            Request::put("/v1/objects")
                .header("content-type", "application/octet-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recovered.status(), StatusCode::OK);
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn tcp_duplex_batch_produces_result_while_upload_remains_open() {
    use futures_util::StreamExt;
    use std::convert::Infallible;
    let directory = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = Config::default();
    config.storage.root = directory.path().into();
    config.server.listen = address.to_string();
    let coordinator = Coordinator::open(config).await.unwrap();
    let object = coordinator
        .put(Bytes::from_static(b"duplex TCP payload"))
        .await
        .unwrap();
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(server::serve_on_listener(
        coordinator.clone(),
        cancellation.clone(),
        listener,
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(1);
    tx.send(encode_frame(&Frame {
        kind: 2,
        status: 0,
        ordinal: 19,
        hash: object.hash,
        payload: Bytes::new(),
    }))
    .await
    .unwrap();
    let source = async_stream::stream! {
        while let Some(bytes) = rx.recv().await { yield Ok::<_,Infallible>(bytes); }
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    let response = client
        .post(format!(
            "http://{address}/v1/objects/batch-get?ordered=false"
        ))
        .header("content-type", BATCH_MEDIA_TYPE)
        .body(reqwest::Body::wrap_stream(source))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let mut incoming = response.bytes_stream();
    let mut result = Vec::new();
    while result.len() < 60 + b"duplex TCP payload".len() {
        result.extend_from_slice(
            &tokio::time::timeout(std::time::Duration::from_secs(2), incoming.next())
                .await
                .expect("duplex result must arrive before request EOF")
                .unwrap()
                .unwrap(),
        );
    }
    let frames = parse_frames(&result);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].ordinal, 19);
    assert_eq!(frames[0].payload, "duplex TCP payload");
    drop(tx);
    assert!(incoming.next().await.is_none());
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn expired_drain_aborts_stalled_tcp_upload_before_returning() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let directory = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = Config::default();
    config.storage.root = directory.path().into();
    config.server.listen = address.to_string();
    config.server.shutdown_timeout_seconds = 1;
    let coordinator = Coordinator::open(config).await.unwrap();
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(server::serve_on_listener(
        coordinator.clone(),
        cancellation.clone(),
        listener,
    ));
    let mut stalled = tokio::net::TcpStream::connect(address).await.unwrap();
    stalled.write_all(b"PUT /v1/objects HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/octet-stream\r\nContent-Length: 1\r\n\r\n").await.unwrap();
    // Allow the request to enter its body read before triggering graceful drain.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    cancellation.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err(), "expired drain must report nonzero failure");
    let mut data = Vec::new();
    match tokio::time::timeout(
        std::time::Duration::from_secs(1),
        stalled.read_to_end(&mut data),
    )
    .await
    {
        Ok(_) => {}
        Err(_) => panic!("stalled TCP connection survived forced drain"),
    }
    assert_eq!(coordinator.status().await.unwrap().storage.objects, 0);
}

#[tokio::test]
async fn retained_read_response_reports_retryable_capacity_exhaustion() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.root = directory.path().into();
    config.resources.max_object_mib = 1;
    config.resources.max_inflight_read_mib = 1;
    config.cache.payload_mib = 0;
    let coordinator = Coordinator::open(config).await.unwrap();
    let object = coordinator
        .put(Bytes::from(vec![8; 1024 * 1024]))
        .await
        .unwrap();
    let app = server::router(coordinator.clone(), CancellationToken::new()).unwrap();
    let uri = format!("/v1/objects/{}", hex::encode(object.hash));
    let first = app
        .clone()
        .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let second = app
        .clone()
        .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(first);
    let third = app
        .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(third.status(), StatusCode::OK);
    drop(third);
    coordinator.shutdown().await.unwrap();
}
