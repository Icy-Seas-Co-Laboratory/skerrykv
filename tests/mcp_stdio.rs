use std::{
    process::{Command, Stdio},
    time::Duration,
};

use bytes::Bytes;
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ReadResourceRequestParams},
    transport::{ConfigureCommandExt, TokioChildProcess},
};
use serde_json::json;
use skerrykv::{config::Config, coordinator::Coordinator, mcp::Credentials, server};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

#[test]
fn stdio_mode_rejects_plain_http_to_non_loopback_before_connecting() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_skerry"))
        .args(["mcp", "stdio", "--connect", "http://example.com:7788"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("launch skerry MCP stdio command");
    drop(child.stdin.take());
    let output = child.wait_with_output().expect("wait for skerry");
    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "stdio mode must keep stdout protocol-only"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("plain HTTP MCP connections are allowed only to loopback hosts"),
        "unexpected diagnostic: {stderr}"
    );
}

#[test]
fn stdio_mode_rejects_credentials_and_query_in_connect_url() {
    let output = Command::new(env!("CARGO_BIN_EXE_skerry"))
        .args([
            "mcp",
            "stdio",
            "--connect",
            "https://user:pass@example.com:7788/?token=secret",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("launch skerry MCP stdio command");
    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "stdio mode must keep stdout protocol-only"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("without credentials, query, or fragment"));
    assert!(
        !stderr.contains("secret"),
        "diagnostics must not echo URL credentials or query values"
    );
}

#[tokio::test]
async fn stdio_bridge_discovers_tools_and_reads_objects_from_the_running_service()
-> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let mut config = Config::default();
    config.storage.root = root.path().to_path_buf();
    config.cache.payload_mib = 0;
    config.writes.batch_max_delay_ms = 1;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    config.server.listen = address.to_string();

    // Keep the coordinator open for the entire bridge session. If the stdio
    // process tried to open the store itself, it would collide with this lock.
    let coordinator = Coordinator::open(config).await?;
    let content = Bytes::from_static(b"stdio MCP bridge bytes\0\xff");
    let hash = blake3::hash(&content).to_hex().to_string();
    coordinator.put(content.clone()).await?;
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server_coordinator = coordinator.clone();
    let server_task =
        tokio::spawn(async move { server::serve(server_coordinator, server_shutdown).await });

    let base_url = format!("http://{address}");
    let client_http = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client_http
                .get(format!("{base_url}/v1/status"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    let command = tokio::process::Command::new(env!("CARGO_BIN_EXE_skerry")).configure(|cmd| {
        cmd.arg("mcp")
            .arg("stdio")
            .arg("--connect")
            .arg(&base_url)
            .env_remove("SKERRY_TOKEN")
            .env_remove("SKERRY_MCP_TOKEN")
            .env("SKERRY_ROOT", root.path());
    });
    let transport = TokioChildProcess::builder(command)
        .stderr(Stdio::piped())
        .spawn()?
        .0;
    let client = tokio::time::timeout(Duration::from_secs(10), ().serve(transport)).await??;
    let tools = tokio::time::timeout(Duration::from_secs(10), client.list_all_tools()).await??;
    assert!(
        tools.iter().any(|tool| tool.name == "get_object"),
        "stdio stdout must contain valid MCP JSON-RPC tool discovery"
    );

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        client.call_tool(
            CallToolRequestParams::new("get_object")
                .with_arguments(json!({"hash":hash}).as_object().unwrap().clone()),
        ),
    )
    .await??;
    assert_ne!(result.is_error, Some(true));
    let value = result
        .structured_content
        .expect("structured get_object result");
    assert_eq!(value["exists"], true);
    assert_eq!(
        value["data_base64"],
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &content)
    );

    let templates = tokio::time::timeout(
        Duration::from_secs(10),
        client.list_resource_templates(None),
    )
    .await??;
    assert!(
        templates
            .resource_templates
            .iter()
            .any(|template| template.uri_template == "skerry://objects/{hash}")
    );
    let resource = tokio::time::timeout(
        Duration::from_secs(10),
        client.read_resource(ReadResourceRequestParams::new(format!(
            "skerry://objects/{hash}"
        ))),
    )
    .await??;
    assert_eq!(resource.contents.len(), 1);
    let expected_blob =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &content);
    match &resource.contents[0] {
        rmcp::model::ResourceContents::BlobResourceContents { blob, .. } => {
            assert_eq!(blob, &expected_blob);
        }
        _ => panic!("expected binary resource contents"),
    }

    client.cancel().await?;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), server_task).await???;
    coordinator.shutdown().await?;
    drop(root);
    Ok(())
}

#[tokio::test]
async fn stdio_bridge_accepts_legacy_2025_initialize_and_keeps_stdout_jsonrpc_only()
-> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let mut config = Config::default();
    config.storage.root = root.path().to_path_buf();
    config.cache.payload_mib = 0;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    config.server.listen = address.to_string();
    let coordinator = Coordinator::open(config).await?;
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server_coordinator = coordinator.clone();
    let server_task =
        tokio::spawn(async move { server::serve(server_coordinator, server_shutdown).await });
    let base_url = format!("http://{address}");
    let client_http = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client_http
                .get(format!("{base_url}/v1/status"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_skerry"))
        .arg("mcp")
        .arg("stdio")
        .arg("--connect")
        .arg(&base_url)
        .env_remove("SKERRY_TOKEN")
        .env_remove("SKERRY_MCP_TOKEN")
        .env("SKERRY_ROOT", root.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().expect("piped child stdin");
    let stdout = child.stdout.take().expect("piped child stdout");
    let mut stdout = BufReader::new(stdout);

    let initialize = json!({
        "jsonrpc":"2.0",
        "id":1,
        "method":"initialize",
        "params":{
            "protocolVersion":"2025-11-25",
            "capabilities":{},
            "clientInfo":{"name":"legacy-stdio-test","version":"1"}
        }
    });
    stdin
        .write_all(format!("{initialize}\n").as_bytes())
        .await?;
    let initialized = read_jsonrpc_line(&mut stdout).await?;
    assert_eq!(initialized["id"], 1);
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");

    let notification = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
    let tools_list = json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}});
    stdin
        .write_all(format!("{notification}\n{tools_list}\n").as_bytes())
        .await?;
    let tools_response = read_jsonrpc_line(&mut stdout).await?;
    assert_eq!(tools_response["id"], 2);
    assert!(tools_response["result"]["tools"].as_array().is_some());
    assert!(
        tools_response["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "get_object")
    );

    drop(stdin);
    drop(stdout);
    tokio::time::timeout(Duration::from_secs(10), child.wait()).await??;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), server_task).await???;
    coordinator.shutdown().await?;
    drop(root);
    Ok(())
}

#[tokio::test]
async fn stdio_bridge_put_uses_write_scope_and_deduplicates_canonical_content() -> anyhow::Result<()>
{
    let root = tempfile::tempdir()?;
    let mut config = Config::default();
    config.storage.root = root.path().to_path_buf();
    config.cache.payload_mib = 0;
    config.writes.batch_max_delay_ms = 1;
    config.mcp.read_only = false;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    config.server.listen = address.to_string();
    let coordinator = Coordinator::open(config).await?;
    let shutdown = CancellationToken::new();
    let router = server::router_with_mcp_credentials(
        coordinator.clone(),
        shutdown.clone(),
        Credentials {
            service: None,
            read: Some("stdio-read-scope".into()),
            write: Some("stdio-write-scope".into()),
            maintenance: None,
            gateway: None,
        },
    )?;
    let server_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        }
    });
    let base_url = format!("http://{address}");
    let client_http = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client_http
                .get(format!("{base_url}/v1/status"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    let launch = |token: &'static str| {
        let base_url = base_url.clone();
        let root = root.path().to_path_buf();
        async move {
            let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_skerry"));
            command
                .arg("mcp")
                .arg("stdio")
                .arg("--connect")
                .arg(base_url)
                .env_remove("SKERRY_TOKEN")
                .env("SKERRY_MCP_TOKEN", token)
                .env("SKERRY_ROOT", root)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let transport = TokioChildProcess::builder(command)
                .stderr(Stdio::piped())
                .spawn()?
                .0;
            anyhow::Ok(tokio::time::timeout(Duration::from_secs(10), ().serve(transport)).await??)
        }
    };

    let writer = launch("stdio-write-scope").await?;
    let data = b"authorized stdio ingest\0\xff";
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, data);
    let make_put = || {
        CallToolRequestParams::new("put_object")
            .with_arguments(json!({"data_base64":encoded}).as_object().unwrap().clone())
    };
    let first =
        tokio::time::timeout(Duration::from_secs(10), writer.call_tool(make_put())).await??;
    assert_ne!(first.is_error, Some(true));
    let first = first
        .structured_content
        .expect("structured first PUT result");
    let expected_hash = blake3::hash(data).to_hex().to_string();
    assert_eq!(first["hash"], expected_hash);
    assert_eq!(first["duplicate"], false);

    let second =
        tokio::time::timeout(Duration::from_secs(10), writer.call_tool(make_put())).await??;
    assert_ne!(second.is_error, Some(true));
    assert_eq!(second.structured_content.unwrap()["duplicate"], true);
    assert_eq!(coordinator.status().await?.storage.objects, 1);
    writer.cancel().await?;

    let reader = launch("stdio-read-scope").await?;
    let denied =
        tokio::time::timeout(Duration::from_secs(10), reader.call_tool(make_put())).await?;
    assert!(
        denied.is_err()
            || denied
                .as_ref()
                .is_ok_and(|value| value.is_error == Some(true)),
        "read scoped client unexpectedly wrote: {denied:?}"
    );
    reader.cancel().await?;
    assert_eq!(coordinator.status().await?.storage.objects, 1);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), server_task).await???;
    coordinator.shutdown().await?;
    drop(root);
    Ok(())
}

async fn read_jsonrpc_line(
    reader: &mut (impl AsyncBufReadExt + Unpin),
) -> anyhow::Result<serde_json::Value> {
    let mut line = String::new();
    let bytes =
        tokio::time::timeout(Duration::from_secs(10), reader.read_line(&mut line)).await??;
    anyhow::ensure!(bytes > 0, "stdio bridge closed before a JSON-RPC response");
    let value: serde_json::Value = serde_json::from_str(&line)?;
    anyhow::ensure!(
        value["jsonrpc"] == "2.0",
        "stdio stdout contained a non-JSON-RPC line: {line}"
    );
    Ok(value)
}
