use std::process::Command;

fn skerry() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_skerry"));
    command.env_remove("SKERRY_TOKEN");
    command
}

#[test]
fn help_lists_operational_commands() {
    let output = skerry().arg("--help").output().unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    for command in [
        "init",
        "serve",
        "tui",
        "status",
        "health",
        "stop",
        "verify",
        "rebuild-index",
        "import-segments",
        "bench",
        "cache",
    ] {
        assert!(text.contains(command), "help omitted {command}");
    }
    let tui_help = skerry().args(["tui", "--help"]).output().unwrap();
    let tui_text = String::from_utf8(tui_help.stdout).unwrap();
    assert!(tui_text.contains("--connect"));
    assert!(tui_text.contains("--config"));
    let init_help = skerry().args(["init", "--help"]).output().unwrap();
    assert!(
        String::from_utf8(init_help.stdout)
            .unwrap()
            .contains("--path")
    );
}

#[test]
fn init_creates_config_and_offline_verify_accepts_path() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("store");
    let init = skerry()
        .args(["init", "--path"])
        .arg(&root)
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    assert!(root.join("config.toml").is_file());
    let verify = skerry()
        .env("SKERRY_ROOT", temp.path().join("different-root"))
        .arg("verify")
        .arg("--path")
        .arg(&root)
        .output()
        .unwrap();
    assert!(
        verify.status.success(),
        "{}",
        String::from_utf8_lossy(&verify.stderr)
    );
}

#[test]
fn health_exit_code_follows_http_health_status() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    for (response, expected) in [("200 OK", true), ("503 Service Unavailable", false)] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 2048];
            let length = stream.read(&mut request).unwrap();
            assert!(String::from_utf8_lossy(&request[..length]).starts_with("GET /v1/health "));
            write!(
                stream,
                "HTTP/1.1 {response}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
            )
            .unwrap();
        });
        let output = skerry()
            .args(["health", "--url", &format!("http://{address}")])
            .output()
            .unwrap();
        server.join().unwrap();
        assert_eq!(
            output.status.success(),
            expected,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
