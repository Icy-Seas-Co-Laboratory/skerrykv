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
