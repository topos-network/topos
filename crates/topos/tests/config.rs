use std::path::PathBuf;
use std::process::Command;

use assert_cmd::prelude::*;

#[test]
fn test_handle_command_init() -> Result<(), Box<dyn std::error::Error>> {
    let temporary_test_folder = "/tmp/topos/handle_command_init";

    let mut cmd = Command::cargo_bin("topos")?;
    cmd.arg("node")
        .arg("init")
        .arg("--home")
        .arg(temporary_test_folder);

    let output = cmd.assert().success();
    let result: &str = std::str::from_utf8(&output.get_output().stdout)?;

    assert!(result.contains("Created node config file"));

    let home = PathBuf::from(temporary_test_folder);

    // Verification: check that the config file was created
    let config_path = home.join("node").join("default").join("config.toml");
    assert!(config_path.exists());

    // Further verification might include checking the contents of the config file
    let config_contents = std::fs::read_to_string(&config_path).unwrap();

    assert!(config_contents.contains("[base]"));
    assert!(config_contents.contains("name = \"default\""));
    assert!(config_contents.contains("[tce]"));
    assert!(config_contents.contains("[sequencer]"));

    std::fs::remove_dir_all(temporary_test_folder)?;
    Ok(())
}

#[test]
fn test_handle_command_init_with_custom_name() -> Result<(), Box<dyn std::error::Error>> {
    let temporary_test_folder = "/tmp/topos/test_handle_command_init_with_custom_name";
    let node_name = "TEST_NODE";

    let mut cmd = Command::cargo_bin("topos")?;
    cmd.arg("node")
        .arg("init")
        .arg("--home")
        .arg(temporary_test_folder)
        .arg("--name")
        .arg(node_name);

    let output = cmd.assert().success();
    let result: &str = std::str::from_utf8(&output.get_output().stdout)?;

    assert!(result.contains("Created node config file"));

    let home = PathBuf::from(temporary_test_folder);

    // Verification: check that the config file was created
    let config_path = home.join("node").join(node_name).join("config.toml");
    assert!(config_path.exists());

    // Further verification might include checking the contents of the config file
    let config_contents = std::fs::read_to_string(&config_path).unwrap();
    assert!(config_contents.contains("[base]"));
    assert!(config_contents.contains(node_name));
    assert!(config_contents.contains("[tce]"));
    assert!(config_contents.contains("[sequencer]"));

    std::fs::remove_dir_all(temporary_test_folder)?;
    Ok(())
}
