use std::io::{self, Write};
use tempfile::NamedTempFile;

#[test]
fn load_corrupted_archive() -> Result<(), Box<dyn std::error::Error>> {
    //TODO: Write test.
    /* let mut file = NamedTempFile::new()?;
    writeln!(file, "A test\nActual content\nMore content\nAnother test")?;

    let mut cmd = Command::cargo_bin("grrs")?;
    cmd.arg("test").arg(file.path());
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("test\nAnother test"));
    */
    Ok(())
}
