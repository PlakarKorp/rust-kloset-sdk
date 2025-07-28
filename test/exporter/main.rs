use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt};
use async_trait::async_trait;

use crate::exporter::kloset_exporter::Exporter as KlosetExporter;
use crate::exporter::sdk_exporter::run_exporter;
use crate::importer::kloset_importer::FileInfo;


pub struct DummyExporter {
    root_path: String,
    stored_files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    directories: Arc<Mutex<Vec<String>>>,
}

impl DummyExporter {
    pub fn new() -> Self {
        Self {
            root_path: "/dummy/export".to_string(),
            stored_files: Arc::new(Mutex::new(HashMap::new())),
            directories: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl KlosetExporter for DummyExporter {
    fn root(&self) -> String {
        println!("DummyExporter: root() -> {}", self.root_path);
        self.root_path.clone()
    }

    async fn create_directory(&self, pathname: String) -> anyhow::Result<()> {
        println!("DummyExporter: create_directory({})", pathname);
        self.directories.lock().await.push(pathname);
        Ok(())
    }

    async fn store_file(
        &self,
        pathname: String,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        size: i64,
    ) -> anyhow::Result<()> {
        println!("DummyExporter: store_file({}, {} bytes)", pathname, size);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        self.stored_files.lock().await.insert(pathname, buffer);
        Ok(())
    }

    async fn set_permissions(&self, pathname: String, file_info: FileInfo) -> anyhow::Result<()> {
        println!(
            "DummyExporter: set_permissions({}, mode = {:o}, uid = {}, gid = {})",
            pathname, file_info.mode, file_info.uid, file_info.gid
        );
        Ok(())
    }

    async fn close(&self) -> anyhow::Result<()> {
        println!("DummyExporter: close()");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let exporter = DummyExporter::new();

    exporter.create_directory("test_dir".into()).await?;
    exporter
        .store_file("file.txt".into(), Box::new(tokio::io::empty()), 0)
        .await?;
    exporter
        .set_permissions(
            "file.txt".into(),
            FileInfo {
                name: "".to_string(),
                mode: 0o644,
                mod_time: Default::default(),
                dev: 0,
                ino: 0,
                uid: 1000,
                gid: 1000,
                nlink: 0,
                username: "".to_string(),
                groupname: "".to_string(),
                size: 0,
                flags: 0,
            },
        )
        .await?;
    exporter.close().await?;

    println!("DummyExporter operations completed successfully.");

    run_exporter(exporter)
        .await
        .map_err(|e| anyhow::anyhow!("Exporter failed: {}", e))?;

    Ok(())
}
