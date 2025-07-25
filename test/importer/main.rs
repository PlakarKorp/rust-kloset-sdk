mod importer;

use importer::kloset_importer::*;
use chrono::Utc;
use futures::stream;
use futures::Stream;
use std::pin::Pin;
use async_trait::async_trait;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::ReadBuf;
use std::task::{Context, Poll};

struct DummyImporter;

#[async_trait]
impl Importer for DummyImporter {
    fn origin(&self) -> String {
        "dummy_origin"
    }

    fn r#type(&self) -> String {
        "dummy_type"
    }

    fn root(&self) -> String {
        "/dummy/root"
    }

    async fn scan(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = ScanResult> + Send>>, anyhow::Error> {
        // Simulate some content
        let content = b"Hello from dummy reader!\n".to_vec();

        // Wrap it in an AsyncRead object using `tokio::io::BufReader`
        let reader: Box<dyn AsyncRead + Send + Unpin> =
            Box::new(tokio::io::BufReader::new(std::io::Cursor::new(content)));

        let record = ScanRecord {
            pathname: "/dummy/path.txt".to_string(),
            target: "".to_string(),
            file_info: FileInfo {
                name: "path.txt".to_string(),
                size: 27,
                mode: 644,
                mod_time: Utc::now(),
                dev: 0,
                ino: 0,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                username: "dummy".to_string(),
                groupname: "dummygroup".to_string(),
                flags: 0,
            },
            file_attributes: 0,
            xattr: Some(ExtendedAttribute {
                name: "user.comment".to_string(),
                kind: ExtendedAttributeType::Extended,
            }),
            reader: Some(reader),
        };

        let results = vec![ScanResult::Record(record)];

        Ok(Box::pin(stream::iter(results)))
    }


    async fn close(&self) -> Result<(), anyhow::Error> {
        println!("DummyImporter closed.");
        Ok(())
    }
}

// Minimal dummy AsyncRead that just returns EOF
struct DummyReader;

impl AsyncRead for DummyReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(())) // EOF
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let importer = DummyImporter;

    println!("Origin: {}", importer.origin());
    println!("Type: {}", importer.r#type());
    println!("Root: {}", importer.root());

    let mut stream = importer.scan().await?;

    println!("\n--- Scan Results ---");
    use futures::StreamExt;
    while let Some(result) = stream.next().await {
        match result {
            ScanResult::Record(record) => {
                println!("Pathname: {}", record.pathname);
                println!("Target: {}", record.target);
                println!("Size: {}", record.file_info.size);
                println!("Username: {}", record.file_info.username);
                println!("Groupname: {}", record.file_info.groupname);

                if let Some(mut reader) = record.reader {
                    let mut content = Vec::new();
                    reader.read_to_end(&mut content).await?;
                    println!("--- File content ---\n{}", String::from_utf8_lossy(&content));
                } else {
                    println!("No reader attached to this record.");
                }
            }
            ScanResult::Error(err) => {
                println!("Error at {}: {}", err.pathname, err.err);
            }
        }
    }

    importer.close().await?;

    crate::importer::sdk_importer::run_importer(importer)
        .await
        .map_err(|e| anyhow::anyhow!("Importer server failed: {}", e))?;

    Ok(())
}
