use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::io::ReadBuf;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::pin::Pin;
use std::boxed::Box;
use std::task::{Context, Poll};
use std::io::Cursor;
use anyhow::Result;
use log::log;
use crate::storage::kloset_storage::{Store, Mode, MAC};

struct MemoryReader {
    cursor: Cursor<Vec<u8>>,
}

impl AsyncRead for MemoryReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let n = std::io::Read::read(&mut self.cursor, buf.initialize_unfilled())?;
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

fn to_async_reader(data: Vec<u8>) -> Pin<Box<dyn AsyncRead + Send + Sync>> {
    Box::pin(MemoryReader { cursor: Cursor::new(data) })
}

pub struct DummyStore {
    location: String,
    mode: Mode,
    data: Arc<Mutex<HashMap<String, HashMap<MAC, Vec<u8>>>>>, // "states", "packfiles", "locks"
}

impl DummyStore {
    pub fn new(location: &str, mode: Mode) -> Self {
        let mut categories = HashMap::new();
        categories.insert("states".to_string(), HashMap::new());
        categories.insert("packfiles".to_string(), HashMap::new());
        categories.insert("locks".to_string(), HashMap::new());

        DummyStore {
            location: location.to_string(),
            mode,
            data: Arc::new(Mutex::new(categories)),
        }
    }

    fn with_category<F, R>(&self, cat: &str, f: F) -> R
    where
        F: FnOnce(&mut HashMap<MAC, Vec<u8>>) -> R,
    {
        let mut data = self.data.lock().unwrap();
        f(data.get_mut(cat).expect("invalid category"))
    }

    fn read_all<R: AsyncRead + Unpin>(mut rd: R) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                loop {
                    let n = rd.read(&mut tmp).await?;
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                Ok::<_, std::io::Error>(())
            })
        })?;
        Ok(buf)
    }
}

#[async_trait]
impl Store for DummyStore {

    async fn create(&self, _config: Vec<u8>) -> Result<()> {
        Ok(())
    }

    async fn open(&self) -> Result<Vec<u8>> {
        Ok(b"dummy config".to_vec())
    }

    fn location(&self) -> String {
        self.location.clone()
    }

    fn mode(&self) -> Mode {
        self.mode.clone()
    }

    fn size(&self) -> i64 {
        let data = self.data.lock().unwrap();
        data.values().flat_map(|m| m.values()).map(|v| v.len() as i64).sum()
    }

    async fn get_states(&self) -> Result<Vec<MAC>> {
        Ok(self.with_category("states", |cat| cat.keys().cloned().collect()))
    }

    async fn put_state(&self, mac: MAC, rd: Pin<Box<dyn AsyncRead + Send + Sync>>) -> Result<i64> {
        let buf = Self::read_all(rd)?;
        let len = buf.len() as i64;
        self.with_category("states", |cat| {
            cat.insert(mac, buf);
        });
        Ok(len)
    }

    async fn get_state(&self, mac: MAC) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>> {
        self.with_category("states", |cat| {
            cat.get(&mac)
                .cloned()
                .map(to_async_reader)
                .ok_or_else(|| anyhow::anyhow!("not found"))
        })
    }

    async fn delete_state(&self, mac: MAC) -> Result<()> {
        self.with_category("states", |cat| {
            cat.remove(&mac);
        });
        Ok(())
    }

    async fn get_packfiles(&self) -> Result<Vec<MAC>> {
        Ok(self.with_category("packfiles", |cat| cat.keys().cloned().collect()))
    }

    async fn put_packfile(&self, mac: MAC, rd: Pin<Box<dyn AsyncRead + Send + Sync>>) -> Result<i64> {
        let buf = Self::read_all(rd)?;
        let len = buf.len() as i64;
        self.with_category("packfiles", |cat| {
            cat.insert(mac, buf);
        });
        Ok(len)
    }

    async fn get_packfile(&self, mac: MAC) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>> {
        self.with_category("packfiles", |cat| {
            cat.get(&mac)
                .cloned()
                .map(to_async_reader)
                .ok_or_else(|| anyhow::anyhow!("not found"))
        })
    }

    async fn get_packfile_blob(&self, mac: MAC, offset: u64, length: u32) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>> {
        self.with_category("packfiles", |cat| {
            cat.get(&mac)
                .map(|data| {
                    let start = offset as usize;
                    let end = (offset + length as u64).min(data.len() as u64) as usize;
                    let slice = data.get(start..end).unwrap_or(&[]);
                    to_async_reader(slice.to_vec())
                })
                .ok_or_else(|| anyhow::anyhow!("not found"))
        })
    }

    async fn delete_packfile(&self, mac: MAC) -> Result<()> {
        self.with_category("packfiles", |cat| {
            cat.remove(&mac);
        });
        Ok(())
    }

    async fn get_locks(&self) -> Result<Vec<MAC>> {
        Ok(self.with_category("locks", |cat| cat.keys().cloned().collect()))
    }

    async fn put_lock(&self, mac: MAC, rd: Pin<Box<dyn AsyncRead + Send + Sync>>) -> Result<i64> {
        let buf = Self::read_all(rd)?;
        let len = buf.len() as i64;
        self.with_category("locks", |cat| {
            cat.insert(mac, buf);
        });
        Ok(len)
    }

    async fn get_lock(&self, mac: MAC) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>> {
        self.with_category("locks", |cat| {
            cat.get(&mac)
                .cloned()
                .map(to_async_reader)
                .ok_or_else(|| anyhow::anyhow!("not found"))
        })
    }

    async fn delete_lock(&self, mac: MAC) -> Result<()> {
        self.with_category("locks", |cat| {
            cat.remove(&mac);
        });
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

mod pkg;

use tokio::io::AsyncReadExt;
use std::pin::Pin;
use std::io::Cursor;

use crate::dummy_store::{DummyStore};
use crate::storage::kloset_storage::{Store, MAC, Mode};

use crate::storage::sdk_storage::run_storage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = DummyStore::new(&"dummy://inmem".to_string(), Mode::ReadWrite);

    // Create dummy config
    store.create(vec![]).await?;
    store.open().await?;

    // Create a MAC (just use 32 zero bytes for testing)
    let mac = MAC([0u8; 32]);

    // Create some test data
    let data = b"hello, dummy store!".to_vec();
    let cursor = Cursor::new(data.clone());
    let reader: Pin<Box<dyn tokio::io::AsyncRead + Send + Sync>> = Box::pin(cursor);

    // Put state
    let written_len = store.put_state(mac.clone(), reader).await?;
    println!("Written {} bytes for state", written_len);

    // Get state
    let mut reader = store.get_state(mac.clone()).await?;
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await?;
    println!("Read back: {:?}", String::from_utf8_lossy(&out));

    // Delete state
    store.delete_state(mac.clone()).await?;
    let result = store.get_state(mac.clone()).await;
    println!("State after deletion: {:?}", result.as_ref().map(|_| "Ok(Pin<Box<dyn AsyncRead + Send + Sync>>>"));

    ///////////////////////////////////

    // Run the storage server
    run_storage(store)
        .await
        .map_err(|e| anyhow::anyhow!("Storage server failed: {}", e))?;
    Ok(())
}
