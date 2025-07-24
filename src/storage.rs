pub mod kloset_storage {
    use async_trait::async_trait;
    use tokio::io::AsyncRead;
    use std::pin::Pin;
    use std::boxed::Box;

    #[derive(Clone)]
    pub enum Mode {
        ReadOnly,
        ReadWrite,
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct MAC(pub [u8; 32]);

    #[async_trait]
    pub trait Store: Send + Sync {
        async fn create(&self, config: Vec<u8>) -> anyhow::Result<()>;
        async fn open(&self) -> anyhow::Result<Vec<u8>>;

        fn location(&self) -> String;
        fn mode(&self) -> Mode;
        fn size(&self) -> i64;

        async fn get_states(&self) -> anyhow::Result<Vec<MAC>>;
        async fn put_state(
            &self,
            mac: MAC,
            rd: Pin<Box<dyn AsyncRead + Send + Sync>>,
        ) -> anyhow::Result<i64>;
        async fn get_state(
            &self,
            mac: MAC,
        ) -> anyhow::Result<Pin<Box<dyn AsyncRead + Send + Sync>>>;
        async fn delete_state(&self, mac: MAC) -> anyhow::Result<()>;

        async fn get_packfiles(&self) -> anyhow::Result<Vec<MAC>>;
        async fn put_packfile(
            &self,
            mac: MAC,
            rd: Pin<Box<dyn AsyncRead + Send + Sync>>,
        ) -> anyhow::Result<i64>;
        async fn get_packfile(
            &self,
            mac: MAC,
        ) -> anyhow::Result<Pin<Box<dyn AsyncRead + Send + Sync>>>;
        async fn get_packfile_blob(
            &self,
            mac: MAC,
            offset: u64,
            length: u32,
        ) -> anyhow::Result<Pin<Box<dyn AsyncRead + Send + Sync>>>;
        async fn delete_packfile(&self, mac: MAC) -> anyhow::Result<()>;

        async fn get_locks(&self) -> anyhow::Result<Vec<MAC>>;
        async fn put_lock(
            &self,
            lock_id: MAC,
            rd: Pin<Box<dyn AsyncRead + Send + Sync>>,
        ) -> anyhow::Result<i64>;
        async fn get_lock(
            &self,
            lock_id: MAC,
        ) -> anyhow::Result<Pin<Box<dyn AsyncRead + Send + Sync>>>;
        async fn delete_lock(&self, lock_id: MAC) -> anyhow::Result<()>;

        async fn close(&self) -> anyhow::Result<()>;
    }
}

pub mod sdk_storage {
    
    mod grpc_utils {
        use std::{
           pin::Pin,
           sync::Arc,
           task::{Context, Poll},
        };
        
        use bytes::Bytes;
        use tokio::io::{AsyncRead, AsyncReadExt};
        use tokio::sync::Mutex;
        use tonic::{Status, Streaming};
        
        use crate::pkg::store::{PutLockRequest, PutPackfileRequest, PutStateRequest};
        
        pub trait HasChunk {
            fn get_chunk(&self) -> Bytes;
        }

        impl HasChunk for PutStateRequest {
            fn get_chunk(&self) -> Bytes {
                Bytes::from(self.chunk.clone())
            }
        }

        impl HasChunk for PutPackfileRequest {
            fn get_chunk(&self) -> Bytes {
                Bytes::from(self.chunk.clone())
            }
        }

        impl HasChunk for PutLockRequest
        {
            fn get_chunk(&self) -> Bytes {
                Bytes::from(self.chunk.clone())
            }
        }

        pub struct StreamingChunkReader<T> {
            stream: Arc<Mutex<Streaming<T>>>,
            current_chunk: Option<Bytes>,
            pos: usize,
        }

        impl<T> StreamingChunkReader<T> {
            pub fn new(stream: Arc<Mutex<Streaming<T>>>) -> Self {
                StreamingChunkReader {
                    stream,
                    current_chunk: None,
                    pos: 0,
                }
            }
        }

        impl<T> StreamingChunkReader<T>
        where
            T: HasChunk + Send + Sync + 'static,
        {
            async fn poll_next_chunk(&mut self) -> Result<bool, Status> {
                let mut locked = self.stream.lock().await;
                if let Some(msg) = locked.message().await? {
                    self.current_chunk = Some(msg.get_chunk());
                    self.pos = 0;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }

        impl<T> AsyncRead for StreamingChunkReader<T>
        where
            T: HasChunk + Send + Sync + 'static,
        {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                let me = &mut *self;

                if me.current_chunk.is_none() || me.pos >= me.current_chunk.as_ref().unwrap().len() {
                    let fut = me.poll_next_chunk();
                    tokio::pin!(fut);

                    match fut.poll(cx) {
                        Poll::Ready(Ok(has_chunk)) => {
                            if !has_chunk {
                                return Poll::Ready(Ok(()));
                            }
                        }
                        Poll::Ready(Err(status)) => {
                            return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, status.to_string())));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                if let Some(chunk) = &me.current_chunk {
                    let remaining = &chunk[me.pos..];
                    let to_copy = std::cmp::min(remaining.len(), buf.remaining());
                    buf.put_slice(&remaining[..to_copy]);
                    me.pos += to_copy;
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }

        const BUFFER_SIZE: usize = 16 * 1024; // 16 KB

        pub async fn send_chunks<R, F, Fut>(mut reader: R, mut send_fn: F) -> Result<(), Status>
        where
            R: AsyncRead + Unpin,
            F: FnMut(bytes::Bytes) -> Fut,
            Fut: Future<Output = Result<(), Status>>,
        {
            let mut buf = [0u8; 8192];
            loop {
                let n = reader.read(&mut buf).await.map_err(|e| {
                    Status::internal(format!("Read error: {}", e))
                })?;
                if n == 0 {
                    break;
                }
                let chunk = bytes::Bytes::copy_from_slice(&buf[..n]);
                send_fn(chunk).await?;
            }
            Ok(())
        }

    }

    use std::pin::Pin;
    use std::sync::Arc;
    
    use tokio::sync::{mpsc, Mutex};
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::Stream;
    use tonic::{Request, Response, Status, Streaming};
    
    use crate::pkg::store::*;
    use crate::pkg::store::store_server::Store as GrpcStore;
    use crate::storage::kloset_storage::{MAC as KlosetMAC, Store as KlosetStorage};
    
    use grpc_utils::{send_chunks, StreamingChunkReader};
    
    pub struct StoragePluginServer<T: KlosetStorage + Send + Sync + 'static> {
        pub storage: T,
    }

    #[tonic::async_trait]
    impl<T: KlosetStorage + Send + Sync + 'static> GrpcStore for StoragePluginServer<T> {
        async fn create(
            &self,
            request: Request<CreateRequest>,
        ) -> Result<Response<CreateResponse>, Status> {
            let req = request.into_inner();

            self.storage
                .create(req.config)
                .await
                .map_err(|e| Status::internal(format!("Storage create failed: {}", e)))?;

            Ok(Response::new(CreateResponse {}))
        }

        async fn open(
            &self,
            _request: Request<OpenRequest>,
        ) -> Result<Response<OpenResponse>, Status> {
            let config_bytes = self
                .storage
                .open()
                .await
                .map_err(|e| Status::internal(format!("Storage open failed: {}", e)))?;

            Ok(Response::new(OpenResponse {
                config: config_bytes,
            }))
        }

        async fn close(
            &self,
            _request: Request<CloseRequest>,
        ) -> Result<Response<CloseResponse>, Status> {
            self.storage
                .close()
                .await
                .map_err(|e| Status::internal(format!("Storage close failed: {}", e)))?;

            Ok(Response::new(CloseResponse {}))
        }

        async fn get_location(
            &self,
            _request: Request<GetLocationRequest>,
        ) -> Result<Response<GetLocationResponse>, Status> {
            let location = self.storage.location();

            Ok(Response::new(GetLocationResponse {
                location,
            }))
        }

        async fn get_mode(
            &self,
            _request: Request<GetModeRequest>,
        ) -> Result<Response<GetModeResponse>, Status> {
            let mode = self.storage.mode();

            Ok(Response::new(GetModeResponse {
                mode: mode as i32,
            }))
        }

        async fn get_size(
            &self,
            _request: Request<GetSizeRequest>,
        ) -> Result<Response<GetSizeResponse>, Status> {
            let size = self.storage.size();

            Ok(Response::new(GetSizeResponse {
                size,
            }))
        }

        async fn get_states(
            &self,
            _request: Request<GetStatesRequest>,
        ) -> Result<Response<GetStatesResponse>, Status> {
            // Execute the backend method to get the states
            let states = self.storage.get_states()
                .await.map_err(|e| Status::internal(format!("failed to get states: {}", e)))?;

            // Map each state byte array into a MAC message
            let macs = states.into_iter()
                .map(|state| Mac {
                    value: state.0.to_vec(),
                })
                .collect();

            Ok(Response::new(GetStatesResponse {
                macs,
            }))
        }

        async fn put_state(
            &self,
            request: Request<Streaming<PutStateRequest>>,
        ) -> Result<Response<PutStateResponse>, Status> {
            let mut stream = request.into_inner();

            // Retrieve the MAC from the first message
            let first_msg = stream
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("Missing initial request"))?;

            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&first_msg.mac.as_ref().ok_or_else(|| {
                    Status::invalid_argument("Missing MAC in initial request")
                })?.value);
                KlosetMAC(arr)
            };

            // Execute the backend method to put the state
            let reader = Box::pin(StreamingChunkReader::new(Arc::new(Mutex::new(stream))));
            let size = self
                .storage
                .put_packfile(mac, reader)
                .await
                .map_err(|e| Status::internal(format!("put_state failed: {}", e)))?;

            Ok(Response::new(PutStateResponse {
                bytes_written: size,
            }))
        }

        type GetStateStream = Pin<Box<dyn Stream<Item=Result<GetStateResponse, Status>> + Send>>;

        async fn get_state(
            &self,
            request: Request<GetStateRequest>,
        ) -> Result<Response<Self::GetStateStream>, Status> {
            // Retrieve the MAC from the request
            let req = request.into_inner();
            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&req.mac.unwrap().value);
                KlosetMAC(arr)
            };

            // Execute the backend method to get the state reader
            let reader = self
                .storage
                .get_state(mac)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            // Create a channel to send chunks
            let (tx, rx) = mpsc::channel(8);

            // Spawn a task to stream chunks asynchronously
            tokio::spawn(async move {
                let result = send_chunks(reader, |chunk| {
                    let msg = GetStateResponse { chunk: chunk.to_vec().into() };
                    let tx = tx.clone();
                    async move {
                        tx.send(Ok(msg)).await.map_err(|e| {
                            Status::internal(format!("Failed to send chunk: {}", e))
                        })
                    }
                }).await;

                if let Err(e) = result {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
            });

            // Wrap receiver into a stream and return it
            Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as Self::GetStateStream))
        }

        async fn delete_state(
            &self,
            request: Request<DeleteStateRequest>,
        ) -> Result<Response<DeleteStateResponse>, Status> {
            let req = request.into_inner();

            // Extract the MAC from the request
            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&req.mac.unwrap().value);
                KlosetMAC(arr)
            };

            // Execute the backend method to delete the state
            self.storage
                .delete_state(mac)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            Ok(Response::new(DeleteStateResponse {}))
        }

        async fn get_packfiles(
            &self,
            _request: Request<GetPackfilesRequest>,
        ) -> Result<Response<GetPackfilesResponse>, Status> {
            let states = self.storage.get_packfiles()
                .await.map_err(|e| Status::internal(format!("failed to get packfiles: {}", e)))?;

            let macs = states.into_iter()
                .map(|state| Mac {
                    value: state.0.to_vec(),
                })
                .collect();

            Ok(Response::new(GetPackfilesResponse {
                macs,
            }))
        }

        async fn put_packfile(
            &self,
            request: Request<Streaming<PutPackfileRequest>>,
        ) -> Result<Response<PutPackfileResponse>, Status> {
            let mut stream = request.into_inner();

            let first_msg = stream
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("Missing initial request"))?;

            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&first_msg.mac.as_ref().ok_or_else(|| {
                    Status::invalid_argument("Missing MAC in initial request")
                })?.value);
                KlosetMAC(arr)
            };

            let stream = Arc::new(Mutex::new(stream));
            let reader = Box::pin(StreamingChunkReader::new(stream));

            let size = self
                .storage
                .put_packfile(mac, reader)
                .await
                .map_err(|e| Status::internal(format!("put_packfile failed: {}", e)))?;

            Ok(Response::new(PutPackfileResponse {
                bytes_written: size,
            }))
        }

        type GetPackfileStream = Pin<Box<dyn Stream<Item = Result<GetPackfileResponse, Status>> + Send>>;

        async fn get_packfile(
            &self,
            request: Request<GetPackfileRequest>,
        ) -> Result<Response<Self::GetPackfileStream>, Status> {
            let req = request.into_inner();
            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&req.mac.unwrap().value);
                KlosetMAC(arr)
            };

            let reader = self
                .storage
                .get_packfile(mac)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            let (tx, rx) = mpsc::channel(8);

            tokio::spawn(async move {
                let result = send_chunks(reader, |chunk| {
                    let msg = GetPackfileResponse { chunk: chunk.to_vec().into() };
                    let tx = tx.clone();
                    async move {
                        tx.send(Ok(msg)).await.map_err(|e| {
                            Status::internal(format!("Failed to send chunk: {}", e))
                        })
                    }
                }).await;

                if let Err(e) = result {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
            });

            Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as Self::GetPackfileStream))
        }

        type GetPackfileBlobStream = Pin<Box<dyn Stream<Item=Result<GetPackfileBlobResponse, Status>> + Send>>;

        async fn get_packfile_blob(
            &self,
            request: Request<GetPackfileBlobRequest>,
        ) -> Result<Response<Self::GetPackfileBlobStream>, Status> {
            let req = request.into_inner();
            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&req.mac.unwrap().value);
                KlosetMAC(arr)
            };
            let offset = req.offset;
            let length = req.length;

            let reader = self
                .storage
                .get_packfile_blob(mac, offset, length)
                .await
                .map_err(|e| Status::internal(format!("failed to get packfile blob: {}", e)))?;

            let (tx, rx) = mpsc::channel(8);

            tokio::spawn(async move {
                let result = send_chunks(reader, |chunk| {
                    let msg = GetPackfileBlobResponse { chunk: chunk.to_vec().into() };
                    let tx = tx.clone();
                    async move {
                        tx.send(Ok(msg)).await.map_err(|e| {
                            Status::internal(format!("Failed to send chunk: {}", e))
                        })
                    }
                }).await;

                if let Err(e) = result {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
            });

            Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as Self::GetPackfileBlobStream))
        }

        async fn delete_packfile(
            &self,
            request: Request<DeletePackfileRequest>,
        ) -> Result<Response<DeletePackfileResponse>, Status> {
            let req = request.into_inner();

            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&req.mac.unwrap().value);
                KlosetMAC(arr)
            };

            self.storage
                .delete_packfile(mac)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            Ok(Response::new(DeletePackfileResponse {}))
        }

        async fn get_locks(
            &self,
            _request: Request<GetLocksRequest>,
        ) -> Result<Response<GetLocksResponse>, Status> {
            let locks = self.storage.get_locks()
                .await
                .map_err(|e| Status::internal(format!("failed to get locks: {}", e)))?;

            let macs = locks.into_iter()
                .map(|lock| Mac {
                    value: lock.0.to_vec(),
                })
                .collect();

            Ok(Response::new(GetLocksResponse { macs }))
        }

        async fn put_lock(
            &self,
            request: Request<Streaming<PutLockRequest>>,
        ) -> Result<Response<PutLockResponse>, Status> {
            let mut stream = request.into_inner();

            let first_msg = stream
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("Missing initial request"))?;

            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&first_msg.mac.as_ref().ok_or_else(|| {
                    Status::invalid_argument("Missing MAC in initial request")
                })?.value);
                KlosetMAC(arr)
            };

            let stream = Arc::new(Mutex::new(stream));
            let reader = Box::pin(StreamingChunkReader::new(stream));

            let size = self
                .storage
                .put_packfile(mac, reader)
                .await
                .map_err(|e| Status::internal(format!("put_lock failed: {}", e)))?;

            Ok(Response::new(PutLockResponse {
                bytes_written: size,
            }))
        }

        type GetLockStream = Pin<Box<dyn Stream<Item = Result<GetLockResponse, Status>> + Send>>;

        async fn get_lock(
            &self,
            request: Request<GetLockRequest>,
        ) -> Result<Response<Self::GetLockStream>, Status> {
            let req = request.into_inner();
            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&req.mac.unwrap().value);
                KlosetMAC(arr)
            };

            let reader = self.storage
                .get_lock(mac)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            let (tx, rx) = mpsc::channel(8);

            tokio::spawn(async move {
                let result = send_chunks(reader, |chunk| {
                    let msg = GetLockResponse { chunk: chunk.to_vec().into() };
                    let tx = tx.clone();
                    async move {
                        tx.send(Ok(msg)).await.map_err(|e| {
                            Status::internal(format!("Failed to send chunk: {}", e))
                        })
                    }
                }).await;

                if let Err(e) = result {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
            });

            Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as Self::GetLockStream))
        }

        async fn delete_lock(
            &self,
            request: Request<DeleteLockRequest>,
        ) -> Result<Response<DeleteLockResponse>, Status> {
            let req = request.into_inner();

            let mac = {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&req.mac.unwrap().value);
                KlosetMAC(arr)
            };

            self.storage
                .delete_lock(mac)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            Ok(Response::new(DeleteLockResponse {}))
        }
    }

    use crate::pkg::store::store_server::StoreServer; // Generated gRPC server trait
    use tonic::transport::Server;

    pub async fn run_storage<T>(storage: T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: KlosetStorage + Send + Sync + 'static,
    {
        // TODO: Set up the gRPC server address, localhost with port 50051, it has to be changed to an appropriate address
        let addr = "[::1]:50051".parse()?;

        let svc = StoreServer::new(StoragePluginServer { storage });

        // Start the tonic gRPC server
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await?;

        Ok(())
    }
}