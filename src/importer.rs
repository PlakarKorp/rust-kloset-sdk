pub mod kloset_importer {
    use async_trait::async_trait;
    use std::pin::Pin;
    use futures::stream::Stream;
    use tokio::io::AsyncRead;

    #[async_trait]
    pub trait Importer: Send + Sync + 'static {
        fn origin(&self) -> &str;
        fn r#type(&self) -> &str;
        fn root(&self) -> &str;

        async fn scan(
            &self,
        ) -> Result<Pin<Box<dyn Stream<Item = ScanResult> + Send>>, anyhow::Error>;

        async fn close(&self) -> Result<(), anyhow::Error>;
    }

    pub enum ScanResult {
        Record(ScanRecord),
        Error(ScanError),
    }

    pub struct ScanRecord {
        pub pathname: String,
        pub target: String,
        pub file_info: FileInfo,
        pub file_attributes: u32,
        pub xattr: Option<ExtendedAttribute>,
        pub reader: Option<Box<dyn AsyncRead + Send + Unpin>>,
    }

    pub struct ScanError {
        pub pathname: String,
        pub err: anyhow::Error,
    }

    use chrono::{DateTime, Utc};

    pub struct FileInfo {
        pub name: String,
        pub size: i64,
        pub mode: u32,
        pub mod_time: DateTime<Utc>,
        pub dev: u64,
        pub ino: u64,
        pub uid: u64,
        pub gid: u64,
        pub nlink: u32,
        pub username: String,
        pub groupname: String,
        pub flags: u32,
    }

    pub enum ExtendedAttributeType {
        Unspecified,
        Extended,
        Ads,
    }

    pub struct ExtendedAttribute {
        pub name: String,
        pub r#type: ExtendedAttributeType,
    }
}

pub mod sdk_importer {

    use crate::importer::kloset_importer::{
        ExtendedAttribute as KlosetExtendedAttribute,
        ExtendedAttributeType as KlosetExtendedAttributeType,
        FileInfo as KlosetFileInfo,
        Importer as KlosetImporter,
        ScanError as KlosetScanError,
        ScanRecord as KlosetScanRecord,
        ScanResult as KlosetScanResult,
    };
    use crate::pkg::importer::*;
    use crate::pkg::importer::importer_server::{Importer as GRPCImporter};

    use prost_types::Timestamp;
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio::io::AsyncRead;
    use tokio::io::AsyncReadExt;
    use tokio_stream::Stream;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};


    pub struct ImporterPluginServer<I: KlosetImporter> {
        importer: I,
        holding_readers: Arc<tokio::sync::Mutex<HashMap<String, Box<dyn AsyncRead + Send + Unpin>>>>,
    }

    #[tonic::async_trait]
    impl<T: KlosetImporter + Send + Sync + 'static> GRPCImporter for ImporterPluginServer<T> {
        async fn info(&self, _request: Request<InfoRequest>) -> Result<Response<InfoResponse>, Status> {
            let resp = InfoResponse {
                origin: self.importer.origin().to_string(),
                r#type: self.importer.r#type().to_string(),
                root: self.importer.root().to_string(),
            };

            Ok(Response::new(resp))
        }

        type ScanStream = Pin<Box<dyn Stream<Item = Result<ScanResponse, Status>> + Send>>;

        async fn scan(&self, _request: Request<ScanRequest>) -> Result<Response<Self::ScanStream>, Status> {
            let mut rx_results = self.importer.scan().await.map_err(|e| {
                Status::internal(format!("Importer scan failed: {e}"))
            })?;

            let (tx, rx) = tokio::sync::mpsc::channel(8);
            let readers = self.holding_readers.clone();

            tokio::spawn(async move {
                while let Some(result) = rx_results.next().await {
                    match result {
                        KlosetScanResult::Record(record) => {
                            let pathname = record.pathname.clone();

                            fn is_dir(mode: u32) -> bool {
                                mode & 0o170000 == 0o040000
                            }

                            if !is_dir(record.file_info.mode) {
                                if let Some(reader) = record.reader {
                                    let mut lock = readers.lock().await;
                                    lock.insert(pathname.clone(), reader);
                                }
                            }

                            let xattr = record.xattr.map(|x| ExtendedAttribute {
                                name: x.name,
                                r#type: x.r#type as i32,
                            });

                            let scan_response = ScanResponse {
                                pathname,
                                result: Some(scan_response::Result::Record(ScanRecord {
                                    target: record.target,
                                    fileinfo: Some(ScanRecordFileInfo{
                                        name: record.file_info.name,
                                        size: record.file_info.size,
                                        mode: record.file_info.mode,
                                        mod_time: Some(Timestamp{
                                            seconds: record.file_info.mod_time.timestamp(),
                                            nanos: record.file_info.mod_time.timestamp_subsec_nanos() as i32,
                                        }),
                                        dev: record.file_info.dev,
                                        ino: record.file_info.ino,
                                        uid: record.file_info.uid,
                                        gid: record.file_info.gid,
                                        nlink: record.file_info.nlink,
                                        username: record.file_info.username,
                                        groupname: record.file_info.groupname,
                                        flags: record.file_info.flags,
                                    }),
                                    file_attributes: record.file_attributes,
                                    xattr,
                                })),
                            };

                            if tx.send(Ok(scan_response)).await.is_err() {
                                break;
                            }
                        }

                        KlosetScanResult::Error(error) => {
                            let scan_response = ScanResponse {
                                pathname: error.pathname,
                                result: Some(scan_response::Result::Error(ScanError {
                                    message: format!("{:?}", error.err),
                                })),
                            };

                            if tx.send(Ok(scan_response)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });

            Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as Self::ScanStream))
        }


        type OpenReaderStream = Pin<Box<dyn Stream<Item = Result<OpenReaderResponse, Status>> + Send + 'static>>;

        async fn open_reader(
            &self,
            request: Request<OpenReaderRequest>,
        ) -> Result<Response<Self::OpenReaderStream>, Status> {
            let pathname = request.into_inner().pathname;

            let reader = {
                let mut map = self.holding_readers.lock().await;
                map.remove(&pathname)
            };

            if reader.is_none() {
                return Err(Status::not_found("Reader not found for given pathname"));
            }

            let mut reader = reader.unwrap();

            // Create a stream that reads chunks from the reader
            let stream = async_stream::try_stream! {
        let mut buf = [0u8; 16 * 1024]; // 16 KB buffer
        loop {
            let n = reader.read(&mut buf).await.map_err(|e| Status::internal(format!("read error: {e}")))?;
            if n == 0 {
                break;
            }

            yield OpenReaderResponse {
                chunk: bytes::Bytes::copy_from_slice(&buf[..n]).into(),
            };
        }
    };

            Ok(Response::new(Box::pin(stream) as Self::OpenReaderStream))
        }


        async fn close_reader(
            &self,
            request: Request<CloseReaderRequest>,
        ) -> Result<Response<CloseReaderResponse>, Status> {
            Ok(Response::new(CloseReaderResponse {}))
        }

        async fn close(&self, request: Request<CloseRequest>) -> Result<Response<CloseResponse>, Status> {
            self.importer.close().await.map_err(|e| {
                Status::internal(format!("Importer close failed: {e}"))
            })?;

            Ok(Response::new(CloseResponse {}))
        }
    }

    use crate::pkg::importer::importer_server::ImporterServer;
    use tonic::transport::Server;

    pub async fn run_importer<T>(importer: T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: crate::importer::kloset_importer::Importer + Send + Sync + 'static,
    {
        // TODO: Set up the gRPC server address, localhost with port 50051, it has to be changed to an appropriate address
        let addr = "[::1]:50051".parse()?;

        let svc = ImporterServer::new(ImporterPluginServer { importer, holding_readers: Arc::new(tokio::sync::Mutex::new(HashMap::new())) });

        // Start the tonic gRPC server
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await?;

        Ok(())
    }
}