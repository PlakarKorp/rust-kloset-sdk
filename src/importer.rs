pub mod kloset_importer {
    use async_trait::async_trait;
    use std::pin::Pin;
    use futures::stream::Stream;
    use tokio::io::AsyncRead;

    #[async_trait]
    pub trait Importer: Send + Sync + 'static {
        fn origin(&self) -> String;
        fn r#type(&self) -> String;
        fn root(&self) -> String;

        async fn scan(
            &self,
        ) -> anyhow::Result<Pin<Box<dyn Stream<Item = ScanResult> + Send>>>;

        async fn close(&self) -> anyhow::Result<()>;
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

    use std::io::{Read, Write};

    pub struct Options {
        pub hostname: String,
        pub operating_system: String,
        pub architecture: String,
        pub cwd: String,
        pub max_concurrency: i64,

        pub stdin: Box<dyn Read + Send>,
        pub stdout: Box<dyn Write + Send>,
        pub stderr: Box<dyn Write + Send>,
    }

    unsafe impl Send for Options {}
    unsafe impl Sync for Options {}

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::future::Future;

    pub type ImporterFn = Arc<
        dyn Fn(
            //ctx ??
            Arc<Options>,
            String,
            HashMap<String, String>,
        ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Importer + Send + Sync>, tonic::Status>> + Send>>
        + Send
        + Sync,
    >;
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
        Options as KlosetImporterOptions,
        ImporterFn,
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
    use tokio::sync::Mutex;

    struct ImporterPluginServer {
        constructor: ImporterFn,
        importer: Arc<Mutex<Option<Box<dyn KlosetImporter + Send + Sync>>>>,
        holding_readers: Arc<tokio::sync::Mutex<HashMap<String, Box<dyn AsyncRead + Send + Unpin>>>>,
    }

    #[tonic::async_trait]
    impl GRPCImporter for ImporterPluginServer {
        async fn init(
            &self,
            request: Request<InitRequest>,
        ) -> Result<Response<InitResponse>, Status> {
            let req = request.into_inner();

            let opt = req.options.clone().ok_or_else(|| {
                Status::invalid_argument("Options must be provided for initialization")
            })?;

            // Build Options struct
            let options = Arc::new(KlosetImporterOptions {
                hostname: opt.hostname,
                operating_system: opt.os,
                architecture: opt.arch,
                cwd: opt.cwd,
                max_concurrency: opt.maxconcurrency,

                stdin: Box::new(std::io::stdin()),
                stdout: Box::new(std::io::stdout()),
                stderr: Box::new(std::io::stderr()),
            });

            // Call the constructor
            let importer = (self.constructor)(
                //ctx ??
                options,
                req.proto,
                req.config,
            )
                .await?;

            // Save it
            let mut lock = self.importer.lock().await;
            *lock = Some(importer);

            Ok(Response::new(InitResponse { error: None }))
        }

        async fn info(&self, _request: Request<InfoRequest>) -> Result<Response<InfoResponse>, Status> {
            let resp = InfoResponse {
                origin: self.importer
                    .lock().await
                    .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                    .origin().to_string(),
                r#type: self.importer
                    .lock().await
                    .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                    .r#type().to_string(),
                root: self.importer
                    .lock().await
                    .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                    .root().to_string(),
            };

            Ok(Response::new(resp))
        }

        type ScanStream = Pin<Box<dyn Stream<Item = Result<ScanResponse, Status>> + Send>>;

        async fn scan(&self, _request: Request<ScanRequest>) -> Result<Response<Self::ScanStream>, Status> {
            let mut rx_results = self.importer
                .lock().await
                .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                .scan().await.map_err(|e| {
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
            self.importer
                .lock().await
                .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                .close().await.map_err(|e| {
                Status::internal(format!("Importer close failed: {e}"))
            })?;

            Ok(Response::new(CloseResponse {}))
        }
    }

    use crate::pkg::importer::importer_server::ImporterServer;
    use tonic::transport::Server;

    pub async fn run_importer(constructor: ImporterFn) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
        
        // let incoming = ??;
        // 
        // let importer = ImporterPluginServer {
        //     constructor,
        //     importer: Arc::new(Mutex::new(None)),
        //     holding_readers: Arc::new(Default::default()),
        // };
        // 
        // let svc = ImporterServer::new(importer);
        // 
        // let result = Server::builder()
        //     .add_service(svc)
        //     .serve_with_incoming(incoming)
        //     .await?;
        // 
        // Ok(())
    }
}