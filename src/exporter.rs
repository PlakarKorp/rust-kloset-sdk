pub mod kloset_exporter {
    use async_trait::async_trait;
    use tokio::io::AsyncRead;

    use crate::importer::kloset_importer::FileInfo;


    #[async_trait]
    pub trait Exporter: Send + Sync {
        fn root(&self) -> String;

        async fn create_directory(&self, pathname: String) -> anyhow::Result<()>;

        async fn store_file(
            &self,
            pathname: String,
            reader: Box<dyn AsyncRead + Send + Unpin>,
            size: i64,
        ) -> anyhow::Result<()>;

        async fn set_permissions(&self, pathname: String, file_info: FileInfo) -> anyhow::Result<()>;

        async fn close(&self) -> anyhow::Result<()>;
    }

    use std::io::Write;

    pub struct Options {
        pub max_concurrency: u64,

        pub stdout: Box<dyn Write + Send>,
        pub stderr: Box<dyn Write + Send>,
    }

    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    pub type ExporterFn = Arc<
        dyn Fn(
            //ctx,
            Arc<Options>,
            String,
            HashMap<String, String>,
        ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Exporter + Send + Sync>, tonic::Status>> + Send>>
        + Send
        + Sync,
    >;

}

pub mod sdk_exporter {
    use std::collections::HashMap;
    use bytes::BufMut;
    use futures::TryFutureExt;
    use std::io::Cursor;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    use tonic::{Request, Response, Status, Streaming};
    use tokio::sync::Mutex;

    use crate::exporter::kloset_exporter::{
        Exporter as KlosetExporter,
        Options as KlosetExporterOptions,
        ExporterFn,
    };
    use crate::importer::kloset_importer::FileInfo;
    use crate::pkg::exporter::*;
    use crate::pkg::exporter::exporter_server::{Exporter as GrpcExporter};



    pub struct ExporterPluginServer {
        constructor: ExporterFn,
        exporter: Arc<Mutex<Option<Box<dyn KlosetExporter + Send + Sync>>>>,
    }


    #[tonic::async_trait]
    impl GrpcExporter for ExporterPluginServer {

        async fn init(
            &self,
            request: tonic::Request<InitRequest>,
        ) -> Result<tonic::Response<InitResponse>, tonic::Status> {
            let req = request.into_inner();

            let opt = req.options
                .ok_or_else(|| Status::invalid_argument("options missing"))?;

            // Build exporter options
            let options = Arc::new(KlosetExporterOptions {
                max_concurrency: 4, // or get from env/args

                stdout: Box::new(std::io::stdout()),
                stderr: Box::new(std::io::stderr()),
            });

            let exporter = (self.constructor)(
                //ctx,
                options,
                req.proto,
                req.config,
            )
                .await?;

            let mut lock = self.exporter.lock().await;
            *lock = Some(exporter);

            Ok(tonic::Response::new(InitResponse {}))
        }

        async fn root(&self, _request: Request<RootRequest>) -> Result<Response<RootResponse>, Status> {
            let root = self.exporter
                .lock().await
                .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                .root();
            Ok(Response::new(RootResponse {
                root_path: root,
            }))
        }

        async fn create_directory(
            &self,
            request: Request<CreateDirectoryRequest>,
        ) -> Result<Response<CreateDirectoryResponse>, Status> {
            let pathname = request.into_inner().pathname;

            self.exporter
                .lock().await
                .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                .create_directory(pathname)
                .await.map_err(|e| Status::internal(format!("failed to create directory: {}", e)))?;

            Ok(Response::new(CreateDirectoryResponse {}))
        }


        async fn store_file(
            &self,
            request: Request<Streaming<StoreFileRequest>>,
        ) -> Result<Response<StoreFileResponse>, Status> {
            let mut stream = request.into_inner();

            let first = stream
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("stream is empty"))?;

            let (pathname, size) = match first.r#type {
                Some(store_file_request::Type::Header(header)) => (header.pathname, header.size as i64),
                _ => {
                    return Err(Status::invalid_argument(
                        "first message must be a Header",
                    ));
                }
            };

            let mut buffer = Vec::with_capacity(size as usize);
            while let Some(chunk) = stream.message().await? {
                if let Some(store_file_request::Type::Data(data)) = chunk.r#type {
                    buffer.put_slice(&data.chunk);
                }
            }

            let reader = Cursor::new(buffer);

            self.exporter
                .lock().await
                .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                .store_file(pathname, Box::new(reader), size)
                .await.map_err(|e| Status::internal(format!("store_file failed: {}", e)))?;

            Ok(Response::new(StoreFileResponse {}))
        }

        async fn set_permissions(
            &self,
            request: Request<SetPermissionsRequest>,
        ) -> Result<Response<SetPermissionsResponse>, Status> {
            let req = request.into_inner();

            let fi = req.file_info
                .ok_or_else(|| Status::invalid_argument("file_info missing"))?;

            use chrono::{DateTime, NaiveDateTime, Utc};
            use prost_types::Timestamp;
            
            fn prost_timestamp_to_chrono(ts: prost_types::Timestamp) -> Result<DateTime<Utc>, Status> {
                let ndt = NaiveDateTime::from_timestamp_opt(ts.seconds, ts.nanos as u32)
                    .ok_or_else(|| Status::invalid_argument("invalid mod_time"))?;
                Ok(DateTime::<Utc>::from_utc(ndt, Utc))
            }
            
            let file_info = FileInfo {
                name: fi.name,
                size: fi.size,
                mode: fi.mode.into(),
                mod_time: fi.mod_time
                    .ok_or_else(|| Status::invalid_argument("mod_time missing"))
                    .and_then(prost_timestamp_to_chrono)?,
                dev: fi.dev,
                ino: fi.ino,
                uid: fi.uid,
                gid: fi.gid,
                nlink: fi.nlink,
                username: fi.username,
                groupname: fi.groupname,
                flags: fi.flags,
            };

            self.exporter
                .lock().await
                .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                .set_permissions(req.pathname, file_info)
                .await.map_err(|e| Status::internal(format!("set_permissions failed: {}", e)))?;

            Ok(Response::new(SetPermissionsResponse {}))
        }

        async fn close(
            &self,
            _request: Request<CloseRequest>,
        ) -> Result<Response<CloseResponse>, Status> {
            self.exporter
                .lock().await
                .as_ref().ok_or_else(|| { Status::failed_precondition("Importer not initialized") })?
                .close()
                .await.map_err(|e| Status::internal(format!("close failed: {}", e)))?;

            Ok(Response::new(CloseResponse {}))
        }
    }

    use crate::pkg::exporter::exporter_server::ExporterServer;

    pub async fn run_exporter(constructor: ExporterFn) -> Result<(), Box<dyn std::error::Error>> {
        todo!();
        
        // let incoming = ??;
        // 
        // let exporter = ExporterPluginServer {
        //     constructor,
        //     exporter: Arc::new(Mutex::new(None)),
        // };
        // 
        // let svc = ExporterServer::new(exporter);
        // 
        // tonic::transport::Server::builder()
        //     .add_service(svc)
        //     .serve_with_incoming(incoming)
        //     .await?;
        // 
        // Ok(())
    }

}

