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
}

pub mod sdk_exporter {

    use bytes::BufMut;
    use futures::TryFutureExt;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;
    use tonic::{Request, Response, Status, Streaming};

    use crate::exporter::kloset_exporter::Exporter as KlosetExporter;
    use crate::importer::kloset_importer::FileInfo;
    use crate::pkg::exporter::*;
    use crate::pkg::exporter::exporter_server::{Exporter as GrpcExporter};


    pub struct ExporterPluginServer<T: KlosetExporter + Send + Sync + 'static> {
        pub storage: T,
    }

    #[tonic::async_trait]
    impl<T: KlosetExporter + Send + Sync + 'static> GrpcExporter for ExporterPluginServer<T> {
        async fn root(&self, _request: Request<RootRequest>) -> Result<Response<RootResponse>, Status> {
            let root = self.storage.root();
            Ok(Response::new(RootResponse {
                root_path: root,
            }))
        }

        async fn create_directory(
            &self,
            request: Request<CreateDirectoryRequest>,
        ) -> Result<Response<CreateDirectoryResponse>, Status> {
            let pathname = request.into_inner().pathname;

            self.storage
                .create_directory(pathname)
                .map_err(|e| Status::internal(format!("failed to create directory: {}", e)))?;

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

            self.storage
                .store_file(pathname, Box::new(reader), size)
                .map_err(|e| Status::internal(format!("store_file failed: {}", e)))?;

            Ok(Response::new(StoreFileResponse {}))
        }

        async fn set_permissions(
            &self,
            request: Request<SetPermissionsRequest>,
        ) -> Result<Response<SetPermissionsResponse>, Status> {
            let req = request.into_inner();

            let fi = req.file_info
                .ok_or_else(|| Status::invalid_argument("file_info missing"))?;

            let file_info = FileInfo {
                name: fi.name,
                size: fi.size,
                mode: fi.mode.into(),
                mod_time: fi.mod_time
                    .ok_or_else(|| Status::invalid_argument("mod_time missing"))?
                    .try_into()
                    .map_err(|e| Status::invalid_argument(format!("invalid mod_time: {}", e)))?,
                dev: fi.dev,
                ino: fi.ino,
                uid: fi.uid,
                gid: fi.gid,
                nlink: fi.nlink,
                username: fi.username,
                groupname: fi.groupname,
                flags: fi.flags,
            };

            self.storage
                .set_permissions(req.pathname, file_info)
                .map_err(|e| Status::internal(format!("set_permissions failed: {}", e)))?;

            Ok(Response::new(SetPermissionsResponse {}))
        }

        async fn close(
            &self,
            _request: Request<CloseRequest>,
        ) -> Result<Response<CloseResponse>, Status> {
            self.storage
                .close()
                .map_err(|e| Status::internal(format!("close failed: {}", e)))?;

            Ok(Response::new(CloseResponse {}))
        }
    }
}

