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
        pub kind: ExtendedAttributeType,
    }
}