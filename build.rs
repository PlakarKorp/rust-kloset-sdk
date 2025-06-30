fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/pkg")
        //.compile(&["proto/importer.proto"], &["proto"])?;
        .compile_protos(&["proto/importer.proto"], &["proto"])?;
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/pkg")
        //.compile(&["proto/exporter.proto"], &["proto"])?;
        .compile_protos(&["proto/exporter.proto"], &["proto"])?;
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/pkg")
        //.compile(&["proto/storage.proto"], &["proto"])?;
        .compile_protos(&["proto/storage.proto"], &["proto"])?;
    Ok(())
}