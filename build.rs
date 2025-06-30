use std::fs;
use std::io::Write;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pkg_path = Path::new("src/pkg");

    // Create the directory if it doesn't exist
    if !pkg_path.exists() {
        fs::create_dir_all(pkg_path)?;
    }

    // Create or overwrite the mod.rs file
    let mod_rs_path = pkg_path.join("mod.rs");
    let mut mod_rs = fs::File::create(mod_rs_path)?;
    writeln!(
        mod_rs,
        "pub mod exporter;\npub mod importer;\npub mod store;"
    )?;

    // Compile the .proto files into Rust
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/pkg")
        .compile_protos(&["proto/importer.proto"], &["proto"])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/pkg")
        .compile_protos(&["proto/exporter.proto"], &["proto"])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/pkg")
        .compile_protos(&["proto/storage.proto"], &["proto"])?;

    Ok(())
}