build-win:
    cargo build --target x86_64-pc-windows-gnu --release
    realpath target/x86_64-pc-windows-gnu/release/extract_to_sqlite_rs.exe | x
