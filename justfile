build-win:
    cargo build --target x86_64-pc-windows-gnu --release
    realpath target/x86_64-pc-windows-gnu/release/extract_to_sqlite_rs.exe | wl-copy

machete:
    # cargo install cargo-machete
    cargo machete

vulture:
    # cargo install cargo-machete
    just machete
