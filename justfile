build-win:
    cargo build --target x86_64-pc-windows-gnu --no-default-features --release
    realpath target/x86_64-pc-windows-gnu/release/extract_to_sqlite_rs.exe | wl-copy

machete:
    # cargo install cargo-machete
    cargo machete

vulture:
    # cargo install cargo-machete
    just machete

run:
    cargo run -- -c ~/.config/database_exporter/config_local2.toml

docs:
    cd mdbook && \
    mdbook build  -d ../docs/
