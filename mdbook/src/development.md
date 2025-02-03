# Development

## Compiling
### Generally
```sh
cargo run -- -c ~/.config/database_exporter/config_local2.toml --help
```

On all major operating systems, except Windows, there is support for automatically creating a `database.duckdb`:

```
cargo build --release --features "duckdb"
```


### Windows

If compiling for windows using the GNU target, it's necessary to use an optimized build, it seems some issues are inlined away this way:

> [!NOTE]
> Database Exporter is developed in an Arch Based Docker container using [Distrobox](https://github.com/89luca89/distrobox)

> [!WARNING]
> Windows **must** compile:
>  * Without the "duckdb" feature, and
>  * With `--release` optimizations

```sh
# Add the Windows Target
rustup target add x86_64-pc-windows-gnu

# Install the linker
sudo pacman -S extra/mingw-w64-gcc

# Compile
cargo build --target x86_64-pc-windows-gnu --no-default-features --release
```

If this does not build, consider trying:

```sh
rustup target add x86_64-pc-windows-msvc
```




