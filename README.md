# Krusti

### Running with cargo

Currently seems to be a problem with `rust-rdkafka` and statically linking `librdkafka`. Configure the `CARGO_FEATURE_DYNAMIC_LINKING` or add the `dynamic_linking` feature to `Cargo.toml` e.g.
```bash
CARGO_FEATURE_DYNAMIC_LINKING=1 cargo build
```
