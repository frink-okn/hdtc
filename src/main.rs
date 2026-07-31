//! The `hdtc` binary. Everything it does lives in the library target so that
//! downstream crates — notably KGF's `kgf-store` — can link the format layer
//! without shelling out. See `src/format.rs` for the surface those crates use.

fn main() -> anyhow::Result<()> {
    hdtc::run()
}
