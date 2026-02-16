pub(crate) mod builder;

// Old architecture module (unused in pipeline, kept for reference)
#[allow(dead_code)]
pub(crate) mod id_triple;

// Exports used by old code (kept for reference/testing)
#[allow(unused_imports)]
pub use builder::build_bitmap_triples;
#[allow(unused_imports)]
pub use id_triple::generate_id_triples;
