pub(crate) mod pfc;

// Old architecture modules (unused in pipeline, kept for reference)
#[allow(dead_code)]
pub(crate) mod sst;
#[allow(dead_code)]
pub(crate) mod builder;

// Only export what the pipeline uses
pub use builder::DictCounts;
