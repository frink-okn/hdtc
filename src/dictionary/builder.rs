//! Dictionary builder types.

/// Counts for each dictionary section.
#[derive(Debug, Default, Clone)]
pub struct DictCounts {
    pub shared: u64,
    pub subjects: u64,
    pub predicates: u64,
    pub objects: u64,
    #[allow(dead_code)]
    pub graphs: u64,
}
