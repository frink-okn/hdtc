use anyhow::{Context, Result, bail};
use oxrdf::NamedNode;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct GraphMapRule {
    path: PathBuf,
    is_file: bool,
    graph: Arc<str>,
}

/// Canonicalized named-graph assignment rules shared by RDF and HDT inputs.
#[derive(Debug, Clone, Default)]
pub struct GraphAssignments {
    rules: Vec<GraphMapRule>,
    default_graph: Option<Arc<str>>,
}

/// The most-specific source mappings resolved once per input file.
#[derive(Debug, Clone, Default)]
pub struct SourceGraphAssignment {
    mapped_graphs: Vec<Arc<str>>,
    default_graph: Option<Arc<str>>,
}

impl GraphAssignments {
    pub fn parse(graph_maps: &[String], default_graph: Option<&str>) -> Result<Self> {
        let default_graph = default_graph.map(validate_graph_iri).transpose()?;
        let mut rules = Vec::with_capacity(graph_maps.len());

        for raw in graph_maps {
            let (path, graph) = raw
                .split_once('=')
                .with_context(|| format!("Invalid --graph-map '{raw}': expected PATH=URI"))?;
            if path.is_empty() || graph.is_empty() {
                bail!("Invalid --graph-map '{raw}': expected non-empty PATH=URI");
            }
            let canonical = std::fs::canonicalize(path)
                .with_context(|| format!("Failed to canonicalize graph-map path {path}"))?;
            let is_file = canonical.is_file();
            if !is_file && !canonical.is_dir() {
                bail!("Graph-map path is neither a file nor directory: {path}");
            }
            rules.push(GraphMapRule {
                path: canonical,
                is_file,
                graph: validate_graph_iri(graph)?,
            });
        }

        Ok(Self {
            rules,
            default_graph,
        })
    }

    pub fn for_source(&self, source: &Path) -> Result<SourceGraphAssignment> {
        let source = std::fs::canonicalize(source)
            .with_context(|| format!("Failed to canonicalize input path {}", source.display()))?;
        let mut best_specificity: Option<(bool, usize)> = None;
        let mut mapped_graphs = Vec::new();

        for rule in &self.rules {
            let matches = if rule.is_file {
                source == rule.path
            } else {
                source.starts_with(&rule.path)
            };
            if !matches {
                continue;
            }

            let specificity = (rule.is_file, rule.path.components().count());
            match best_specificity {
                None => {
                    best_specificity = Some(specificity);
                    mapped_graphs.clear();
                    mapped_graphs.push(rule.graph.clone());
                }
                Some(best) if specificity > best => {
                    best_specificity = Some(specificity);
                    mapped_graphs.clear();
                    mapped_graphs.push(rule.graph.clone());
                }
                Some(best) if specificity == best => mapped_graphs.push(rule.graph.clone()),
                Some(_) => {}
            }
        }

        mapped_graphs.sort_unstable();
        mapped_graphs.dedup();
        Ok(SourceGraphAssignment {
            mapped_graphs,
            default_graph: self.default_graph.clone(),
        })
    }
}

impl SourceGraphAssignment {
    /// Apply the normative assignment order to one parsed statement.
    pub fn memberships(&self, explicit_graph: Option<Arc<str>>) -> Vec<Option<Arc<str>>> {
        let mut named = Vec::with_capacity(1 + self.mapped_graphs.len());
        if let Some(graph) = explicit_graph {
            named.push(graph);
        }
        named.extend(self.mapped_graphs.iter().cloned());
        named.sort_unstable();
        named.dedup();

        if named.is_empty()
            && let Some(graph) = &self.default_graph
        {
            named.push(graph.clone());
        }

        if named.is_empty() {
            vec![None]
        } else {
            named.into_iter().map(Some).collect()
        }
    }

    pub fn mapped_graphs(&self) -> &[Arc<str>] {
        &self.mapped_graphs
    }

    pub fn default_graph(&self) -> Option<&str> {
        self.default_graph.as_deref()
    }

    pub fn default_graph_term(&self) -> Option<&Arc<str>> {
        self.default_graph.as_ref()
    }
}

fn validate_graph_iri(value: &str) -> Result<Arc<str>> {
    NamedNode::new(value)
        .with_context(|| format!("Graph name must be an absolute IRI: {value}"))?;
    Ok(Arc::from(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_is_additive_and_falls_back_to_default_layer() {
        let assignment = SourceGraphAssignment::default();
        assert_eq!(assignment.memberships(None), vec![None]);

        let assignment = SourceGraphAssignment {
            mapped_graphs: vec!["urn:mapped".into()],
            default_graph: Some("urn:fallback".into()),
        };
        assert_eq!(
            assignment.memberships(Some("urn:explicit".into())),
            vec![Some("urn:explicit".into()), Some("urn:mapped".into())]
        );
        assert_eq!(
            assignment.memberships(None),
            vec![Some("urn:mapped".into())]
        );
    }
}
