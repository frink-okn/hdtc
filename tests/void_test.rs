//! Integration tests for `hdtc void`.
//!
//! Tests VoID statistics computation covering: dataset-level stats, property partitions,
//! class partitions with nested property/target-class partitions, and blank-node mode.

mod common;

use common::write_file;
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;

// ---------------------------------------------------------------------------
// Test fixture — an N-Triples dataset with rdf:type triples
// ---------------------------------------------------------------------------

/// A small dataset with classes, typed instances, and various property/object combinations.
///
/// Subjects: alice (Person + Employee), bob (Person), carol (Person)
/// alice: type Person, type Employee, name, knows bob, worksFor corp
/// bob: type Person, name, knows alice
/// carol: type Person, name
/// corp: (untyped) name
///
/// Total triples: 12
const VOID_NT: &str = r#"<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Employee> .
<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/worksFor> <http://example.org/corp> .
<http://example.org/bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/bob> <http://example.org/name> "Bob" .
<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> .
<http://example.org/carol> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/carol> <http://example.org/name> "Carol" .
<http://example.org/corp> <http://example.org/name> "ACME" .
"#;

/// The same dataset but without any rdf:type triples (tests the no-class-partition path).
const NO_TYPE_NT: &str = r#"<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/name> "Bob" .
"#;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an HDT from inline N-Triples content and return its path.
fn make_hdt(temp_dir: &Path, content: &str, name: &str) -> std::path::PathBuf {
    let nt_path = temp_dir.join(format!("{name}.nt"));
    write_file(&nt_path, content.as_bytes());

    let hdt_path = temp_dir.join(format!("{name}.hdt"));
    let work_dir = temp_dir.join(format!("{name}_work"));

    let status = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args([
            "create",
            nt_path.to_str().unwrap(),
            "-o",
            hdt_path.to_str().unwrap(),
            "--base-uri",
            "http://example.org/dataset",
            "--temp-dir",
            work_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute hdtc create");

    assert!(
        status.status.success(),
        "hdtc create failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );

    hdt_path
}

/// Run `hdtc void` and return (success, stdout, stderr).
fn run_void(hdt_path: &Path, extra_args: &[&str]) -> (bool, String, String) {
    let mut args = vec!["void", hdt_path.to_str().unwrap()];
    args.extend_from_slice(extra_args);

    let output = Command::new(env!("CARGO_BIN_EXE_hdtc"))
        .args(&args)
        .output()
        .expect("Failed to execute hdtc void");

    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Parse N-Triples output into a vec of (subject, predicate, object) string tuples.
///
/// - Subjects and objects keep their N-Triples formatting (`<IRI>`, `_:bN`, `"lit"^^<type>`).
/// - Predicates have the angle-bracket delimiters stripped for easy string comparison.
fn parse_ntriples(output: &str) -> Vec<(String, String, String)> {
    output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            // Strip trailing " ."
            let line = line.trim_end_matches('.').trim();
            // Split into exactly 3 tokens (subject, predicate, object).
            // N-Triples guarantees that predicates are always IRIs with no spaces.
            let tokens: Vec<&str> = line.splitn(3, ' ').collect();
            assert_eq!(tokens.len(), 3, "Unexpected N-Triples line: {line}");
            // Predicates are always IRIs — strip <> for easier comparison in assertions.
            let pred = tokens[1]
                .trim_start_matches('<')
                .trim_end_matches('>');
            (
                tokens[0].to_string(),
                pred.to_string(),
                tokens[2].trim().to_string(),
            )
        })
        .collect()
}

/// Collect all object values for a given (subject, predicate) pair.
fn objects_for(
    triples: &[(String, String, String)],
    subject: &str,
    predicate: &str,
) -> Vec<String> {
    triples
        .iter()
        .filter(|(s, p, _)| s == subject && p == predicate)
        .map(|(_, _, o)| o.clone())
        .collect()
}

/// Find objects matching a predicate across all subjects, returning (subject, object) pairs.
fn find_by_predicate<'a>(
    triples: &'a [(String, String, String)],
    predicate: &str,
) -> Vec<(&'a str, &'a str)> {
    triples
        .iter()
        .filter(|(_, p, _)| p == predicate)
        .map(|(s, _, o)| (s.as_str(), o.as_str()))
        .collect()
}

/// Build a map from subject to all its (predicate, object) pairs.
fn subject_map(triples: &[(String, String, String)]) -> HashMap<String, Vec<(String, String)>> {
    let mut map: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for (s, p, o) in triples {
        map.entry(s.clone()).or_default().push((p.clone(), o.clone()));
    }
    map
}

/// Find a class partition node for the given class IRI within a dataset.
fn find_class_partition(
    triples: &[(String, String, String)],
    smap: &HashMap<String, Vec<(String, String)>>,
    dataset_node: &str,
    class_iri: &str,
) -> Option<String> {
    triples
        .iter()
        .filter(|(s, p, _)| s == dataset_node && p == "http://rdfs.org/ns/void#classPartition")
        .map(|(_, _, o)| o.clone())
        .find(|cp| {
            smap.get(cp).is_some_and(|pairs| {
                pairs
                    .iter()
                    .any(|(p, o)| p == "http://rdfs.org/ns/void#class" && o == class_iri)
            })
        })
}

/// Find a property partition node for the given predicate IRI within a parent node.
fn find_property_partition(
    triples: &[(String, String, String)],
    smap: &HashMap<String, Vec<(String, String)>>,
    parent_node: &str,
    pred_iri: &str,
) -> Option<String> {
    triples
        .iter()
        .filter(|(s, p, _)| s == parent_node && p == "http://rdfs.org/ns/void#propertyPartition")
        .map(|(_, _, o)| o.clone())
        .find(|pp| {
            smap.get(pp).is_some_and(|pairs| {
                pairs
                    .iter()
                    .any(|(p, o)| p == "http://rdfs.org/ns/void#property" && o == pred_iri)
            })
        })
}

/// Extract the void:triples integer value for a given subject node.
fn get_void_triples_count(
    smap: &HashMap<String, Vec<(String, String)>>,
    node: &str,
) -> Option<u64> {
    get_void_int(smap, node, "http://rdfs.org/ns/void#triples")
}

/// Extract an integer value for a given subject/predicate pair.
fn get_void_int(
    smap: &HashMap<String, Vec<(String, String)>>,
    node: &str,
    predicate: &str,
) -> Option<u64> {
    smap.get(node).and_then(|pairs| {
        pairs.iter().find(|(p, _)| p == predicate).and_then(|(_, o)| {
            // Parse "N"^^<xsd:integer>
            o.strip_prefix('"')
                .and_then(|s| s.split('"').next())
                .and_then(|s| s.parse::<u64>().ok())
        })
    })
}

// ---------------------------------------------------------------------------
// Test: dataset-level statistics
// ---------------------------------------------------------------------------

#[test]
fn test_void_dataset_stats() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_stats");
    let (ok, stdout, stderr) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);

    let ds = "<http://example.org/ds>";

    // rdf:type void:Dataset
    assert!(
        objects_for(&triples, ds, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            .contains(&"<http://rdfs.org/ns/void#Dataset>".to_string()),
        "Missing void:Dataset type"
    );

    // void:triples = 11 (VOID_NT has 11 triples)
    assert!(
        objects_for(&triples, ds, "http://rdfs.org/ns/void#triples")
            .contains(&"\"11\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_string()),
        "Expected 11 triples, got: {:?}",
        objects_for(&triples, ds, "http://rdfs.org/ns/void#triples")
    );
}

// ---------------------------------------------------------------------------
// Test: property partitions at dataset level
// ---------------------------------------------------------------------------

#[test]
fn test_void_property_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_props");
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Dataset should have void:propertyPartition links.
    let prop_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#propertyPartition")
        .map(|(_, _, o)| o.clone())
        .collect();
    assert!(!prop_parts.is_empty(), "No dataset-level property partitions found");

    // Each property partition should have void:property and void:triples.
    for part in &prop_parts {
        let preds: Vec<String> = smap
            .get(part)
            .map(|pairs| pairs.iter().map(|(p, _)| p.clone()).collect())
            .unwrap_or_default();
        assert!(
            preds.contains(&"http://rdfs.org/ns/void#property".to_string()),
            "Property partition {part} missing void:property"
        );
        assert!(
            preds.contains(&"http://rdfs.org/ns/void#triples".to_string()),
            "Property partition {part} missing void:triples"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: class partitions
// ---------------------------------------------------------------------------

#[test]
fn test_void_class_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_classes");
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Find class partition nodes.
    let class_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#classPartition")
        .map(|(_, _, o)| o.clone())
        .collect();

    // We have 2 classes: Person and Employee.
    assert_eq!(class_parts.len(), 2, "Expected 2 class partitions, got {}", class_parts.len());

    // Each class partition must have: void:class, void:entities, void:triples.
    for cp in &class_parts {
        let preds: Vec<String> = smap
            .get(cp)
            .map(|pairs| pairs.iter().map(|(p, _)| p.clone()).collect())
            .unwrap_or_default();
        assert!(preds.contains(&"http://rdfs.org/ns/void#class".to_string()),
            "Class partition {cp} missing void:class");
        assert!(preds.contains(&"http://rdfs.org/ns/void#entities".to_string()),
            "Class partition {cp} missing void:entities");
        assert!(preds.contains(&"http://rdfs.org/ns/void#triples".to_string()),
            "Class partition {cp} missing void:triples");
    }

    // Collect class→entities mapping.
    let mut class_entities: HashMap<String, String> = HashMap::new();
    for cp in &class_parts {
        if let Some(pairs) = smap.get(cp) {
            let class_iri = pairs.iter().find(|(p, _)| p == "http://rdfs.org/ns/void#class")
                .map(|(_, o)| o.clone());
            let entities = pairs.iter().find(|(p, _)| p == "http://rdfs.org/ns/void#entities")
                .map(|(_, o)| o.clone());
            if let (Some(c), Some(e)) = (class_iri, entities) {
                class_entities.insert(c, e);
            }
        }
    }

    // Person: alice, bob, carol → 3 entities
    assert_eq!(
        class_entities.get("<http://example.org/Person>").map(|s| s.as_str()),
        Some("\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        "Person should have 3 entities"
    );

    // Employee: alice → 1 entity
    assert_eq!(
        class_entities.get("<http://example.org/Employee>").map(|s| s.as_str()),
        Some("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        "Employee should have 1 entity"
    );
}

// ---------------------------------------------------------------------------
// Test: nested property partitions within class partitions
// ---------------------------------------------------------------------------

#[test]
fn test_void_class_property_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_class_props");
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Find Person class partition.
    let class_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#classPartition")
        .map(|(_, _, o)| o.clone())
        .collect();

    let person_cp = class_parts.iter().find(|cp| {
        smap.get(*cp).is_some_and(|pairs| {
            pairs.iter().any(|(p, o)| {
                p == "http://rdfs.org/ns/void#class" && o == "<http://example.org/Person>"
            })
        })
    });
    assert!(person_cp.is_some(), "Person class partition not found");
    let person_cp = person_cp.unwrap();

    // Person class partition should have void:propertyPartition links.
    let prop_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == person_cp && p == "http://rdfs.org/ns/void#propertyPartition")
        .map(|(_, _, o)| o.clone())
        .collect();
    assert!(!prop_parts.is_empty(), "Person class partition has no property partitions");

    // Collect (pred_iri → triple_count) for Person's property partitions.
    let mut person_props: HashMap<String, String> = HashMap::new();
    for pp in &prop_parts {
        if let Some(pairs) = smap.get(pp) {
            let pred = pairs.iter().find(|(p, _)| p == "http://rdfs.org/ns/void#property")
                .map(|(_, o)| o.clone());
            let count = pairs.iter().find(|(p, _)| p == "http://rdfs.org/ns/void#triples")
                .map(|(_, o)| o.clone());
            if let (Some(pred), Some(count)) = (pred, count) {
                person_props.insert(pred, count);
            }
        }
    }

    // Person class partition should include rdf:type, name, knows properties.
    // rdf:type: alice has 2 rdf:type triples (Person + Employee) and is a Person instance,
    // bob and carol each have 1 rdf:type triple → total 4 for Person class.
    assert_eq!(
        person_props.get("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>").map(|s| s.as_str()),
        Some("\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        "Person rdf:type property partition should have 4 triples"
    );

    // name: alice, bob, carol each have 1 name triple → 3
    assert_eq!(
        person_props.get("<http://example.org/name>").map(|s| s.as_str()),
        Some("\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        "Person name property partition should have 3 triples"
    );

    // knows: alice knows bob, bob knows alice → 2
    assert_eq!(
        person_props.get("<http://example.org/knows>").map(|s| s.as_str()),
        Some("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        "Person knows property partition should have 2 triples"
    );
}

// ---------------------------------------------------------------------------
// Test: object class partitions (target class breakdown)
// ---------------------------------------------------------------------------

#[test]
fn test_void_object_class_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_obj_class");
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Find Person class partition.
    let class_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#classPartition")
        .map(|(_, _, o)| o.clone())
        .collect();

    let person_cp = class_parts.iter().find(|cp| {
        smap.get(*cp).is_some_and(|pairs| {
            pairs.iter().any(|(p, o)| {
                p == "http://rdfs.org/ns/void#class" && o == "<http://example.org/Person>"
            })
        })
    }).expect("Person class partition not found");

    // Find the "knows" property partition within Person.
    let prop_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == person_cp && p == "http://rdfs.org/ns/void#propertyPartition")
        .map(|(_, _, o)| o.clone())
        .collect();

    let knows_pp = prop_parts.iter().find(|pp| {
        smap.get(*pp).is_some_and(|pairs| {
            pairs.iter().any(|(p, o)| {
                p == "http://rdfs.org/ns/void#property" && o == "<http://example.org/knows>"
            })
        })
    });
    assert!(knows_pp.is_some(), "'knows' property partition not found in Person class");
    let knows_pp = knows_pp.unwrap();

    // "knows" property partition should have objectClassPartition links.
    let target_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == knows_pp && p == "http://ldf.fi/void-ext#objectClassPartition")
        .map(|(_, _, o)| o.clone())
        .collect();
    assert!(!target_parts.is_empty(), "'knows' property partition has no objectClassPartition");

    // At least one target should be void:class Person (alice knows bob:Person, bob knows alice:Person).
    let has_person_target = target_parts.iter().any(|tp| {
        smap.get(tp).is_some_and(|pairs| {
            pairs.iter().any(|(p, o)| {
                p == "http://rdfs.org/ns/void#class" && o == "<http://example.org/Person>"
            })
        })
    });
    assert!(has_person_target, "Expected Person as a target class for 'knows'");
}

// ---------------------------------------------------------------------------
// Test: no rdf:type triples → no class partitions, only dataset stats
// ---------------------------------------------------------------------------

#[test]
fn test_void_no_type_triples() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), NO_TYPE_NT, "void_no_type");
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);
    let ds = "<http://example.org/ds>";

    // Should have dataset stats.
    let void_triples = objects_for(&triples, ds, "http://rdfs.org/ns/void#triples");
    assert_eq!(
        void_triples,
        vec!["\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>"],
        "Expected 3 triples"
    );

    // Should have no class partitions.
    let class_parts = find_by_predicate(&triples, "http://rdfs.org/ns/void#classPartition");
    assert!(class_parts.is_empty(), "Unexpected class partitions: {class_parts:?}");
}

// ---------------------------------------------------------------------------
// Test: --use-blank-nodes flag
// ---------------------------------------------------------------------------

#[test]
fn test_void_blank_nodes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_blank");
    let (ok, stdout, _) = run_void(
        &hdt_path,
        &["--dataset-uri", "http://example.org/ds", "--use-blank-nodes"],
    );
    assert!(ok);

    let triples = parse_ntriples(&stdout);

    // All partition nodes should be blank nodes (_:bN), not URI-based partition URIs.
    // Property partitions should have blank node objects.
    let prop_part_objs: Vec<String> = triples
        .iter()
        .filter(|(_, p, _)| p == "http://rdfs.org/ns/void#propertyPartition")
        .map(|(_, _, o)| o.clone())
        .collect();
    assert!(!prop_part_objs.is_empty(), "No property partitions found");
    for obj in &prop_part_objs {
        assert!(
            obj.starts_with("_:b"),
            "Expected blank node for property partition, got: {obj}"
        );
    }

    // Class partitions should also be blank nodes.
    let class_part_objs: Vec<String> = triples
        .iter()
        .filter(|(_, p, _)| p == "http://rdfs.org/ns/void#classPartition")
        .map(|(_, _, o)| o.clone())
        .collect();
    for obj in &class_part_objs {
        assert!(
            obj.starts_with("_:b"),
            "Expected blank node for class partition, got: {obj}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: output to file
// ---------------------------------------------------------------------------

#[test]
fn test_void_output_to_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), NO_TYPE_NT, "void_outfile");
    let out_path = temp_dir.path().join("void.nt");

    let (ok, stdout, stderr) = run_void(
        &hdt_path,
        &[
            "--dataset-uri",
            "http://example.org/ds",
            "-o",
            out_path.to_str().unwrap(),
        ],
    );
    assert!(ok, "hdtc void failed: {stderr}");
    // Nothing written to stdout when writing to file.
    assert!(stdout.is_empty(), "Unexpected stdout: {stdout}");
    // File must exist and contain N-Triples.
    let contents = std::fs::read_to_string(&out_path).expect("output file not found");
    assert!(contents.contains("void#triples"), "Output file missing void#triples");
}

// ---------------------------------------------------------------------------
// Test: missing HDT file → error
// ---------------------------------------------------------------------------

#[test]
fn test_void_missing_file() {
    let (ok, _, _) = run_void(Path::new("/nonexistent/file.hdt"), &[]);
    assert!(!ok, "Expected failure for missing HDT file");
}

// ---------------------------------------------------------------------------
// Test: multi-typed objects create multiple target partitions
// ---------------------------------------------------------------------------

/// Fixture where objects have multiple types, testing that target class partitions
/// are created for each type of the object.
///
/// alice (Person): knows bob, knows carol
/// bob (Person + Employee): (typed with 2 classes)
/// carol (Person + Manager): (typed with 2 classes)
///
/// In Person's "knows" property partition, bob appears as target with types
/// {Person, Employee} and carol with types {Person, Manager}.
const MULTI_TYPED_OBJECTS_NT: &str = r#"<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Employee> .
<http://example.org/carol> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/carol> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Manager> .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/knows> <http://example.org/carol> .
"#;

#[test]
fn test_void_multi_typed_object_target_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), MULTI_TYPED_OBJECTS_NT, "void_multi_obj");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Find the Person class partition.
    let person_cp = find_class_partition(&triples, &smap, ds, "<http://example.org/Person>");
    assert!(person_cp.is_some(), "Person class partition not found");
    let person_cp = person_cp.unwrap();

    // Find Person's "knows" property partition.
    let knows_pp = find_property_partition(&triples, &smap, &person_cp, "<http://example.org/knows>");
    assert!(knows_pp.is_some(), "'knows' property partition not found in Person");
    let knows_pp = knows_pp.unwrap();

    // "knows" should have objectClassPartition entries.
    let target_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == &knows_pp && p == "http://ldf.fi/void-ext#objectClassPartition")
        .map(|(_, _, o)| o.clone())
        .collect();

    // bob has types {Person, Employee}, carol has types {Person, Manager}.
    // Target classes should include: Person, Employee, Manager.
    let target_classes: Vec<String> = target_parts
        .iter()
        .filter_map(|tp| {
            smap.get(tp).and_then(|pairs| {
                pairs
                    .iter()
                    .find(|(p, _)| p == "http://rdfs.org/ns/void#class")
                    .map(|(_, o)| o.clone())
            })
        })
        .collect();

    assert!(
        target_classes.contains(&"<http://example.org/Person>".to_string()),
        "Expected Person as target class, got: {target_classes:?}"
    );
    assert!(
        target_classes.contains(&"<http://example.org/Employee>".to_string()),
        "Expected Employee as target class, got: {target_classes:?}"
    );
    assert!(
        target_classes.contains(&"<http://example.org/Manager>".to_string()),
        "Expected Manager as target class, got: {target_classes:?}"
    );

    // Person target class count should be 2 (both bob and carol are Person).
    let person_target = target_parts.iter().find(|tp| {
        smap.get(*tp).is_some_and(|pairs| {
            pairs
                .iter()
                .any(|(p, o)| p == "http://rdfs.org/ns/void#class" && o == "<http://example.org/Person>")
        })
    });
    let person_count = get_void_triples_count(&smap, person_target.unwrap());
    assert_eq!(person_count, Some(2), "Person target should have count 2 (bob + carol)");

    // Employee target class count should be 1 (only bob).
    let emp_target = target_parts.iter().find(|tp| {
        smap.get(*tp).is_some_and(|pairs| {
            pairs
                .iter()
                .any(|(p, o)| p == "http://rdfs.org/ns/void#class" && o == "<http://example.org/Employee>")
        })
    });
    let emp_count = get_void_triples_count(&smap, emp_target.unwrap());
    assert_eq!(emp_count, Some(1), "Employee target should have count 1 (bob only)");

    // Manager target class count should be 1 (only carol).
    let mgr_target = target_parts.iter().find(|tp| {
        smap.get(*tp).is_some_and(|pairs| {
            pairs
                .iter()
                .any(|(p, o)| p == "http://rdfs.org/ns/void#class" && o == "<http://example.org/Manager>")
        })
    });
    let mgr_count = get_void_triples_count(&smap, mgr_target.unwrap());
    assert_eq!(mgr_count, Some(1), "Manager target should have count 1 (carol only)");
}

// ---------------------------------------------------------------------------
// Test: untyped target partitions have no void:class
// ---------------------------------------------------------------------------

#[test]
fn test_void_untyped_target_has_no_class() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_untyped_tgt");
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Find Person's "name" property partition — its objects are literals (untyped).
    let person_cp = find_class_partition(&triples, &smap, ds, "<http://example.org/Person>");
    assert!(person_cp.is_some(), "Person class partition not found");

    let name_pp = find_property_partition(
        &triples,
        &smap,
        person_cp.as_ref().unwrap(),
        "<http://example.org/name>",
    );
    assert!(name_pp.is_some(), "'name' property partition not found in Person");

    // The "name" property partition targets are all literals → untyped.
    let target_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == name_pp.as_ref().unwrap() && p == "http://ldf.fi/void-ext#objectClassPartition")
        .map(|(_, _, o)| o.clone())
        .collect();

    assert!(!target_parts.is_empty(), "Expected at least one target partition for 'name'");

    // Untyped target partitions must NOT have a void:class predicate.
    for tp in &target_parts {
        let has_class = smap
            .get(tp)
            .is_some_and(|pairs| pairs.iter().any(|(p, _)| p == "http://rdfs.org/ns/void#class"));
        assert!(
            !has_class,
            "Untyped target partition {tp} should not have void:class"
        );
    }

    // But they must still have rdf:type void:Dataset and void:triples.
    for tp in &target_parts {
        let pairs = smap.get(tp).expect("target partition not found in smap");
        assert!(
            pairs.iter().any(|(p, o)| p == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                && o == "<http://rdfs.org/ns/void#Dataset>"),
            "Untyped target partition {tp} missing rdf:type void:Dataset"
        );
        assert!(
            pairs.iter().any(|(p, _)| p == "http://rdfs.org/ns/void#triples"),
            "Untyped target partition {tp} missing void:triples"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: mixed typed and literal objects for same predicate
// ---------------------------------------------------------------------------

/// Fixture: person knows both a typed entity and has a literal name.
/// The "worksFor" predicate targets both a typed Company and a literal "Self-employed".
const MIXED_OBJECTS_NT: &str = r#"<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/corp> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Company> .
<http://example.org/alice> <http://example.org/worksFor> <http://example.org/corp> .
<http://example.org/alice> <http://example.org/worksFor> "Self-employed" .
"#;

#[test]
fn test_void_mixed_typed_and_literal_targets() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), MIXED_OBJECTS_NT, "void_mixed_obj");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Find Person's "worksFor" property partition.
    let person_cp = find_class_partition(&triples, &smap, ds, "<http://example.org/Person>");
    assert!(person_cp.is_some(), "Person class partition not found");

    let works_pp = find_property_partition(
        &triples,
        &smap,
        person_cp.as_ref().unwrap(),
        "<http://example.org/worksFor>",
    );
    assert!(works_pp.is_some(), "'worksFor' property partition not found");

    let target_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| {
            s == works_pp.as_ref().unwrap()
                && p == "http://ldf.fi/void-ext#objectClassPartition"
        })
        .map(|(_, _, o)| o.clone())
        .collect();

    // Should have both typed (Company) and untyped target partitions.
    let typed_targets: Vec<&String> = target_parts
        .iter()
        .filter(|tp| {
            smap.get(*tp)
                .is_some_and(|pairs| pairs.iter().any(|(p, _)| p == "http://rdfs.org/ns/void#class"))
        })
        .collect();
    let untyped_targets: Vec<&String> = target_parts
        .iter()
        .filter(|tp| {
            smap.get(*tp)
                .is_some_and(|pairs| !pairs.iter().any(|(p, _)| p == "http://rdfs.org/ns/void#class"))
        })
        .collect();

    assert_eq!(typed_targets.len(), 1, "Expected 1 typed target (Company)");
    assert_eq!(untyped_targets.len(), 1, "Expected 1 untyped target (literal)");

    // Typed target should be Company with count 1.
    let company_target = typed_targets[0];
    let company_class = smap
        .get(company_target)
        .and_then(|pairs| {
            pairs
                .iter()
                .find(|(p, _)| p == "http://rdfs.org/ns/void#class")
                .map(|(_, o)| o.clone())
        });
    assert_eq!(
        company_class.as_deref(),
        Some("<http://example.org/Company>"),
        "Typed target should be Company"
    );
    assert_eq!(
        get_void_triples_count(&smap, company_target),
        Some(1),
        "Company target count should be 1"
    );

    // Untyped target should have count 1.
    assert_eq!(
        get_void_triples_count(&smap, untyped_targets[0]),
        Some(1),
        "Untyped target count should be 1"
    );
}

// ---------------------------------------------------------------------------
// Test: count consistency — class triples = sum of property partition counts
// ---------------------------------------------------------------------------

#[test]
fn test_void_class_triples_equal_sum_of_property_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_count_consistency");
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // For each class partition, verify void:triples = sum of property partition void:triples.
    let class_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#classPartition")
        .map(|(_, _, o)| o.clone())
        .collect();

    for cp in &class_parts {
        let class_triple_count = get_void_triples_count(&smap, cp)
            .expect("class partition missing void:triples");

        // Sum property partition counts.
        let prop_parts: Vec<String> = triples
            .iter()
            .filter(|(s, p, _)| s == cp && p == "http://rdfs.org/ns/void#propertyPartition")
            .map(|(_, _, o)| o.clone())
            .collect();

        let sum: u64 = prop_parts
            .iter()
            .map(|pp| get_void_triples_count(&smap, pp).unwrap_or(0))
            .sum();

        let class_name = smap
            .get(cp)
            .and_then(|pairs| {
                pairs
                    .iter()
                    .find(|(p, _)| p == "http://rdfs.org/ns/void#class")
                    .map(|(_, o)| o.clone())
            })
            .unwrap_or_else(|| "unknown".to_string());

        assert_eq!(
            class_triple_count, sum,
            "Class partition {class_name}: void:triples ({class_triple_count}) != \
             sum of property partitions ({sum})"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: dataset property counts include untyped subjects
// ---------------------------------------------------------------------------

#[test]
fn test_void_dataset_property_counts_include_untyped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_ds_includes_untyped");
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Find the dataset-level property partition for "name".
    let ds_prop_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#propertyPartition")
        .map(|(_, _, o)| o.clone())
        .collect();

    let name_ds_pp = ds_prop_parts.iter().find(|pp| {
        smap.get(*pp).is_some_and(|pairs| {
            pairs
                .iter()
                .any(|(p, o)| p == "http://rdfs.org/ns/void#property" && o == "<http://example.org/name>")
        })
    });
    assert!(name_ds_pp.is_some(), "Dataset-level 'name' property partition not found");

    // Dataset-level name count should be 4 (alice, bob, carol, corp — corp is untyped).
    let ds_name_count = get_void_triples_count(&smap, name_ds_pp.unwrap());
    assert_eq!(
        ds_name_count,
        Some(4),
        "Dataset 'name' count should be 4 (includes untyped corp)"
    );

    // Person class partition's name count should be 3 (alice, bob, carol — not corp).
    let person_cp = find_class_partition(&triples, &smap, ds, "<http://example.org/Person>");
    let name_person_pp = find_property_partition(
        &triples,
        &smap,
        person_cp.as_ref().unwrap(),
        "<http://example.org/name>",
    );
    let person_name_count = get_void_triples_count(&smap, name_person_pp.as_ref().unwrap());
    assert_eq!(
        person_name_count,
        Some(3),
        "Person 'name' count should be 3 (excludes untyped corp)"
    );
}

// ---------------------------------------------------------------------------
// Test: only rdf:type triples (no other predicates)
// ---------------------------------------------------------------------------

const TYPE_ONLY_NT: &str = r#"<http://example.org/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ClassA> .
<http://example.org/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ClassA> .
"#;

#[test]
fn test_void_type_only_triples() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), TYPE_ONLY_NT, "void_type_only");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Dataset should have 2 triples total.
    let ds_triples = objects_for(&triples, ds, "http://rdfs.org/ns/void#triples");
    assert_eq!(
        ds_triples,
        vec!["\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"],
    );

    // ClassA partition should exist with 2 entities.
    let class_a_cp = find_class_partition(&triples, &smap, ds, "<http://example.org/ClassA>");
    assert!(class_a_cp.is_some(), "ClassA partition not found");

    let entities = smap
        .get(class_a_cp.as_ref().unwrap())
        .and_then(|pairs| {
            pairs
                .iter()
                .find(|(p, _)| p == "http://rdfs.org/ns/void#entities")
                .map(|(_, o)| o.clone())
        });
    assert_eq!(
        entities.as_deref(),
        Some("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        "ClassA should have 2 entities"
    );

    // ClassA triple count = 2 (both rdf:type triples).
    let class_triple_count = get_void_triples_count(&smap, class_a_cp.as_ref().unwrap());
    assert_eq!(class_triple_count, Some(2));
}

// ---------------------------------------------------------------------------
// Test: instance with many types
// ---------------------------------------------------------------------------

const MANY_TYPES_NT: &str = r#"<http://example.org/item> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Type1> .
<http://example.org/item> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Type2> .
<http://example.org/item> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Type3> .
<http://example.org/item> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Type4> .
<http://example.org/item> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Type5> .
<http://example.org/item> <http://example.org/name> "Item" .
"#;

#[test]
fn test_void_many_types_same_instance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), MANY_TYPES_NT, "void_many_types");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Should have 5 class partitions (Type1..Type5).
    let class_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#classPartition")
        .map(|(_, _, o)| o.clone())
        .collect();
    assert_eq!(class_parts.len(), 5, "Expected 5 class partitions");

    // Each class partition should have 1 entity and 6 triples (5 type + 1 name).
    for cp in &class_parts {
        let entities = get_void_int(&smap, cp, "http://rdfs.org/ns/void#entities");
        assert_eq!(entities, Some(1), "Each class should have 1 entity");

        let triple_count = get_void_triples_count(&smap, cp);
        assert_eq!(triple_count, Some(6), "Each class should have 6 triples (5 type + 1 name)");
    }
}

// ---------------------------------------------------------------------------
// Test: self-referential triple (subject knows itself)
// ---------------------------------------------------------------------------

const SELF_REF_NT: &str = r#"<http://example.org/person> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/person> <http://example.org/knows> <http://example.org/person> .
"#;

#[test]
fn test_void_self_referential_triple() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), SELF_REF_NT, "void_self_ref");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Person class partition.
    let person_cp = find_class_partition(&triples, &smap, ds, "<http://example.org/Person>");
    assert!(person_cp.is_some(), "Person partition not found");

    // "knows" should have Person as target class (self-reference).
    let knows_pp = find_property_partition(
        &triples,
        &smap,
        person_cp.as_ref().unwrap(),
        "<http://example.org/knows>",
    );
    assert!(knows_pp.is_some(), "'knows' property partition not found");

    let target_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| {
            s == knows_pp.as_ref().unwrap()
                && p == "http://ldf.fi/void-ext#objectClassPartition"
        })
        .map(|(_, _, o)| o.clone())
        .collect();

    let has_person_target = target_parts.iter().any(|tp| {
        smap.get(tp).is_some_and(|pairs| {
            pairs
                .iter()
                .any(|(p, o)| p == "http://rdfs.org/ns/void#class" && o == "<http://example.org/Person>")
        })
    });
    assert!(has_person_target, "Self-referencing triple should have Person as target class");
}

// ---------------------------------------------------------------------------
// Test: circular references between typed instances
// ---------------------------------------------------------------------------

const CIRCULAR_NT: &str = r#"<http://example.org/p1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/p2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/p1> <http://example.org/knows> <http://example.org/p2> .
<http://example.org/p2> <http://example.org/knows> <http://example.org/p1> .
"#;

#[test]
fn test_void_circular_references() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), CIRCULAR_NT, "void_circular");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Person: 2 entities, 4 triples (2 type + 2 knows).
    let person_cp = find_class_partition(&triples, &smap, ds, "<http://example.org/Person>");
    assert!(person_cp.is_some(), "Person partition not found");

    let entities = get_void_int(&smap, person_cp.as_ref().unwrap(), "http://rdfs.org/ns/void#entities");
    assert_eq!(entities, Some(2), "Person should have 2 entities");

    let total = get_void_triples_count(&smap, person_cp.as_ref().unwrap());
    assert_eq!(total, Some(4), "Person should have 4 triples");
}

// ---------------------------------------------------------------------------
// Test: multi-class triples counted independently
// ---------------------------------------------------------------------------

/// item has ClassA and ClassB. other has only ClassA.
/// item: type ClassA, type ClassB, p1 val1, p1 val2, p2 val3
/// other: type ClassA, p1 val4
///
/// ClassA: 2 entities, triples = {item: 2type+2p1+1p2=5} + {other: 1type+1p1=2} = 7
/// ClassB: 1 entity, triples = {item: 2type+2p1+1p2=5}
const MULTI_CLASS_INDEPENDENT_NT: &str = r#"<http://example.org/item> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ClassA> .
<http://example.org/item> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ClassB> .
<http://example.org/item> <http://example.org/p1> "val1" .
<http://example.org/item> <http://example.org/p1> "val2" .
<http://example.org/item> <http://example.org/p2> "val3" .
<http://example.org/other> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ClassA> .
<http://example.org/other> <http://example.org/p1> "val4" .
"#;

#[test]
fn test_void_multi_class_independent_counts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), MULTI_CLASS_INDEPENDENT_NT, "void_multi_class");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // ClassA: 2 entities.
    let class_a = find_class_partition(&triples, &smap, ds, "<http://example.org/ClassA>");
    assert!(class_a.is_some(), "ClassA partition not found");
    let a_entities = get_void_int(&smap, class_a.as_ref().unwrap(), "http://rdfs.org/ns/void#entities");
    assert_eq!(a_entities, Some(2), "ClassA should have 2 entities");

    // ClassA triples: item(2type+2p1+1p2) + other(1type+1p1) = 7.
    let a_triples = get_void_triples_count(&smap, class_a.as_ref().unwrap());
    assert_eq!(a_triples, Some(7), "ClassA should have 7 triples");

    // ClassB: 1 entity.
    let class_b = find_class_partition(&triples, &smap, ds, "<http://example.org/ClassB>");
    assert!(class_b.is_some(), "ClassB partition not found");
    let b_entities = get_void_int(&smap, class_b.as_ref().unwrap(), "http://rdfs.org/ns/void#entities");
    assert_eq!(b_entities, Some(1), "ClassB should have 1 entity");

    // ClassB triples: item only (2type+2p1+1p2) = 5.
    let b_triples = get_void_triples_count(&smap, class_b.as_ref().unwrap());
    assert_eq!(b_triples, Some(5), "ClassB should have 5 triples");
}

// ---------------------------------------------------------------------------
// Test: dataset property count sum equals total triples
// ---------------------------------------------------------------------------

#[test]
fn test_void_dataset_property_counts_sum_to_total() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_ds_sum");
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Get total triples.
    let total = get_void_int(&smap, ds, "http://rdfs.org/ns/void#triples")
        .expect("void:triples missing");

    // Sum all dataset-level property partition counts.
    let ds_prop_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#propertyPartition")
        .map(|(_, _, o)| o.clone())
        .collect();

    let sum: u64 = ds_prop_parts
        .iter()
        .map(|pp| get_void_triples_count(&smap, pp).unwrap_or(0))
        .sum();

    assert_eq!(
        total, sum,
        "Dataset void:triples ({total}) should equal sum of property partition counts ({sum})"
    );
}

// ---------------------------------------------------------------------------
// Test: blank node classes are excluded from class partitions
// ---------------------------------------------------------------------------

/// Fixture with blank nodes used as rdf:type objects (common in OWL ontologies).
/// alice has both a URI class (Person) and a blank-node class (_:restriction).
/// bob has ONLY a blank-node class.
///
/// Expected: only Person class partition exists; bob is treated as untyped.
const BLANK_NODE_CLASS_NT: &str = r#"<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> _:restriction1 .
<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> _:restriction2 .
<http://example.org/bob> <http://example.org/name> "Bob" .
"#;

#[test]
fn test_void_blank_node_classes_excluded() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), BLANK_NODE_CLASS_NT, "void_bnode_class");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);

    let ds = "<http://example.org/ds>";

    // Only 1 class partition should exist (Person); blank node classes are excluded.
    let class_parts: Vec<String> = triples
        .iter()
        .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#classPartition")
        .map(|(_, _, o)| o.clone())
        .collect();
    assert_eq!(
        class_parts.len(),
        1,
        "Expected 1 class partition (Person only), got {}: {:?}",
        class_parts.len(),
        class_parts
    );

    // The class partition should be Person.
    let person_cp = find_class_partition(&triples, &smap, ds, "<http://example.org/Person>");
    assert!(person_cp.is_some(), "Person class partition not found");

    // Person should have 1 entity (alice only — bob's only type was a blank node).
    let entities = get_void_int(
        &smap,
        person_cp.as_ref().unwrap(),
        "http://rdfs.org/ns/void#entities",
    );
    assert_eq!(entities, Some(1), "Person should have 1 entity (alice)");

    // Person partition triples: alice has rdf:type Person (1) + rdf:type _:restriction (1) + name (1) = 3
    // But the blank-node rdf:type is still a triple with alice as subject and alice IS typed as Person,
    // so ALL of alice's triples are counted in Person's partition.
    let person_triples = get_void_triples_count(&smap, person_cp.as_ref().unwrap());
    assert_eq!(
        person_triples,
        Some(3),
        "Person should have 3 triples (type Person + type _:bnode + name)"
    );

    // bob's triples should NOT be counted in any class partition (only untyped).
    // Dataset-level name count = 2 (alice + bob), Person's name count = 1 (alice only).
    let name_ds_pp = {
        let ds_prop_parts: Vec<String> = triples
            .iter()
            .filter(|(s, p, _)| s == ds && p == "http://rdfs.org/ns/void#propertyPartition")
            .map(|(_, _, o)| o.clone())
            .collect();
        ds_prop_parts
            .into_iter()
            .find(|pp| {
                smap.get(pp).is_some_and(|pairs| {
                    pairs.iter().any(|(p, o)| {
                        p == "http://rdfs.org/ns/void#property"
                            && o == "<http://example.org/name>"
                    })
                })
            })
    };
    assert!(name_ds_pp.is_some());
    assert_eq!(
        get_void_triples_count(&smap, name_ds_pp.as_ref().unwrap()),
        Some(2),
        "Dataset 'name' count should be 2 (alice + bob)"
    );

    let name_person_pp = find_property_partition(
        &triples,
        &smap,
        person_cp.as_ref().unwrap(),
        "<http://example.org/name>",
    );
    assert_eq!(
        get_void_triples_count(&smap, name_person_pp.as_ref().unwrap()),
        Some(1),
        "Person 'name' count should be 1 (alice only, bob is untyped)"
    );
}

// ---------------------------------------------------------------------------
// Test: partition URI format matches Python tool (MD5-based URIs)
// ---------------------------------------------------------------------------

#[test]
fn test_void_partition_uri_format() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_uri_fmt");
    let dataset_uri = "http://example.org/ds";
    let (ok, stdout, _) = run_void(&hdt_path, &["--dataset-uri", dataset_uri]);
    assert!(ok);

    let triples = parse_ntriples(&stdout);

    // Compute expected dataset-level property partition URI for rdf:type:
    // md5("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") → known MD5 hash.
    let rdf_type_iri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let rdf_type_hash = format!("{:x}", md5::compute(rdf_type_iri.as_bytes()));
    let expected_type_part_uri = format!("<{dataset_uri}/property/{rdf_type_hash}>");

    // The rdf:type property partition URI should appear as a subject in the output.
    let has_type_part = triples.iter().any(|(s, _, _)| s == &expected_type_part_uri);
    assert!(
        has_type_part,
        "Expected rdf:type property partition with URI {expected_type_part_uri}"
    );

    // Compute expected class partition URI for Person:
    // md5("http://example.org/Person") → known MD5 hash.
    let person_iri = "http://example.org/Person";
    let person_hash = format!("{:x}", md5::compute(person_iri.as_bytes()));
    let expected_person_uri = format!("<{dataset_uri}/class/{person_hash}>");

    let has_person_part = triples.iter().any(|(s, _, _)| s == &expected_person_uri);
    assert!(
        has_person_part,
        "Expected Person class partition with URI {expected_person_uri}"
    );
}

// ---------------------------------------------------------------------------
// Test fixture for datatype and language partitions
// ---------------------------------------------------------------------------

/// Dataset with typed literals, language-tagged literals, and plain literals.
///
/// alice: type Person, age 30^^xsd:integer, name "Alice" (plain), label "Alice"@en, label "Alicia"@es
/// bob: type Person, age 25^^xsd:integer, name "Bob" (plain), label "Bob"@en
/// corp: (untyped) revenue 1000000^^xsd:decimal, name "ACME" (plain)
///
/// Total triples: 11
const DATATYPE_NT: &str = r#"<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/alice> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/alice> <http://example.org/label> "Alice"@en .
<http://example.org/alice> <http://example.org/label> "Alicia"@es .
<http://example.org/bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> .
<http://example.org/bob> <http://example.org/age> "25"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/bob> <http://example.org/name> "Bob" .
<http://example.org/bob> <http://example.org/label> "Bob"@en .
<http://example.org/corp> <http://example.org/revenue> "1000000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://example.org/corp> <http://example.org/name> "ACME" .
"#;

// ---------------------------------------------------------------------------
// Helpers for datatype/language partition tests
// ---------------------------------------------------------------------------

/// Find a datatype partition node for the given datatype IRI within a parent property partition.
fn find_datatype_partition(
    triples: &[(String, String, String)],
    smap: &HashMap<String, Vec<(String, String)>>,
    parent_node: &str,
    datatype_iri: &str,
) -> Option<String> {
    triples
        .iter()
        .filter(|(s, p, _)| {
            s == parent_node && p == "http://ldf.fi/void-ext#datatypePartition"
        })
        .map(|(_, _, o)| o.clone())
        .find(|dt_part| {
            smap.get(dt_part).is_some_and(|pairs| {
                pairs.iter().any(|(p, o)| {
                    p == "http://ldf.fi/void-ext#datatype"
                        && o == &format!("<{datatype_iri}>")
                })
            })
        })
}

/// Find a language partition node for the given language tag within a parent datatype partition.
fn find_language_partition(
    triples: &[(String, String, String)],
    smap: &HashMap<String, Vec<(String, String)>>,
    parent_node: &str,
    lang_tag: &str,
) -> Option<String> {
    triples
        .iter()
        .filter(|(s, p, _)| {
            s == parent_node && p == "http://ldf.fi/void-ext#languagePartition"
        })
        .map(|(_, _, o)| o.clone())
        .find(|lang_part| {
            smap.get(lang_part).is_some_and(|pairs| {
                pairs.iter().any(|(p, o)| {
                    p == "http://ldf.fi/void-ext#language"
                        && o == &format!("\"{lang_tag}\"")
                })
            })
        })
}

// ---------------------------------------------------------------------------
// Test: dataset-level datatype partitions
// ---------------------------------------------------------------------------

#[test]
fn test_void_dataset_no_datatype_partitions() {
    // Datatype partitions should NOT appear in dataset-level property partitions.
    // They should only appear in class-level property partitions.
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), DATATYPE_NT, "void_dt_dataset");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);
    let ds = "<http://example.org/ds>";

    // Dataset-level property partition for "age" should exist with triple count.
    let age_pp = find_property_partition(
        &triples,
        &smap,
        ds,
        "<http://example.org/age>",
    )
    .expect("Missing dataset-level property partition for ex:age");
    assert_eq!(
        get_void_triples_count(&smap, &age_pp),
        Some(2),
        "Expected 2 triples for age property partition"
    );

    // But NO datatype partitions should exist under it.
    let dt_count: usize = triples
        .iter()
        .filter(|(s, p, _)| {
            s == &age_pp && p == "http://ldf.fi/void-ext#datatypePartition"
        })
        .count();
    assert_eq!(
        dt_count, 0,
        "Dataset-level property partition should have no datatype partitions"
    );
}

// ---------------------------------------------------------------------------
// Test: dataset-level language partitions (nested inside rdf:langString)
// ---------------------------------------------------------------------------

#[test]
fn test_void_dataset_no_language_partitions() {
    // Language partitions (nested under rdf:langString datatype partitions) should NOT
    // appear in dataset-level property partitions.
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), DATATYPE_NT, "void_lang_dataset");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);
    let ds = "<http://example.org/ds>";

    let label_pp = find_property_partition(
        &triples,
        &smap,
        ds,
        "<http://example.org/label>",
    )
    .expect("Missing dataset-level property partition for ex:label");
    assert_eq!(
        get_void_triples_count(&smap, &label_pp),
        Some(3),
        "Expected 3 triples for label property partition"
    );

    // No datatype partitions at dataset level.
    let dt_count: usize = triples
        .iter()
        .filter(|(s, p, _)| {
            s == &label_pp && p == "http://ldf.fi/void-ext#datatypePartition"
        })
        .count();
    assert_eq!(
        dt_count, 0,
        "Dataset-level label property partition should have no datatype partitions"
    );
}

// ---------------------------------------------------------------------------
// Test: class-level datatype partitions
// ---------------------------------------------------------------------------

#[test]
fn test_void_class_datatype_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), DATATYPE_NT, "void_dt_class");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);
    let ds = "<http://example.org/ds>";

    // Find the Person class partition.
    let person_cp = find_class_partition(
        &triples,
        &smap,
        ds,
        "<http://example.org/Person>",
    )
    .expect("Missing Person class partition");

    // Find property partition for "age" within Person class partition.
    let age_pp = find_property_partition(
        &triples,
        &smap,
        &person_cp,
        "<http://example.org/age>",
    )
    .expect("Missing age property partition under Person");

    // alice and bob both have integer ages → xsd:integer count = 2.
    let int_dt = find_datatype_partition(
        &triples,
        &smap,
        &age_pp,
        "http://www.w3.org/2001/XMLSchema#integer",
    )
    .expect("Missing xsd:integer datatype partition under Person/age");
    assert_eq!(
        get_void_triples_count(&smap, &int_dt),
        Some(2),
        "Expected 2 triples in Person/age xsd:integer"
    );

    // Find property partition for "label" within Person class partition.
    let label_pp = find_property_partition(
        &triples,
        &smap,
        &person_cp,
        "<http://example.org/label>",
    )
    .expect("Missing label property partition under Person");

    // Person/label has 3 language-tagged triples.
    let lang_dt = find_datatype_partition(
        &triples,
        &smap,
        &label_pp,
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
    )
    .expect("Missing rdf:langString datatype partition under Person/label");
    assert_eq!(
        get_void_triples_count(&smap, &lang_dt),
        Some(3),
        "Expected 3 triples in Person/label rdf:langString"
    );

    // Language partitions within Person/label/langString.
    let en_part = find_language_partition(&triples, &smap, &lang_dt, "en")
        .expect("Missing @en language partition under Person/label/langString");
    assert_eq!(
        get_void_triples_count(&smap, &en_part),
        Some(2),
        "Expected 2 triples in Person/label @en"
    );

    let es_part = find_language_partition(&triples, &smap, &lang_dt, "es")
        .expect("Missing @es language partition under Person/label/langString");
    assert_eq!(
        get_void_triples_count(&smap, &es_part),
        Some(1),
        "Expected 1 triple in Person/label @es"
    );
}

// ---------------------------------------------------------------------------
// Test: non-literal objects do NOT appear in datatype partitions
// ---------------------------------------------------------------------------

#[test]
fn test_void_no_datatype_for_non_literals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), DATATYPE_NT, "void_dt_nolit");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);
    let ds = "<http://example.org/ds>";

    // rdf:type property partition has only URI objects — no datatype partitions.
    let type_pp = find_property_partition(
        &triples,
        &smap,
        ds,
        &format!("<{}>", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
    )
    .expect("Missing rdf:type property partition");

    let dt_parts: Vec<&(String, String, String)> = triples
        .iter()
        .filter(|(s, p, _)| {
            s == &type_pp && p == "http://ldf.fi/void-ext#datatypePartition"
        })
        .collect();
    assert!(
        dt_parts.is_empty(),
        "rdf:type property partition should have no datatype partitions (objects are URIs), got {}",
        dt_parts.len()
    );
}

// ---------------------------------------------------------------------------
// Test: datatype partitions with blank-node mode
// ---------------------------------------------------------------------------

#[test]
fn test_void_datatype_blank_node_mode() {
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), DATATYPE_NT, "void_dt_bnode");
    let (ok, stdout, stderr) = run_void(
        &hdt_path,
        &["--dataset-uri", "http://example.org/ds", "--use-blank-nodes"],
    );
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);

    // Datatype partition nodes should be blank nodes (_:bN).
    let dt_part_triples: Vec<&(String, String, String)> = triples
        .iter()
        .filter(|(_, p, _)| p == "http://ldf.fi/void-ext#datatypePartition")
        .collect();
    assert!(
        !dt_part_triples.is_empty(),
        "Expected at least one datatypePartition triple"
    );
    for (_, _, o) in &dt_part_triples {
        assert!(
            o.starts_with("_:b"),
            "Datatype partition node should be blank node in blank-node mode, got: {o}"
        );
    }

    // Language partition nodes should also be blank nodes.
    let lang_part_triples: Vec<&(String, String, String)> = triples
        .iter()
        .filter(|(_, p, _)| p == "http://ldf.fi/void-ext#languagePartition")
        .collect();
    assert!(
        !lang_part_triples.is_empty(),
        "Expected at least one languagePartition triple"
    );
    for (_, _, o) in &lang_part_triples {
        assert!(
            o.starts_with("_:b"),
            "Language partition node should be blank node in blank-node mode, got: {o}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: datatype partition counts sum correctly
// ---------------------------------------------------------------------------

#[test]
fn test_void_class_datatype_counts_sum() {
    // Datatype partition counts at class level should sum to the property partition total.
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), DATATYPE_NT, "void_dt_sum");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);
    let ds = "<http://example.org/ds>";

    // Find Person class partition → name property partition.
    let person_cp = find_class_partition(
        &triples,
        &smap,
        ds,
        "<http://example.org/Person>",
    )
    .unwrap();
    let name_pp = find_property_partition(
        &triples,
        &smap,
        &person_cp,
        "<http://example.org/name>",
    )
    .unwrap();
    let total = get_void_triples_count(&smap, &name_pp).unwrap();

    // Sum all datatype partition counts under the class-level property partition.
    let dt_sum: u64 = triples
        .iter()
        .filter(|(s, p, _)| {
            s == &name_pp && p == "http://ldf.fi/void-ext#datatypePartition"
        })
        .map(|(_, _, o)| get_void_triples_count(&smap, o).unwrap_or(0))
        .sum();

    assert_eq!(
        dt_sum, total,
        "Sum of datatype partition counts ({dt_sum}) should equal property partition total ({total})"
    );

    // For "label" under Person, langString count = sum of language partition counts.
    let label_pp = find_property_partition(
        &triples,
        &smap,
        &person_cp,
        "<http://example.org/label>",
    )
    .unwrap();
    let lang_dt = find_datatype_partition(
        &triples,
        &smap,
        &label_pp,
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
    )
    .unwrap();
    let lang_total = get_void_triples_count(&smap, &lang_dt).unwrap();

    let lang_sum: u64 = triples
        .iter()
        .filter(|(s, p, _)| {
            s == &lang_dt && p == "http://ldf.fi/void-ext#languagePartition"
        })
        .map(|(_, _, o)| get_void_triples_count(&smap, o).unwrap_or(0))
        .sum();

    assert_eq!(
        lang_sum, lang_total,
        "Sum of language partition counts ({lang_sum}) should equal langString total ({lang_total})"
    );
}

// ---------------------------------------------------------------------------
// Test: revenue property has xsd:decimal datatype partition
// ---------------------------------------------------------------------------

#[test]
fn test_void_dataset_no_decimal_datatype_partition() {
    // Dataset-level revenue property partition should have no datatype partitions.
    // (corp has no rdf:type, so revenue won't appear in any class partition either.)
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), DATATYPE_NT, "void_dt_decimal");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);
    let ds = "<http://example.org/ds>";

    let revenue_pp = find_property_partition(
        &triples,
        &smap,
        ds,
        "<http://example.org/revenue>",
    )
    .expect("Missing dataset-level property partition for ex:revenue");
    assert_eq!(
        get_void_triples_count(&smap, &revenue_pp),
        Some(1),
        "Expected 1 triple for revenue property partition"
    );
    let dt_count: usize = triples
        .iter()
        .filter(|(s, p, _)| {
            s == &revenue_pp && p == "http://ldf.fi/void-ext#datatypePartition"
        })
        .count();
    assert_eq!(dt_count, 0, "Dataset-level revenue should have no datatype partitions");
}

// ---------------------------------------------------------------------------
// Test: existing void output unchanged (VOID_NT fixture still works)
// ---------------------------------------------------------------------------

#[test]
fn test_void_existing_fixture_still_works() {
    // Ensure the original VOID_NT fixture still produces correct results.
    // Dataset-level property partitions should NOT have datatype partitions.
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), VOID_NT, "void_existing");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);
    let ds = "<http://example.org/ds>";

    // Dataset-level stats should still be correct.
    assert_eq!(
        get_void_int(&smap, ds, "http://rdfs.org/ns/void#triples"),
        Some(11),
        "Expected 11 triples"
    );

    // Dataset-level name property partition should exist but with no datatype partitions.
    let name_pp = find_property_partition(
        &triples,
        &smap,
        ds,
        "<http://example.org/name>",
    )
    .expect("Missing name property partition");
    let dt_count: usize = triples
        .iter()
        .filter(|(s, p, _)| {
            s == &name_pp && p == "http://ldf.fi/void-ext#datatypePartition"
        })
        .count();
    assert_eq!(
        dt_count, 0,
        "Dataset-level name property partition should have no datatype partitions"
    );
}

// ---------------------------------------------------------------------------
// Test: single property with mixed datatype objects
// ---------------------------------------------------------------------------

/// A single property ("value") has objects of multiple types: xsd:integer, xsd:string
/// (plain literal), rdf:langString (@en, @de), and xsd:date. This exercises the
/// grouping logic in write_datatype_partitions where one property partition must produce
/// multiple datatype sub-partitions plus a langString partition with nested languages.
///
/// alice: type Thing, value 42^^xsd:integer, value "hello", value "hola"@es, value "hi"@en, value "2025-01-01"^^xsd:date
/// bob: type Thing, value 7^^xsd:integer, value "world", value "Welt"@de
///
/// Total "value" triples: 8
///   xsd:integer: 2 (42, 7)
///   xsd:string:  2 (hello, world)
///   xsd:date:    1 (2025-01-01)
///   rdf:langString: 3 (hola@es, hi@en, Welt@de)
///     @en: 1, @es: 1, @de: 1
const MIXED_DATATYPE_NT: &str = r#"<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Thing> .
<http://example.org/alice> <http://example.org/value> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/alice> <http://example.org/value> "hello" .
<http://example.org/alice> <http://example.org/value> "hola"@es .
<http://example.org/alice> <http://example.org/value> "hi"@en .
<http://example.org/alice> <http://example.org/value> "2025-01-01"^^<http://www.w3.org/2001/XMLSchema#date> .
<http://example.org/bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Thing> .
<http://example.org/bob> <http://example.org/value> "7"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/bob> <http://example.org/value> "world" .
<http://example.org/bob> <http://example.org/value> "Welt"@de .
"#;

#[test]
fn test_void_mixed_datatypes_single_property() {
    // Mixed datatypes: datatype partitions should only appear at class level.
    let temp_dir = tempfile::tempdir().unwrap();
    let hdt_path = make_hdt(temp_dir.path(), MIXED_DATATYPE_NT, "void_mixed_dt");
    let (ok, stdout, stderr) =
        run_void(&hdt_path, &["--dataset-uri", "http://example.org/ds"]);
    assert!(ok, "hdtc void failed: {stderr}");

    let triples = parse_ntriples(&stdout);
    let smap = subject_map(&triples);
    let ds = "<http://example.org/ds>";

    // Dataset-level "value" property partition should have 8 triples but no datatype partitions.
    let value_pp = find_property_partition(
        &triples,
        &smap,
        ds,
        "<http://example.org/value>",
    )
    .expect("Missing dataset-level property partition for ex:value");
    assert_eq!(
        get_void_triples_count(&smap, &value_pp),
        Some(8),
        "Expected 8 triples for value property partition"
    );
    let dt_count: usize = triples
        .iter()
        .filter(|(s, p, _)| {
            s == &value_pp && p == "http://ldf.fi/void-ext#datatypePartition"
        })
        .count();
    assert_eq!(dt_count, 0, "Dataset-level value should have no datatype partitions");

    // Class-level (Thing) should have datatype partitions.
    let thing_cp = find_class_partition(&triples, &smap, ds, "<http://example.org/Thing>")
        .expect("Missing Thing class partition");
    let class_value_pp = find_property_partition(
        &triples,
        &smap,
        &thing_cp,
        "<http://example.org/value>",
    )
    .expect("Missing value property partition under Thing");

    // xsd:integer: 2 triples.
    let class_int_dt = find_datatype_partition(
        &triples,
        &smap,
        &class_value_pp,
        "http://www.w3.org/2001/XMLSchema#integer",
    )
    .expect("Missing xsd:integer under Thing/value");
    assert_eq!(get_void_triples_count(&smap, &class_int_dt), Some(2));

    // xsd:string: 2 triples (plain literals).
    let class_str_dt = find_datatype_partition(
        &triples,
        &smap,
        &class_value_pp,
        "http://www.w3.org/2001/XMLSchema#string",
    )
    .expect("Missing xsd:string under Thing/value");
    assert_eq!(get_void_triples_count(&smap, &class_str_dt), Some(2));

    // xsd:date: 1 triple.
    let class_date_dt = find_datatype_partition(
        &triples,
        &smap,
        &class_value_pp,
        "http://www.w3.org/2001/XMLSchema#date",
    )
    .expect("Missing xsd:date under Thing/value");
    assert_eq!(get_void_triples_count(&smap, &class_date_dt), Some(1));

    // rdf:langString: 3 triples with nested language partitions.
    let class_lang_dt = find_datatype_partition(
        &triples,
        &smap,
        &class_value_pp,
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
    )
    .expect("Missing rdf:langString under Thing/value");
    assert_eq!(get_void_triples_count(&smap, &class_lang_dt), Some(3));

    let en_part = find_language_partition(&triples, &smap, &class_lang_dt, "en")
        .expect("Missing @en language partition under Thing/value");
    assert_eq!(get_void_triples_count(&smap, &en_part), Some(1));

    let es_part = find_language_partition(&triples, &smap, &class_lang_dt, "es")
        .expect("Missing @es language partition under Thing/value");
    assert_eq!(get_void_triples_count(&smap, &es_part), Some(1));

    let de_part = find_language_partition(&triples, &smap, &class_lang_dt, "de")
        .expect("Missing @de language partition under Thing/value");
    assert_eq!(get_void_triples_count(&smap, &de_part), Some(1));

    // Sum of all class-level datatype partition counts should equal property partition total.
    let dt_sum: u64 = triples
        .iter()
        .filter(|(s, p, _)| {
            s == &class_value_pp && p == "http://ldf.fi/void-ext#datatypePartition"
        })
        .map(|(_, _, o)| get_void_triples_count(&smap, o).unwrap_or(0))
        .sum();
    assert_eq!(
        dt_sum, 8,
        "Sum of class-level datatype partition counts ({dt_sum}) should equal 8"
    );
}
