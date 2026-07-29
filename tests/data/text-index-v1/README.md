# Frozen hdtc text-index fixture

`legacy.hdt.text` was built from `tests/data/representative.nt` by hdtc commit
`793e0f7` with Tantivy 0.26.1 (index format 7), analyzer 1, and the original
version-1 manifest. `legacy.hdt` is the exact source HDT named by its manifest.

This fixture must not be regenerated during ordinary test maintenance. It is a
compatibility artifact: every supported Tantivy upgrade must still open it and
return the expected plain-token, prefix, and stemmed results. Add a new frozen
fixture when a new hdtc schema is published.

Tantivy's empty lock files are deliberately omitted. The integration test copies
the fixture into a temporary directory before opening it, where Tantivy may
create runtime lock files without modifying the source tree or contending with
another test process.
