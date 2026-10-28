# European Urban Geography & Accessibility Platform

A local-first batch data engineering project for reproducible spatial analytics.
The first slice measures mapped-building proximity to mapped parks in a small
Coimbra study rectangle. It is a foundation for a Spark optimization experiment
and an eventual S3 / Iceberg / Glue / Athena lakehouse.

**Metric:** distance from each building's point-on-surface to a park polygon,
in metres using Portugal TM06 (EPSG:3763), with an inclusive 500 m threshold.
This is straight-line proximity, not walking access, public-access verification,
residential building identification, or population coverage.

## Run locally

Requirements: Docker Desktop with Linux containers and approximately 4 GB available
RAM. The image installs Python 3.11 and Java 17; host Python/Java are not used.
The image's first build downloads software dependencies, including Spark.
No AWS credentials or account are required. Commands work from the repository root
in PowerShell or a POSIX shell.

```sh
docker compose build
docker compose run --rm pipeline pytest -q
docker compose run --rm pipeline urban-geo run
```

`run` downloads a bounded real Overture extract for pinned release 2026-09-23.1
and processes it locally. The data-transfer budget is 64 MiB including source
metadata, with a 16 MiB extracted-output limit. Source selection and park halo
are in `configs/coimbra_pilot.json`; increasing limits requires review/approval.

Inspect `artifacts/coimbra-pilot/summary.json`, `building_proximity/`,
`grid_proximity/`, `plan.txt`, and `spark-events/`. Raw Parquet and provenance
live under `data/raw/<release>/<area>/<config-hash>/`. Data and artifacts are
ignored by Git. Public upstream releases can be removed; retain the small raw
extract for exact offline replay.

To reprocess retained data without network access, choose a new output directory:

```sh
docker compose run --rm pipeline urban-geo process --output artifacts/coimbra-replay
```

`urban-geo extract` reuses a completed raw extract only after validating its
configuration and file hashes. Processing refuses an existing output directory;
it never appends duplicates or overwrites an old run. Failures leave an unpublished
`.staging-*` / `.run-*` directory for diagnosis. Consumers must use a completed
`manifest.json` or `summary.json`, not discover arbitrary staging files.

An optional municipality filter intersects the pilot rectangle with a pinned
DGT boundary. Acquire that small boundary explicitly, then process retained inputs:

```sh
docker compose run --rm pipeline python scripts/acquire_boundary.py
docker compose run --rm pipeline urban-geo process --boundary-config configs/coimbra_boundary.json --output artifacts/coimbra-boundary-pilot
```

Boundary acquisition is capped at 4 MiB. Processing loads and validates local
boundary files without downloading them. Selection includes polygon edges,
excludes hole interiors, and preserves full building and park geometry. Park
candidates retain the existing distance halo. Results describe the configured
pilot rectangle intersected with the municipality. Facts and grid totals record
`selection_version`, `boundary_id`, `boundary_release` and
`boundary_geometry_sha256`; the summary also records source-file hashes and
boundary attribution. Runs without the option retain rectangle selection and
write null boundary fields.

## Local Iceberg

The image pins Iceberg 1.10.2 for Spark 3.5 / Scala 2.12. Rebuild the image after
updating the runtime; all JARs are downloaded and checksum-checked at build time.
The table format is explicitly v2, independently of the runtime library version.

Publish a completed processing run as the first local building-fact snapshot:

```sh
docker compose run --rm pipeline python scripts/publish_iceberg.py --run artifacts/coimbra-boundary-pilot
```

This creates `local.analytics.building_proximity` in a filesystem Hadoop catalog
under ignored `data/lakehouse/`, partitioned by source release and area. It stores
the fact rows in Parquet and uses Iceberg metadata to track the committed snapshot.
The command verifies the input file inventory and hashes, validates facts, compares
every stored row with the input, and prints the snapshot ID. The source summary,
including boundary attribution, is retained in table metadata; its hash is also
recorded in the snapshot. Spark event logs go to ignored `artifacts/iceberg-events/`.

The first-snapshot publisher accepts one completed run per table. Repeating the input validates
the existing rows and returns `reused` without creating a snapshot. A different run
is refused; use a deliberately chosen new `--table-name` for an isolated import.
Release history uses the separate command below. Multi-writer workflows require
separate validation. Grid outputs remain in the reference processing
run's Parquet files.
The local filesystem catalog provides a single-writer demonstration; AWS catalog
and query-engine compatibility require separate validation.

### Release history

Publish comparable releases to a separate history table:

```sh
docker compose run --rm pipeline python scripts/publish_release.py --run artifacts/coimbra-boundary-pilot
```

The default table is `local.analytics.building_proximity_history`. Each history
table fixes the study area, boundary and metric definition; source releases occupy
separate release/area partitions. A new release is published as another partition.
A different run for an existing release requires `--replace` together with
`--expected-snapshot-id` set to the table's current snapshot ID. Replacement uses
an explicit release/area filter and preserves other release partitions. Older
snapshots remain readable. Replaying the same input reuses the current partition,
including a retry after a successful replacement with its original expected ID.

History rows include `source_run_sha256`; each write commits the full source summary
and its hash in the same snapshot as the data. This preserves per-release source
and attribution evidence without a separately updated ledger. The original
one-run table and reference Parquet outputs remain available. Use a new table name
when changing the analysis definition. The acceptance tests use tiny synthetic
releases; publication does not fetch new source data.

### History schema evolution

Explicitly add nullable `processed_at_utc` to an existing history table:

```sh
docker compose run --rm pipeline python scripts/evolve_history_schema.py --expected-schema-id 0
```

Use the table's current `schema_id` from the publication receipt (0 for a new base
history table). This is distinct from its data snapshot ID. The migration commits
new schema metadata, preserves existing field IDs, and leaves Parquet/manifest
files and the current data snapshot unchanged. Repeating the completed migration
returns `reused`, including with its original expected schema ID.

Existing rows read as NULL for this field. New or corrected publications use the
processing run's `completed_at_utc`, normalized to UTC and truncated to milliseconds.
This records processing completion, independently of source-release and publication
time. Replay preserves legacy NULLs using the source write's schema; timestamps
from newer writes must still match their source metadata exactly.

The publisher supports both the base and extended schemas. New tables start with
the base schema until explicitly evolved. Unknown columns, incompatible types and
missing timezone information for a new timestamp-bearing write are rejected.
There is no automatic schema merge or implicit backfill. Historical SQL snapshot
reads retain the schema associated with their snapshot.

### Compare retained real releases

`configs/coimbra_pilot_october.json` pins Overture 2026-10-21.0 with the same pilot
rectangle, metric and limits as the retained 2026-09-23.1 baseline. Extract once,
then process, compare and publish from retained inputs:

```sh
docker compose run --rm pipeline urban-geo extract --config configs/coimbra_pilot_october.json
docker compose run --rm pipeline urban-geo process --config configs/coimbra_pilot_october.json --boundary-config configs/coimbra_boundary.json --output artifacts/coimbra-october-pilot
docker compose run --rm pipeline python scripts/compare_releases.py --first-run artifacts/coimbra-boundary-pilot --second-run artifacts/coimbra-october-pilot --output artifacts/coimbra-september-october-comparison.json
docker compose run --rm pipeline python scripts/publish_release.py --run artifacts/coimbra-october-pilot
```

Choose new output/report paths for another processing attempt. The comparison is
bounded local QA using retained Parquet; it does not start Spark or fetch data.
It verifies input hashes, analysis comparability, fact metadata and IDs. The JSON
report separates release totals and ID membership from changes among shared IDs:
500 m match status, nearest park ID, distance and grid assignment. The default
numerical distance tolerance is 0.0001 m; unmatched distances remain unknown.
Reports preserve both input-run hashes and refuse an existing output path.

ID appearance/disappearance and proximity differences describe mapped records.
They do not establish construction, demolition or walking access. Iceberg stores
each release as a separate observation partition; filter/group by source release
when querying so observations from different releases are not double-counted.

### Query release and grid summaries with SQL

```sh
docker compose run --rm pipeline python scripts/report_grids.py --output artifacts/gold-grid-report.json
```

This local acceptance runner starts Spark, pins the current Iceberg history
snapshot once, and evaluates the SQL models in `src/urban_geography/sql/`. Supply
`--snapshot-id` to read a retained older snapshot. The report records the snapshot,
analysis definition, source-run hashes and SQL hashes; an existing output path is
refused. It creates only temporary session views and a JSON report, with no table
writes or new Iceberg snapshots.

Grid rows describe occupied cells of the selected pilot building cohort. Empty
cells are absent, and edge cells describe only selected buildings, not full-cell
coverage. Each source release is aggregated separately. Release shares divide
summed matched counts by summed building counts, rather than averaging cell
percentages. Unmatched buildings remain in the denominator.

The runner rejects invalid reporting keys/flags, duplicate release/area/building
keys, mixed processing runs in a partition and inconsistent analysis metadata.
It is limited to 40,000 input facts. These are local Spark SQL acceptance models;
The materializer below saves grid summaries; dbt and Athena validation remain future work.

### Release and study-area dimension contracts

`src/urban_geography/dimensions.py` defines typed release/study-area records from
validated completed runs. Source release IDs are natural keys; a study-area
SHA256 versions the pilot rectangle, CRS, selection rule and pinned boundary.
Source release, proximity threshold and processing/retrieval details do not change
that area key. The contract retains exact source-run bindings and rejects
competing runs for one release/area or incomparable analyses.

`dimension_frames` creates temporary Spark relations, and `join_run_dimensions`
checks unique keys, complete references, matching attributes and unchanged fact
counts before returning enriched reference facts. These are local contracts and
join acceptance checks used by the publisher below. The release label date is
parsed from the pinned identifier; acquisition and processing times remain
separate run provenance.

### Publish a matched dimension pair

```sh
docker compose run --rm pipeline python scripts/publish_dimensions.py --run artifacts/coimbra-boundary-pilot --run artifacts/coimbra-october-pilot
```

The publisher pins the current fact-history snapshot once, or accepts an explicit
`--fact-snapshot-id`. Supplied completed runs must reproduce all base history
facts in that snapshot. It preflights both dimension registries before writing:
`local.analytics.dim_releases` and `local.analytics.dim_study_areas`. New keys
append; conflicting descriptions of existing keys fail. The fact tables stay
unchanged. `--table-prefix` selects another registry pair.

After verified table readback and joins, it publishes an immutable completion
manifest under `data/dimension-publications/<publication-sha256>/manifest.json`.
The receipt prints that directory. `read_dimension_publication` in
`src/urban_geography/dimension_publication.py` opens the matched fact/dimension
snapshots recorded there and returns enriched base facts plus both dimensions.
Use that completed publication for a consistent pair. Registry commits happen
separately, and direct queries of their latest states can include keys from an
unfinished attempt. A retry finishes that attempt without duplicating keys;
exact published retries create no files or snapshots. Older publications remain
readable while their referenced snapshots/files are retained.

This protocol uses a single writer and local directory rename for completion
visibility. It does not establish a transaction across tables, multi-writer
safety, power-loss durability or an S3/cloud publication protocol.

### Save Gold grid summaries in Iceberg

```sh
docker compose run --rm pipeline python scripts/publish_gold.py --dimension-publication data/dimension-publications/<publication-sha256>
```

Use the completed dimension-publication directory printed by the previous step.
This evaluates the same grid SQL and saves it to
`local.analytics.gold_grid_proximity`. Each row describes one occupied grid cell
in one source release and area, within one immutable Gold calculation run.
Release totals are calculated by the existing release SQL over these saved counts.

The receipt includes `gold_run_sha256` and the Gold table snapshot ID. Always
select one Gold run: later source/SQL versions append another calculation, which
can contain summaries for the same source release. The reader
`read_gold_run` in `src/urban_geography/gold_storage.py` filters the chosen run,
checks its content hash and returns grid rows, release totals and the full
analysis definition. It reads the saved Gold table without scanning source
building or dimension tables. The recorded SQL version must match the installed
models; retain the matching code version for historical reads.

Rows and source/SQL lineage commit together in one Iceberg snapshot. New runs
append; exact retries verify and reuse stored results without new table files or
snapshots. Counts preserve unmatched buildings in the denominator. Table-wide
storage is limited to 40,000 grid rows, with the existing source limits unchanged.
This remains a local single-writer v2 table; snapshot/lineage retention and broader
crash/orphan maintenance are separate work.

### Demonstrate the complete retained-data path

```sh
docker compose run --rm pipeline python scripts/run_local_demo.py --boundary-config configs/coimbra_boundary.json --output artifacts/local-demo-01
```

This offline demonstration requires both retained raw releases and the pinned
boundary files. It never downloads inputs. The default source configurations are
the September and October Coimbra pilots; pass two `--config` options to select
another already retained comparable pair. The example explicitly includes CAOP
selection; omitting `--boundary-config` uses the supported bbox-only selection.

Choose a fresh output directory, disjoint from retained input roots and the live
`data/lakehouse` warehouse. The runner processes each release twice, checks all
results with the independent Shapely/PROJ verifier and compares repeated fact/grid
rows. It then builds a separate warehouse containing fact history, the explicit
nullable schema extension, persisted dimensions and saved Gold summaries.

The demonstration verifies older-snapshot readback, exact publication retries,
unchanged table/completion files on retry and fresh-session reads. It writes
`acceptance.json` only after every check passes. Existing output directories are
refused; failed attempts remain available for inspection. Use a new directory for
another demonstration. Spark logs and intermediate outputs are kept inside that
attempt. The final JSON is validation evidence; analytical rows still flow from
Parquet to Iceberg through the existing publication APIs.

### Measure the retained pilot proximity baseline

```sh
docker compose run --rm pipeline python scripts/benchmark_proximity.py --boundary-config configs/coimbra_boundary.json --output artifacts/proximity-baseline-01
```

This offline command requires the retained October inputs and matching completed
`artifacts/coimbra-october-pilot` reference. Use `--config`, `--reference-run` and
`--raw-root` to select another already retained pilot. Boundary selection must
match the reference. Choose a fresh output directory; inputs and the live warehouse
are preserved. Existing feature/byte limits and a two-million logical-pair cap apply.

It materializes projected input caches, then runs one warmup and five measured
trials of the unchanged proximity operator. Each trial collects every result field
and checks keys, park IDs, match flags, grids and metre distances against the
reference. Startup and input preparation are reported separately. Results are
recomputed; JVM/OS caches are not reset. The report retains input hashes, runtime
settings, per-trial executed plans, Spark event logs and selected task metrics.
Unavailable metrics remain null; failed or timed-out attempts retain evidence and
produce no successful timing summary. The default 60-second trial timeout uses
cooperative Spark cancellation; it is not a hard process deadline.

To compare the current operator with a conservative envelope prefilter:

```sh
docker compose run --rm pipeline python scripts/benchmark_proximity.py --boundary-config configs/coimbra_boundary.json --compare-envelope --output artifacts/proximity-envelope-01
```

Both variants use the same projected input caches. Each has its own warmup,
followed by five rounds containing one measured trial per variant; the order in
each round is shuffled with a recorded seed (`--order-seed`, default 1729).
The prefilter keeps points inside each park's metre-expanded bounding rectangle,
then applies the same exact full-polygon distance and nearest-park rules. It is
opt-in for measurement; normal processing keeps the existing operator.

The report adds `variant_collect_seconds` and a separate untimed count of logical
envelope candidate rows. This count does not measure actual distance-function
calls. The legacy `measured_collect_seconds` field still describes the current
operator. Any incomplete trial suppresses both variants' timing summaries.

These are single-machine pilot measurements including driver collection. They do
not establish a speedup or distributed scaling. No source download or Iceberg
publication occurs.

### Describe the retained workload

```sh
docker compose run --rm pipeline python scripts/profile_workload.py --boundary-config configs/coimbra_boundary.json --output artifacts/workload-profile-01
```

This offline command reuses the benchmark's retained-input/reference checks and
requires a fresh output directory. It describes the same selected cohort: full
building/park geometry complexity, projected WKB sizes, envelope candidates per
building and park, occupied grid counts and cached input partition row counts.
Zero-candidate features and empty input partitions remain in their distributions.

`profile.json` is written after selection and input-hash checks. Geometry WKB
bytes describe serialized geometry, not Spark heap usage. Logical candidate counts
are separate from exact matches and actual distance calls. Occupied grids and
input row partitions do not establish runtime task/shuffle skew. This command
describes data; its elapsed time is not a comparative performance measurement.
It neither downloads sources nor writes lakehouse tables.

`configs/coimbra_performance_trial.json` selects a separate wider Coimbra window
with the same pinned release and acquisition limits. Use its `--config` argument
for extraction/processing, then supply the matching completed `--reference-run`
to profiling/benchmark commands. Give each study area its own fresh output paths.

## Development checks

```sh
docker compose run --rm pipeline ruff check .
docker compose run --rm pipeline ruff format --check .
docker compose run --rm pipeline mypy
docker compose run --rm pipeline pytest -q
```

Tests include offline Spark/Sedona/Iceberg integration and an independent Shapely/PROJ
oracle. CI defines the same checks and never fetches real source data or deploys
resources. Dependency changes require `docker compose run --rm pipeline uv lock`
and a rebuild. The Python lock and JAR SHA256 digests are pinned; OS package
updates during image build mean rebuilds are not fully bit-for-bit identical.
The current image targets Linux amd64.

## Repository guide

- `src/urban_geography/`: bounded acquisition, run contracts, spatial SQL and CLI.
- `configs/`: versioned study areas and download limits.
- `tests/`: geometry semantics, transfer limits, pruning, provenance and publication policy.
- `scripts/verify_pilot.py`: independent Shapely/PROJ verification and replay comparison.
- `scripts/compare_releases.py`: bounded QA of comparable retained source releases.
- `scripts/report_grids.py`: snapshot-pinned local SQL grid/release reporting.
- `scripts/publish_dimensions.py`: matched persisted dimensions with pinned fact lineage.
- `scripts/publish_gold.py`: immutable saved grid summaries and source/SQL lineage.
- `scripts/run_local_demo.py`: isolated offline processing-to-Gold demonstration.
- `scripts/benchmark_proximity.py`: retained-pilot baseline trials, equivalence and task metrics.
- `scripts/profile_workload.py`: bounded geometry, candidate and input-partition descriptions.
- `scripts/check_publication.py`: checks tracked paths and Git history; `--secrets` also
  runs the pinned Gitleaks image against committed history without network access.

The processing pipeline writes reference Parquet outputs, with an optional local
Iceberg v2 fact publication step. dbt, orchestration and AWS deployment are future work.

## Data attribution

© OpenStreetMap contributors, Overture Maps Foundation. Overture buildings and base
land-use data are distributed under ODbL; retain upstream `sources` attribution
and review derived-database obligations before redistributing datasets.
See [Overture attribution and licensing](https://docs.overturemaps.org/attribution/).
Raw extracts and analytical output datasets are not included in this repository.

DGT CAOP boundaries use [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/).
Required attribution: “Informação geográfica cedida pela Direção-Geral do Território”.
The acquisition retains the official metadata and licence evidence alongside the
boundary. See [DGT CAOP](https://www.dgterritorio.gov.pt/atividades/cartografia/cartografia-tematica/caop).
