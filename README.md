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

New processing runs use the exact distance inner join and record
`processing_implementation: exact_distance_join_v1` in `summary.json`.
The metric remains `park_proximity_v1`: full-polygon metre distances, the inclusive
threshold, nearest-park tie-break, unmatched buildings and grid assignment retain
the same meaning. The original query remains available as the benchmark baseline.
Older completed runs without the implementation label remain readable.

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
Metadata inspection resolves and refreshes the table through Iceberg's Spark
catalog API, preserving full provenance without Python opening a local metadata
file. Publication commands still use the local catalog; this does not enable
S3/Glue publishing.

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

Add `--compare-distance-join` to include an explicit exact-distance inner join
without the manual envelope screen. It can be combined with `--compare-envelope`
for three-way interleaved trials (one warmup and five measured actions each).
All variants share nearest-park ranking, unmatched-building preservation and grid
assignment. Inspect the saved plans to see which spatial operators the optimizer
actually selected; a different query shape does not guarantee a faster plan.

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

## AWS foundation (local validation)

The first AWS configuration is in `infra/aws/`: separate private S3 buckets for
lakehouse data and temporary query results, a Glue catalog database and an Athena
engine 3 workgroup that starts disabled. Both buckets request SSE-S3 encryption,
disable object ACLs and deny requests without TLS. The lake retains object
versions; only the separate query-results prefix has seven-day expiration.
Athena enforces a proposed 100 MiB per-query scan cutoff. That cutoff is not an
overall spending cap.

Terraform 1.13.5 and AWS provider 6.19.0 are pinned independently of the Python/Spark
runtime. With that Terraform version available, initialize and validate locally:

```sh
terraform -chdir=infra/aws init -backend=false -input=false
terraform -chdir=infra/aws fmt -check -recursive
terraform -chdir=infra/aws validate
terraform -chdir=infra/aws test
```

Initialization downloads the locked provider. All nine test runs explicitly use
a mock AWS provider and a plan command; they need no AWS credentials and create
no infrastructure. Mock tests verify configuration, not live AWS permissions or
Spark/Athena compatibility. The normal pipeline Docker image does not include
Terraform.

The pilot region is eu-north-1 (Stockholm). Actual deployment still needs a
reviewed target account, regional cost estimate, execution limits and teardown
procedure. Earlier Ireland releases must be rebuilt and frozen for the new target.
This increment defines no Glue processing job, table registration or dbt models.

### Prepare the proposed Glue runtime

Render a local startup recipe with synthetic target values:

```sh
docker compose run --rm pipeline python scripts/prepare_glue_runtime.py --deployment-name urban-geo-test --account-id 123456789012 --output artifacts/glue-runtime-example.json
```

The recipe pins Glue 5.1, Sedona 1.8.0 and custom Iceberg 1.10.2 with its matching
AWS bundle. It records catalog/S3 settings, versioned artifact checksums and
proposed JAR locations without contacting AWS or uploading anything. This is a
partial startup recipe; managed job resources and execution remain separate work.
Local artifact preparation and the writer entry point are described below.

`scripts/check_glue_runtime.py` checks the checksum-verified AWS bundle with the
three existing runtime JARs inside the supported container. Supply
`--aws-bundle /path/to/iceberg-aws-bundle-1.10.2.jar` and a fresh `--output`
directory. Run with Docker networking disabled. It compares projections with a
separate local control and checks temporary local Iceberg readback/history.
The configured Glue catalog is never queried; this is not managed Glue or
Athena acceptance. Existing local defaults and dependencies are unchanged.

### Package the Python project offline

```sh
docker compose run --rm pipeline python scripts/package_glue_project.py --output artifacts/glue-project-package-new
```

This builds the project's wheel with the pinned offline tools, checks that its
Python and SQL payload matches the source, and verifies a fresh offline install.
The check excludes checkout import paths and confirms SQL resources and the CLI
entry point load from the installed wheel. It installs no dependencies and does
not start Spark; the environment provides PySpark.

Use a fresh output directory. The completed output contains the wheel, hashes,
source inventory, manifest and build/install/import logs. It is the project
artifact; the separate dependency-bundle step below combines it with frozen
dependencies. Managed startup validation and job resources remain separate work.
No upload or AWS connection is performed.

### Freeze the Python dependency bundle

Download only the existing locked runtime wheels for Glue 5.1 / CPython 3.11 /
x86_64, then assemble them with a previously verified project package:

    docker compose run --rm pipeline python scripts/fetch_glue_dependencies.py --output artifacts/glue-python-wheel-cache-new

    docker compose run --rm pipeline python scripts/package_glue_bundle.py --wheel-cache artifacts/glue-python-wheel-cache-new --project-package artifacts/glue-project-package-new --output artifacts/glue-python-bundle-new

The first command explicitly downloads hash-locked binary wheels from PyPI;
it does not change versions or fetch source archives. The second works offline
from those retained inputs, creates a deterministic .gluewheels.zip and checks
its installation in a fresh environment. The acceptance run additionally disabled
Docker networking during assembly. PySpark and Py4J stay provided by the runtime.
Use fresh output directories; inspect the completed manifest and logs.

The checker verifies dependency versions and import origins, installed SQL
resources and native-library operations without starting Spark. This local Linux
check does not validate a managed Glue startup, upload artifacts or create AWS
resources. Job configuration and cloud execution remain separate work.


### Prepare a retained input folder

Prepare a small completed processing run for later delivery to the writer:

    docker compose run --rm pipeline python scripts/prepare_glue_input.py --run artifacts/completed-run --summary-sha256 SUMMARY_SHA256 --output artifacts/glue-input-new

Replace SUMMARY_SHA256 with the independently selected SHA256 of that run's
summary.json. The command copies only the pinned summary and its listed fact/grid
Parquet files into a fresh output folder, recording hashes, sizes and limits in
input-manifest.json. It checks the copied bytes again before publishing the folder.

The pilot source-payload ceiling is 16 MiB including the summary; a smaller
configured byte ceiling is respected. Preparation also bounds summary size,
Parquet file count and row counts, and rejects the extended workload profile.
It preserves the original Parquet bytes and starts no Spark session. This is local
input preparation; upload and managed Glue job execution remain separate work.


### Prepared-input writer entry point

scripts/run_glue_writer.py accepts a prepared local input directory, explicit
summary SHA256, deployment name and account ID. It derives the catalog/database
and expected settings from the startup recipe. The job must supply those settings
and the verified JAR/Python artifacts before this entry point runs.

The writer verifies the input manifest before obtaining its Spark session, checks
Spark 3.5.6 and the expected settings, and invokes immutable Iceberg publication
with exact readback/replay. It stops its session on success or failure and emits
a completion receipt on standard output only after the final input check.

For this bounded pilot, the driver reads the verified Parquet rows with PyArrow
and creates a Spark DataFrame with the explicit fact schema. Spark distributes
the rows; executors need no access to the driver's local input paths. This
driver materialization is limited to the prepared pilot contract, not a large
dataset ingestion path. Software/input uploads, job resources and managed Glue
validation remain separate work.


### Bounded S3 input retrieval

urban_geography.glue_s3_input.download_prepared_input accepts a caller-supplied
S3 client plus explicit summary/manifest SHA256 pins and the reviewed deployment
target. It reads only the manifest and exact listed keys under
inputs/SUMMARY_SHA256/, uses bounded GET ranges and ExpectedBucketOwner, verifies
the received sizes/hashes, and publishes a fresh prepared local folder.

The transfer records returned object version IDs and closes every response body.
The existing prepared-input reader validates the restored folder before it is
published. No bucket listing, upload, client construction or Spark startup occurs
in this function. The configured SDK factory and S3 writer entry point below
supply the client policy. HTTP/IAM behavior and managed execution remain to be
validated.


### S3 writer bootstrap and SDK policy

scripts/run_glue_s3_writer.py connects verified S3 retrieval to the writer.
It requires --summary-sha256, --manifest-sha256, --deployment-name and --account-id;
--scratch-root defaults to /tmp. A fresh temporary input folder is verified before
the writer obtains its Spark session. The S3 client closes before publication;
the owned input folder is removed after success or failure.

The optional managed-client factory requires the Glue 5.1 provided boto3 and
botocore 1.40.61. It fixes eu-north-1, TLS verification, SigV4, virtual addressing,
5-second connect and 10-second read timeouts, one connection slot and one total
request attempt. Configured endpoint URL overrides are ignored. These socket/
attempt settings do not replace an overall job timeout or spending limits.

The normal pipeline dependencies/image are unchanged. scripts/check_glue_sdk.py
checks the optional SDK using dummy credentials and botocore.Stubber; it needs
the pinned SDK available separately. Offline checks validate real SDK request
shapes and the local Iceberg flow. Live S3, IAM, uploads, job resources and
managed Glue startup remain unvalidated.


### Frozen Glue release and controlled delivery

scripts/prepare_glue_release.py assembles the current verified Python bundle,
managed job script, four pinned JARs and prepared pilot input into a fresh local
folder. It rechecks the archived project against current source and dependencies
against uv.lock. release.json records exact hashes, sizes, derived S3 locations
and proposed Glue arguments; its SHA256 is the explicit delivery pin.

scripts/upload_glue_release.py --release PATH --release-sha256 SHA256 prints the
verified plan by default and constructs no SDK client. Its explicit --execute
mode uploads only the listed runtime/input objects using create-only conditional
writes, expected bucket ownership, SHA256 checksums and SSE-S3 encryption. Each
object must pass bounded readback with a version ID. Existing matching bytes are
reused; mismatches fail. The input manifest and release completion record follow
their payloads. Partial uploads are retained for an explicit retry.

The uploader does not create buckets, IAM roles, Glue jobs or Iceberg tables.
scripts/check_glue_delivery.py checks create/replay/error requests against the
pinned SDK using Stubber and dummy credentials without AWS traffic. Managed Glue
installation, live HTTP/IAM behavior and deployment remain unvalidated.


### Release-scoped IAM configuration

scripts/prepare_glue_iam.py --release PATH --release-sha256 SHA256 --output
PRIVATE.tfvars.json exports Terraform inputs from the verified release. The file
contains the target, explicit identity pins and exact object keys; keep it local.

The optional glue_release input in infra/aws defines an unattached delivery
policy and a separate Glue service role. Delivery allows PUT/GET only on listed
objects and denies unencrypted/non-conditional writes and deletion of runtime/
input objects. Execution reads those files and confines table storage writes to
warehouse/building_proximity/, with catalog permissions for that table and
seven-day dedicated logs. Omitting glue_release creates no IAM/log resources.

The future job must use the exported log-group prefix. Delivery identity
attachment, job definition, restricted PassRole and actual AWS enforcement remain
separate work. The configuration is tested with mocked plans; it has not been
applied and does not authorize uploads or jobs.


### Optional pinned Glue job definition

Adding --include-job to scripts/prepare_glue_iam.py exports the exact release
manifest bytes alongside the IAM inputs. Terraform checks their explicit hash
and release scope before defining the optional Glue 5.1 Spark job. IAM-only
exports and the original foundation still omit the job by default.

The proposed job uses two G.1X workers, a ten-minute timeout, zero retries,
one concurrent run and no queuing. Its frozen script, software/input arguments,
logging settings and release-specific scratch location are non-overridable
job arguments. Worker/timeout fields are job settings; AWS StartJobRun can
supply per-run overrides, so these settings are not a spending cap.

A separate unattached deployment policy covers only this job and PassRole for
its exact Glue execution role. It grants no StartJobRun permission. There are
no schedules or triggers. Applying this configuration, granting run permission
and launching jobs remain separate approved steps. The new definition is tested
with mocked plans, including the retained release; managed startup remains
unvalidated.


### Local Glue trial control

scripts/run_glue_pilot.py --release PATH --release-sha256 SHA256 prints a verified
plan by default. Its separately approved --execute mode requires a fresh --output
under this checkout's artifacts directory. It checks the saved Glue job and the
versioned, pinned S3 completion record before submitting one fixed start request.
No arbitrary per-run arguments, worker counts or retry IDs are accepted.

The controller flushes intent and run identity to an append-only local journal,
observes only that run and requests a stop on drift, observation failure or its
watchdog limit. Stop acknowledgement and confirmed terminal state remain distinct.
An uncertain submission is never automatically retried; reconcile job history
before another attempt. Failed/unconfirmed outcomes exit nonzero and retain evidence.

The proposed watch window is 15 minutes / 90 observations, followed by at most
2 minutes / 12 stop observations, using ten-second intervals. These controls do
not establish an account spending cap or guarantee shutdown if the controlling
process or service is unavailable. SUCCEEDED records Glue status; output readback
and cross-engine acceptance remain separate checks.

These control-plane helpers live under scripts and are not included in the worker
wheel. scripts/check_glue_control.py validates the pinned SDK offline with Stubber,
dummy credentials and fake time. No new run permission is granted by these tools;
actual trial identity, resource/cost approval and live validation remain required.


### Athena dbt project preparation

analytics/athena locks dbt-core 1.12.6 and dbt-athena 1.11.1 in a separate
Python 3.11 Linux environment. It is not included in the Spark worker wheel.

scripts/prepare_dbt_project.py requires an explicit source schema, Iceberg snapshot
ID, source-run SHA256 and fresh output under artifacts. It prepares a staging
model and generates grid/release models from the same SQL used by local Gold
reports. The staging model is ephemeral; the Gold models are configured as views.
Preparation writes local files only. The supplied hash labels the snapshot;
it does not independently establish its provenance.

scripts/check_dbt_project.py uses the isolated tooling environment to parse that
prepared project with the real Athena adapter, dummy connection settings and
blocked Python network calls. It verifies the dependency graph and records three
models, one source and seven parsed data tests. Parsing does not execute the SQL
or those data tests. Athena execution, verified snapshot provenance, a scoped live
profile, query limits/permissions and cross-engine readback remain separate work.


### Writer snapshot readback

scripts/writer_readback.py verifies an explicitly pinned writer publication
receipt against the prepared input and a configured Iceberg catalog. It checks
target/settings, schema, provenance and bounded snapshot statistics, reads the
exact snapshot and compares all fact rows with the verified input. Metadata,
input and receipt are checked again before it returns a binding for dbt.
The reader owns and stops its session and performs no catalog/table writes.

scripts/check_writer_readback.py demonstrates this path using retained pilot
input and a fresh local HadoopCatalog warehouse under artifacts. It runs the
existing writer, verifies the output in a separate reader session and prepares
a dbt fixture directly from the observed snapshot ID. Its receipts keep live
AWS/Athena and writer-job identity verification separate. It does not configure
an AWS session, submit jobs or run dbt models/data tests in Athena.


### Athena read/query preparation

scripts/athena_read_plan.py prepares a credential-free dbt profile, one fixed
snapshot-pinned Gold SELECT, its request template and private Terraform inputs
under a fresh artifacts directory. It reuses the canonical grid/release SQL,
limits the returned release groups to two, disables result reuse and records
an explicit 10..100 MiB per-query workgroup scan cutoff. It has no execution mode.
The supplied snapshot/run identifiers remain unverified until live readback.

Its workgroup preflight makes one GetWorkGroup request and requires the named,
separately enabled workgroup with enforced engine 3, exact scan cutoff and the
expected result location, bucket-owner and encryption settings. It never enables a workgroup
or submits a query. Caller identity, provenance, cumulative budgets, persisted
idempotency tokens, monitoring/cancellation and result acceptance remain future
execution gates; the per-query cutoff is not a total spending cap.

infra/aws can optionally define an unattached Athena reader policy with
enable_athena_reader_policy. It scopes query APIs to one workgroup, reads to the
pilot catalog/table and S3 prefix, and result writes to the dedicated queries
prefix. Explicit denies preserve source objects and the source catalog entry.
It creates no runner role/attachment, grants no workgroup-update permission,
and leaves the workgroup configured disabled. It does not grant the additional
catalog permissions needed to build dbt views. No configuration has been applied.

scripts/check_athena_read_plan.py checks the real locked adapter's profile
credentials, SDK request shape and Stubber responses against a mocked Terraform
contract. It performs no AWS requests and does not execute Athena SQL.


### dbt Gold view materialization checks

infra/aws can also define a separate, standalone and unattached policy using
`enable_dbt_view_builder_policy` (false by default). It retains the reader's
source protections and adds database/relation discovery plus create/replace and
Glue version maintenance for exactly grid_proximity and release_proximity.
It grants no physical table-data writes, database creation or relation deletion.
The workgroup remains disabled. IAM scopes relation names, not their object type
or SQL text; whole-identity permissions still require review before a live trial.

Prepared projects now include macros/require_view.sql. It refuses to replace a
physical table with a Gold view, including when full refresh is requested.
Existing views can be replaced; source facts remain in Iceberg. The view models
store SQL definitions rather than separate copies of their result rows.

scripts/check_dbt_views.py invokes the actual locked dbt run/materialization path
with dummy credentials, SDK-validated synthetic responses and blocked networking.
It checks creation, replacement/version expiry, table collisions with and without
full refresh, and query failure. It compares compiled/submitted SQL with the pinned
source graph and records private traces. No AWS query or dbt data test executes.
That checker exercises the adapter's default query submission/polling path.
The query controller below adds a separate injected-client boundary. Live
source/caller integration, full dbt process supervision, result acceptance and
teardown remain separate work.


### dbt view query control

scripts/dbt_view_plan.py derives two ordered CREATE OR REPLACE VIEW statements
from the verified prepared project and shared Gold SQL. scripts/dbt_view_control.py
accepts an injected Athena client and a fresh local journal. It checks the next
exact view recipe, records a durable request/token before submission, rechecks
the workgroup and project, and monitors each owned query through the shared
Athena controller. It blocks further submissions after any failed or uncertain
outcome. There is no live execution CLI or automatic retry/replay.

The fixed plan admits at most two queries of 16 KiB each, with a 300-second
per-query watchdog, an earlier 600-second session deadline and the existing
60-second bounded cancellation confirmation. It accounts for observed scan bytes
and reserves a full per-query allowance before admitting another query within a
100 MiB cumulative allowance. These are admission/observation controls, not an
AWS billing guarantee. The deadline does not supervise dbt's whole process or
other catalog calls. Use the existing single-attempt, timeout-configured SDK
factory for the injected client.

Verified terminal DDL metadata is returned to the adapter from the controller's
cache, followed by at most one bounded DDL results request per query. This keeps
the adapter from starting its own polling loop. DDL completion does not establish
source provenance, catalog/view readback, Gold result correctness or dbt test
acceptance. The existing SELECT recovery command does not accept these view
intents; uncertain view submissions remain blocked for separate reconciliation.

scripts/check_dbt_view_control.py runs the real locked dbt adapter through this
boundary with dummy credentials and SDK-validated responses. Offline scenarios
cover successful views, watchdog cancellation, uncertain submission, metadata
drift and terminal query failure. No AWS request, dbt data test or Spark job runs.


### Source-checked dbt view builds

scripts/dbt_view_runner.py joins the view controller to the existing caller,
Glue catalog and exact Iceberg snapshot-readback checks. It validates local inputs
and expected identity, verifies the source before entering the model executor,
and rechecks the caller, source catalog, input/receipt pins and prepared project
before each view submission and after both models. A rejected gate prevents
another token/submission; a failed final gate produces no acceptance receipt.
Saved intent, source proof and final-gate hashes bind the resulting receipt.

Its owned-resource entry point uses the existing frozen-credential SDK clients
and isolated Spark reader, checks the default reader runtime before credentials,
and closes clients/clears the credential environment on every exit. The owned entry point now uses the supervised executor below by default, with an
explicit executor override available for focused checks. There is no live execution CLI or automatic
rollback: a rejected run can leave an already created/replaced first view.

scripts/check_dbt_view_runner.py supplies the real locked dbt adapter and SDK
clients, a simulated source reader and synthetic AWS responses. Cases cover
success, wrong caller, wrong readback, catalog drift between queries and final
catalog drift. It verifies that initial gates prevent dbt startup, later gates
block the appropriate submission/receipt, and owned clients/credentials close.
By default no AWS request, Spark startup or dbt data-test execution occurs in this check. The explicit data-test option is described below.

Source-checked model completion is separate from view catalog readback, Gold
result verification and dbt data-test acceptance. The supervised executor below covers the model process and catalog-call limits.
The opt-in bounded data-test phase is described below. View/Gold acceptance,
separately approved live validation, actual costs and teardown remain separate work.


### Supervised dbt model executor

The owned view runner defaults to scripts/dbt_model_executor.py. It starts one
fresh Linux dbt worker using the locked analytics interpreter. Credentials and
AWS clients stay in the parent. The worker verifies its pinned request/project,
uses the Athena connection/session factory to route requests through the parent,
and restores its adapter state before reporting model outcomes. It receives no
AWS credentials and refuses ordinary direct socket connections/DNS lookups.

The inherited local socket carries bounded JSON messages, with no listener or
published port. The parent routes query operations through ViewController and
limits catalog work to one database and the three expected relations. It allows
at most 64 requests total, 32 metadata requests, no additional discovery/history
pages and at most 12 observed versions per view. Version deletion is confined
to observed versions older than the four retained versions for the two views.
Only fields needed by the adapter cross the connection; SDK timestamps remain
in the parent. Requests and bounded responses are retained before delivery.

The model worker has a 720-second deadline, with a five-second limit on partial
request frames. The parent always stops/checks its owned process group, including
worker descendants, on failure or interruption. Deadline handling waits for an
in-flight bounded SDK/controller operation and its cancellation confirmation;
it is not an instantaneous hard kill of the parent or a billing guarantee.
Existing query limits remain unchanged. Initial Spark readback and final source
observations are outside the model-worker deadline.

scripts/check_dbt_view_runner.py --process-executor exercises this default path
with real dbt in the child and SDK-validated synthetic responses in the parent.
It creates no real cloud resources or views. The default scope is the two view models. The opt-in data-test phase below
extends the same worker/controller path; view/Gold acceptance, live IAM/service
validation, cost reconciliation and approved teardown remain separate work.


### Bounded dbt data tests

run_owned_views(..., data_tests=True) adds a separate supervised dbt test worker
only after both view models succeed. The default remains view-only. The seven
existing tests check five required fact columns, unique building IDs and Gold
count/share invariants. scripts/dbt_test_plan.py independently renders their
fixed SQL from the pinned project. scripts/dbt_test_control.py admits each exact
SELECT once, using the same source gates, workgroup checks, durable query tokens,
monitoring and cancellation controls. No failure-row table is created.

Each test returns a failure count and two decision flags. The controller accepts
only one bounded, typed summary with consistent values; pagination, extra rows
and malformed results are refused. A legitimate nonzero count allows remaining
tests to finish for diagnostics but prevents the final receipt. Query/protocol
failures block further submissions. The runner requires all seven observed zero
counts and matching dbt pass outcomes before its final source check/receipt.
By themselves these checks do not establish view catalog identity or independent
Gold correctness. Optional catalog readback is described below; the independent
Gold-result acceptance flag remains false.

The explicit test phase has its own limits: seven queries of at most 16 KiB,
300 seconds per query shortened by a 600-second session deadline, and the
existing bounded cancellation confirmation. It reserves the existing 100 MiB
per-query workgroup cutoff before admitting each query within a proposed 700 MiB
cumulative scan allowance. This opt-in allowance is separate from the unchanged
two-view phase; it is an admission control, not a billing hard cap or approval
to execute cloud queries. The two workers run sequentially, each with the
existing 720-second process deadline and owned process-group cleanup. Initial
source readback and final source checks remain separate phases.

scripts/check_dbt_view_runner.py --process-executor --data-tests runs the actual
locked dbt worker twice using SDK-validated simulated AWS/source responses.
Scenarios success, test_quality and test_watchdog check seven passing tests,
a known Gold-quality failure and owned test-query cancellation respectively.
There are no real AWS requests, live data-quality acceptance or Spark startup
in these checks. Live IAM/service behavior, view/Gold readback, costs and approved
teardown remain separate work.


### dbt view catalog readback

run_owned_views(..., view_readback=True) reads the two named Glue view records
after model completion and again after the optional data-test phase. It checks
same-account/database identity, ordinary Athena VIRTUAL_VIEW metadata, ordered
columns/types and the encoded SQL against the pinned prepared model, including
the source snapshot and run hash. Changed versions, definitions or parameters
between the two observations prevent the final receipt. Source/caller gates
and existing query/worker limits remain in place. The option defaults to false.

scripts/dbt_view_catalog.py supports a bounded ordinary Athena Presto-view
encoding. Its SQL comparison ignores whitespace/comments and normalizes word
case/ordinary identifier quoting while preserving string literals, operators
and grouping. It is a conservative token comparison: new encodings, additional
fields or engine rewrites need review instead of relaxed matching. The encoded
text is limited to 64 KiB, decoded definition to 24 KiB, SQL to 16 KiB and table
parameters to 4 KiB. These are validation limits after the bounded SDK call
returns, not HTTP transport byte limits. Lake Formation/multi-dialect views,
resource links, federated tables and physical data locations are unsupported.

Both observations are retained with versions, hashes, decoded SQL and columns;
their file hashes bind them to the source-checked receipt. When opted in, a
successful receipt sets view_catalog_verified=true. This means the supported
definitions matched at both observations. Separate catalog calls are not an
atomic snapshot or a lock: intervening/restored changes or later changes are
not excluded. It does not establish the writer identity, whole-role security
or independent Gold-result correctness. The optional release-result phase below
adds a separate comparison; without that option result_verified remains false.

This adds at most four exact Glue GetTable calls, with no new Athena query,
client, IAM permission or data-scan allowance. A failed readback preserves the
failure stage and prevents acceptance; views already created are not rolled back.

scripts/check_dbt_view_runner.py --process-executor --data-tests --view-readback
uses actual dbt workers and SDK-validated synthetic AWS/source responses. Success,
view_sql and view_drift exercise matching views, a wrong source snapshot before
data tests and a version change after all seven tests pass. These checks execute
no real AWS request or Spark job. Actual Athena catalog encoding/SQL formatting,
service permissions and Gold results still need separately approved live
validation. The optional release-result phase below adds the separate reference comparison.


### Release-view result comparison

run_owned_views(..., data_tests=True, view_readback=True, gold_results=True)
adds one parent-owned SELECT from release_proximity after both models and all
seven data tests succeed. scripts/dbt_gold_result.py uses the existing query
controller to own its exact SQL/request/token and monitor/cancel it. This phase
runs through the existing Athena client; it starts no additional dbt worker.
Gold verification requires the test/readback options and defaults to false.

The query returns at most two release groups so unexpected extra groups cause
rejection. Its one permitted GetQueryResults call requests at most three rows
(header, expected row, extra-row sentinel). The controller requires the exact
six columns/types and one non-null row, with no continuation page. It reuses
the existing pinned-summary comparison: release, area, run hash and integer
building counts must match exactly; the finite share must agree within 1e-12.
The reference is the retained processing summary, read independently of this
new Athena query. This compares release totals/share; it does not establish
per-grid equality or revalidate the spatial classification.

Terminal query metadata is checked again before and after result retrieval.
The reference/request pins are rechecked, and the bounded response and comparison
proof are saved. The runner still requires its final unchanged-view observations
and caller/source gates, then rechecks proof/response hashes before writing an
acceptance receipt. Only this completed opt-in path sets result_verified=true.
A later gate failure can retain a successful comparison as partial evidence but
still withholds the runner receipt. There is no rollback or automatic replay.

The extra phase admits one query of at most 16 KiB, with the existing 100 MiB
per-query cutoff and a separate proposed 100 MiB scan-admission allowance,
a 300-second query watchdog shortened by a 600-second session deadline, and
bounded cancellation confirmation. LIMIT bounds returned groups, not bytes
scanned to calculate them. Existing phase limits/defaults are unchanged. The
allowance is opt-in preparation, not AWS execution approval or a billing cap.
Results are limited to 64 KiB after the SDK call returns; this is not an HTTP
streaming byte cap. No S3 client or new IAM policy is added. GetQueryResults
still requires the role's existing scoped S3 GetObject permission for results.

scripts/check_dbt_view_runner.py --process-executor --data-tests --view-readback
--gold-results uses actual dbt workers with SDK-validated synthetic AWS/source
responses. Cases cover a matching row, wrong count, extra row, Gold-query
watchdog and final view drift after a matching comparison. A default-path check
keeps the two-model behavior unchanged. No real AWS query, Spark job or live
cross-engine/result acceptance occurs. The conservative view-catalog format
profile and live permission/service checks remain unvalidated. An approved AWS
trial, measured costs and verified teardown remain separate work.
