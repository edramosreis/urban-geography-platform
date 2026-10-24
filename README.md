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

## Development checks

```sh
docker compose run --rm pipeline ruff check .
docker compose run --rm pipeline ruff format --check .
docker compose run --rm pipeline mypy
docker compose run --rm pipeline pytest -q
```

Tests include offline Spark/Sedona integration and an independent Shapely/PROJ
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
- `scripts/check_publication.py`: checks tracked paths and Git history; `--secrets` also
  runs the pinned Gitleaks image against committed history without network access.

The current implementation is a local Spark/Sedona pipeline that writes Parquet.
Iceberg, dbt, orchestration and AWS deployment are future work.

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
