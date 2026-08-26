# Netflix Incremental Data Engineering Pipeline --- Complete Project Documentation

## Purpose of this document

This is the implementation and reconstruction guide for the Netflix
incremental data-engineering project. It documents the conceptual model,
physical database layers, incremental ingestion architecture, metadata
controls, Airflow orchestration, Docker/network integration, quality
gates, Power BI handoff, testing strategy, known failure modes and the
recommended order for rebuilding the project from scratch.

The objective is reproducibility: a future implementation should reach
the working architecture without repeating the avoidable debugging paths
encountered during development.

------------------------------------------------------------------------

# 1. System requirements and engineering goals

The pipeline has six primary requirements.

**Correctness.** A source batch must not be marked successful unless
ingestion, transformations and required quality checks succeed.

**Incrementality.** A new source batch should be processed without
rebuilding the entire project manually.

**Idempotency.** Retries and duplicate source deliveries should not
create uncontrolled duplication.

**Observability.** Batch state, schema changes and failures should be
traceable.

**Separation of concerns.** Source landing, OLTP modelling, analytical
modelling, orchestration and BI consumption are distinct layers.

**Reproducibility.** Infrastructure, SQL, Python, DAG definitions and
configuration templates should live in source control while secrets
remain external.

------------------------------------------------------------------------

# 2. Conceptual architecture

The conceptual flow is:

``` text
External Netflix catalogue extracts
             |
             v
      Source landing zone
             |
             v
 Schema + source validation
             |
             v
    Batch registration
             |
             v
     Raw/staging layer
             |
             v
       OLTP / 3NF
             |
             v
       OLAP / star
             |
             v
     Data quality gates
             |
             v
   Checkpoint / success
             |
             +----> source archive
             |
             v
          Power BI
```

Apache Airflow controls the transitions between those states.

The important architectural rule is that Airflow is an orchestrator.
PostgreSQL remains responsible for relational persistence and
transformations, while Python modules implement reusable pipeline
coordination, ingestion validation and metadata behaviour.

------------------------------------------------------------------------

# 3. Source model

The source consists of two related datasets.

## 3.1 Titles

The title source describes Netflix catalogue items and analytical
metrics. The implemented schema contract expects:

``` text
id
title
type
description
release_year
age_certification
runtime
genres
production_countries
seasons
imdb_id
imdb_score
imdb_votes
tmdb_popularity
tmdb_score
```

The source contains both descriptive attributes and analytical measures.

## 3.2 Credits

The credits source relates people to titles:

``` text
person_id
id
name
character
role
```

`id` links a credit to a title. A title can have many credits and a
person can appear across many titles.

------------------------------------------------------------------------

# 4. Conceptual data model

The core business entities are:

``` text
TITLE
  |
  +----< CREDIT >---- PERSON
  |
  +----< TITLE_GENRE >---- GENRE
  |
  +----< TITLE_COUNTRY >-- COUNTRY
```

This model resolves repeating/many-to-many relationships into relational
structures.

A title can belong to multiple genres and countries. Storing those as
normalized relationships is more robust than retaining list-like strings
in the operational model.

------------------------------------------------------------------------

# 5. Staging/raw model

The staging layer acts as a source-aligned landing area.

Core raw tables:

``` text
raw_title
raw_credit
```

Responsibilities:

-   preserve incoming source values;
-   provide a stable SQL boundary;
-   separate file parsing from relational transformations;
-   allow source-level audits;
-   provide analytical metrics that intentionally may not belong in the
    normalized OLTP entity.

Do not treat raw/staging as the final business model.

------------------------------------------------------------------------

# 6. OLTP / 3NF model

The normalized operational layer contains the relational representation
of the catalogue.

## `title`

Represents a unique catalogue title using its source title identifier as
the natural key.

## `person`

Represents a unique credited person.

## `credit`

Represents a person's participation in a title, including role and
character information where available.

## `genre`

Reference entity for normalized genres.

## `country`

Reference entity for production countries.

## `title_genre`

Bridge/associative table resolving the title-to-genre many-to-many
relationship.

## `title_country`

Bridge/associative table resolving the title-to-country many-to-many
relationship.

### Why 3NF here?

The OLTP layer aims to reduce redundancy, protect relational integrity
and create reusable source-of-truth entities. It is not optimized
primarily for Power BI query simplicity.

------------------------------------------------------------------------

# 7. OLAP dimensional model

The analytical layer lives in the `analytics` schema.

## 7.1 `analytics.dim_title`

Current-state title dimension. It uses a warehouse-generated surrogate
key (`title_sk`) while retaining the source title ID as a
natural/business key.

Relevant descriptive fields include title name, type, age certification
and release year.

The surrogate key prevents warehouse relationships from being tightly
coupled to a source identifier.

## 7.2 `analytics.dim_person`

Person dimension using `person_sk` as a surrogate key and `person_id` as
the source identifier.

## 7.3 `analytics.dim_genre`

Genre dimension using `genre_sk`.

## 7.4 `analytics.bridge_title_genre`

Resolves the many-to-many relationship between `dim_title` and
`dim_genre`.

## 7.5 `analytics.fact_credits`

Records title/person credit relationships. The row itself is
analytically meaningful, making the structure conceptually similar to a
factless fact table.

The implemented warehouse later introduces a warehouse-generated
`credit_sk` row identifier while preserving `title_sk` and `person_sk`
as dimensional foreign keys.

## 7.6 `analytics.fact_title_metrics`

One analytical row per title containing measures such as:

``` text
imdb_score
imdb_votes
tmdb_popularity
tmdb_score
```

These measures are intentionally analytical and can be sourced from
staging while title descriptive information is sourced through the
normalized model.

------------------------------------------------------------------------

# 8. Historical modelling decision

The project does not fabricate Slowly Changing Dimension Type 2 history
simply to demonstrate the pattern.

A static/current-state catalogue extract does not provide genuine
repeated historical observations. Creating artificial validity periods
would imply source history that did not exist.

The use of warehouse surrogate keys leaves room for a future SCD
strategy when real repeated snapshots become available.

------------------------------------------------------------------------

# 9. Incremental pipeline architecture

The final system processes explicit ingestion batches.

A batch has identity and state. The pipeline does not rely on "whatever
happens to be the latest row" when it can propagate exact IDs created
during ingestion.

Conceptual lifecycle:

``` text
DETECTED
   |
   v
VALIDATED
   |
   v
RUNNING
   |
   +---- failure ----> FAILED
   |
   v
TRANSFORMED
   |
   v
QUALITY PASSED
   |
   v
SUCCESS
   |
   v
ARCHIVED
```

Exact status names may vary by implementation, but the invariant is
important: success must mean the entire required processing boundary
succeeded.

------------------------------------------------------------------------

# 10. Batch metadata and stale recovery

Batch metadata solves several operational problems:

-   which source was processed;
-   whether it succeeded;
-   which exact batch a transformation should consume;
-   whether a previous run was abandoned;
-   whether the same source has already been handled.

A batch that remains `RUNNING` without completion beyond the stale
threshold can be treated as abandoned and moved to a failed/recoverable
state before new work begins.

The Airflow DAG currently invokes stale recovery before ingestion and
uses a 60-minute abandonment threshold.

Do not automatically classify every long-running batch as failed without
selecting a threshold appropriate to expected workload duration.

------------------------------------------------------------------------

# 11. Source checksums

The schema/ingestion code calculates SHA-256 over source-file bytes.

Pseudo-process:

``` text
open source in binary mode
read chunks
update SHA-256 digest
return hexadecimal digest
```

Benefits:

-   duplicate detection;
-   content identity independent of filename;
-   auditability;
-   retry safety;
-   protection from accidental reprocessing.

Do not use file modification time as the only source identity.

------------------------------------------------------------------------

# 12. Schema contract and drift management

Schema drift should be detected before a malformed source reaches
downstream transformations.

The contract contains:

``` text
column name
required/optional status
logical datatype
schema version
```

Examples of drift:

**Missing required column** --- usually blocking.

**New optional column** --- may be accepted depending on policy.

**Datatype change** --- requires compatibility analysis.

**Renamed column** --- blocking until mapped/contract updated.

**Column order change** --- should not matter if ingestion maps by
names.

Schema-change history should be auditable so a known accepted change is
not repeatedly treated as an unknown event.

------------------------------------------------------------------------

# 13. Checkpoints / watermarks

A checkpoint represents the last successfully processed boundary.

Critical rule:

``` text
FAILED PIPELINE != ADVANCED CHECKPOINT
```

The checkpoint must advance only after all transformations and required
quality gates succeed.

This prevents a failed batch from being silently skipped on the next
run.

When implementing checkpoint logic:

1.  read current successful checkpoint;
2.  identify eligible batch;
3.  process inside well-defined transactional/failure boundaries;
4.  run quality validation;
5.  record success;
6.  advance checkpoint;
7.  archive source.

------------------------------------------------------------------------

# 14. Pipeline coordinator

The transformation coordinator (`run_transformation_pipeline`) provides
a reusable production entry point.

The Airflow DAG should not duplicate all OLTP, OLAP, quality,
batch-state and checkpoint logic.

Preferred separation:

``` text
DAG
  |
  +--> determine WHEN / dependency order / retries
  |
pipeline_runner
  |
  +--> determine WHAT pipeline sequence to execute
  |
SQL + domain Python modules
  |
  +--> implement transformations and controls
```

This makes the transformation pipeline testable outside Airflow.

------------------------------------------------------------------------

# 15. Source-batch runner

The source batch runner owns source-level workflow such as:

-   detecting the required file pair;
-   invoking ingestion;
-   returning exact batch identifiers;
-   archiving the processed pair after success.

The ingestion result should be structured metadata, for example:

``` python
{
    "titles_batch_id": ...,
    "credits_batch_id": ...
}
```

Downstream tasks must use those exact IDs.

------------------------------------------------------------------------

# 16. Airflow DAG design

The final continuous DAG performs:

``` text
wait_for_source_batch
        ↓
recover_stale_batches
        ↓
ingest_new_batch
        ↓
resolve_latest_batches
        ↓
run_transformation_pipeline
        ↓
archive_source_files
```

## 16.1 Sensor

The source sensor checks that both:

``` text
titles.csv
credits.csv
```

exist in the container-visible incoming directory.

Using `mode="reschedule"` is preferable for a long wait because the
sensor does not need to monopolize a worker slot while sleeping.

## 16.2 Stale recovery task

Calls the batch tracker before accepting new work.

## 16.3 Ingestion task

Calls `ingest_incoming_batch()`.

"No new batch" is an expected operational state, not necessarily an
exception. Airflow skip semantics are useful here.

## 16.4 XCom and batch resolution

Airflow automatically stores a PythonOperator return value for XCom use.

The most important implementation rule:

``` text
xcom_pull(task_ids="...") must use the exact producer task_id.
```

A mismatch can make a valid ingestion appear to have returned nothing.

## 16.5 Transformation task

Receives explicit titles/credits batch IDs and calls the production
pipeline coordinator.

## 16.6 Archive task

Runs only after successful transformation. This dependency is
intentional and must not be moved before quality/transform success.

------------------------------------------------------------------------

# 17. DAG identity versus pipeline identity

Separate orchestration identity from persistent ETL lineage.

Recommended concept:

``` python
DAG_ID = "netflix_incremental_pipeline_continuous"
PIPELINE_NAME = "netflix_incremental_pipeline"
```

Why:

-   the Airflow scheduler tracks DAG history by DAG ID;
-   the ETL metadata tables may already contain lineage under the
    original pipeline name;
-   changing scheduling strategy should not necessarily create a new ETL
    lineage;
-   using a new DAG ID can avoid legacy scheduling metadata interfering
    with the continuous orchestration definition.

Do not repeatedly rename DAG IDs without reason because it fragments
Airflow operational history.

------------------------------------------------------------------------

# 18. Continuous scheduling

The final design uses:

``` python
schedule="@continuous"
max_active_runs=1
catchup=False
```

`@continuous` starts the next DAG run after the previous run completes.

`max_active_runs=1` is important because warehouse mutations should not
overlap unless concurrency was deliberately engineered and tested.

The source sensor means a continuous run can wait for a new complete
batch rather than executing transformation logic with no data.

------------------------------------------------------------------------

# 19. Airflow retries and timeouts

Use retries for transient failures, not for deterministic schema errors.

Example categories:

**Retryable**

-   temporary database connectivity issue;
-   transient network failure;
-   scheduler/worker interruption.

**Usually not fixed by retries**

-   required source column missing;
-   incompatible schema;
-   invalid SQL;
-   broken task ID;
-   wrong filesystem path.

Exponential backoff reduces pressure on an unavailable dependency.

Task timeouts prevent a broken operation from remaining active
indefinitely.

------------------------------------------------------------------------

# 20. Docker and filesystem paths

One of the most common local orchestration errors is confusing host
paths with container paths.

A path that exists in VS Code on macOS is not automatically present at
the same absolute location inside Airflow.

The sensor and ingestion code must reference the path created by Docker
volume mappings.

Example container path used by the project:

``` text
/opt/airflow/project/data/incoming
```

Before debugging Python logic, prove the files exist inside the
scheduler/worker container:

``` bash
docker compose exec airflow-scheduler ls -la /opt/airflow/project/data/incoming
```

If the host sees the CSV and the container does not, fix volume
mapping/path configuration first.

------------------------------------------------------------------------

# 21. Mac → VMware → PostgreSQL networking

The development topology introduces three distinct connectivity tests.

## Test 1 --- Windows local PostgreSQL

Confirm PostgreSQL itself is running.

## Test 2 --- macOS to Windows VM

From macOS:

``` bash
nc -vz "$NETFLIX_PG_HOST" 5432
```

## Test 3 --- Docker/Airflow to Windows VM

Test from the Airflow network/container.

Only after all three work should you debug application credentials/SQL.

### Common network causes

-   VMware guest IP changed;
-   NAT/bridged configuration changed;
-   Windows firewall blocks inbound TCP 5432;
-   PostgreSQL `listen_addresses` is too restrictive;
-   `pg_hba.conf` does not authorize the client network;
-   wrong PostgreSQL instance/port;
-   Docker configuration points to `localhost`.

Do not weaken firewall or authentication rules broadly merely to make a
development connection work. Grant the smallest network/access scope
needed.

------------------------------------------------------------------------

# 22. Airflow database separation

Airflow's metadata PostgreSQL database and the Netflix warehouse are
different systems even if both use PostgreSQL.

Airflow metadata stores:

``` text
DAG runs
task instances
XCom
scheduler metadata
Airflow configuration metadata
```

The Netflix PostgreSQL database stores:

``` text
raw source data
OLTP entities
analytics dimensions/facts
ETL metadata/checkpoints
```

Never point transformation SQL at the Airflow metadata database.

------------------------------------------------------------------------

# 23. Airflow connection/security design

The DAG must not contain raw passwords.

Use an Airflow connection such as:

``` text
netflix_postgres
```

or environment-driven connection configuration.

The repository may commit `.env.example`; it must not commit `.env`.

Before GitHub publication run a secret scan/grep across SQL, Python,
notebooks and Git history. Development SQL often contains temporary
credentials. Rotate any credential that has ever been exposed.

------------------------------------------------------------------------

# 24. Data quality framework

Quality checks are pipeline gates.

## 24.1 Primary/surrogate key checks

Validate:

-   non-null surrogate keys;
-   uniqueness;
-   expected primary-key behaviour.

## 24.2 Referential integrity

Validate:

``` text
fact_credits.title_sk       -> dim_title.title_sk
fact_credits.person_sk      -> dim_person.person_sk
bridge_title_genre.title_sk -> dim_title.title_sk
bridge_title_genre.genre_sk -> dim_genre.genre_sk
fact_title_metrics.title_sk -> dim_title.title_sk
```

## 24.3 Metric sanity

Examples:

-   IMDb score is in the expected domain when present;
-   IMDb votes are not negative;
-   title counts are non-zero;
-   title types are recognized.

## 24.4 Reconciliation

Where logically valid:

``` text
movie count + show count = total title count
```

Do not turn one observed development row count into a permanent rule
unless the source is frozen.

------------------------------------------------------------------------

# 25. Power BI integration

Power BI belongs downstream of the `analytics` schema.

Recommended access model:

``` text
Power BI
   |
read-only PostgreSQL login
   |
analytics schema
```

The Power BI account requires only:

-   database connect;
-   schema usage;
-   table select.

It should not receive warehouse mutation or superuser privileges.

The Power BI semantic/report layer should calculate
presentation/business measures but should not compensate for broken
upstream relational modelling.

------------------------------------------------------------------------

# 26. Correct build order from scratch

This order is designed to eliminate the largest classes of debugging
waste.

## Phase 1 --- Repository scaffold

Create:

``` text
dags/
src/
data/incoming/
data/archive/
data/raw/
existing_sql/
notebooks/
docs/
logs/
config/
powerbi/
```

Add `.gitignore` immediately.

**Gate:** no credentials or generated runtime files are tracked.

## Phase 2 --- Source profiling

Profile titles and credits before designing constraints.

Check:

-   row counts;
-   nulls;
-   uniqueness;
-   list-like fields;
-   datatypes;
-   malformed values.

**Gate:** source behaviour is understood.

## Phase 3 --- Conceptual and OLTP model

Create normalized title/person/credit/genre/country relationships.

**Gate:** referential integrity and expected row reconciliation pass.

## Phase 4 --- Staging

Create raw landing tables and repeatable file ingestion.

**Gate:** both source files can be loaded and audited.

## Phase 5 --- OLAP model

Create analytics dimensions, bridges and facts.

**Gate:** BI questions can be answered without reaching back into
unrelated staging/public tables.

## Phase 6 --- Quality SQL

Build automated checks.

**Gate:** deliberately bad data causes the correct validation failure.

## Phase 7 --- Python database layer

Centralize connection behaviour and transactions.

**Gate:** simple read/write test succeeds without embedding credentials
in code.

## Phase 8 --- Batch metadata

Implement batch registration, states and stale recovery.

**Gate:** success/failure/recovery scenarios are reproducible.

## Phase 9 --- Schema contract/checksum

Validate columns/types and calculate source fingerprints before
transformation.

**Gate:** duplicate and incompatible sources are handled intentionally.

## Phase 10 --- Incremental ingestion

Return explicit batch IDs.

**Gate:** two sequential valid batches process without manual database
cleanup.

## Phase 11 --- Pipeline coordinator

Create one production Python entry point for OLTP + OLAP + quality +
checkpoint flow.

**Gate:** it can run successfully outside Airflow.

## Phase 12 --- Docker/Airflow runtime

Build Airflow with required providers.

**Gate:** services healthy and DAG import errors zero.

## Phase 13 --- Network verification

Test Mac → VM, Docker → VM, Airflow hook → PostgreSQL.

**Gate:** `SELECT 1` works through Airflow.

## Phase 14 --- Airflow orchestration

Add tasks incrementally, beginning with connectivity and then
ingestion/transformation.

**Gate:** manual DAG succeeds twice.

## Phase 15 --- Continuous source detection

Add sensor and `@continuous` only after idempotency is proven.

**Gate:** dropping a new pair into incoming triggers one successful
processing cycle.

## Phase 16 --- Failure injection

Test duplicate, partial, malformed, stale and transformation-failure
scenarios.

**Gate:** metadata/checkpoints/archive state remains correct.

## Phase 17 --- Power BI

Create least-privilege BI role and connect analytics schema.

**Gate:** dashboard metrics reconcile with SQL.

## Phase 18 --- GitHub

Secret scan, clean working tree, documentation, commit and push.

------------------------------------------------------------------------

# 27. Bugs encountered and how to avoid them

## 27.1 Duplicate imports / duplicate functions

During rapid DAG iteration it is easy to import the same callable twice
or leave obsolete helper functions in the file.

**Avoidance:** after every structural edit, run a formatter/linter and
inspect the DAG file for one canonical path per operation.

## 27.2 XCom task-ID mismatch

An ingestion result can exist while downstream code pulls from a
different task ID.

**Avoidance:** define task IDs once or verify every
`xcom_pull(task_ids=...)` against the actual operator declaration.

## 27.3 Redundant ingestion paths

Having both `ingest_new_source_batch()` and an older
`ingest_source_batch()` in the same DAG creates ambiguity.

**Avoidance:** delete/deprecate old DAG-level wrappers once the
production path is selected.

## 27.4 Sensor path mismatch

Host `data/incoming` can contain files while Airflow sees nothing.

**Avoidance:** inspect the exact path from inside the scheduler
container before changing sensor code.

## 27.5 Scheduled DAG legacy behaviour

Changing scheduling semantics on an existing DAG can leave confusing
historical runs/metadata.

**Avoidance:** keep ETL `PIPELINE_NAME` stable but use a deliberate
orchestration `DAG_ID` when introducing a materially new continuous
scheduling lifecycle.

## 27.6 CLI syntax/version mismatch

Airflow CLI subcommands/options change across major versions.

**Avoidance:** run:

``` bash
airflow dags --help
airflow tasks --help
```

inside the installed container instead of relying on commands copied
from another Airflow version.

## 27.7 Confusing "no source" with failure

A continuously running pipeline frequently has nothing new to process.

**Avoidance:** use a sensor to wait and/or `AirflowSkipException` for
expected no-work conditions.

## 27.8 Container `localhost`

The warehouse is in a VM, not the Airflow container.

**Avoidance:** configure the reachable VM IP/hostname and test TCP from
every network boundary.

## 27.9 Advancing checkpoint too early

If a checkpoint advances before quality checks finish, a failed batch
may be skipped later.

**Avoidance:** commit the successful checkpoint at the end of the
successful transaction/workflow boundary.

## 27.10 Archiving too early

Moving files before transformation success makes recovery harder.

**Avoidance:** archive is the final downstream Airflow task.

## 27.11 Overly strict null constraints

Real catalogue fields such as age certification can legitimately be
missing.

**Avoidance:** profile nulls before defining `NOT NULL`; constraints
should represent business truth, not assumptions.

## 27.12 Wrong natural-key datatype

Source IDs that look numeric can actually be textual identifiers.

**Avoidance:** profile raw IDs and preserve source semantics; do not
infer datatype only from a sample.

## 27.13 Incomplete analytical schema

If BI queries must join back into raw/OLTP for common dimensions, the
analytical model is incomplete.

**Avoidance:** test the star schema using real analytical questions
before declaring it finished.

## 27.14 Credentials in development SQL

Temporary troubleshooting scripts can accidentally contain passwords.

**Avoidance:** never commit real passwords; scan SQL/notebooks before
pushing and rotate exposed credentials.

------------------------------------------------------------------------

# 28. Troubleshooting decision tree

## Airflow does not start

``` bash
docker compose config
docker compose ps
docker compose logs --tail=200
```

Fix infrastructure before debugging DAG logic.

## DAG does not appear

Check:

1.  DAG file mounted into container;
2.  Python syntax;
3.  import errors;
4.  required provider installed;
5.  unique `dag_id`;
6.  scheduler/dag-processor health.

## DAG exists but does not run

Check:

1.  paused state;
2.  active runs;
3.  scheduler logs;
4.  schedule definition;
5.  task dependencies.

## Sensor waits although files exist

Check inside container:

``` bash
ls -la /opt/airflow/project/data/incoming
```

Verify exact filenames and volume mount.

## Ingestion runs but downstream says no metadata

Inspect ingestion XCom and verify producer `task_id`.

## PostgreSQL cannot be reached

Test in this order:

``` text
PostgreSQL local
Mac -> VM:5432
Docker -> VM:5432
Airflow connection -> SELECT 1
```

## Pipeline keeps reprocessing the same source

Inspect:

-   checksum;
-   batch state;
-   duplicate detection;
-   checkpoint;
-   archive result.

## New batch never processes

Inspect:

-   previous active continuous run;
-   sensor state;
-   `max_active_runs`;
-   whether both files are present;
-   whether content is considered a duplicate.

------------------------------------------------------------------------

# 29. Validation checklist

Before calling the project complete:

-   [ ] both source contracts are explicit;
-   [ ] source checksums work;
-   [ ] duplicate source handling is tested;
-   [ ] partial batch does not transform;
-   [ ] malformed schema is blocked;
-   [ ] stale batch recovery works;
-   [ ] OLTP keys reconcile;
-   [ ] OLAP keys reconcile;
-   [ ] quality checks fail on deliberately bad data;
-   [ ] checkpoints do not advance on failure;
-   [ ] archive happens only on success;
-   [ ] Airflow DAG imports cleanly;
-   [ ] continuous scheduling does not overlap warehouse mutation;
-   [ ] container sees incoming source files;
-   [ ] Airflow reaches warehouse using the configured connection;
-   [ ] Power BI uses read-only access;
-   [ ] no secrets are tracked;
-   [ ] README and this documentation match the implementation.

------------------------------------------------------------------------

# 30. Git workflow

Before the first public push:

``` bash
git status
git diff
git ls-files
```

Search for secrets before staging.

Then:

``` bash
git add README.md docs/PROJECT_DOCUMENTATION.md
git add .
git status
git commit -m "docs: document Netflix incremental data pipeline"
git push -u origin main
```

If the current branch is not `main`, use:

``` bash
git branch --show-current
git push -u origin <current-branch>
```

Do not use `git add .` until you have verified `.gitignore` and
confirmed that `.env`, credentials, local logs, raw sensitive files and
generated runtime artifacts are excluded.

------------------------------------------------------------------------

# 31. Recommended `.gitignore`

At minimum:

``` gitignore
.env
.venv/
venv/
__pycache__/
*.py[cod]
.pytest_cache/
.DS_Store
.vscode/settings.json
*.log

logs/*
!logs/.gitkeep

data/incoming/*.csv
data/raw/*
```

Whether archive/test fixture CSVs belong in Git should be decided
deliberately. Small synthetic fixtures can be useful; real or large
source extracts usually should not be versioned.

------------------------------------------------------------------------

# 32. Future production evolution

The current project is a strong local/portfolio architecture. A hardened
production version could evolve toward:

-   managed Airflow;
-   managed PostgreSQL;
-   object storage instead of local incoming/archive folders;
-   cloud secret manager;
-   CI/CD;
-   automated unit/integration tests;
-   alerting/incident integration;
-   metrics/observability;
-   data lineage/catalogue;
-   partitioned large facts;
-   real historical snapshots/SCD2 where source history exists;
-   infrastructure as code.

These are extensions, not prerequisites for understanding the
engineering demonstrated by the current implementation.

------------------------------------------------------------------------

# 33. Final architecture summary

The project demonstrates the progression:

``` text
CSV files
  ↓
validated source batches
  ↓
raw/staging PostgreSQL
  ↓
normalized OLTP
  ↓
dimensional OLAP
  ↓
automated quality gates
  ↓
checkpointed incremental processing
  ↓
automatic archival
  ↓
Power BI
```

wrapped by:

``` text
Python + PostgreSQL + Airflow + Docker + batch metadata + schema governance
```

The most important improvement over the original one-off warehouse
workflow is not simply that Airflow runs SQL. The improvement is that
the entire lifecycle of a source batch is now explicit: arrival,
validation, identity, ingestion, transformation, quality,
success/failure state, checkpoint and archival.

That is what makes the project a repeatable data-engineering pipeline
rather than a collection of scripts.
