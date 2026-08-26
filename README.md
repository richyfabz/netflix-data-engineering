# Netflix Incremental Data Engineering Pipeline

An end-to-end, incremental analytics engineering project that turns
Netflix catalogue source files into a governed PostgreSQL warehouse and
analytics model, orchestrated by Apache Airflow and prepared for Power
BI consumption.

The project is designed as more than a one-off CSV-to-dashboard
exercise. It implements repeatable batch ingestion, schema contracts,
source-file fingerprinting, stale-batch recovery, incremental OLTP/OLAP
transformations, data-quality gates, checkpoints, idempotent processing,
automatic source archival, and continuous Airflow orchestration.

> **Security note:** credentials, database passwords and local `.env`
> values must never be committed. Use `.env.example` and Airflow
> connection/environment configuration.

------------------------------------------------------------------------

## 1. Project objective

The goal is to model a realistic data-engineering workflow around
Netflix catalogue data:

1.  Receive a complete `titles.csv` + `credits.csv` source batch.
2.  Detect whether the source is new and structurally valid.
3.  Register and track the ingestion batch.
4.  Load source data into controlled staging/raw structures.
5.  Transform it into a normalized OLTP model.
6.  Transform the operational model into an analytical dimensional
    model.
7.  Validate warehouse integrity and analytical metrics.
8.  Commit pipeline checkpoints only after successful processing.
9.  Archive processed source files.
10. Expose the analytics schema to Power BI through a read-only database
    account.
11. Repeat automatically when another source batch arrives.

The result demonstrates SQL, Python, PostgreSQL, dimensional modelling,
incremental ETL/ELT, Apache Airflow, Docker, data quality,
orchestration, batch metadata, Power BI and production-style failure
handling.

------------------------------------------------------------------------

## 2. High-level architecture

``` text
                     SOURCE SYSTEM
               titles.csv + credits.csv
                         |
                         v
                data/incoming/
                         |
                         v
              +---------------------+
              | Apache Airflow DAG  |
              | source-file sensor  |
              +----------+----------+
                         |
                         v
               stale batch recovery
                         |
                         v
               source batch ingestion
                 /       |       \
                /        |        \
        schema contract checksum  batch tracking
                \        |        /
                 \       |       /
                         v
                 PostgreSQL RAW/STAGING
                         |
                         v
                  OLTP / 3NF MODEL
      title -- credit -- person -- genre -- country
        |                    |
        +-- title_genre -----+
        +-- title_country ---+
                         |
                         v
                   OLAP / ANALYTICS
              +----------------------+
              | dim_title            |
              | dim_person           |
              | dim_genre            |
              | bridge_title_genre   |
              | fact_credits         |
              | fact_title_metrics   |
              +----------+-----------+
                         |
                         v
                  DATA QUALITY GATES
                         |
                         v
               CHECKPOINT / BATCH SUCCESS
                         |
                         v
                  SOURCE ARCHIVAL
                         |
                         v
                      POWER BI
```

### Runtime topology

The development environment can span multiple systems:

``` text
Airflow container
      |
Docker Desktop on macOS
      |
macOS host networking
      |
VMware Fusion network
      |
Windows VM : 5432
      |
PostgreSQL warehouse
      |
Power BI
```

`localhost` inside an Airflow container is the container itself, not a
PostgreSQL instance running inside a Windows VM. Always use the VM
address reachable from Docker.

------------------------------------------------------------------------

## 3. Data layers

### Source layer

The pipeline expects a complete pair:

``` text
data/incoming/
├── titles.csv
└── credits.csv
```

A source batch is not processed until both files exist.

### Staging/raw layer

The raw tables preserve source-shaped data before business
transformations. The project uses `raw_title` and `raw_credit` as
landing structures. This provides a controlled boundary between external
files and the relational model.

### OLTP layer

The operational model is normalized to reduce duplication and correctly
represent many-to-many relationships.

Core entities include:

-   `title`
-   `person`
-   `credit`
-   `genre`
-   `country`
-   `title_genre`
-   `title_country`

`title_genre` and `title_country` resolve many-to-many relationships
instead of storing repeating values inside a title record.

### OLAP layer

The `analytics` schema is designed for analytical querying and BI.

Key objects include:

-   `analytics.dim_title`
-   `analytics.dim_person`
-   `analytics.dim_genre`
-   `analytics.bridge_title_genre`
-   `analytics.fact_credits`
-   `analytics.fact_title_metrics`

`fact_credits` behaves as a factless/relationship fact: the existence of
a row records that a person was credited on a title in a particular
role. `fact_title_metrics` stores measurable title-level values such as
IMDb score, IMDb votes, TMDB popularity and TMDB score.

Surrogate keys decouple analytical relationships from source natural
keys.

------------------------------------------------------------------------

## 4. Incremental processing design

The project evolved from a static warehouse load into a repeatable
incremental pipeline.

Each ingestion run works with explicit batch identifiers for the titles
and credits files. Those IDs are propagated through the pipeline rather
than asking downstream tasks to guess which batch is newest.

Important behaviours include:

-   source fingerprinting/checksums;
-   duplicate source detection;
-   schema validation before ingestion;
-   batch status tracking;
-   stale `RUNNING` batch recovery;
-   incremental transformation;
-   checkpoint management;
-   retry-safe orchestration;
-   source archival only after success.

A failed run must not falsely advance the successful processing
checkpoint.

------------------------------------------------------------------------

## 5. Schema contract and drift detection

Incoming files are checked against an explicit schema contract before
they are accepted.

Expected title fields include:

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

Expected credit fields include:

``` text
person_id
id
name
character
role
```

The contract distinguishes required columns and expected logical
datatypes.

This protects the pipeline from silent failures caused by:

-   renamed columns;
-   removed required columns;
-   unexpected required fields;
-   incompatible datatype changes;
-   malformed source structures.

Schema changes can be recorded in pipeline metadata so accepted and
blocked changes are auditable.

------------------------------------------------------------------------

## 6. Source fingerprinting and idempotency

A SHA-256 checksum is calculated from source-file bytes.

This enables the ingestion layer to distinguish:

``` text
same filename + same content      -> duplicate
same filename + different content -> new content
new filename + known content      -> duplicate content
```

The exact implementation rules are controlled by the batch-ingestion
layer, but the principle is that processing identity should be based on
data, not only filenames.

This is essential for safe retries and continuous scheduling.

------------------------------------------------------------------------

## 7. Airflow orchestration

The final orchestration uses a continuous DAG identity separate from the
ETL pipeline metadata name.

Conceptually:

``` python
DAG_ID = "netflix_incremental_pipeline_continuous"
PIPELINE_NAME = "netflix_incremental_pipeline"
```

This keeps Airflow scheduling identity separate from persistent ETL
lineage.

The production dependency chain is:

``` text
wait_for_source_batch
        |
        v
recover_stale_batches
        |
        v
ingest_new_batch
        |
        v
resolve_latest_batches
        |
        v
run_transformation_pipeline
        |
        v
archive_source_files
```

### Source sensor

The sensor checks for both required files and waits rather than
repeatedly failing the DAG when no data exists.

### Stale recovery

Before processing a new batch, abandoned batches left in `RUNNING` state
beyond the configured threshold are recovered/failed.

### Ingestion

The ingestion task validates and registers the exact source batch. If no
valid new batch exists, the task can skip cleanly instead of classifying
"no data" as a system failure.

### Batch-ID propagation

The ingestion result is passed through Airflow XCom. Downstream
processing uses those exact IDs. Task IDs used by `xcom_pull()` must
match the Airflow task definitions exactly.

### Transformation

The orchestration layer calls the production transformation coordinator.
The DAG remains thin while substantial business/database logic stays in
reusable Python/SQL modules.

### Archival

Files are moved out of `data/incoming/` only after the transformation
pipeline completes successfully.

------------------------------------------------------------------------

## 8. Reliability controls

The DAG includes production-style controls:

-   `catchup=False`
-   one active DAG run at a time
-   retries
-   exponential retry backoff
-   maximum retry delays
-   task execution timeouts
-   DAG run timeout
-   rescheduling sensor behaviour
-   clean skip semantics when no new batch exists

`max_active_runs=1` protects a shared warehouse from concurrent mutation
by overlapping pipeline runs.

------------------------------------------------------------------------

## 9. Data quality

Quality validation belongs inside the pipeline, not only in exploratory
notebooks.

Typical checks include:

### Structural checks

-   required tables exist;
-   required columns exist;
-   schema contracts match expected source structure.

### Key checks

-   surrogate keys are not null;
-   surrogate keys are unique;
-   foreign keys resolve;
-   bridge/fact rows do not contain orphan references.

### Metric checks

-   populated IMDb scores are within the valid rating domain;
-   vote counts are non-negative;
-   title counts are non-zero;
-   expected title types remain interpretable.

### Reconciliation checks

Where the source/business rules allow it:

``` text
Movies + Shows = Total Titles
```

Avoid hard-coding a historical row-count baseline unless the source
dataset is intentionally frozen.

------------------------------------------------------------------------

## 10. Repository structure

A representative project layout is:

``` text
netflix-data-engineering/
├── dags/
│   ├── 00_airflow_healthcheck.py
│   └── netflix_incremental_pipeline.py
├── src/
│   ├── database.py
│   ├── ingestion.py
│   ├── schema_contract.py
│   ├── batch_tracker.py
│   ├── source_batch_runner.py
│   └── pipeline_runner.py
├── data/
│   ├── incoming/
│   │   └── .gitkeep
│   ├── raw/
│   ├── archive/
│   │   └── .gitkeep
│   ├── backup_original/
│   └── Tampered file/
├── existing_sql/
├── notebooks/
├── docs/
│   └── PROJECT_DOCUMENTATION.md
├── logs/
├── config/
├── powerbi/
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

The exact local tree may contain additional SQL scripts, notebooks and
validation assets.

------------------------------------------------------------------------

## 11. Environment variables and secrets

Never hard-code database passwords in DAGs, Python modules, SQL scripts
or documentation.

Example:

``` dotenv
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=netflix-airflow:local

NETFLIX_PG_HOST=<WINDOWS_VM_REACHABLE_IP>
NETFLIX_PG_PORT=5432
NETFLIX_PG_DATABASE=postgres
NETFLIX_PG_USER=<POSTGRES_USER>
NETFLIX_PG_PASSWORD=<POSTGRES_PASSWORD>
NETFLIX_PG_SSLMODE=prefer

NETFLIX_AIRFLOW_CONN_ID=netflix_postgres
```

Before publishing the repository, search the full Git history and
working tree for credentials. If a real password was ever committed,
rotate it; deleting it only from the latest file is not sufficient.

------------------------------------------------------------------------

## 12. Local setup

### Prerequisites

-   Git
-   Python 3
-   PostgreSQL
-   Docker Desktop
-   Docker Compose V2
-   Apache Airflow 3.x-compatible project image
-   PostgreSQL Airflow provider
-   Power BI Desktop if reproducing the BI layer
-   VMware Fusion only if using the same Mac/Windows topology

### Clone and configure

``` bash
git clone <repository-url>
cd netflix-data-engineering
cp .env.example .env
```

Fill `.env` locally. Do not commit it.

### Validate Docker Compose

``` bash
docker compose config
```

### Build and start

Use the commands defined by the repository's Compose/Makefile setup,
commonly:

``` bash
docker compose build
docker compose up -d
docker compose ps
```

### Confirm DAG import

``` bash
docker compose exec airflow-scheduler airflow dags list
```

Check import errors using the Airflow CLI command supported by the
installed Airflow version or through the UI.

------------------------------------------------------------------------

## 13. Database connectivity

If PostgreSQL runs in a Windows VMware guest, first obtain its reachable
IPv4 address.

From macOS:

``` bash
nc -vz "$NETFLIX_PG_HOST" "$NETFLIX_PG_PORT"
```

Then prove Docker/container connectivity separately.

Finally prove an Airflow PostgreSQL connection with a read-only query
such as:

``` sql
SELECT 1;
```

Only enable warehouse mutation after all connectivity layers succeed.

Typical causes of failure:

-   VM address changed;
-   wrong VMware NAT/bridged mode;
-   Windows Defender Firewall blocks 5432;
-   PostgreSQL `listen_addresses` does not expose the required
    interface;
-   `pg_hba.conf` does not permit the client;
-   Airflow is pointed at `localhost`;
-   credentials refer to the wrong PostgreSQL instance.

------------------------------------------------------------------------

## 14. Running a batch

Place a complete pair in:

``` text
data/incoming/titles.csv
data/incoming/credits.csv
```

The continuous DAG should detect the pair, ingest it, execute
transformations and archive the source after success.

For controlled testing, trigger the DAG manually from the Airflow UI or
CLI.

Do not rename arbitrary files to `titles.csv`/`credits.csv` unless their
schema actually satisfies the source contract.

------------------------------------------------------------------------

## 15. Testing failure scenarios

A serious incremental pipeline should be tested with more than a happy
path.

Recommended cases:

1.  valid new source pair;
2.  exact duplicate source pair;
3.  only `titles.csv` present;
4.  only `credits.csv` present;
5.  missing required column;
6.  incompatible datatype drift;
7.  malformed CSV;
8.  transformation failure;
9.  quality-check failure;
10. stale `RUNNING` batch;
11. Airflow retry;
12. scheduler restart;
13. new valid batch after a previous success.

For every test verify:

-   Airflow task state;
-   batch metadata state;
-   warehouse row integrity;
-   checkpoint behaviour;
-   archive behaviour;
-   ability to process the next batch.

------------------------------------------------------------------------

## 16. Power BI layer

Power BI should connect to the analytical schema with a dedicated
read-only PostgreSQL role, not the PostgreSQL superuser.

The BI role should receive only the minimum permissions required,
conceptually:

``` sql
GRANT CONNECT ON DATABASE <database> TO <bi_reader>;
GRANT USAGE ON SCHEMA analytics TO <bi_reader>;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO <bi_reader>;
```

Never commit the role's password.

The BI model should consume the dimensional analytics layer rather than
rebuilding operational transformations in DAX.

------------------------------------------------------------------------

## 17. Design decisions

### Why OLTP and OLAP are separate

OLTP optimizes clean relational representation and integrity. OLAP
optimizes analytical joins, aggregations and BI usability.

### Why surrogate keys

Warehouse surrogate keys decouple analytical entities from source-system
natural keys and leave room for future historical dimension strategies.

### Why no fabricated SCD Type 2 history

The original catalogue extract does not inherently contain repeated
historical observations. Synthetic history would misrepresent the
source. The model is designed so historical strategies can be added when
real temporal data becomes available.

### Why Airflow DAG code stays thin

Orchestration should coordinate work, not become the only place business
logic exists. Reusable Python and SQL modules are easier to test and
reason about.

### Why archive only after success

Moving source files too early can make failed runs difficult to
reproduce. Successful transformation is the boundary for archival.

------------------------------------------------------------------------

## 18. Key lessons

The most important engineering lessons from building the project were:

-   validate network paths before debugging Airflow SQL;
-   distinguish the Airflow metadata database from the warehouse;
-   never assume container `localhost` means host/VM `localhost`;
-   keep DAG IDs, pipeline names and task IDs explicit;
-   XCom consumers must pull from the exact producer task ID;
-   sensors should model "waiting for data," not failures;
-   source identity should use content checksums;
-   checkpoints advance only after successful processing;
-   source archival belongs at the end;
-   test duplicate, malformed and partial batches;
-   avoid destructive metadata resets as a first response to scheduler
    problems;
-   verify CLI syntax against the installed Airflow version;
-   never publish passwords contained in local SQL experiments.

------------------------------------------------------------------------

## 19. Portfolio summary

> Designed and implemented an end-to-end incremental analytics
> engineering pipeline for Netflix catalogue data. Built PostgreSQL
> staging, normalized OLTP and dimensional OLAP layers; implemented
> schema contracts, SHA-256 source fingerprinting, batch metadata,
> stale-run recovery, checkpoints, data-quality gates and automatic
> source archival; orchestrated continuous processing with Apache
> Airflow in Docker; and prepared a read-only analytics layer for Power
> BI.

### Technologies

`Python` · `SQL` · `PostgreSQL` · `Apache Airflow` · `Docker` ·
`Docker Compose` · `OLTP` · `Dimensional Modelling` · `ETL/ELT` ·
`Data Quality` · `Power BI` · `DAX` · `Git`

------------------------------------------------------------------------

## 20. Detailed documentation

See [`docs/PROJECT_DOCUMENTATION.md`](docs/PROJECT_DOCUMENTATION.md) for
the full architecture, build-from-scratch procedure, orchestration
design, troubleshooting guide and lessons learned.
