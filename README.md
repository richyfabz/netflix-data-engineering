# Netflix Incremental Data Engineering Pipeline

An end-to-end, incremental analytics engineering project that turns Netflix catalogue source files into a governed PostgreSQL warehouse and Power BI analytics solution.

The current architecture follows a controlled promotion path:

**CSV source files → Apache Airflow → DEV → UAT OLTP → UAT OLAP → quality gate → PRODUCTION views → Power BI.**

Pipeline metadata, batch history, schema-drift records, quality results and watermarks are maintained separately in the `etl` schema.

The project is designed as more than a one-off CSV-to-dashboard

exercise. It implements repeatable batch ingestion, schema contracts,

source-file fingerprinting, stale-batch recovery, incremental OLTP/OLAP

transformations, data-quality gates, checkpoints, idempotent processing,

automatic source archival, and continuous Airflow orchestration.

> **Security note:** credentials, database passwords and local `.env`

> values must never be committed. Use `.env.example` and Airflow

> connection/environment configuration.

---

## 1. Project objective

The goal is to model a realistic data-engineering workflow around

Netflix catalogue data:

1. Receive a complete `titles.csv` + `credits.csv` source batch.

2. Detect whether the source is new and structurally valid.

3. Register and track the ingestion batch.

4. Load source data into controlled staging/raw structures.

5. Transform it into a normalized OLTP model.

6. Transform the operational model into an analytical dimensional

    model.

7. Validate warehouse integrity and analytical metrics.

8. Commit pipeline checkpoints only after successful processing.

9. Archive processed source files.

10. Expose the analytics schema to Power BI through a read-only database

    account.

11. Repeat automatically when another source batch arrives.

The result demonstrates SQL, Python, PostgreSQL, dimensional modelling,

incremental ETL/ELT, Apache Airflow, Docker, data quality,

orchestration, batch metadata, Power BI and production-style failure

handling.

---

## 2. High-level architecture

The project was refactored from the earlier raw/OLTP/analytics naming into explicit **DEV, UAT and PRODUCTION stages**. This makes the movement of data through the pipeline clearer and separates ingestion, transformation/testing and BI-serving responsibilities.

```text
                    SOURCE SYSTEM
              titles.csv + credits.csv
                         |
                         v
                  data/incoming/
                         |
                         v
                 APACHE AIRFLOW
        sensor / schema validation / checksum
                         |
                         v
+------------------------------------------------------+
| DEV                                                  |
| Validated source-shaped landing/staging data         |
+---------------------------+--------------------------+
                            |
                            v
+------------------------------------------------------+
| UAT_OLTP                                             |
| Normalized relational / 3NF transformation layer    |
+---------------------------+--------------------------+
                            |
                            v
+------------------------------------------------------+
| UAT_OLAP                                             |
| Dimensional analytical model                        |
| dimensions / bridges / facts                        |
+---------------------------+--------------------------+
                            |
                            v
                    DATA QUALITY GATE
                            |
                 pass ------+------ fail
                  |                    |
                  v                    v
+--------------------------------+   stop / record failure
| PRODUCTION                     |
| Stable BI-facing views         |
+---------------+----------------+
                |
                v
             POWER BI
                |
                v
 dashboards / RLS / drill-through / bookmarks / tooltips
```

The `etl` schema operates alongside these stages and stores operational metadata such as batch history, pipeline control/watermarks, quality results and schema-change history.

### Runtime topology

The local development environment spans multiple systems:

```text
Apache Airflow
    |
Docker Desktop on macOS
    |
macOS host / VMware networking
    |
Windows 11 VM
    |
PostgreSQL 18 warehouse
    |
Power BI Desktop
```

This is a **local production-style architecture**. It demonstrates environment separation and controlled data promotion without claiming to be a cloud production deployment.

---

## 3. DEV → UAT → PRODUCTION data stages

### Source

The pipeline expects a complete source pair:

```text
data/incoming/
├── titles.csv
└── credits.csv
```

Both files are treated as one logical batch. Schema validation occurs before the pair is allowed into DEV.

### DEV — validated landing layer

The `dev` schema is the first database stage after source validation. It preserves source-shaped data and acts as the controlled landing area for accepted batches.

Responsibilities include:

- receiving validated title and credit data;
- preserving the ingestion boundary between external files and transformed models;
- carrying the ingestion timestamp created at source ingestion;
- preventing invalid source pairs from reaching downstream layers.

### UAT_OLTP — normalized relational layer

`uat_oltp` transforms DEV data into the normalized relational/3NF model.

Core tables include:

```text
uat_oltp.title
uat_oltp.person
uat_oltp.genre
uat_oltp.country
uat_oltp.credit
uat_oltp.title_genre
uat_oltp.title_country
```

`title_genre` and `title_country` resolve many-to-many relationships instead of storing repeating values inside the title record.

### UAT_OLAP — dimensional analytical layer

`uat_olap` converts the normalized model into the dimensional structures used for analytical validation and downstream BI serving.

```text
uat_olap.dim_title
uat_olap.dim_person
uat_olap.dim_genre
uat_olap.dim_country
uat_olap.dim_date

uat_olap.bridge_title_genre
uat_olap.bridge_title_country

uat_olap.fact_credits
uat_olap.fact_title_metrics
```

`fact_credits` acts as a relationship/factless fact between people and titles. `fact_title_metrics` stores title-level analytical metrics such as IMDb score, IMDb votes, TMDb popularity and TMDb score.

Surrogate keys such as `title_sk`, `person_sk`, `genre_sk`, `country_sk` and `date_sk` are used for analytical relationships.

### Quality gate

Data is not promoted to the BI-facing production contract merely because transformation completed. The pipeline validates structural integrity, keys, relationships and analytical metrics first.

A failed quality gate prevents the successful checkpoint from advancing.

### PRODUCTION — BI-serving layer

The `production` schema is the stable reporting contract exposed to Power BI through purpose-built views.

Known production views include:

```text
production.title_overview
production.genre_performance
production.country_performance
production.top_people
production.pipeline_health
production.country_top_titles
```

`production.country_top_titles` was added specifically to provide the correct grain for the country-level Top Titles/poster experience.

Power BI can also use the required `uat_olap` dimensional tables for semantic-model relationships, while production views provide clean BI-facing outputs for reporting use cases.

### ETL control plane

The `etl` schema is not another business-data promotion stage. It is the pipeline control plane.

Important metadata objects include:

```text
etl.batch_history
etl.pipeline_control
etl.quality_results
etl.schema_change_history
```

They track batches, checksums, processing state, quality outcomes, schema drift and the last successful watermark.

---

## Architecture refactor

The current schema architecture replaced the earlier staging/public/analytics/serving naming with:

```text
dev
uat_oltp
uat_olap
production
etl
```

The main refactor migration is:

```text
sql/00_migrations/20260829_dev_uat_prod_refactor.sql
```

This refactor was validated end-to-end before the Power BI layer was finalized.

---

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

---

## 5. Schema contract and drift detection

Incoming files are checked against an explicit schema contract before

they are accepted.

Expected title fields include:

```text

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

```text

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

---

## 6. Source fingerprinting and idempotency

A SHA-256 checksum is calculated from source-file bytes.

This enables the ingestion layer to distinguish:

```text

same filename + same content      -> duplicate

same filename + different content -> new content

new filename + known content      -> duplicate content

```

The exact implementation rules are controlled by the batch-ingestion

layer, but the principle is that processing identity should be based on

data, not only filenames.

This is essential for safe retries and continuous scheduling.

---

## 7. Airflow orchestration

The final orchestration uses a continuous DAG identity separate from the

ETL pipeline metadata name.

Conceptually:

```python

DAG_ID = "netflix_incremental_pipeline_continuous"

PIPELINE_NAME = "netflix_incremental_pipeline"

```

This keeps Airflow scheduling identity separate from persistent ETL

lineage.

The effective pipeline flow is:

```text
wait_for_source_batch
        |
        v
recover_stale_batches
        |
        v
schema validation + checksum
        |
        v
DEV ingestion
        |
        v
UAT_OLTP synchronization
        |
        v
UAT_OLAP synchronization
        |
        v
quality checks
        |
        v
batch completion / watermark
        |
        v
archive source files
        |
        v
PRODUCTION views
        |
        v
Power BI
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

---

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

---

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

```text

Movies + Shows = Total Titles

```

Avoid hard-coding a historical row-count baseline unless the source

dataset is intentionally frozen.

---

## 10. Repository structure

A representative project layout is:

```text

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

---

## 11. Environment variables and secrets

Never hard-code database passwords in DAGs, Python modules, SQL scripts

or documentation.

Example:

```dotenv

AIRFLOW_UID=50000

AIRFLOW_IMAGE_NAME=netflix-airflow\:local

NETFLIX_PG_HOST=\<WINDOWS_VM_REACHABLE_IP>

NETFLIX_PG_PORT=5432

NETFLIX_PG_DATABASE=postgres

NETFLIX_PG_USER=\<POSTGRES_USER>

NETFLIX_PG_PASSWORD=\<POSTGRES_PASSWORD>

NETFLIX_PG_SSLMODE=prefer

NETFLIX_AIRFLOW_CONN_ID=netflix_postgres

```

Before publishing the repository, search the full Git history and

working tree for credentials. If a real password was ever committed,

rotate it; deleting it only from the latest file is not sufficient.

---

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

```bash

git clone \<repository-url>

cd netflix-data-engineering

cp .env.example .env

```

Fill `.env` locally. Do not commit it.

### Validate Docker Compose

```bash

docker compose config

```

### Build and start

Use the commands defined by the repository's Compose/Makefile setup,

commonly:

```bash

docker compose build

docker compose up -d

docker compose ps

```

### Confirm DAG import

```bash

docker compose exec airflow-scheduler airflow dags list

```

Check import errors using the Airflow CLI command supported by the

installed Airflow version or through the UI.

---

## 13. Database connectivity

If PostgreSQL runs in a Windows VMware guest, first obtain its reachable

IPv4 address.

From macOS:

```bash

nc -vz "$NETFLIX_PG_HOST" "$NETFLIX_PG_PORT"

```

Then prove Docker/container connectivity separately.

Finally prove an Airflow PostgreSQL connection with a read-only query

such as:

```sql

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

---

## 14. Running a batch

Place a complete pair in:

```text

data/incoming/titles.csv

data/incoming/credits.csv

```

The continuous DAG should detect the pair, ingest it, execute

transformations and archive the source after success.

For controlled testing, trigger the DAG manually from the Airflow UI or

CLI.

Do not rename arbitrary files to `titles.csv`/`credits.csv` unless their

schema actually satisfies the source contract.

---

## 15. Testing failure scenarios

A serious incremental pipeline should be tested with more than a happy

path.

Recommended cases:

1. valid new source pair;

2. exact duplicate source pair;

3. only `titles.csv` present;

4. only `credits.csv` present;

5. missing required column;

6. incompatible datatype drift;

7. malformed CSV;

8. transformation failure;

9. quality-check failure;

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

---

## 16. Power BI analytics layer

Power BI is the presentation and self-service analytics layer of the project. It sits after the PRODUCTION serving stage: purpose-built `production` views provide the stable BI contract, while the required `uat_olap` dimensions, bridges and facts support the semantic model and relationship-driven analysis. Business transformations remain in PostgreSQL rather than being rebuilt repeatedly inside report visuals.

### Report pages

The finished Power BI report contains six pages:

| Page | Purpose |
|---|---|
| **Executive Overview** | High-level catalogue KPIs, trends and distribution analysis |
| **Content Analysis** | Deeper analysis of genres, ratings, countries, actors and directors |
| **Geography & People** | Geographic distribution and cast/crew analysis |
| **Country Details** | Drill-through analysis for a selected country |
| **Country Tooltip** | Contextual country-level information |
| **Title Tooltip** | Contextual title-level information |

### Executive Overview

The Executive Overview provides the high-level catalogue view. It combines KPIs with interactive visuals covering total titles, IMDb/TMDb performance, votes, popularity, release activity, genre distribution, country contribution and movie/show distribution.

### Content Analysis

The Content Analysis page explores catalogue composition and performance through country, audience-rating, genre, director and actor analysis.

### Geography & People

This page combines geographic analysis with cast and crew analysis. It includes:

- country ranking by title count;
- interactive world map;
- credits by role;
- top credited people;
- top country and top contributor KPIs;
- total countries and total cast/crew KPIs;
- filters for release year, type, genre and country.

Bookmark navigation switches the central analytical area between **Country Insights**, **World Map** and **Search Country Details**.

### Search Country Details and drill-through

The Search Country Details view provides a dedicated country-selection workflow:

```text
Search/select country
        |
        v
Selected country context
        |
        v
View Country Details
        |
        v
Country Details drill-through page
```

The selected `country_name` is passed into the Country Details page, so users do not need to apply the same filter again.

### Country Details

For the selected country, the drill-through page presents:

- country name;
- total titles;
- average TMDb score;
- average IMDb score;
- top contributor;
- content released over time;
- top-rated title posters;
- top genres;
- movie vs. show distribution.

### Tooltips

Dedicated tooltip pages add context without overcrowding the main dashboard.

- **Country Tooltip** provides country-level metrics such as titles, scores and top contributor.
- **Title Tooltip** contains title-level fields such as title, type, release year, IMDb score and TMDb score.

### Reusable measures

The semantic model uses reusable measures across report pages, including:

- `Total Titles`
- `Average IMDb Score`
- `Average TMDb Scores`
- `Averagee TMDb Popularity`
- `Total IMDb Votes`
- `Countries`
- `Total Cast & Crew`
- `Titles Credited`
- `Titles by Genre`
- `Titile by Country`
- `Top Country`
- `Top Contributor`

> Measure names above preserve the names used in the finished PBIX model.

### Dynamic Row-Level Security

The report implements dynamic country-level Row-Level Security (RLS).

A dedicated `RLS_UserCountryAccess` mapping table associates a user identity with one or more permitted `country_sk` values.

```text
USERPRINCIPALNAME()
        |
        v
RLS_UserCountryAccess
        |
        v
country_sk
        |
        v
dim_country
        |
        v
bridge_title_country
        |
        v
dim_title
        |
        v
facts / credits / people
```

The role filters the access table using the signed-in user's principal name. Security-filter propagation then carries the permitted country set through the semantic model.

Testing confirmed that:

- one user can be assigned multiple countries;
- different users see different permitted country sets in the same report;
- country KPIs and rankings respect the restriction;
- title, credit and contributor visuals also respond to the security context.

This makes the RLS implementation model-driven rather than a visible slicer pretending to provide security.

### Power BI database access

Power BI should connect to the analytical schema with a dedicated read-only PostgreSQL role rather than the PostgreSQL superuser.

```sql
GRANT CONNECT ON DATABASE <database> TO <bi_reader>;
GRANT USAGE ON SCHEMA analytics TO <bi_reader>;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO <bi_reader>;
```

The role should receive only the permissions required for reporting, and its password must never be committed.

### Interaction design

The final report combines:

- slicers;
- bookmark navigation;
- drill-through;
- tooltip pages;
- reset actions;
- cross-filtering;
- country search;
- poster-based title exploration;
- dynamic RLS.

### Power BI validation

The BI layer was validated by:

1. testing report filters and visual interactions;
2. confirming bookmark-driven views;
3. checking country search and drill-through context;
4. checking tooltip behaviour;
5. testing RLS with different simulated user identities;
6. validating multi-country access;
7. confirming that RLS propagates into titles, credits and contributor analysis.

The result is an interactive analytical application on top of the warehouse rather than a static collection of charts.

---

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

---

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

---

## 19. Portfolio summary

> Designed and implemented an end-to-end incremental analytics engineering pipeline for Netflix catalogue data. Built PostgreSQL staging, normalized OLTP and dimensional OLAP layers; implemented schema contracts, SHA-256 source fingerprinting, batch metadata, stale-run recovery, checkpoints, data-quality gates and automatic source archival; and orchestrated continuous processing with Apache Airflow in Docker.
>
> Built the Power BI analytical layer on top of the warehouse with reusable measures, interactive report pages, bookmarks, tooltips, country search, drill-through navigation and dynamic country-level Row-Level Security. Validated that user-specific country access propagates through titles, credits, contributors and related report metrics.

### Technologies

`Python` · `SQL` · `PostgreSQL` · `Apache Airflow` · `Docker` · `Docker Compose` · `OLTP` · `Dimensional Modelling` · `ETL/ELT` · `Data Quality` · `Power BI` · `DAX` · `Dynamic RLS` · `Git`

---

## 20. Power BI portfolio assets

For a public GitHub portfolio, add screenshots and a short demo under `docs/`:

```text
docs/
├── images/
│   ├── executive-overview.png
│   ├── content-analysis.png
│   ├── geography-people.png
│   ├── country-details.png
│   ├── data-model.png
│   └── dynamic-rls-test.png
└── demo/
    └── powerbi-walkthrough.mp4
```

A useful demo sequence is:

1. Executive Overview and report filtering.
2. Content Analysis.
3. Geography & People.
4. Country Insights / World Map / Search Country Details bookmark navigation.
5. Country search and drill-through into Country Details.
6. Tooltip interactions.
7. Dynamic RLS comparison between users with different permitted countries.

Do not include credentials, private account details or sensitive connection information in screenshots or recordings.

---

## 21. Detailed documentation

See [`docs/PROJECT_DOCUMENTATION.md`](docs/PROJECT_DOCUMENTATION.md) for the full architecture, build-from-scratch procedure, orchestration design, troubleshooting guide and lessons learned.
