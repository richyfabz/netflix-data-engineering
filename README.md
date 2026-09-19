**# Netflix Incremental Data Engineering Pipeline**

An end-to-end, incremental analytics engineering project that turns

Netflix catalogue source files into a governed PostgreSQL warehouse and

analytics model, orchestrated by Apache Airflow and prepared for Power

BI consumption.

The project is designed as more than a one-off CSV-to-dashboard

exercise. It implements repeatable batch ingestion, schema contracts,

source-file fingerprinting, stale-batch recovery, incremental OLTP/OLAP

transformations, data-quality gates, checkpoints, idempotent processing,

automatic source archival, and continuous Airflow orchestration.

\> \*\*Security note:\*\* credentials, database passwords and local \`.env\`

\> values must never be committed. Use \`.env.example\` and Airflow

\> connection/environment configuration.

**------------------------------------------------------------------------**

**## 1. Project objective**

The goal is to model a realistic data-engineering workflow around

Netflix catalogue data:

1\.  Receive a complete \`titles.csv\` + \`credits.csv\` source batch.

2\.  Detect whether the source is new and structurally valid.

3\.  Register and track the ingestion batch.

4\.  Load source data into controlled staging/raw structures.

5\.  Transform it into a normalized OLTP model.

6\.  Transform the operational model into an analytical dimensional

    model.

7\.  Validate warehouse integrity and analytical metrics.

8\.  Commit pipeline checkpoints only after successful processing.

9\.  Archive processed source files.

10\. Expose the analytics schema to Power BI through a read-only database

    account.

11\. Repeat automatically when another source batch arrives.

The result demonstrates SQL, Python, PostgreSQL, dimensional modelling,

incremental ETL/ELT, Apache Airflow, Docker, data quality,

orchestration, batch metadata, Power BI and production-style failure

handling.

**------------------------------------------------------------------------**

**## 2. High-level architecture**

\`\`\` text

                     SOURCE SYSTEM

               titles.csv + credits.csv

                         |

                         v

                data/incoming/

                         |

                         v

              +---------------------+

              \| Apache Airflow DAG  |

              \| source-file sensor  |

              +----------+----------+

                         |

                         v

               stale batch recovery

                         |

                         v

               source batch ingestion

                 /       |       \\

                /        |        \\

        schema contract checksum  batch tracking

                \        |        /

                 \       |       /

                         v

                 PostgreSQL RAW/STAGING

                         |

                         v

                  OLTP / 3NF MODEL

      title -- credit -- person -- genre -- country

        \|                    |

        +-- title\_genre -----+

        +-- title\_country ---+

                         |

                         v

                   OLAP / ANALYTICS

              +----------------------+

              \| dim\_title            |

              \| dim\_person           |

              \| dim\_genre            |

              \| bridge\_title\_genre   |

              \| fact\_credits         |

              \| fact\_title\_metrics   |

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

\`\`\`

**### Runtime topology**

The development environment can span multiple systems:

\`\`\` text

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

\`\`\`

\`localhost\` inside an Airflow container is the container itself, not a

PostgreSQL instance running inside a Windows VM. Always use the VM

address reachable from Docker.

**------------------------------------------------------------------------**

**## 3. Data layers**

**### Source layer**

The pipeline expects a complete pair:

\`\`\` text

data/incoming/

├── titles.csv

└── credits.csv

\`\`\`

A source batch is not processed until both files exist.

**### Staging/raw layer**

The raw tables preserve source-shaped data before business

transformations. The project uses \`raw\_title\` and \`raw\_credit\` as

landing structures. This provides a controlled boundary between external

files and the relational model.

**### OLTP layer**

The operational model is normalized to reduce duplication and correctly

represent many-to-many relationships.

Core entities include:

\-   \`title\`

\-   \`person\`

\-   \`credit\`

\-   \`genre\`

\-   \`country\`

\-   \`title\_genre\`

\-   \`title\_country\`

\`title\_genre\` and \`title\_country\` resolve many-to-many relationships

instead of storing repeating values inside a title record.

**### OLAP layer**

The \`analytics\` schema is designed for analytical querying and BI.

Key objects include:

\-   \`analytics.dim\_title\`

\-   \`analytics.dim\_person\`

\-   \`analytics.dim\_genre\`

\-   \`analytics.bridge\_title\_genre\`

\-   \`analytics.fact\_credits\`

\-   \`analytics.fact\_title\_metrics\`

\`fact\_credits\` behaves as a factless/relationship fact: the existence of

a row records that a person was credited on a title in a particular

role. \`fact\_title\_metrics\` stores measurable title-level values such as

IMDb score, IMDb votes, TMDB popularity and TMDB score.

Surrogate keys decouple analytical relationships from source natural

keys.

**------------------------------------------------------------------------**

**## 4. Incremental processing design**

The project evolved from a static warehouse load into a repeatable

incremental pipeline.

Each ingestion run works with explicit batch identifiers for the titles

and credits files. Those IDs are propagated through the pipeline rather

than asking downstream tasks to guess which batch is newest.

Important behaviours include:

\-   source fingerprinting/checksums;

\-   duplicate source detection;

\-   schema validation before ingestion;

\-   batch status tracking;

\-   stale \`RUNNING\` batch recovery;

\-   incremental transformation;

\-   checkpoint management;

\-   retry-safe orchestration;

\-   source archival only after success.

A failed run must not falsely advance the successful processing

checkpoint.

**------------------------------------------------------------------------**

**## 5. Schema contract and drift detection**

Incoming files are checked against an explicit schema contract before

they are accepted.

Expected title fields include:

\`\`\` text

id

title

type

description

release\_year

age\_certification

runtime

genres

production\_countries

seasons

imdb\_id

imdb\_score

imdb\_votes

tmdb\_popularity

tmdb\_score

\`\`\`

Expected credit fields include:

\`\`\` text

person\_id

id

name

character

role

\`\`\`

The contract distinguishes required columns and expected logical

datatypes.

This protects the pipeline from silent failures caused by:

\-   renamed columns;

\-   removed required columns;

\-   unexpected required fields;

\-   incompatible datatype changes;

\-   malformed source structures.

Schema changes can be recorded in pipeline metadata so accepted and

blocked changes are auditable.

**------------------------------------------------------------------------**

**## 6. Source fingerprinting and idempotency**

A SHA-256 checksum is calculated from source-file bytes.

This enables the ingestion layer to distinguish:

\`\`\` text

same filename + same content      -> duplicate

same filename + different content -> new content

new filename + known content      -> duplicate content

\`\`\`

The exact implementation rules are controlled by the batch-ingestion

layer, but the principle is that processing identity should be based on

data, not only filenames.

This is essential for safe retries and continuous scheduling.

**------------------------------------------------------------------------**

**## 7. Airflow orchestration**

The final orchestration uses a continuous DAG identity separate from the

ETL pipeline metadata name.

Conceptually:

\`\`\` python

DAG\_ID = "netflix\_incremental\_pipeline\_continuous"

PIPELINE\_NAME = "netflix\_incremental\_pipeline"

\`\`\`

This keeps Airflow scheduling identity separate from persistent ETL

lineage.

The production dependency chain is:

\`\`\` text

wait\_for\_source\_batch

        |

        v

recover\_stale\_batches

        |

        v

ingest\_new\_batch

        |

        v

resolve\_latest\_batches

        |

        v

run\_transformation\_pipeline

        |

        v

archive\_source\_files

\`\`\`

**### Source sensor**

The sensor checks for both required files and waits rather than

repeatedly failing the DAG when no data exists.

**### Stale recovery**

Before processing a new batch, abandoned batches left in \`RUNNING\` state

beyond the configured threshold are recovered/failed.

**### Ingestion**

The ingestion task validates and registers the exact source batch. If no

valid new batch exists, the task can skip cleanly instead of classifying

"no data" as a system failure.

**### Batch-ID propagation**

The ingestion result is passed through Airflow XCom. Downstream

processing uses those exact IDs. Task IDs used by \`xcom\_pull()\` must

match the Airflow task definitions exactly.

**### Transformation**

The orchestration layer calls the production transformation coordinator.

The DAG remains thin while substantial business/database logic stays in

reusable Python/SQL modules.

**### Archival**

Files are moved out of \`data/incoming/\` only after the transformation

pipeline completes successfully.

**------------------------------------------------------------------------**

**## 8. Reliability controls**

The DAG includes production-style controls:

\-   \`catchup=False\`

\-   one active DAG run at a time

\-   retries

\-   exponential retry backoff

\-   maximum retry delays

\-   task execution timeouts

\-   DAG run timeout

\-   rescheduling sensor behaviour

\-   clean skip semantics when no new batch exists

\`max\_active\_runs=1\` protects a shared warehouse from concurrent mutation

by overlapping pipeline runs.

**------------------------------------------------------------------------**

**## 9. Data quality**

Quality validation belongs inside the pipeline, not only in exploratory

notebooks.

Typical checks include:

**### Structural checks**

\-   required tables exist;

\-   required columns exist;

\-   schema contracts match expected source structure.

**### Key checks**

\-   surrogate keys are not null;

\-   surrogate keys are unique;

\-   foreign keys resolve;

\-   bridge/fact rows do not contain orphan references.

**### Metric checks**

\-   populated IMDb scores are within the valid rating domain;

\-   vote counts are non-negative;

\-   title counts are non-zero;

\-   expected title types remain interpretable.

**### Reconciliation checks**

Where the source/business rules allow it:

\`\`\` text

Movies + Shows = Total Titles

\`\`\`

Avoid hard-coding a historical row-count baseline unless the source

dataset is intentionally frozen.

**------------------------------------------------------------------------**

**## 10. Repository structure**

A representative project layout is:

\`\`\` text

netflix-data-engineering/

├── dags/

│   ├── 00\_airflow\_healthcheck.py

│   └── netflix\_incremental\_pipeline.py

├── src/

│   ├── database.py

│   ├── ingestion.py

│   ├── schema\_contract.py

│   ├── batch\_tracker.py

│   ├── source\_batch\_runner.py

│   └── pipeline\_runner.py

├── data/

│   ├── incoming/

│   │   └── .gitkeep

│   ├── raw/

│   ├── archive/

│   │   └── .gitkeep

│   ├── backup\_original/

│   └── Tampered file/

├── existing\_sql/

├── notebooks/

├── docs/

│   └── PROJECT\_DOCUMENTATION.md

├── logs/

├── config/

├── powerbi/

├── Dockerfile

├── docker-compose.yaml

├── requirements.txt

├── .env.example

├── .gitignore

└── README.md

\`\`\`

The exact local tree may contain additional SQL scripts, notebooks and

validation assets.

**------------------------------------------------------------------------**

**## 11. Environment variables and secrets**

Never hard-code database passwords in DAGs, Python modules, SQL scripts

or documentation.

Example:

\`\`\` dotenv

AIRFLOW\_UID=50000

AIRFLOW\_IMAGE\_NAME=netflix-airflow\:local

NETFLIX\_PG\_HOST=\<WINDOWS\_VM\_REACHABLE\_IP>

NETFLIX\_PG\_PORT=5432

NETFLIX\_PG\_DATABASE=postgres

NETFLIX\_PG\_USER=\<POSTGRES\_USER>

NETFLIX\_PG\_PASSWORD=\<POSTGRES\_PASSWORD>

NETFLIX\_PG\_SSLMODE=prefer

NETFLIX\_AIRFLOW\_CONN\_ID=netflix\_postgres

\`\`\`

Before publishing the repository, search the full Git history and

working tree for credentials. If a real password was ever committed,

rotate it; deleting it only from the latest file is not sufficient.

**------------------------------------------------------------------------**

**## 12. Local setup**

**### Prerequisites**

\-   Git

\-   Python 3

\-   PostgreSQL

\-   Docker Desktop

\-   Docker Compose V2

\-   Apache Airflow 3.x-compatible project image

\-   PostgreSQL Airflow provider

\-   Power BI Desktop if reproducing the BI layer

\-   VMware Fusion only if using the same Mac/Windows topology

**### Clone and configure**

\`\`\` bash

git clone \<repository-url>

cd netflix-data-engineering

cp .env.example .env

\`\`\`

Fill \`.env\` locally. Do not commit it.

**### Validate Docker Compose**

\`\`\` bash

docker compose config

\`\`\`

**### Build and start**

Use the commands defined by the repository's Compose/Makefile setup,

commonly:

\`\`\` bash

docker compose build

docker compose up -d

docker compose ps

\`\`\`

**### Confirm DAG import**

\`\`\` bash

docker compose exec airflow-scheduler airflow dags list

\`\`\`

Check import errors using the Airflow CLI command supported by the

installed Airflow version or through the UI.

**------------------------------------------------------------------------**

**## 13. Database connectivity**

If PostgreSQL runs in a Windows VMware guest, first obtain its reachable

IPv4 address.

From macOS:

\`\`\` bash

nc -vz "$NETFLIX\_PG\_HOST" "$NETFLIX\_PG\_PORT"

\`\`\`

Then prove Docker/container connectivity separately.

Finally prove an Airflow PostgreSQL connection with a read-only query

such as:

\`\`\` sql

SELECT 1;

\`\`\`

Only enable warehouse mutation after all connectivity layers succeed.

Typical causes of failure:

\-   VM address changed;

\-   wrong VMware NAT/bridged mode;

\-   Windows Defender Firewall blocks 5432;

\-   PostgreSQL \`listen\_addresses\` does not expose the required

    interface;

\-   \`pg\_hba.conf\` does not permit the client;

\-   Airflow is pointed at \`localhost\`;

\-   credentials refer to the wrong PostgreSQL instance.

**------------------------------------------------------------------------**

**## 14. Running a batch**

Place a complete pair in:

\`\`\` text

data/incoming/titles.csv

data/incoming/credits.csv

\`\`\`

The continuous DAG should detect the pair, ingest it, execute

transformations and archive the source after success.

For controlled testing, trigger the DAG manually from the Airflow UI or

CLI.

Do not rename arbitrary files to \`titles.csv\`/\`credits.csv\` unless their

schema actually satisfies the source contract.

**------------------------------------------------------------------------**

**## 15. Testing failure scenarios**

A serious incremental pipeline should be tested with more than a happy

path.

Recommended cases:

1\.  valid new source pair;

2\.  exact duplicate source pair;

3\.  only \`titles.csv\` present;

4\.  only \`credits.csv\` present;

5\.  missing required column;

6\.  incompatible datatype drift;

7\.  malformed CSV;

8\.  transformation failure;

9\.  quality-check failure;

10\. stale \`RUNNING\` batch;

11\. Airflow retry;

12\. scheduler restart;

13\. new valid batch after a previous success.

For every test verify:

\-   Airflow task state;

\-   batch metadata state;

\-   warehouse row integrity;

\-   checkpoint behaviour;

\-   archive behaviour;

\-   ability to process the next batch.

**------------------------------------------------------------------------**

**## 16. Power BI analytics layer**

Power BI is the presentation and self-service analytics layer of the project.
It consumes the dimensional PostgreSQL analytics model rather than recreating
the warehouse transformations inside the report.

The finished report contains six pages:

- `Executive Overview`
- `Content Analysis`
- `Geography & People`
- `Country Details`
- `Country Tooltip`
- `Title Tooltip`

The first four are analytical report pages, while the final two provide
context-sensitive tooltip experiences.

### Executive Overview

The Executive Overview provides the high-level catalogue view. It includes
summary KPIs and interactive analysis of:

- total titles;
- average IMDb score;
- average TMDb score;
- total IMDb votes;
- average TMDb popularity;
- release activity over time;
- genre distribution;
- country contribution;
- movie vs. show distribution.

Release year, content type and other report slicers allow the overview to be
explored without changing the underlying model.

### Content Analysis

The Content Analysis page moves from the overall catalogue into content
composition and performance. Its visuals include:

- countries by total titles;
- titles by audience rating;
- top genres by average IMDb rating;
- top directors by titles;
- top actors by titles;
- catalogue-level score and volume KPIs.

This page is designed to answer questions about what kinds of content exist in
the catalogue and which genres, people and countries contribute most strongly
to it.

### Geography & People

The Geography & People page combines geographic analysis with cast and crew
analysis. It includes:

- country ranking by title count;
- an interactive world map;
- credits split by role;
- top credited people;
- top country;
- top contributor;
- total countries;
- total cast and crew.

The page supports filtering by release year, type, genre and country.

A bookmark-driven navigation control switches between `Country Insights`,
`World Map` and `Search Country Details` views without requiring separate
report pages for each interaction.

### Country search and drill-through

The `Search Country Details` view provides a dedicated country-selection
workflow. A user can select a country and open the Country Details page using
the `View Country Details` action.

Country context is preserved through drill-through so the detail page opens
for the selected country instead of requiring the user to filter the page
again.

### Country Details

The Country Details page is a drill-through analysis page. For the selected
country it presents:

- country name;
- total titles;
- average TMDb score;
- average IMDb score;
- top contributor;
- content released over time;
- top-rated title posters;
- top genres;
- movie vs. show distribution.

This turns the broader geographic dashboard into a focused country-level
analysis experience.

### Report tooltips

Two dedicated tooltip pages provide additional context without overcrowding
the main report:

- `Country Tooltip` surfaces country-level KPIs such as title count, scores and
  top contributor.
- `Title Tooltip` surfaces title-level context such as title, type, release
  year, IMDb score and TMDb score.

### Measures

The semantic model uses reusable measures for report calculations. Measures
observed in the finished report include:

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
- title-level IMDb/TMDb score measures

The report keeps analytical logic reusable across pages rather than embedding
separate calculations into individual visuals.

### Dynamic row-level security

The report implements dynamic country-level Row-Level Security (RLS).

A dedicated `RLS_UserCountryAccess` mapping table associates user email
addresses with permitted `country_sk` values. That access mapping is related
to the country dimension, allowing the permitted country set to propagate
through the analytical model.

This design supports both:

- users with access to one country; and
- users with access to multiple countries.

RLS testing confirmed that the same report can produce different geographic
views for different users. The security context also propagates beyond the
country visual itself: title counts, cast and crew metrics, contributors,
credit-role analysis and other dependent visuals respond to the permitted
country set.

This is important because RLS is enforced through the semantic model rather
than simulated with a visible report slicer.

### Power BI security and database access

Power BI should connect to the analytical schema with a dedicated read-only
PostgreSQL role, not the PostgreSQL superuser.

Conceptually:

```sql
GRANT CONNECT ON DATABASE <database> TO <bi_reader>;
GRANT USAGE ON SCHEMA analytics TO <bi_reader>;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO <bi_reader>;
```

The BI role should receive only the minimum permissions required, and its
password must never be committed.

### Interaction design

The final report uses several Power BI interaction patterns together:

- slicers for report exploration;
- bookmark navigation for switching analytical views;
- drill-through for country-level investigation;
- dedicated tooltip pages;
- reset actions;
- cross-filtering between visuals;
- a country search workflow;
- poster-based title exploration.

The goal is not only to display metrics but to allow a user to move naturally
from catalogue-level questions to geographic, people and title-level detail.

### Power BI validation

The BI layer was tested for both analytical behaviour and security behaviour.

Validation included:

- confirming country selections filter dependent visuals;
- confirming drill-through preserves the selected country;
- confirming country search opens the expected detail context;
- testing bookmark-driven views;
- checking tooltip behaviour;
- testing dynamic RLS with different user identities;
- confirming multi-country access behaves as expected;
- confirming RLS filters related title, credit and contributor analysis.

The final result is an interactive analytical application on top of the
warehouse rather than a static collection of charts.

**------------------------------------------------------------------------**

**## 17. Design decisions**

**### Why OLTP and OLAP are separate**

OLTP optimizes clean relational representation and integrity. OLAP

optimizes analytical joins, aggregations and BI usability.

**### Why surrogate keys**

Warehouse surrogate keys decouple analytical entities from source-system

natural keys and leave room for future historical dimension strategies.

**### Why no fabricated SCD Type 2 history**

The original catalogue extract does not inherently contain repeated

historical observations. Synthetic history would misrepresent the

source. The model is designed so historical strategies can be added when

real temporal data becomes available.

**### Why Airflow DAG code stays thin**

Orchestration should coordinate work, not become the only place business

logic exists. Reusable Python and SQL modules are easier to test and

reason about.

**### Why archive only after success**

Moving source files too early can make failed runs difficult to

reproduce. Successful transformation is the boundary for archival.

**------------------------------------------------------------------------**

**## 18. Key lessons**

The most important engineering lessons from building the project were:

\-   validate network paths before debugging Airflow SQL;

\-   distinguish the Airflow metadata database from the warehouse;

\-   never assume container \`localhost\` means host/VM \`localhost\`;

\-   keep DAG IDs, pipeline names and task IDs explicit;

\-   XCom consumers must pull from the exact producer task ID;

\-   sensors should model "waiting for data," not failures;

\-   source identity should use content checksums;

\-   checkpoints advance only after successful processing;

\-   source archival belongs at the end;

\-   test duplicate, malformed and partial batches;

\-   avoid destructive metadata resets as a first response to scheduler

    problems;

\-   verify CLI syntax against the installed Airflow version;

\-   never publish passwords contained in local SQL experiments.

**------------------------------------------------------------------------**

**## 19. Portfolio summary**

> Designed and implemented an end-to-end incremental analytics engineering
> pipeline for Netflix catalogue data. Built PostgreSQL staging, normalized
> OLTP and dimensional OLAP layers; implemented schema contracts, SHA-256
> source fingerprinting, batch metadata, stale-run recovery, checkpoints,
> data-quality gates and automatic source archival; and orchestrated continuous
> processing with Apache Airflow in Docker.
>
> Built the Power BI analytical layer on top of the warehouse with reusable
> measures, interactive report pages, bookmarks, tooltips, country search,
> drill-through navigation and dynamic country-level Row-Level Security.
> Validated that user-specific country access propagates through titles,
> credits, contributors and related report metrics.

**### Technologies**

\`Python\` · \`SQL\` · \`PostgreSQL\` · \`Apache Airflow\` · \`Docker\` ·

\`Docker Compose\` · \`OLTP\` · \`Dimensional Modelling\` · \`ETL/ELT\` ·

\`Data Quality\` · \`Power BI\` · \`DAX\` · \`Git\`

**------------------------------------------------------------------------**


**## 20. Portfolio evidence**

Recommended repository assets for the finished project:

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

The most useful demo sequence is:

1. open the Executive Overview;
2. demonstrate report filtering;
3. open Geography & People;
4. switch between Country Insights, World Map and Search Country Details;
5. search/select a country and drill through to Country Details;
6. demonstrate title/country tooltip behaviour;
7. finish with a short dynamic-RLS comparison showing different permitted
   countries for different users.

Avoid committing credentials, connection secrets or screenshots that expose
private account information.

**------------------------------------------------------------------------**

**## 21. Detailed documentation**


See [\`docs/PROJECT\_DOCUMENTATION.md\`]\(docs/PROJECT\_DOCUMENTATION.md) for

the full architecture, build-from-scratch procedure, orchestration

design, troubleshooting guide and lessons learned.