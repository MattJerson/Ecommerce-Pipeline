# Real-Time Analytics Pipeline

> An end-to-end data engineering project simulating a live e-commerce platform — from raw event ingestion to a monitored, tested, and orchestrated analytics dashboard.

[![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-black?logo=apachekafka)](https://kafka.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.10-orange?logo=dbt)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-lightblue?logo=apacheairflow)](https://airflow.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://docker.com)
[![Metabase](https://img.shields.io/badge/Metabase-Dashboard-509EE3?logo=metabase)](https://metabase.com)
[![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?logo=prometheus)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-Observability-F46800?logo=grafana)](https://grafana.com)
[![CI](https://github.com/MattJerson/Ecommerce-Pipeline/actions/workflows/pipeline_ci.yml/badge.svg)](https://github.com/MattJerson/Ecommerce-Pipeline/actions)

---

## 🔗 Live Demo

👉 **[View Live Dashboard](#)** — hosted on Railway (free tier)

> The dashboard reflects real-time simulated order, click, and signup event data processed through the full pipeline.

---

## 📌 Project Overview

This project builds a production-style data pipeline that processes simulated e-commerce events — orders, page clicks, and user signups — in real time. It demonstrates the full responsibilities of a Data Engineer: ingesting, streaming, storing, transforming, validating, orchestrating, monitoring, and serving data using the modern industry-standard stack.

**Business questions it answers:**
- Which product categories generate the most revenue?
- What is the session-to-order conversion rate by device and referrer?
- How does daily revenue trend across categories?
- What is the order status breakdown at any point in time?

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Python + Faker        Apache Kafka            PostgreSQL           │
│  ──────────────  ──▶  ──────────────  ──▶    ──────────────        │
│  Produces 3 event      Topics:                Raw tables:           │
│  streams at 2/sec      - orders               - raw_orders          │
│                        - page_clicks          - raw_page_clicks     │
│                        - user_signups         - raw_user_signups    │
│                                                       │             │
│                        Great Expectations             │             │
│                        ──────────────────             │             │
│                        Validates raw data  ◀──────────┘             │
│                        before transforms                            │
│                                │                                    │
│                                ▼                                    │
│                           dbt Core                                  │
│                        ──────────────                               │
│                        Staging models:                              │
│                        - stg_orders                                 │
│                        - stg_page_clicks                            │
│                                │                                    │
│                                ▼                                    │
│                          Mart models:                               │
│                        - mart_daily_revenue                         │
│                        - mart_traffic_funnel                        │
│                                │                                    │
│          ┌─────────────────────┴──────────────────┐                │
│          ▼                                         ▼                │
│       Metabase                              Grafana                 │
│   ──────────────                        ──────────────              │
│   Analytics dashboard                   Pipeline monitoring         │
│   - Daily Revenue                       - DB row counts             │
│   - Revenue by Category                 - Active connections        │
│   - Order Status                        - DB size over time         │
│   - Conversion by Device                                            │
│                                                                     │
│  ╔═══════════════════════════════════════════════════════════════╗  │
│  ║  Apache Airflow — orchestrates every stage hourly             ║  │
│  ║  9-task DAG: data gate → validation → dbt → quality check    ║  │
│  ╚═══════════════════════════════════════════════════════════════╝  │
│                                                                     │
│  ╔═══════════════════════════════════════════════════════════════╗  │
│  ║  GitHub Actions CI/CD — runs dbt tests on every push         ║  │
│  ╚═══════════════════════════════════════════════════════════════╝  │
│                                                                     │
│  All services containerised with Docker Compose (9 containers)     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Event Simulation | Python + Faker | Generates realistic orders, clicks, signups at 2 events/sec |
| Streaming | Apache Kafka + Zookeeper | 3 topics with manual offset commits for reliability |
| Storage | PostgreSQL 15 | Raw ingestion tables + dbt-transformed marts |
| Transformation | dbt Core 1.10 | Staging → mart model pattern with 9 data quality tests |
| Data Quality | Great Expectations 1.8 | Validates raw tables before dbt runs |
| Orchestration | Apache Airflow 2.8 | 9-task DAG with data gates, retries, and quality checks |
| Analytics Dashboard | Metabase | 4-chart live dashboard (revenue, categories, funnel, status) |
| Monitoring | Prometheus + Grafana | DB metrics, row counts, active connections, DB size |
| Metrics Collection | StatsD Exporter | Translates Airflow metrics into Prometheus format |
| CI/CD | GitHub Actions | Auto-runs dbt models + tests on every push to main |
| Containerisation | Docker + Compose | 9 services, fully reproducible with one command |
| Deployment | Railway | Free-tier cloud hosting with public URL |

---

## 📁 Project Structure

```
ecommerce-pipeline/
├── producer/
│   └── producer.py              # Kafka producer — 3 topics, weighted event mix
├── consumer/
│   └── consumer.py              # Kafka consumer — manual offset commit, auto table creation
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml      # Source definitions + column tests
│       │   ├── stg_orders.sql
│       │   └── stg_page_clicks.sql
│       └── marts/
│           ├── mart_daily_revenue.sql    # Revenue by day + category
│           └── mart_traffic_funnel.sql   # Conversion rate by page/device
├── airflow/
│   └── dags/
│       └── pipeline_dag.py      # 9-task DAG with data gates + quality checks
├── validation/
│   └── validate.py              # Great Expectations — 11 checks across 3 tables
├── monitoring/
│   ├── prometheus.yml           # Scrape config for Airflow + PostgreSQL
│   └── statsd_mapping.yml       # Airflow → Prometheus metric mapping
├── .github/
│   └── workflows/
│       └── pipeline_ci.yml      # CI: spins up Postgres, seeds data, runs dbt
├── Dockerfile                   # Custom Airflow image with dbt-postgres
├── docker-compose.yml           # 9 services: Kafka, Postgres, Airflow, Metabase, Prometheus, Grafana
├── .env                         # Local environment variables
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Python 3.10+
- Git

### 1. Clone the repository

```bash
git clone https://github.com/MattJerson/Ecommerce-Pipeline.git
cd Ecommerce-Pipeline
```

### 2. Set up environment variables

Create a `.env` file in the root:

```env
POSTGRES_USER=pipeline_user
POSTGRES_PASSWORD=pipeline_pass
POSTGRES_DB=ecommerce
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://pipeline_user:pipeline_pass@postgres/ecommerce
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW_ADMIN_PASSWORD=admin
```

### 3. Start all services

```bash
docker compose up -d
```

This starts 9 containers. Wait about 60 seconds for Airflow to initialize, then verify:

```bash
docker ps
```

### 4. Set up Python environment

```bash
python -m venv venv
source venv/bin/activate   # Mac/Linux
pip install kafka-python psycopg2-binary faker dbt-postgres great_expectations
```

### 5. Start the event producer

Open a terminal and run:

```bash
python producer/producer.py
```

You'll see live events streaming:
```
2026-05-04 10:00:01  INFO  📨  [orders        ]  event_type=order_placed   id=3f9a1b2c…
2026-05-04 10:00:01  INFO  📨  [page_clicks   ]  event_type=page_click     id=7d4e8f1a…
```

### 6. Start the consumer

Open a second terminal:

```bash
python consumer/consumer.py
```

### 7. Run data validation

```bash
python validation/validate.py
```

All 3 tables should show ✅ PASSED.

### 8. Run dbt transformations

```bash
cd dbt_project
dbt run      # builds all 4 models
dbt test     # runs 9 data quality tests
dbt docs serve --port 8081   # opens lineage graph
```

### 9. Access all services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin123 |
| Metabase | http://localhost:3000 | set on first login |
| Grafana | http://localhost:3001 | admin / admin123 |
| Prometheus | http://localhost:9090 | — |

---

## 📊 dbt Data Models

```
raw_orders         raw_page_clicks      raw_user_signups
     │                   │
     ▼                   ▼
stg_orders        stg_page_clicks    ← staging: cleaned, type-cast, nulls handled
     │                   │
     └────────┬──────────┘
              ▼
  mart_daily_revenue                 ← revenue by day + category + payment method
  mart_traffic_funnel                ← conversion rate by page, device, referrer
```

**Data quality tests (9 total):**
- `not_null` on `order_id`, `session_id`, `user_id`
- `unique` on `order_id`, `session_id`
- `accepted_values` on `status`, `payment_method`, `category`, `device`, `signup_method`

---

## 🔍 Data Validation (Great Expectations)

The validation suite runs 11 checks across all 3 raw tables before dbt transforms:

| Check | Table |
|---|---|
| Required columns exist | All 3 |
| No null IDs | All 3 |
| Table has at least 1 row | All 3 |
| `total_amount` > 0 | raw_orders |
| `quantity` between 1–100 | raw_orders |
| No duplicate `order_id` | raw_orders |
| `status` in known values | raw_orders |
| `payment_method` in known values | raw_orders |
| `device` in known values | raw_page_clicks |
| `duration_secs` > 0 | raw_page_clicks |
| Email format valid | raw_user_signups |

If any check fails, the pipeline exits with a non-zero code — blocking dbt from running on bad data.

---

## 🔁 Airflow DAG

The `ecommerce_pipeline` DAG runs every hour with 9 tasks in sequence:

```
start
  └── check_raw_data          ← fails early if no new data in last hour
        └── run_great_expectations
              └── dbt_debug
                    └── dbt_run_staging
                          └── dbt_test_staging
                                └── dbt_run_marts
                                      └── dbt_test_marts
                                            └── check_mart_freshness
                                                  └── dbt_docs_generate
                                                        └── log_success
                                                              └── end
```

Key design decisions: `catchup=False`, `max_active_runs=1`, 2 retries per task.

---

## 📈 Metabase Dashboard

4 charts powered by dbt mart models:

1. **Daily Revenue** — line chart of gross revenue over time
2. **Revenue by Category** — bar chart across Electronics, Sports, Home, Fashion, Books
3. **Order Status Breakdown** — pie chart of pending/confirmed/shipped/delivered/cancelled
4. **Conversion Rate by Device** — bar chart comparing mobile, desktop, tablet

---

## 📡 Monitoring (Prometheus + Grafana)

Grafana dashboard at `http://localhost:3001` tracks:

- Live row counts per table (`pg_stat_user_tables_n_live_tup`)
- Active PostgreSQL connections (`pg_stat_activity_count`)
- Database size in bytes (`pg_database_size_bytes`)
- Row counts across all pipeline tables (bar chart)

---

## 💡 Key Engineering Decisions

**Why Kafka over direct database writes?**
Kafka decouples the producer and consumer — if PostgreSQL goes down, events aren't lost. They queue in the topic and are consumed once the DB recovers. Manual offset commits (`enable_auto_commit=False`) mean no message is marked processed until it's safely written to the DB.

**Why dbt for transformations?**
dbt brings software engineering practices to SQL — version control, testing, and auto-generated documentation. The staging → mart pattern separates cleaning logic from business logic, making models easier to test and maintain independently.

**Why Great Expectations before dbt?**
Running validation before transformations means bad data never reaches the mart layer. A pipeline that fails loudly on bad data is more trustworthy than one that silently produces wrong numbers.

**Why Airflow for orchestration?**
Airflow's DAG model makes dependencies explicit — staging tests must pass before marts run, mart freshness is verified after dbt completes. This mirrors how production pipelines handle task dependencies and failure recovery.

**Why keep everything local + Docker?**
Running everything locally with Docker Compose keeps infrastructure costs at $0 while still demonstrating the same architecture patterns used in cloud deployments. The same `docker-compose.yml` can be deployed to any cloud provider that supports containers.

---

## ☁️ How I Would Scale This in Production

| Current (Local) | Production Version |
|---|---|
| Kafka in Docker | Confluent Cloud or AWS MSK |
| PostgreSQL in Docker | Amazon RDS or Google Cloud SQL |
| dbt Core (local) | dbt Cloud with scheduled jobs |
| Airflow standalone | AWS MWAA or Astronomer |
| Metabase in Docker | Metabase Cloud or Tableau |
| Prometheus + Grafana | Datadog or AWS CloudWatch |
| Manual offset commits | Kafka Connect for scalable ingestion |

---

## 🙋 Author

**Matt Jersonfigueroa**
[LinkedIn](https://linkedin.com/in/yourprofile) · [GitHub](https://github.com/MattJerson)

---

## 📄 License

MIT License — feel free to fork and build on this project.