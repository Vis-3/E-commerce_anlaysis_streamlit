# E-Commerce Analytics Platform

A real-time analytics platform built with a modern data engineering stack. Streams live events through Kafka, processes them into PostgreSQL, serves insights via a FastAPI backend with Redis caching, and visualises everything in a Streamlit dashboard.

---

## Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 15 (star schema, partitioned tables, materialized views) |
| Streaming | Apache Kafka 7.5 + Python consumer |
| Orchestration | Apache Airflow 2.7.3 |
| API | FastAPI + Redis cache |
| ML | Scikit-learn (churn prediction, RFM segmentation) |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker Compose |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Data Sources                         │
│              event_generator.py (Kafka Producer)            │
└─────────────────────────┬───────────────────────────────────┘
                          │
                    Kafka Topics
              (user_events · transactions)
                          │
          ┌───────────────┴───────────────┐
          │                               │
  transaction_consumer.py         spark/streaming/
  (Python → PostgreSQL)           kafka_consumer.py
                                  (Spark → realtime_metrics)
          └───────────────┬───────────────┘
                          │
                    PostgreSQL
              ┌───────────┴──────────────┐
              │   Star Schema + Views    │
              │  Airflow DAGs refresh    │
              └───────────┬──────────────┘
                          │
                 FastAPI + Redis Cache
                 (20+ REST endpoints)
                          │
               Streamlit Dashboard
              (5 pages · auto-refresh)
```

---

## Project Structure

```
ecommerce-analytics/
├── docker/
│   └── docker-compose.yml          # All infrastructure services
├── database/
│   └── postgres/
│       ├── schema.sql              # Star schema, partitions, indexes
│       ├── seed_data.sql           # 10K users, 1K products, 100K transactions
│       ├── migrations/             # Phase migration scripts
│       └── queries/                # Complex analytical SQL
│           ├── rfm_segmentation.sql
│           ├── cohort_analysis.sql
│           ├── recommendations.sql
│           └── realtime_dashboard.sql
├── kafka/
│   ├── producers/
│   │   └── event_generator.py      # Simulates user sessions → Kafka
│   └── consumers/
│       └── transaction_consumer.py # Kafka → PostgreSQL (no Spark needed)
├── spark/
│   └── streaming/
│       └── kafka_consumer.py       # Spark Structured Streaming → realtime_metrics
├── airflow/
│   └── dags/
│       ├── hourly_refresh_dag.py   # Refresh materialized views
│       ├── data_quality_dag.py     # 7 data quality checks
│       ├── daily_analytics_dag.py  # RFM + churn score updates
│       └── model_retraining_dag.py # Weekly ML model retraining
├── api/
│   ├── main.py                     # FastAPI app + lifespan + middleware
│   ├── config.py                   # pydantic-settings configuration
│   ├── dependencies.py             # DB pool, Redis, cache helpers
│   ├── models/
│   │   ├── schemas.py              # 20+ Pydantic response models
│   │   └── ml_models.py            # Churn model loader + heuristic fallback
│   └── routers/
│       ├── health.py               # /health, /health/db, /health/redis
│       ├── customers.py            # RFM, churn, segments, at-risk, top
│       ├── recommendations.py      # User CF, item CF, trending
│       ├── products.py             # Top products, categories, inventory
│       └── dashboard.py            # KPIs, revenue, geo, payments
├── dashboards/
│   ├── streamlit_app.py            # Home page + sidebar
│   ├── api_client.py               # HTTP wrapper for all API endpoints
│   ├── components/
│   │   ├── style.py                # CSS injection, colour constants, badges
│   │   ├── kpi_tiles.py            # KPI card components
│   │   └── charts.py              # Plotly chart builders
│   └── pages/
│       ├── 01_Overview.py          # Live KPIs, revenue trends, geo map
│       ├── 02_Customers.py         # RFM segments, churn leaderboard
│       ├── 03_Recommendations.py   # Trending + collaborative filtering
│       ├── 04_Products.py          # Top products, inventory alerts
│       └── 05_Explorer.py          # Customer + product deep-dive
├── requirements.txt
└── INSTRUCTIONS.md                 # Full setup and testing guide
```

---

## Quick Start

See **[INSTRUCTIONS.md](INSTRUCTIONS.md)** for the full guide. Summary:

```bash
# 1. Start infrastructure
cd docker && docker compose up -d

# 2. Start API (new terminal)
cd .. && uvicorn api.main:app --port 8000 --reload

# 3. Start dashboard (new terminal)
cd dashboards && streamlit run streamlit_app.py

# 4. Start event producer (new terminal)
cd kafka/producers && python event_generator.py --rate 10

# 5. Start transaction consumer (new terminal)
cd kafka/consumers && python transaction_consumer.py
```

| Service | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| FastAPI Docs | http://localhost:8000/docs |
| Airflow UI | http://localhost:8080 (admin / admin) |

---

## Dashboard Pages

| Page | Description |
|---|---|
| **Home** | Platform overview, service health, navigation |
| **Real-time Overview** | Live KPIs, 30-day revenue trend, hourly bars, geo map — auto-refreshes every 60s |
| **Customer Analytics** | RFM segment distribution, churn risk leaderboard, top 50 by lifetime value |
| **Recommendations** | Trending products, personalised user recommendations, item-based CF |
| **Product Analytics** | Top products, category breakdown, inventory stockout alerts |
| **Explorer** | Deep-dive into any customer or product by ID |

---

## API Endpoints

```
GET /health                              Service health
GET /health/db                           PostgreSQL latency
GET /health/redis                        Redis latency

GET /dashboard/kpi/realtime              Last-hour KPIs
GET /dashboard/kpi/today-vs-yesterday    Day-over-day comparison
GET /dashboard/kpi/daily                 30-day trend
GET /dashboard/revenue/hourly            Hourly revenue (last 24h)
GET /dashboard/revenue/by-country        Revenue by country
GET /dashboard/customers/new-vs-returning New vs returning split
GET /dashboard/payments                  Payment method breakdown

GET /customers/{id}/rfm                  RFM profile + segment
GET /customers/{id}/churn                Churn probability + risk factors
GET /customers/segments                  All segment summaries
GET /customers/at-risk                   Paginated high-churn customers
GET /customers/top                       Top customers by lifetime value

GET /recommendations/user/{id}           Personalised recommendations
GET /recommendations/similar/{id}        Item-based CF (also-bought)
GET /recommendations/trending            Top 20 products last 24h

GET /products/top                        Top products by revenue
GET /products/top/category               Top categories
GET /products/inventory/alerts           Stockout alerts
GET /products/{id}                       Product detail
```

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `hourly_refresh_dag` | Every hour | Refresh `daily_metrics` + `user_metrics` materialized views |
| `data_quality_dag` | Daily 01:30 | Null checks, orphan detection, anomaly detection, stale view checks |
| `daily_analytics_dag` | Daily 02:00 | Update RFM segments + churn scores for all users |
| `model_retraining_dag` | Weekly (Sun) | Retrain churn (logistic regression) + RFM models, gate on AUC ≥ 0.55 |

---

## Data Model

- **10,000** users with RFM segments and churn scores
- **1,000** products across multiple categories
- **100,000+** historical transactions (partitioned by month)
- **11 RFM segments**: Champions → Lost, scored with NTILE(5)
- **Churn model**: Logistic regression on recency, frequency, monetary features

---

## Performance Benchmarks

All benchmarks were measured locally using Docker on WSL2 (single machine, no network hop).
Production estimates account for network latency, concurrent load, and cold cache conditions.

### Query Execution Times (measured with EXPLAIN ANALYZE)

| Query | Local Warm Cache | Est. Production (single server) | Est. Production (Redis hit) |
|---|---|---|---|
| Real-time KPI (last 1 hour) | 0.24 ms | 5–15 ms | ~1 ms |
| Single user RFM lookup | 0.075 ms | 2–5 ms | ~1 ms |
| 30-day daily KPIs (mat. view) | 0.077 ms | 1–5 ms | ~1 ms |
| Top 10 products (24h join) | 2.6 ms | 10–30 ms | ~1 ms |
| Churn leaderboard (2,945 users) | 7 ms | 20–60 ms | ~1 ms |
| RFM segment distribution | 3.1 ms | 10–25 ms | ~1 ms |

### Why Local Numbers Are Fast

- **Same-machine I/O** — no network round-trip between app and database. Production adds 1–50 ms of network latency alone.
- **Warm buffer cache** — 100K transactions fit in 32 MB, well within PostgreSQL's default 128 MB shared buffer. After the first query all reads are from RAM. Cold cache (fresh restart) would be 5–20x slower on the first hit.
- **No concurrency** — benchmarks run one query at a time. Under 50 concurrent users, expect 5–20x degradation without connection pooling tuning.

### What Stays Fast at Scale

| Technique | Benefit |
|---|---|
| Monthly table partitioning | Time-range queries prune irrelevant partitions at plan time — a last-hour query scans 1 partition regardless of whether the table has 100K or 100M rows |
| Materialized views (`daily_metrics`, `user_metrics`) | 30-day trend and RFM lookups read pre-aggregated views (182 rows, 3 MB) instead of scanning the full transactions table |
| Indexed materialized view | User RFM lookups use a unique btree index — O(log n) regardless of user count |
| Redis caching (TTL 60s–3600s) | Hot dashboard endpoints (KPIs, segments, trending) serve from memory after the first request, zero DB load for repeated calls |
| Connection pooling (`ThreadedConnectionPool`) | API reuses 2–10 persistent DB connections instead of opening a new connection per request (~50 ms overhead each) |

### Honest Scaling Limits

| Threshold | Bottleneck | Mitigation |
|---|---|---|
| >500 concurrent users | PostgreSQL connection exhaustion (pool max=10) | Increase pool size or add PgBouncer |
| >10M transactions | user_metrics mat. view refresh takes minutes | Incremental refresh, read replicas |
| >1M users | Churn leaderboard full-scan becomes slow | Add index on `customer_segment` column |
| High-traffic recommendations | Collaborative filtering recomputed per request | Pre-compute and cache recommendation sets |

---

## Requirements

- Docker + Docker Compose
- Python 3.10+
- Java 11+ (only if running Spark; optional)

```bash
pip install -r requirements.txt
pip install streamlit-autorefresh pydantic-settings
```
