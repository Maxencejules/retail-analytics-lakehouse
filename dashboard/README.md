# Executive Dashboard

Streamlit dashboard for executive stakeholders with:

- Total Revenue
- Average Order Value
- Top 5 Stores
- Top 5 Products
- Revenue Trend Over Time

Includes:
- Date range filter
- Store filter
- Data access abstraction for warehouse and Gold backends

## Data Sources

Two runtime modes are supported:

1. `warehouse` (default): queries Postgres warehouse star schema.
2. `gold`: queries Gold Parquet datasets.

Select source with environment variable:

```bash
export DASHBOARD_DATA_SOURCE=warehouse
```

## Local Run Instructions

### 1. Install dependencies

```bash
pip install -r dashboard/requirements.txt
```

### 2. Configure environment

Warehouse mode example:

```bash
export DASHBOARD_DATA_SOURCE=warehouse
export WAREHOUSE_DSN=postgresql+psycopg://postgres:postgres@localhost:5432/retail_analytics
export WAREHOUSE_SCHEMA=warehouse
```

Gold mode example:

```bash
export DASHBOARD_DATA_SOURCE=gold
export GOLD_BASE_PATH=data/lakehouse/gold
```

### 3. Start dashboard

```bash
streamlit run dashboard/app.py
```

## KPI Definitions

Detailed KPI definitions are documented in:
- [dashboard/docs/kpi-definitions.md](C:/Users/USER/retail-analytics-lakehouse/dashboard/docs/kpi-definitions.md)

