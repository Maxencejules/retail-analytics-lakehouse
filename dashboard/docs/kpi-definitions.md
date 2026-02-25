# KPI Definitions

## Scope

These KPI definitions are used by the executive dashboard in [app.py](C:/Users/USER/retail-analytics-lakehouse/dashboard/app.py).

Filters applied:
- Date range (`start_date` to `end_date`)
- Optional store filter (`store_id` list)

## KPI: Total Revenue

Definition:
- Sum of `revenue` for all transactions that satisfy filters.

Formula:
- `Total Revenue = SUM(revenue)`

Notes:
- Revenue is assumed to be net transaction revenue at fact grain.
- Currency normalization must happen upstream before warehouse or Gold load.

## KPI: Average Order Value (AOV)

Definition:
- Average revenue per transaction in the selected filter scope.

Formula:
- `AOV = SUM(revenue) / COUNT(transactions)`

Notes:
- Fact grain is one row per transaction, so `COUNT(*)` represents order count.
- This is not average item price; it is average transaction value.

## KPI: Top 5 Stores

Definition:
- Stores ranked by total revenue within filter scope.

Ranking logic:
1. Higher total revenue
2. Implicit deterministic tie behavior from backend query engine ordering

Returned fields:
- `store_id`
- `store_name` (warehouse mode; Gold mode uses `store_id` alias)
- `revenue`
- `orders`

## KPI: Top 5 Products

Definition:
- Products ranked by total revenue within filter scope.

Returned fields:
- `product_id`
- `product_name` (warehouse mode; Gold mode uses `product_id` alias)
- `revenue`
- `orders`

Gold-mode note:
- Product ranking is sourced from Gold aggregate datasets.

## KPI: Revenue Trend Over Time

Definition:
- Daily revenue time series over selected date range.

Formula per day:
- `Daily Revenue = SUM(revenue) GROUP BY report_date`

Visualization:
- Line chart with daily points ordered by date.
