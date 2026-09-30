"""Pure dashboard insight text; no UI runtime is required."""

from __future__ import annotations

import pandas as pd


def format_currency(value: float) -> str:
    return f"{value:,.2f}"


def build_highlight_insights(
    *,
    total_revenue: float,
    total_orders: int,
    top_stores: pd.DataFrame,
    top_products: pd.DataFrame,
    revenue_trend: pd.DataFrame,
    product_ranking_complete: bool = True,
) -> list[str]:
    insights: list[str] = []

    if total_orders > 0:
        insights.append(
            f"Average order value tracks at {format_currency(total_revenue / total_orders)} across {total_orders:,} orders."
        )

    if not top_stores.empty and total_revenue > 0:
        stores_frame = top_stores.copy()
        stores_frame["revenue"] = pd.to_numeric(
            stores_frame["revenue"], errors="coerce"
        ).fillna(0.0)
        leader = stores_frame.sort_values("revenue", ascending=False).iloc[0]
        leader_name = str(leader.get("store_name", leader.get("store_id", "Top Store")))
        leader_share = (float(leader["revenue"]) / total_revenue) * 100
        insights.append(
            f"{leader_name} is the top store and contributes {leader_share:.1f}% of selected-period revenue."
        )

    if product_ranking_complete and not top_products.empty and total_revenue > 0:
        products_frame = top_products.copy()
        products_frame["revenue"] = pd.to_numeric(
            products_frame["revenue"], errors="coerce"
        ).fillna(0.0)
        leader = products_frame.sort_values("revenue", ascending=False).iloc[0]
        leader_name = str(
            leader.get("product_name", leader.get("product_id", "Top Product"))
        )
        leader_share = (float(leader["revenue"]) / total_revenue) * 100
        insights.append(
            f"{leader_name} leads products with {leader_share:.1f}% revenue share in this slice."
        )

    if not revenue_trend.empty:
        trend = revenue_trend.copy()
        trend["revenue"] = pd.to_numeric(trend["revenue"], errors="coerce").fillna(0.0)
        peak_row = trend.loc[trend["revenue"].idxmax()]
        peak_date = pd.to_datetime(peak_row["report_date"]).date()
        peak_value = float(peak_row["revenue"])
        insights.append(
            f"Peak daily revenue hit {format_currency(peak_value)} on {peak_date:%b %d, %Y}."
        )

    return insights[:4]
