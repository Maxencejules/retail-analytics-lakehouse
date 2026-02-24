"""Executive Streamlit dashboard for retail analytics."""

from __future__ import annotations

from datetime import timedelta
import logging

import pandas as pd
import streamlit as st

from dashboard.config import DashboardConfig
from dashboard.data_access import DashboardFilters, DashboardRepository, create_repository

LOGGER = logging.getLogger("dashboard.app")


def configure_page() -> None:
    st.set_page_config(
        page_title="Retail Loyalty Executive Dashboard",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
        <style>
            .stMetric {
                background-color: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 0.75rem;
                padding: 0.75rem;
            }
            .dashboard-note {
                font-size: 0.9rem;
                color: #475569;
                margin-top: 0.25rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_resource(show_spinner=False)
def get_repository(config: DashboardConfig) -> DashboardRepository:
    return create_repository(config)


@st.cache_data(show_spinner=False, ttl=300)
def load_summary(_repo: DashboardRepository, filters: DashboardFilters) -> tuple[float, float, int]:
    summary = _repo.kpi_summary(filters)
    return summary.total_revenue, summary.average_order_value, summary.total_orders


@st.cache_data(show_spinner=False, ttl=300)
def load_top_stores(_repo: DashboardRepository, filters: DashboardFilters) -> pd.DataFrame:
    return _repo.top_stores(filters, limit=5)


@st.cache_data(show_spinner=False, ttl=300)
def load_top_products(_repo: DashboardRepository, filters: DashboardFilters) -> pd.DataFrame:
    return _repo.top_products(filters, limit=5)


@st.cache_data(show_spinner=False, ttl=300)
def load_revenue_trend(_repo: DashboardRepository, filters: DashboardFilters) -> pd.DataFrame:
    return _repo.revenue_trend(filters)


def format_currency(value: float) -> str:
    return f"${value:,.2f}"


def build_filters(repo: DashboardRepository) -> DashboardFilters:
    min_date, max_date = repo.available_date_range()
    default_start = max(min_date, max_date - timedelta(days=29))

    selected_range = st.sidebar.date_input(
        label="Date Range",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        start_date, end_date = selected_range
    else:
        start_date = end_date = selected_range

    preliminary_filters = DashboardFilters(start_date=start_date, end_date=end_date)
    available_stores = repo.list_stores(preliminary_filters)
    selected_stores = st.sidebar.multiselect(
        "Store Filter",
        options=available_stores,
        default=[],
        help="Filter KPIs and charts to selected stores.",
    )

    filters = DashboardFilters(
        start_date=start_date,
        end_date=end_date,
        store_ids=tuple(selected_stores),
    )
    filters.validate()
    return filters


def render_dashboard(repo: DashboardRepository, config: DashboardConfig) -> None:
    st.title("Retail Loyalty Executive Dashboard")
    st.markdown(
        f"<div class='dashboard-note'>Data source: <b>{config.data_source}</b></div>",
        unsafe_allow_html=True,
    )

    filters = build_filters(repo)

    total_revenue, average_order_value, total_orders = load_summary(repo, filters)
    top_stores = load_top_stores(repo, filters)
    top_products = load_top_products(repo, filters)
    revenue_trend = load_revenue_trend(repo, filters)

    kpi_col_1, kpi_col_2, kpi_col_3 = st.columns(3)
    with kpi_col_1:
        st.metric("Total Revenue", format_currency(total_revenue))
    with kpi_col_2:
        st.metric("Average Order Value", format_currency(average_order_value))
    with kpi_col_3:
        st.metric("Total Orders", f"{total_orders:,}")

    left_col, right_col = st.columns(2)
    with left_col:
        st.subheader("Top 5 Stores")
        if top_stores.empty:
            st.info("No store data for selected filters.")
        else:
            display_stores = top_stores.copy()
            display_stores["revenue"] = display_stores["revenue"].map(format_currency)
            st.dataframe(
                display_stores.rename(
                    columns={
                        "store_id": "Store ID",
                        "store_name": "Store Name",
                        "revenue": "Revenue",
                        "orders": "Orders",
                    }
                ),
                hide_index=True,
                use_container_width=True,
            )

    with right_col:
        st.subheader("Top 5 Products")
        if top_products.empty:
            st.info("No product data for selected filters.")
        else:
            display_products = top_products.copy()
            display_products["revenue"] = display_products["revenue"].map(format_currency)
            st.dataframe(
                display_products.rename(
                    columns={
                        "product_id": "Product ID",
                        "product_name": "Product Name",
                        "revenue": "Revenue",
                        "orders": "Orders",
                    }
                ),
                hide_index=True,
                use_container_width=True,
            )

    st.subheader("Revenue Trend Over Time")
    if revenue_trend.empty:
        st.info("No trend data for selected filters.")
    else:
        trend_plot = revenue_trend.rename(
            columns={"report_date": "Date", "revenue": "Revenue"}
        ).set_index("Date")
        st.line_chart(trend_plot["Revenue"], use_container_width=True)

    st.caption(
        "KPI definitions and assumptions are documented in dashboard/docs/kpi-definitions.md."
    )


def main() -> None:
    configure_page()
    st.sidebar.header("Filters")

    try:
        config = DashboardConfig.from_env()
        config.validate()
        repo = get_repository(config)
        render_dashboard(repo, config)
    except Exception as exc:
        LOGGER.exception("dashboard_render_failed", exc_info=exc)
        st.error(f"Dashboard failed to load: {exc}")
        st.stop()


if __name__ == "__main__":
    main()

