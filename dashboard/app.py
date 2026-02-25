"""Executive Streamlit dashboard for retail analytics."""

from __future__ import annotations

from datetime import date, timedelta
import logging
from typing import Any

import pandas as pd
import streamlit as st

try:  # pragma: no cover - optional dependency path.
    import altair as alt
except ImportError:  # pragma: no cover - optional dependency path.
    alt = None

from dashboard.config import DashboardConfig
from dashboard.data_access import (
    DashboardFilters,
    DashboardRepository,
    create_repository,
)

LOGGER = logging.getLogger("dashboard.app")
RANGE_PRESET_OPTIONS: tuple[str, ...] = (
    "Last 7 days",
    "Last 30 days",
    "Last 90 days",
    "Quarter to date",
    "Custom",
)

APP_STYLES = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Source+Sans+3:wght@400;500;600&family=IBM+Plex+Mono:wght@500&display=swap');

    :root {
        --bg-primary: #f6f7f3;
        --bg-elevated: #ffffff;
        --bg-accent: #e7f4f4;
        --text-primary: #0f172a;
        --text-secondary: #1f2937;
        --line-subtle: #c2cdd6;
        --accent-primary: #0f766e;
        --accent-secondary: #d97706;
        --focus-ring: #0369a1;
        --focus-ring-shadow: rgba(3, 105, 161, 0.24);
    }

    .stApp {
        background:
            radial-gradient(circle at 8% 0%, #e7f4f4 0, transparent 35%),
            radial-gradient(circle at 98% 4%, #fce9d2 0, transparent 38%),
            linear-gradient(180deg, #fcfcfb 0%, var(--bg-primary) 100%);
        color: var(--text-primary);
        font-family: "Source Sans 3", "Segoe UI", sans-serif;
    }

    h1, h2, h3 {
        color: var(--text-primary);
        font-family: "Space Grotesk", "Trebuchet MS", sans-serif;
        letter-spacing: -0.015em;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #ffffff 0%, #f2f8f7 100%);
        border-right: 1px solid var(--line-subtle);
    }

    [data-testid="stMetric"] {
        background: var(--bg-elevated);
        border: 1px solid var(--line-subtle);
        border-radius: 14px;
        padding: 0.8rem 0.9rem;
        box-shadow: 0 8px 22px rgba(15, 23, 42, 0.06);
    }

    [data-testid="stMetricLabel"] {
        color: var(--text-secondary);
        font-weight: 600;
    }

    [data-testid="stMetricValue"] {
        color: var(--text-primary);
        font-family: "IBM Plex Mono", "Cascadia Code", monospace;
        letter-spacing: -0.02em;
    }

    .dashboard-hero {
        background: linear-gradient(90deg, #0f766e 0%, #0f766e 52%, #d97706 100%);
        border-radius: 16px;
        padding: 1rem 1.2rem 1rem 1.2rem;
        color: #ffffff;
        margin-bottom: 0.9rem;
        box-shadow: 0 10px 24px rgba(15, 118, 110, 0.24);
        animation: slideIn 300ms ease-out;
    }

    .dashboard-hero h1 {
        color: #ffffff;
        font-size: 1.6rem;
        line-height: 1.2;
        margin: 0 0 0.2rem 0;
    }

    .dashboard-hero p {
        margin: 0.15rem 0;
        font-size: 0.95rem;
        line-height: 1.35;
    }

    .dashboard-chip-row {
        display: flex;
        gap: 0.45rem;
        flex-wrap: wrap;
        margin-top: 0.55rem;
    }

    .dashboard-chip {
        background: rgba(255, 255, 255, 0.18);
        border: 1px solid rgba(255, 255, 255, 0.38);
        border-radius: 999px;
        padding: 0.18rem 0.6rem;
        font-size: 0.78rem;
        font-weight: 600;
        color: #ffffff;
    }

    .dashboard-note {
        color: var(--text-secondary);
        font-size: 0.9rem;
        margin: 0.2rem 0 0.75rem 0;
    }

    .insight-panel {
        background: var(--bg-accent);
        border: 1px solid #b7dfda;
        border-radius: 12px;
        padding: 0.78rem 0.9rem 0.68rem 0.9rem;
        margin: 0.75rem 0 0.95rem 0;
    }

    .insight-panel h4 {
        font-family: "Space Grotesk", "Trebuchet MS", sans-serif;
        font-size: 0.95rem;
        margin: 0 0 0.3rem 0;
        color: #0f172a;
    }

    .insight-panel ul {
        margin: 0.2rem 0 0.1rem 1rem;
        padding: 0;
    }

    .insight-panel li {
        margin-bottom: 0.2rem;
        color: #0f172a;
        font-size: 0.92rem;
    }

    /* Keep keyboard navigation visible across Streamlit controls. */
    .stButton > button:focus-visible,
    .stDownloadButton > button:focus-visible,
    .stDateInput input:focus-visible,
    .stMultiSelect [role="combobox"]:focus-visible,
    .stSelectbox [role="combobox"]:focus-visible,
    [data-testid="stDataFrame"] [tabindex]:focus-visible,
    [data-testid="stSidebar"] [tabindex]:focus-visible {
        outline: 3px solid var(--focus-ring) !important;
        outline-offset: 2px !important;
        box-shadow: 0 0 0 3px var(--focus-ring-shadow) !important;
        border-radius: 8px;
    }

    [data-testid="stDataFrame"] thead tr th {
        color: #0f172a;
        font-weight: 700;
    }

    [data-testid="stDataFrame"] tbody tr td {
        color: #0f172a;
    }

    @keyframes slideIn {
        from {
            opacity: 0;
            transform: translateY(8px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }

    @media (max-width: 960px) {
        .dashboard-hero h1 {
            font-size: 1.35rem;
        }

        [data-testid="stMetric"] {
            padding: 0.68rem 0.75rem;
        }
    }

    @media (prefers-reduced-motion: reduce) {
        *, *::before, *::after {
            animation: none !important;
            transition: none !important;
            scroll-behavior: auto !important;
        }
    }
</style>
"""


def configure_page() -> None:
    st.set_page_config(
        page_title="Retail Loyalty Executive Dashboard",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(APP_STYLES, unsafe_allow_html=True)


@st.cache_resource(show_spinner=False)
def get_repository(config: DashboardConfig) -> DashboardRepository:
    return create_repository(config)


@st.cache_data(show_spinner=False, ttl=300)
def load_summary(
    _repo: DashboardRepository, filters: DashboardFilters
) -> tuple[float, float, int]:
    summary = _repo.kpi_summary(filters)
    return summary.total_revenue, summary.average_order_value, summary.total_orders


@st.cache_data(show_spinner=False, ttl=300)
def load_top_stores(
    _repo: DashboardRepository, filters: DashboardFilters
) -> pd.DataFrame:
    return _repo.top_stores(filters, limit=5)


@st.cache_data(show_spinner=False, ttl=300)
def load_top_products(
    _repo: DashboardRepository, filters: DashboardFilters
) -> pd.DataFrame:
    return _repo.top_products(filters, limit=5)


@st.cache_data(show_spinner=False, ttl=300)
def load_revenue_trend(
    _repo: DashboardRepository, filters: DashboardFilters
) -> pd.DataFrame:
    return _repo.revenue_trend(filters)


def format_currency(value: float) -> str:
    return f"${value:,.2f}"


def format_percent_delta(current: float, previous: float | None) -> str | None:
    if previous is None or previous <= 0:
        return None
    delta_pct = ((current - previous) / previous) * 100
    return f"{delta_pct:+.1f}%"


def format_date_range(start_date: date, end_date: date) -> str:
    return f"{start_date:%b %d, %Y} - {end_date:%b %d, %Y}"


def parse_date_range(
    selected_range: Any, fallback: tuple[date, date]
) -> tuple[date, date]:
    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        return selected_range[0], selected_range[1]
    if isinstance(selected_range, list) and len(selected_range) == 2:
        return selected_range[0], selected_range[1]
    if isinstance(selected_range, date):
        return selected_range, selected_range
    return fallback


def clamp_date_range(
    *,
    start_date: date,
    end_date: date,
    min_date: date,
    max_date: date,
) -> tuple[date, date]:
    bounded_start = max(min_date, min(start_date, max_date))
    bounded_end = max(min_date, min(end_date, max_date))
    if bounded_start > bounded_end:
        bounded_start = bounded_end
    return bounded_start, bounded_end


def date_range_for_preset(
    preset: str,
    *,
    min_date: date,
    max_date: date,
) -> tuple[date, date]:
    if preset == "Last 7 days":
        start = max_date - timedelta(days=6)
    elif preset == "Last 90 days":
        start = max_date - timedelta(days=89)
    elif preset == "Quarter to date":
        quarter_start_month = ((max_date.month - 1) // 3) * 3 + 1
        start = date(max_date.year, quarter_start_month, 1)
    else:
        # Defaults to Last 30 days behavior.
        start = max_date - timedelta(days=29)

    return clamp_date_range(
        start_date=start,
        end_date=max_date,
        min_date=min_date,
        max_date=max_date,
    )


def build_filters(repo: DashboardRepository) -> tuple[DashboardFilters, date]:
    min_date, max_date = repo.available_date_range()
    default_start = max(min_date, max_date - timedelta(days=29))

    session_start = st.session_state.get("dashboard_start_date", default_start)
    session_end = st.session_state.get("dashboard_end_date", max_date)
    session_store_ids = tuple(st.session_state.get("dashboard_store_ids", ()))
    session_preset = st.session_state.get("dashboard_range_preset", "Last 30 days")

    if session_preset not in RANGE_PRESET_OPTIONS:
        session_preset = "Last 30 days"

    session_start, session_end = clamp_date_range(
        start_date=session_start,
        end_date=session_end,
        min_date=min_date,
        max_date=max_date,
    )

    st.sidebar.markdown("### Filters")
    st.sidebar.caption("Apply filters in a single batch to avoid noisy reruns.")

    with st.sidebar.form("dashboard_filter_form", clear_on_submit=False):
        selected_preset = st.selectbox(
            label="Range preset",
            options=list(RANGE_PRESET_OPTIONS),
            index=RANGE_PRESET_OPTIONS.index(session_preset),
            help="Use presets for quick comparisons, or choose Custom for exact dates.",
        )

        if selected_preset == "Custom":
            selected_range = st.date_input(
                label="Date range",
                value=(session_start, session_end),
                min_value=min_date,
                max_value=max_date,
                help="Choose a custom date window.",
            )
            candidate_start, candidate_end = parse_date_range(
                selected_range,
                fallback=(session_start, session_end),
            )
            candidate_start, candidate_end = clamp_date_range(
                start_date=candidate_start,
                end_date=candidate_end,
                min_date=min_date,
                max_date=max_date,
            )
        else:
            candidate_start, candidate_end = date_range_for_preset(
                selected_preset,
                min_date=min_date,
                max_date=max_date,
            )
            st.caption(f"Range: {format_date_range(candidate_start, candidate_end)}")

        preliminary_filters = DashboardFilters(
            start_date=candidate_start,
            end_date=candidate_end,
        )
        available_stores = repo.list_stores(preliminary_filters)
        default_store_selection = [
            store for store in session_store_ids if store in available_stores
        ]
        selected_stores = st.multiselect(
            "Store filter",
            options=available_stores,
            default=default_store_selection,
            help="Leave empty to include all stores.",
        )
        button_col_1, button_col_2 = st.columns(2)
        with button_col_1:
            apply_filters = st.form_submit_button("Apply", use_container_width=True)
        with button_col_2:
            reset_filters = st.form_submit_button("Reset", use_container_width=True)

    if reset_filters:
        applied_preset = "Last 30 days"
        start_date, end_date = date_range_for_preset(
            applied_preset,
            min_date=min_date,
            max_date=max_date,
        )
        store_ids: tuple[str, ...] = ()
    elif apply_filters:
        applied_preset = selected_preset
        if selected_preset == "Custom":
            start_date, end_date = candidate_start, candidate_end
        else:
            start_date, end_date = date_range_for_preset(
                selected_preset,
                min_date=min_date,
                max_date=max_date,
            )
        store_ids = tuple(selected_stores)
    else:
        applied_preset = session_preset
        start_date = session_start
        end_date = session_end
        store_ids = session_store_ids

    start_date, end_date = clamp_date_range(
        start_date=start_date,
        end_date=end_date,
        min_date=min_date,
        max_date=max_date,
    )

    st.session_state["dashboard_range_preset"] = applied_preset
    st.session_state["dashboard_start_date"] = start_date
    st.session_state["dashboard_end_date"] = end_date
    st.session_state["dashboard_store_ids"] = list(store_ids)

    filters = DashboardFilters(
        start_date=start_date, end_date=end_date, store_ids=store_ids
    )
    filters.validate()
    return filters, min_date


def previous_period_filters(
    filters: DashboardFilters,
    *,
    min_date: date,
) -> DashboardFilters | None:
    window_days = (filters.end_date - filters.start_date).days + 1
    previous_end = filters.start_date - timedelta(days=1)
    previous_start = previous_end - timedelta(days=window_days - 1)
    if previous_start < min_date:
        return None
    return DashboardFilters(
        start_date=previous_start,
        end_date=previous_end,
        store_ids=filters.store_ids,
    )


def build_highlight_insights(
    *,
    total_revenue: float,
    total_orders: int,
    top_stores: pd.DataFrame,
    top_products: pd.DataFrame,
    revenue_trend: pd.DataFrame,
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

    if not top_products.empty and total_revenue > 0:
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


def render_insight_panel(insights: list[str]) -> None:
    if not insights:
        return

    list_markup = "".join(f"<li>{insight}</li>" for insight in insights)
    st.markdown(
        f"""
        <section class="insight-panel">
            <h4>Operational highlights</h4>
            <ul>{list_markup}</ul>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_hero(config: DashboardConfig, filters: DashboardFilters) -> None:
    store_scope = (
        "All stores" if not filters.store_ids else f"{len(filters.store_ids)} stores"
    )
    st.markdown(
        f"""
        <section class="dashboard-hero">
            <h1>Retail Performance Cockpit</h1>
            <p>Track revenue, order volume, and top contributors with a single operational view.</p>
            <div class="dashboard-chip-row">
                <span class="dashboard-chip">Source: {config.data_source.upper()}</span>
                <span class="dashboard-chip">Range: {format_date_range(filters.start_date, filters.end_date)}</span>
                <span class="dashboard-chip">Scope: {store_scope}</span>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_ranked_revenue_chart(
    data: pd.DataFrame,
    *,
    id_column: str,
    name_column: str,
    title: str,
    color_hex: str,
) -> None:
    st.subheader(title)
    if data.empty:
        st.info("No data found for current filters.")
        return

    chart_data = data.copy()
    chart_data["label"] = (
        chart_data[name_column].fillna(chart_data[id_column]).astype(str)
    )
    chart_data["revenue"] = pd.to_numeric(
        chart_data["revenue"], errors="coerce"
    ).fillna(0.0)
    chart_data["orders"] = pd.to_numeric(chart_data["orders"], errors="coerce").fillna(
        0
    )

    if alt is None:
        st.bar_chart(chart_data.set_index("label")["revenue"], use_container_width=True)
        return

    chart = (
        alt.Chart(chart_data)
        .mark_bar(cornerRadiusEnd=8, color=color_hex)
        .encode(
            x=alt.X("revenue:Q", title="Revenue"),
            y=alt.Y("label:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip("label:N", title="Name"),
                alt.Tooltip("revenue:Q", title="Revenue", format=",.2f"),
                alt.Tooltip("orders:Q", title="Orders", format=","),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)


def render_revenue_trend_chart(revenue_trend: pd.DataFrame) -> None:
    st.subheader("Revenue trend")
    if revenue_trend.empty:
        st.info("No trend data for selected filters.")
        return

    trend_frame = revenue_trend.copy()
    trend_frame["report_date"] = pd.to_datetime(trend_frame["report_date"])
    trend_frame["revenue"] = pd.to_numeric(
        trend_frame["revenue"], errors="coerce"
    ).fillna(0.0)
    trend_frame["7-day average"] = (
        trend_frame["revenue"].rolling(window=7, min_periods=3).mean()
    )

    if alt is None:
        plot = trend_frame.rename(columns={"report_date": "Date", "revenue": "Revenue"})
        fallback_frame = plot.set_index("Date")[["Revenue"]]
        if trend_frame["7-day average"].notna().any():
            fallback_frame["7-day Avg"] = trend_frame["7-day average"].values
        st.line_chart(fallback_frame, use_container_width=True)
        return

    plot_frame = trend_frame.rename(columns={"revenue": "Daily revenue"})[
        ["report_date", "Daily revenue", "7-day average"]
    ]
    melted = plot_frame.melt(
        id_vars="report_date",
        var_name="Series",
        value_name="Revenue",
    ).dropna(subset=["Revenue"])

    line = (
        alt.Chart(melted)
        .mark_line(point={"filled": True, "size": 40}, strokeWidth=2.2)
        .encode(
            x=alt.X("report_date:T", title="Date"),
            y=alt.Y("Revenue:Q", title="Revenue"),
            color=alt.Color(
                "Series:N",
                scale=alt.Scale(
                    domain=["Daily revenue", "7-day average"],
                    range=["#0f766e", "#d97706"],
                ),
                legend=alt.Legend(title=None),
            ),
            tooltip=[
                alt.Tooltip("report_date:T", title="Date"),
                alt.Tooltip("Series:N"),
                alt.Tooltip("Revenue:Q", title="Revenue", format=",.2f"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(line, use_container_width=True)


def format_ranked_table(
    data: pd.DataFrame,
    *,
    id_column: str,
    name_column: str,
    output_id: str,
    output_name: str,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(
            columns=[
                "Rank",
                output_id,
                output_name,
                "Revenue",
                "Orders",
                "Revenue Share %",
            ]
        )

    table = data.copy().sort_values("revenue", ascending=False).reset_index(drop=True)
    table.insert(0, "Rank", table.index + 1)
    table["revenue"] = pd.to_numeric(table["revenue"], errors="coerce").fillna(0.0)
    table["orders"] = (
        pd.to_numeric(table["orders"], errors="coerce").fillna(0).astype(int)
    )
    revenue_total = float(table["revenue"].sum())
    if revenue_total > 0:
        table["Revenue Share %"] = (table["revenue"] / revenue_total) * 100
    else:
        table["Revenue Share %"] = 0.0

    table["Revenue"] = table["revenue"].map(format_currency)
    table["Orders"] = table["orders"].map(lambda value: f"{value:,}")
    return table.rename(
        columns={
            id_column: output_id,
            name_column: output_name,
        }
    )[["Rank", output_id, output_name, "Revenue", "Orders", "Revenue Share %"]]


def render_dashboard(repo: DashboardRepository, config: DashboardConfig) -> None:
    filters, min_date = build_filters(repo)
    render_hero(config, filters)

    prior_filters = previous_period_filters(filters, min_date=min_date)

    with st.spinner("Loading KPIs and charts..."):
        total_revenue, average_order_value, total_orders = load_summary(repo, filters)
        top_stores = load_top_stores(repo, filters)
        top_products = load_top_products(repo, filters)
        revenue_trend = load_revenue_trend(repo, filters)
        prior_summary = (
            load_summary(repo, prior_filters) if prior_filters is not None else None
        )

    revenue_delta = format_percent_delta(
        total_revenue,
        prior_summary[0] if prior_summary is not None else None,
    )
    aov_delta = format_percent_delta(
        average_order_value,
        prior_summary[1] if prior_summary is not None else None,
    )
    orders_delta = format_percent_delta(
        float(total_orders),
        float(prior_summary[2]) if prior_summary is not None else None,
    )

    st.markdown(
        "<p class='dashboard-note'>Values are compared against the previous time window when historical coverage is available.</p>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p class='dashboard-note'>Accessibility: visible keyboard focus indicators and reduced-motion support are enabled.</p>",
        unsafe_allow_html=True,
    )

    kpi_col_1, kpi_col_2, kpi_col_3 = st.columns(3)
    with kpi_col_1:
        st.metric(
            "Total Revenue",
            format_currency(total_revenue),
            delta=revenue_delta,
            help="Change versus the immediately previous period of equal length.",
        )
    with kpi_col_2:
        st.metric(
            "Average Order Value",
            format_currency(average_order_value),
            delta=aov_delta,
            help="Average revenue per order in the selected period.",
        )
    with kpi_col_3:
        st.metric(
            "Total Orders",
            f"{total_orders:,}",
            delta=orders_delta,
            help="Count of orders for the selected filters.",
        )

    insights = build_highlight_insights(
        total_revenue=total_revenue,
        total_orders=total_orders,
        top_stores=top_stores,
        top_products=top_products,
        revenue_trend=revenue_trend,
    )
    render_insight_panel(insights)

    overview_tab, detail_tab = st.tabs(["Overview", "Detailed tables"])

    with overview_tab:
        chart_col_1, chart_col_2 = st.columns(2)
        with chart_col_1:
            render_ranked_revenue_chart(
                top_stores,
                id_column="store_id",
                name_column="store_name",
                title="Top stores by revenue",
                color_hex="#0f766e",
            )
        with chart_col_2:
            render_ranked_revenue_chart(
                top_products,
                id_column="product_id",
                name_column="product_name",
                title="Top products by revenue",
                color_hex="#d97706",
            )

        render_revenue_trend_chart(revenue_trend)

    with detail_tab:
        stores_table = format_ranked_table(
            top_stores,
            id_column="store_id",
            name_column="store_name",
            output_id="Store ID",
            output_name="Store Name",
        )
        products_table = format_ranked_table(
            top_products,
            id_column="product_id",
            name_column="product_name",
            output_id="Product ID",
            output_name="Product Name",
        )

        table_col_1, table_col_2 = st.columns(2)
        with table_col_1:
            st.subheader("Top 5 Stores")
            st.dataframe(
                stores_table,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Revenue Share %": st.column_config.ProgressColumn(
                        "Revenue Share",
                        min_value=0,
                        max_value=100,
                        format="%.1f%%",
                    )
                },
            )
            st.download_button(
                label="Download stores CSV",
                data=stores_table.to_csv(index=False).encode("utf-8"),
                file_name="top_stores.csv",
                mime="text/csv",
            )
        with table_col_2:
            st.subheader("Top 5 Products")
            st.dataframe(
                products_table,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Revenue Share %": st.column_config.ProgressColumn(
                        "Revenue Share",
                        min_value=0,
                        max_value=100,
                        format="%.1f%%",
                    )
                },
            )
            st.download_button(
                label="Download products CSV",
                data=products_table.to_csv(index=False).encode("utf-8"),
                file_name="top_products.csv",
                mime="text/csv",
            )

    st.caption(
        "KPI definitions and assumptions are documented in dashboard/docs/kpi-definitions.md."
    )


def main() -> None:
    configure_page()
    try:
        config = DashboardConfig.from_env()
        config.validate()
        repo = get_repository(config)
        render_dashboard(repo, config)
    except Exception as exc:  # pragma: no cover - streamlit runtime error path.
        LOGGER.exception("dashboard_render_failed", exc_info=exc)
        st.error(f"Dashboard failed to load: {exc}")
        st.stop()


if __name__ == "__main__":
    main()
