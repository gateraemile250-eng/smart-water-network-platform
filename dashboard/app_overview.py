import os
import sys

import altair as alt
import pandas as pd
import streamlit as st


# =========================================================
# Project path
# =========================================================

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# =========================================================
# Dashboard components
# =========================================================

from dashboard.components.styles import (
    load_css,
    kpi_card,
    section_title,
)

from dashboard.queries import (
    get_network_summary,
    get_anomaly_summary,
    get_localization_summary,
    get_daily_anomalies,
)


# =========================================================
# Page configuration
# =========================================================

st.set_page_config(
    page_title="Smart Water Network Intelligence",
    page_icon="💧",
    layout="wide",
)


# =========================================================
# Styling
# =========================================================

load_css()


# =========================================================
# Data loading
# =========================================================

@st.cache_data(ttl=300)
def load_overview_data():
    """Load data required for the overview dashboard."""

    network = get_network_summary().iloc[0]
    anomaly = get_anomaly_summary().iloc[0]
    localization = get_localization_summary().iloc[0]
    daily = get_daily_anomalies()

    return network, anomaly, localization, daily


network, anomaly, localization, daily = load_overview_data()


# =========================================================
# Hero
# =========================================================

st.markdown(
"""<div class="hero">
<div class="hero-title">
💧 Smart Water Network Intelligence
</div>

<div class="hero-subtitle">
Operational anomaly monitoring, investigation
and network localization
</div>

<div class="hero-badge">
DATA ENGINEERING · ML · STREAM PROCESSING · GIS
</div>
</div>""",
    unsafe_allow_html=True,
)


# =========================================================
# Network overview
# =========================================================

section_title(
    "Network Overview",
    "Current infrastructure represented in the analytical platform.",
)

cols = st.columns(4)

with cols[0]:
    kpi_card(
        "Sensors",
        f"{int(network['sensors']):,}",
        "Operational measurement points",
    )

with cols[1]:
    kpi_card(
        "Network Nodes",
        f"{int(network['network_nodes']):,}",
        "Topological network nodes",
    )

with cols[2]:
    kpi_card(
        "Pipes",
        f"{int(network['pipes']):,}",
        "Pipe network assets",
    )

with cols[3]:
    kpi_card(
        "Network Links",
        f"{int(network['network_links']):,}",
        "Total network connections",
    )


# =========================================================
# Detection and localization
# =========================================================

section_title(
    "Detection & Localization",
    "From anomalous observations to candidate network assets.",
)

cols = st.columns(4)

with cols[0]:
    kpi_card(
        "ML Anomalies",
        f"{int(anomaly['ml_anomaly_timestamps']):,}",
        "Detected by the ML pipeline",
    )

with cols[1]:
    kpi_card(
        "ML Alert Rate",
        f"{float(anomaly['ml_anomaly_rate_pct']):.2f}%",
        "Share of evaluated timestamps",
    )

with cols[2]:
    kpi_card(
        "Localized Events",
        f"{int(localization['localized_anomalies']):,}",
        "Events with network candidates",
    )

with cols[3]:
    kpi_card(
        "Localization Coverage",
        f"{float(localization['localization_coverage_pct']):.2f}%",
        "ML anomalies with localization",
    )


# =========================================================
# Anomaly activity
# =========================================================

section_title(
    "Anomaly Activity",
    "Daily operational activity across the analyzed period.",
)

daily = daily.copy()
daily["day"] = pd.to_datetime(daily["day"])

chart_data = daily[
    [
        "day",
        "ml_anomaly_count",
        "baseline_anomaly_count",
    ]
].rename(
    columns={
        "day": "Date",
        "ml_anomaly_count": "ML anomalies",
        "baseline_anomaly_count": "Baseline anomalies",
    }
)

chart_data = chart_data.melt(
    id_vars="Date",
    var_name="Detection",
    value_name="Anomaly count",
)

chart = (
    alt.Chart(chart_data)
    .mark_line(strokeWidth=2)
    .encode(
        x=alt.X(
            "Date:T",
            title="Date",
        ),
        y=alt.Y(
            "Anomaly count:Q",
            title="Anomaly count",
        ),
        color=alt.Color(
            "Detection:N",
            title="Detection",
        ),
        tooltip=[
            alt.Tooltip(
                "Date:T",
                title="Date",
                format="%Y-%m-%d",
            ),
            alt.Tooltip(
                "Detection:N",
                title="Detection",
            ),
            alt.Tooltip(
                "Anomaly count:Q",
                title="Count",
            ),
        ],
    )
    .properties(height=380)
)

st.altair_chart(
    chart,
    use_container_width=True,
)


# =========================================================
# Operational insights
# =========================================================

section_title(
    "Operational Insights",
    "Key outputs from anomaly detection and network localization.",
)

col1, col2 = st.columns(2)

with col1:
    st.markdown(
f"""<div class="insight-card">
<div class="insight-title">
Detection Pipeline
</div>

<div class="insight-value">
{int(anomaly['ml_anomaly_timestamps']):,}
</div>

<div class="insight-text">
ML anomaly timestamps were identified from
{int(anomaly['scoring_timestamps']):,}
evaluated five-minute observations.

<br><br>

Baseline detection identified
<strong>
{int(anomaly['baseline_anomaly_timestamps']):,}
</strong>
anomaly timestamps.
</div>
</div>""",
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
f"""<div class="insight-card">
<div class="insight-title">
Network Localization
</div>

<div class="insight-value">
{float(localization['localization_coverage_pct']):.2f}%
</div>

<div class="insight-text">
Localization results are available for
<strong>
{int(localization['localized_anomalies']):,}
</strong>
anomaly timestamps.

<br><br>

The localization engine generated
<strong>
{int(localization['candidate_rows']):,}
</strong>
candidate rows across
<strong>
{int(localization['candidate_pipes']):,}
</strong>
candidate pipes.
</div>
</div>""",
        unsafe_allow_html=True,
    )


# =========================================================
# Engineering workflow
# =========================================================

section_title(
    "End-to-End Engineering Workflow",
    "How raw measurements become operational intelligence.",
)

workflow = [
    (
        "STEP 01",
        "Sensor Data",
        "Measurements enter the analytical pipeline.",
    ),
    (
        "STEP 02",
        "Streaming & Processing",
        "Kafka and Spark process incoming measurements.",
    ),
    (
        "STEP 03",
        "Data Platform",
        "PostgreSQL stores structured analytical data.",
    ),
    (
        "STEP 04",
        "Anomaly Detection",
        "Baseline and ML methods identify unusual behaviour.",
    ),
    (
        "STEP 05",
        "Investigation",
        "Detected events are evaluated using supporting evidence.",
    ),
    (
        "STEP 06",
        "Network Localization",
        "Candidate network assets are ranked for investigation.",
    ),
]

for start in range(0, len(workflow), 3):

    cols = st.columns(3)

    for col, item in zip(
        cols,
        workflow[start:start + 3],
    ):

        number, title, description = item

        with col:
            st.markdown(
f"""<div class="workflow-card">
<div class="workflow-number">
{number}
</div>

<div class="workflow-title">
{title}
</div>

<div class="workflow-text">
{description}
</div>
</div>""",
                unsafe_allow_html=True,
            )


# =========================================================
# Analysis coverage
# =========================================================

section_title(
    "Analysis Coverage",
    "Scope of the currently loaded analytical dataset.",
)

coverage = st.columns(3)

with coverage[0]:
    st.metric(
        "Analysis Start",
        pd.to_datetime(
            anomaly["scoring_start"]
        ).strftime("%Y-%m-%d"),
    )

with coverage[1]:
    st.metric(
        "Analysis End",
        pd.to_datetime(
            anomaly["scoring_end"]
        ).strftime("%Y-%m-%d %H:%M"),
    )

with coverage[2]:
    st.metric(
        "Observation Interval",
        "5 minutes",
    )


# =========================================================
# Footer
# =========================================================

st.markdown(
"""<div class="footer">
Smart Water Network Platform ·
Data Engineering ·
Machine Learning ·
Network Analytics ·
GIS
</div>""",
    unsafe_allow_html=True,
)