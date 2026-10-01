import pandas as pd
import streamlit as st

from dashboard.components.styles import (
    load_css,
    kpi_card,
    section_title,
)
from dashboard.queries import (
    get_anomaly_summary,
    get_anomaly_detail,
    get_daily_anomalies,
)


# =========================================================
# Page configuration
# =========================================================

st.set_page_config(
    page_title="Anomaly Monitoring | Smart Water",
    page_icon="🚨",
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
def load_anomaly_data():
    summary = get_anomaly_summary().iloc[0]
    daily = get_daily_anomalies()
    detail = get_anomaly_detail()

    return summary, daily, detail


summary, daily, detail = load_anomaly_data()


# =========================================================
# Header
# =========================================================

st.html(
    """
    <div class="hero">
        <div class="hero-title">
            🚨 Anomaly Monitoring
        </div>

        <div class="hero-subtitle">
            Detect, inspect and investigate abnormal network behaviour
            across the analyzed monitoring period.
        </div>

        <div class="hero-badge">
            MACHINE LEARNING · BASELINE DETECTION · INVESTIGATION
        </div>
    </div>
    """
)


# =========================================================
# Detection overview
# =========================================================

section_title(
    "Detection Overview",
    "Summary of anomaly detection results from the analytical pipeline.",
)

col1, col2, col3, col4 = st.columns(4)

with col1:
    kpi_card(
        "ML ANOMALIES",
        f"{int(summary['ml_anomaly_timestamps']):,}",
        "Detected by the ML pipeline",
    )

with col2:
    kpi_card(
        "ML ALERT RATE",
        f"{float(summary['ml_anomaly_rate_pct']):.2f}%",
        "Share of evaluated timestamps",
    )

with col3:
    kpi_card(
        "BASELINE ANOMALIES",
        f"{int(summary['baseline_anomaly_timestamps']):,}",
        "Detected by baseline rules",
    )

with col4:
    kpi_card(
        "OBSERVATIONS",
        f"{int(summary['scoring_timestamps']):,}",
        "Five-minute observations evaluated",
    )


# =========================================================
# Analysis period
# =========================================================

section_title(
    "Analysis Period",
    "Time range covered by the anomaly scoring pipeline.",
)

period_col1, period_col2 = st.columns(2)

with period_col1:
    st.metric(
        "Analysis Start",
        pd.to_datetime(
            summary["scoring_start"]
        ).strftime("%Y-%m-%d %H:%M"),
    )

with period_col2:
    st.metric(
        "Analysis End",
        pd.to_datetime(
            summary["scoring_end"]
        ).strftime("%Y-%m-%d %H:%M"),
    )


# =========================================================
# Daily anomaly activity
# =========================================================

section_title(
    "Anomaly Activity",
    "Daily comparison between machine-learning and baseline detection.",
)

if daily.empty:
    st.info("No daily anomaly data is currently available.")

else:
    chart_data = daily[
        [
            "day",
            "ml_anomaly_count",
            "baseline_anomaly_count",
        ]
    ].copy()

    chart_data["day"] = pd.to_datetime(chart_data["day"])

    chart_data = chart_data.set_index("day")

    chart_data = chart_data.rename(
        columns={
            "ml_anomaly_count": "ML anomalies",
            "baseline_anomaly_count": "Baseline anomalies",
        }
    )

    st.line_chart(
        chart_data,
        use_container_width=True,
    )


# =========================================================
# Investigation filters
# =========================================================

section_title(
    "Investigation",
    "Filter anomaly events and inspect individual observations.",
)

filter_col1, filter_col2, filter_col3 = st.columns(3)

with filter_col1:
    event_type = st.selectbox(
        "Event Type",
        [
            "All events",
            "ML anomalies",
            "Baseline anomalies",
            "Both detectors",
        ],
    )

with filter_col2:
    localization_filter = st.selectbox(
        "Localization",
        [
            "All",
            "Localized",
            "Unlocalized",
        ],
    )

with filter_col3:
    status_filter = st.selectbox(
        "Investigation Status",
        [
            "All",
            "Localized",
            "Unlocalized",
        ],
    )


# =========================================================
# Apply filters
# =========================================================

filtered = detail.copy()

filtered["timestamp"] = pd.to_datetime(
    filtered["timestamp"]
)


if event_type == "ML anomalies":
    filtered = filtered[
        filtered["is_ml_anomaly"]
    ]

elif event_type == "Baseline anomalies":
    filtered = filtered[
        filtered["is_baseline_anomaly"]
    ]

elif event_type == "Both detectors":
    filtered = filtered[
        filtered["is_ml_anomaly"]
        & filtered["is_baseline_anomaly"]
    ]


if localization_filter == "Localized":
    filtered = filtered[
        filtered["has_localization"]
    ]

elif localization_filter == "Unlocalized":
    filtered = filtered[
        ~filtered["has_localization"]
    ]


if status_filter != "All":
    filtered = filtered[
        filtered["investigation_status"] == status_filter
    ]


# =========================================================
# Investigation results
# =========================================================

st.caption(
    f"{len(filtered):,} events match the selected filters."
)


display_columns = [
    "timestamp",
    "ml_anomaly_score",
    "is_ml_anomaly",
    "baseline_sensor_fraction",
    "is_baseline_anomaly",
    "has_localization",
    "investigation_status",
]

available_columns = [
    column
    for column in display_columns
    if column in filtered.columns
]


if filtered.empty:
    st.info(
        "No events match the selected investigation filters."
    )

else:
    display_data = filtered[
        available_columns
    ].copy()

    display_data = display_data.rename(
        columns={
            "timestamp": "Timestamp",
            "ml_anomaly_score": "ML Score",
            "is_ml_anomaly": "ML Anomaly",
            "baseline_sensor_fraction": "Baseline Sensor Fraction",
            "is_baseline_anomaly": "Baseline Anomaly",
            "has_localization": "Localized",
            "investigation_status": "Investigation Status",
        }
    )

    st.dataframe(
        display_data,
        use_container_width=True,
        hide_index=True,
    )


# =========================================================
# Selected event investigation
# =========================================================

section_title(
    "Event Investigation",
    "Select an event to inspect its detection and localization status.",
)


if not filtered.empty:

    timestamps = (
        filtered["timestamp"]
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    selected_timestamp = st.selectbox(
        "Select anomaly timestamp",
        timestamps,
        format_func=lambda value: value.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    )

    selected_rows = filtered[
        filtered["timestamp"] == selected_timestamp
    ]

    selected = selected_rows.iloc[0]


    detail_col1, detail_col2 = st.columns(2)

    with detail_col1:

        st.metric(
            "ML Anomaly Score",
            f"{float(selected['ml_anomaly_score']):.4f}",
        )

        st.metric(
            "Baseline Sensor Fraction",
            f"{float(selected['baseline_sensor_fraction']):.2%}",
        )


    with detail_col2:

        st.metric(
            "ML Detection",
            "Detected"
            if bool(selected["is_ml_anomaly"])
            else "Not detected",
        )

        st.metric(
            "Baseline Detection",
            "Detected"
            if bool(selected["is_baseline_anomaly"])
            else "Not detected",
        )


    # -----------------------------------------------------
    # Investigation status
    # -----------------------------------------------------

    if bool(selected["has_localization"]):

        st.success(
            "Localization results are available for this event."
        )

    else:

        st.info(
            "No localization result is currently available for this event."
        )


# =========================================================
# Footer
# =========================================================

st.html(
    """
    <div class="footer">
        Smart Water Network Intelligence ·
        Anomaly Detection & Investigation
    </div>
    """
)