import pandas as pd
import streamlit as st

from dashboard.components.styles import load_css, kpi_card, section_title
from dashboard.queries import (
    get_anomaly_summary,
    get_localization_summary,
    get_localization_summary_by_timestamp,
    get_localization_candidates,
)


# =========================================================
# Page configuration
# =========================================================

st.set_page_config(
    page_title="Network Localization | Smart Water",
    page_icon="📍",
    layout="wide",
)


# =========================================================
# Styling
# =========================================================

load_css()


# =========================================================
# Load localization data
# =========================================================

@st.cache_data(ttl=300)
def load_localization_data():
    anomaly_summary = get_anomaly_summary().iloc[0]
    localization_summary = get_localization_summary().iloc[0]
    timestamps = get_localization_summary_by_timestamp()

    return anomaly_summary, localization_summary, timestamps


anomaly_summary, localization_summary, timestamps = (
    load_localization_data()
)


# =========================================================
# Header
# =========================================================

st.markdown(
"""<div class="hero">
<div class="hero-title">
📍 Network Localization
</div>

<div class="hero-subtitle">
Connect detected anomalies to candidate network pipes
and investigate their likely network location.
</div>

<div class="hero-badge">
NETWORK ANALYTICS · LOCALIZATION · ASSET INVESTIGATION
</div>
</div>""",
unsafe_allow_html=True,
)


# =========================================================
# Localization overview
# =========================================================

section_title(
    "Localization Overview",
    "Summary of anomaly localization results from the network analysis pipeline.",
)

col1, col2, col3, col4 = st.columns(4)


with col1:
    kpi_card(
        "ML ANOMALIES",
        f"{int(anomaly_summary['ml_anomaly_timestamps']):,}",
        "Anomalous timestamps evaluated",
    )


with col2:
    kpi_card(
        "LOCALIZED EVENTS",
        f"{int(localization_summary['localized_anomalies']):,}",
        "Anomalies with candidate network assets",
    )


with col3:
    kpi_card(
        "UNLOCALIZED EVENTS",
        f"{int(localization_summary['unlocalized_anomalies']):,}",
        "Anomalies without localization results",
    )


with col4:
    kpi_card(
        "LOCALIZATION COVERAGE",
        f"{float(localization_summary['localization_coverage_pct']):.2f}%",
        "ML anomalies with localization",
    )


# =========================================================
# Localization activity
# =========================================================

section_title(
    "Localization Activity",
    "Localized anomaly timestamps and their candidate-pipe coverage.",
)


if not timestamps.empty:

    chart_data = timestamps[
        [
            "timestamp",
            "candidate_pipe_count",
        ]
    ].copy()

    chart_data["timestamp"] = pd.to_datetime(
        chart_data["timestamp"]
    )

    chart_data = chart_data.set_index("timestamp")

    st.line_chart(
        chart_data,
        use_container_width=True,
    )

else:

    st.info(
        "No localization results are currently available."
    )


# =========================================================
# Localization investigation
# =========================================================

section_title(
    "Localization Investigation",
    "Select a localized anomaly timestamp to inspect candidate network pipes.",
)


if not timestamps.empty:

    timestamps = timestamps.copy()

    timestamps["timestamp"] = pd.to_datetime(
        timestamps["timestamp"]
    )

    selected_timestamp = st.selectbox(
        "Select localized anomaly timestamp",
        timestamps["timestamp"].drop_duplicates(),
        format_func=lambda value: value.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    )

    selected_summary = timestamps[
        timestamps["timestamp"] == selected_timestamp
    ].iloc[0]


    # =====================================================
    # Selected event metrics
    # =====================================================

    detail_col1, detail_col2, detail_col3, detail_col4 = (
        st.columns(4)
    )


    with detail_col1:
        st.metric(
            "Candidate Pipes",
            f"{int(selected_summary['candidate_pipe_count']):,}",
        )


    with detail_col2:
        st.metric(
            "Best Candidate Rank",
            str(
                int(
                    selected_summary["best_candidate_rank"]
                )
            ),
        )


    with detail_col3:
        st.metric(
            "Top Candidate Score",
            f"{float(selected_summary['top_candidate_score']):.4f}",
        )


    with detail_col4:
        st.metric(
            "Unique Candidate Pipes",
            f"{int(selected_summary['unique_candidate_pipes']):,}",
        )


    # =====================================================
    # Candidate network assets
    # =====================================================

    candidates = get_localization_candidates(
        selected_timestamp
    )


    section_title(
        "Candidate Network Assets",
        "Network pipes associated with the selected anomaly timestamp.",
    )


    if not candidates.empty:

        display_columns = [
            "candidate_rank",
            "pipe_id",
            "localization_score",
            "link_type",
            "from_node_id",
            "to_node_id",
        ]

        available_columns = [
            column
            for column in display_columns
            if column in candidates.columns
        ]

        st.dataframe(
            candidates[available_columns],
            use_container_width=True,
            hide_index=True,
        )

    else:

        st.info(
            "No candidate network assets are available "
            "for the selected timestamp."
        )


else:

    st.info(
        "No localized anomaly timestamps are currently available."
    )


# =========================================================
# Footer
# =========================================================

st.markdown(
    """
    <div class="footer">
        Smart Water Network Intelligence ·
        Network Localization & Asset Investigation
    </div>
    """,
    unsafe_allow_html=True,
)