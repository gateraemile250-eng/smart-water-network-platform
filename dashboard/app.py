import os
import sys

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
# Page configuration
# =========================================================

st.set_page_config(
    page_title="Smart Water Network Intelligence",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =========================================================
# Application pages
# =========================================================

overview = st.Page(
    "app_overview.py",
    title="Overview",
    icon="💧",
    default=True,
)

anomaly_monitoring = st.Page(
    "pages/1_Anomaly_Monitoring.py",
    title="Anomaly Monitoring",
    icon="🚨",
)

network_localization = st.Page(
    "pages/2_Network_Localization.py",
    title="Network Localization",
    icon="📍",
)


# =========================================================
# Navigation
# =========================================================

pg = st.navigation(
    {
        "Dashboard": [
            overview,
            anomaly_monitoring,
            network_localization,
        ]
    }
)


# =========================================================
# Sidebar branding
# =========================================================

with st.sidebar:

    st.html(
        """
        <div style="padding: 0.4rem 0 1rem 0;">
            <div style="
                font-size: 1.35rem;
                font-weight: 750;
                color: #0f172a;
            ">
                💧 Smart Water
            </div>

            <div style="
                font-size: 0.85rem;
                color: #64748b;
                margin-top: 0.25rem;
            ">
                Network Intelligence Platform
            </div>
        </div>
        """
    )

    st.divider()

    st.caption(
        "Data Engineering · Machine Learning · "
        "Network Analytics · GIS"
    )


# =========================================================
# Run selected page
# =========================================================

pg.run()