import streamlit as st
from textwrap import dedent


def load_css():
    """
    Load the custom visual design system for the dashboard.
    """

    st.markdown(
        dedent(
            """
            <style>

            /* -------------------------------------------------
               Global
            ------------------------------------------------- */

            .main {
                background-color: #f7f9fc;
            }

            .block-container {
                padding-top: 2rem;
                padding-bottom: 3rem;
                max-width: 1400px;
            }

            /* -------------------------------------------------
               Header
            ------------------------------------------------- */

            .hero {
                padding: 1.8rem 2rem;
                border-radius: 18px;
                background: linear-gradient(
                    135deg,
                    #0f172a 0%,
                    #123b5d 55%,
                    #0e7490 100%
                );
                color: white;
                margin-bottom: 1.5rem;
            }

            .hero-title {
                font-size: 2.4rem;
                font-weight: 750;
                margin-bottom: 0.3rem;
            }

            .hero-subtitle {
                font-size: 1.05rem;
                opacity: 0.88;
                margin-bottom: 0;
            }

            .hero-badge {
                display: inline-block;
                margin-top: 1rem;
                padding: 0.35rem 0.75rem;
                border-radius: 999px;
                background-color: rgba(255,255,255,0.12);
                font-size: 0.78rem;
                letter-spacing: 0.04em;
            }

            /* -------------------------------------------------
               Section titles
            ------------------------------------------------- */

            .section-title {
                font-size: 1.15rem;
                font-weight: 700;
                color: #0f172a;
                margin-top: 1rem;
                margin-bottom: 0.8rem;
            }

            .section-caption {
                color: #64748b;
                font-size: 0.88rem;
                margin-top: -0.5rem;
                margin-bottom: 1rem;
            }

            /* -------------------------------------------------
               KPI cards
            ------------------------------------------------- */

            .kpi-card {
                background: white;
                border: 1px solid #e2e8f0;
                border-radius: 16px;
                padding: 1.15rem 1.2rem;
                min-height: 125px;
                box-shadow: 0 2px 8px rgba(15, 23, 42, 0.04);
            }

            .kpi-label {
                color: #64748b;
                font-size: 0.78rem;
                font-weight: 650;
                text-transform: uppercase;
                letter-spacing: 0.055em;
            }

            .kpi-value {
                color: #0f172a;
                font-size: 2rem;
                font-weight: 750;
                line-height: 1.15;
                margin-top: 0.4rem;
            }

            .kpi-description {
                color: #94a3b8;
                font-size: 0.78rem;
                margin-top: 0.4rem;
            }

            /* -------------------------------------------------
               Insight cards
            ------------------------------------------------- */

            .insight-card {
                background: white;
                border: 1px solid #e2e8f0;
                border-radius: 16px;
                padding: 1.25rem 1.35rem;
                min-height: 180px;
                box-shadow: 0 2px 8px rgba(15, 23, 42, 0.035);
            }

            .insight-title {
                color: #0f172a;
                font-weight: 700;
                font-size: 1rem;
                margin-bottom: 0.75rem;
            }

            .insight-value {
                font-size: 1.65rem;
                font-weight: 750;
                color: #0e7490;
            }

            .insight-text {
                color: #475569;
                font-size: 0.88rem;
                line-height: 1.55;
            }

            /* -------------------------------------------------
               Workflow
            ------------------------------------------------- */

            .workflow-card {
                background: white;
                border: 1px solid #e2e8f0;
                border-radius: 14px;
                padding: 1rem;
                min-height: 120px;
            }

            .workflow-number {
                color: #0e7490;
                font-weight: 750;
                font-size: 0.8rem;
            }

            .workflow-title {
                color: #0f172a;
                font-weight: 700;
                margin-top: 0.25rem;
            }

            .workflow-text {
                color: #64748b;
                font-size: 0.82rem;
                margin-top: 0.35rem;
            }

            /* -------------------------------------------------
               Footer
            ------------------------------------------------- */

            .footer {
                text-align: center;
                color: #94a3b8;
                font-size: 0.78rem;
                padding-top: 2rem;
            }

            </style>
            """
        ),
        unsafe_allow_html=True,
    )


def kpi_card(label, value, description=""):
    """
    Render a reusable KPI card.
    """

    html = dedent(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-description">{description}</div>
        </div>
        """
    )

    st.markdown(html, unsafe_allow_html=True)


def section_title(title, caption=None):
    """
    Render a consistent section heading.
    """

    st.markdown(
        dedent(
            f"""
            <div class="section-title">{title}</div>
            """
        ),
        unsafe_allow_html=True,
    )

    if caption:
        st.markdown(
            dedent(
                f"""
                <div class="section-caption">{caption}</div>
                """
            ),
            unsafe_allow_html=True,
        )