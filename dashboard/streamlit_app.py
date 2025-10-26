# -*- coding: utf-8 -*-
"""
Created on Sat Oct 25 22:03:56 2025

@author: ong_w
"""

# streamlit_app.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os

HOMEFOLDER = r"C:\Users\ong_w\OneDrive\Courses\NUS-ISS\MTech EBAC\EBA5004 PLP\PracticeProject\PropInsight Data"
PANELFILE = os.path.join(HOMEFOLDER, 'panel_view.csv')
POLICYFILE = os.path.join(HOMEFOLDER, 'policy_events.csv')

# -----------------------------
# Load Data
# -----------------------------
panel_view = pd.read_csv(PANELFILE, parse_dates=["time"])
policy_events = pd.read_csv(POLICYFILE, parse_dates=["event_date"])

st.title("PropInsight: Sentiment Dashboard with Policy & Forum Filters")

# -----------------------------
# Sidebar Controls
# -----------------------------
st.sidebar.header("Filters")

# Time granularity
level_options = panel_view["level"].unique().tolist()
selected_level = st.sidebar.selectbox("Select Time Granularity", level_options)

# Agency filter
agency_options = policy_events["agency"].unique().tolist()
selected_agencies = st.sidebar.multiselect(
    "Filter Policy Events by Agency",
    options=agency_options,
    default=agency_options
)

# Forum filter (parse from column names like 'sgexpats_overall')
all_forums = sorted({col.split("_")[0] for col in panel_view.columns if col not in ["level","time"]})
selected_forums = st.sidebar.multiselect(
    "Filter by Forum",
    options=all_forums,
    default=all_forums
)

# -----------------------------
# Filter Data
# -----------------------------
df = panel_view[panel_view["level"] == selected_level].copy()

# -----------------------------
# Helper to plot sentiment chart
# -----------------------------
def plot_sentiment(df, series_cols, title, events_df=None):
    fig = go.Figure()

    # Add sentiment lines
    for col in series_cols:
        fig.add_trace(go.Scatter(
            x=df["time"], y=df[col],
            mode="lines+markers",
            name=col
        ))

    # Overlay policy events as hoverable markers
    if events_df is not None and not events_df.empty:
        filtered_events = events_df[events_df["agency"].isin(selected_agencies)]
        if not filtered_events.empty:
            fig.add_trace(go.Scatter(
                x=filtered_events["event_date"],
                y=[df[series_cols].max().max()] * len(filtered_events),  # place markers at top
                mode="markers",
                marker=dict(color="red", size=10, symbol="line-ns-open"),
                name="Policy Events",
                text=filtered_events.apply(
                    lambda r: f"<b>{r['agency']}</b>: {r['policy']}<br>{r['description']}", axis=1
                ),
                hoverinfo="text",
                showlegend=False
            ))

    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Average Sentiment Score",
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2)
    )
    return fig

# -----------------------------
# Top: Overall Sentiment
# -----------------------------
overall_cols = [c for c in df.columns if "overall" in c and c.split("_")[0] in selected_forums]
st.plotly_chart(
    plot_sentiment(df, overall_cols, "Overall Sentiment Trends", policy_events),
    use_container_width=True
)

# -----------------------------
# Bottom: Policy, Price, Affordability
# -----------------------------
col1, col2, col3 = st.columns(3)

with col1:
    policy_cols = [c for c in df.columns if "policy" in c and c.split("_")[0] in selected_forums]
    st.plotly_chart(
        plot_sentiment(df, policy_cols, "Policy Sentiment", policy_events),
        use_container_width=True
    )

with col2:
    price_cols = [c for c in df.columns if "price" in c and c.split("_")[0] in selected_forums]
    st.plotly_chart(
        plot_sentiment(df, price_cols, "Price Sentiment", policy_events),
        use_container_width=True
    )

with col3:
    affordability_cols = [c for c in df.columns if "affordability" in c and c.split("_")[0] in selected_forums]
    st.plotly_chart(
        plot_sentiment(df, affordability_cols, "Affordability Sentiment", policy_events),
        use_container_width=True
    )
