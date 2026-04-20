from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from public_app_helpers import (
    clean_label,
    get_public_snapshot,
    load_detection_timeline,
    load_public_case_studies,
    load_public_creator_signals,
    load_public_recent_detections,
    run_public_game_check,
    time_ago,
)


@st.cache_data(ttl=300)
def snapshot_cached() -> dict:
    return get_public_snapshot()


@st.cache_data(ttl=300)
def creator_signals_cached(limit: int) -> pd.DataFrame:
    return load_public_creator_signals(limit=limit)


@st.cache_data(ttl=300)
def recent_detections_cached(limit: int) -> pd.DataFrame:
    return load_public_recent_detections(limit=limit)


@st.cache_data(ttl=300)
def timeline_cached(days: int) -> pd.DataFrame:
    return load_detection_timeline(days=days)


@st.cache_data(ttl=300)
def case_studies_cached(limit: int) -> pd.DataFrame:
    return load_public_case_studies(limit=limit)


@st.cache_data(ttl=120)
def game_check_cached(game_name: str) -> dict:
    return run_public_game_check(game_name)


st.set_page_config(
    page_title="Early Shift Lite",
    page_icon="ES",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
    .block-container {padding-top: 2rem; padding-bottom: 3rem; max-width: 1200px;}
    .hero {
        padding: 1.4rem 1.5rem;
        border-radius: 20px;
        background: linear-gradient(135deg, #f4efe4 0%, #dce8e2 50%, #eef4fb 100%);
        border: 1px solid #d8d0c0;
        margin-bottom: 1rem;
    }
    .hero-kicker {
        font-size: 0.82rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #546b5e;
        margin-bottom: 0.35rem;
        font-weight: 700;
    }
    .hero-title {
        font-size: 2.2rem;
        line-height: 1.05;
        margin: 0;
        color: #1b2f27;
        font-weight: 800;
    }
    .hero-copy {
        margin-top: 0.8rem;
        color: #2d3e37;
        font-size: 1rem;
        max-width: 52rem;
    }
    .mini-card {
        padding: 1rem 1.1rem;
        border-radius: 16px;
        background: #fbfaf6;
        border: 1px solid #e5dcc9;
        height: 100%;
    }
    .mini-card h4 {
        margin: 0 0 0.45rem 0;
        font-size: 0.95rem;
        color: #2d3e37;
    }
    .mini-card p {
        margin: 0;
        color: #53635c;
        font-size: 0.92rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

snapshot = snapshot_cached()

st.markdown(
    f"""
    <div class="hero">
        <div class="hero-kicker">Public Signal Report</div>
        <h1 class="hero-title">Early Shift</h1>
        <div class="hero-copy">
            Track which creators are actually moving Roblox CCU, separate flash spikes from sticky growth,
            and give studios a fast read on whether a game has momentum or just noise.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)
with stats_col1:
    st.metric("Tracked games", f"{int(snapshot['tracked_games']):,}")
with stats_col2:
    st.metric("YouTube videos indexed", f"{int(snapshot['youtube_videos']):,}")
with stats_col3:
    st.metric("Creator-linked detections", f"{int(snapshot['creator_linked_spikes']):,}")
with stats_col4:
    st.metric("Retention profiles", f"{int(snapshot['retention_profiles']):,}")

intro_col, tool_col = st.columns([1.35, 1.0], gap="large")

with intro_col:
    st.subheader("What this public view shows")
    card_col1, card_col2, card_col3 = st.columns(3)
    with card_col1:
        st.markdown(
            """
            <div class="mini-card">
                <h4>Creator impact</h4>
                <p>Which channels repeatedly show up before a lift, how broad their reach is, and how quickly the lift lands.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with card_col2:
        st.markdown(
            """
            <div class="mini-card">
                <h4>Detection proof</h4>
                <p>Recent creator-linked spikes, median lag from upload to lift, and whether the spike looks sticky or disposable.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with card_col3:
        st.markdown(
            """
            <div class="mini-card">
                <h4>Studio wedge</h4>
                <p>A quick game check that turns the analytics stack into a simple recommendation a developer can use immediately.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.caption(
        "Latest CCU snapshot: "
        f"{snapshot['last_ccu_at']:%Y-%m-%d %H:%M UTC} | "
        "Latest creator-linked spike in this DB: "
        f"{snapshot['last_spike_at']:%Y-%m-%d %H:%M UTC}"
    )

with tool_col:
    st.subheader("Check a Roblox game")
    with st.form("game-check-form", clear_on_submit=False):
        game_name = st.text_input(
            "Game name",
            placeholder="Dress To Impress",
            help="Search the local CCU database plus the last 72 hours of YouTube coverage.",
        )
        submitted = st.form_submit_button("Run check", use_container_width=True, type="primary")

    if submitted and game_name.strip():
        result = game_check_cached(game_name.strip())
        ccu = result["ccu"]

        metric_col1, metric_col2, metric_col3 = st.columns(3)
        with metric_col1:
            st.metric("YouTube signal", f"{result['signal_emoji']} {result['signal']}")
        with metric_col2:
            st.metric("Videos (72h)", result["video_count"])
        with metric_col3:
            growth_value = ccu.get("growth_pct")
            if growth_value is None:
                st.metric("Growth", "N/A")
            else:
                st.metric("Growth", f"{growth_value:+.1f}%")

        if ccu.get("found"):
            st.caption(
                f"Matched game: {clean_label(ccu.get('game_name'))} | "
                f"Current CCU: {int(ccu.get('current_ccu') or 0):,}"
            )
        else:
            st.caption("No close CCU match found in the local snapshot. Showing creator activity only.")

        st.info(result["recommendation"])

        if result["videos"]:
            with st.expander("Recent creator coverage", expanded=True):
                for video in result["videos"][:5]:
                    st.markdown(f"**{video['title']}**")
                    st.caption(f"{video['channel']} | {video.get('published', '')[:19]}")
                    if video.get("url"):
                        st.markdown(f"[Watch on YouTube]({video['url']})")
        else:
            st.caption("No matching YouTube videos found in the last 72 hours.")
    else:
        st.caption("Use this to turn the analytics stack into a plain-English signal for one game.")

st.markdown("---")

st.subheader("Creator impact")
st.caption("The creators below repeatedly appear before lift, not just once.")

creator_df = creator_signals_cached(limit=10)
if creator_df.empty:
    st.info("Not enough creator-linked detections yet to populate this view.")
else:
    st.dataframe(
        creator_df.rename(
            columns={
                "creator": "Creator",
                "spike_detections": "Detections",
                "spike_videos": "Spike Videos",
                "games_covered": "Games",
                "median_growth": "Median Growth",
                "hit_rate_pct": "Hit Rate",
                "median_lag_hours": "Median Lag (h)",
            }
        ),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Detections": st.column_config.NumberColumn(format="%d"),
            "Spike Videos": st.column_config.NumberColumn(format="%d"),
            "Games": st.column_config.NumberColumn(format="%d"),
            "Median Growth": st.column_config.NumberColumn(format="+%.1f%%"),
            "Hit Rate": st.column_config.NumberColumn(format="%.1f%%"),
            "Median Lag (h)": st.column_config.NumberColumn(format="%.1f"),
        },
    )

st.markdown("---")

st.subheader("Detection proof")
timeline_df = timeline_cached(days=14)
recent_df = recent_detections_cached(limit=10)

proof_col1, proof_col2 = st.columns([1.1, 1.2], gap="large")

with proof_col1:
    if timeline_df.empty:
        st.info("No recent detection timeline available.")
    else:
        figure = go.Figure()
        figure.add_bar(
            x=timeline_df["detected_day"],
            y=timeline_df["detections"],
            name="Detections",
            marker_color="#8fb8a7",
        )
        figure.add_scatter(
            x=timeline_df["detected_day"],
            y=timeline_df["median_growth"],
            name="Median growth %",
            yaxis="y2",
            mode="lines+markers",
            line=dict(color="#c06d3d", width=3),
        )
        figure.update_layout(
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
            yaxis=dict(title="Detections"),
            yaxis2=dict(title="Median growth %", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.08, x=0),
        )
        st.plotly_chart(figure, use_container_width=True)

with proof_col2:
    if recent_df.empty:
        st.info("No recent creator-linked detections available.")
    else:
        recent_view = recent_df.copy()
        recent_view["detected_at"] = recent_view["detected_at"].apply(time_ago)
        st.dataframe(
            recent_view.rename(
                columns={
                    "game_name": "Game",
                    "channel_title": "Creator",
                    "growth_percent": "Growth",
                    "current_ccu": "CCU",
                    "stickiness_pct": "Stickiness",
                    "decay_days": "Decay Days",
                    "lag_hours": "Lag (h)",
                    "detected_at": "Detected",
                    "video_url": "Video",
                }
            ),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Growth": st.column_config.NumberColumn(format="+%.1f%%"),
                "CCU": st.column_config.NumberColumn(format="%,d"),
                "Stickiness": st.column_config.NumberColumn(format="%.1f%%"),
                "Decay Days": st.column_config.NumberColumn(format="%.1f"),
                "Lag (h)": st.column_config.NumberColumn(format="%.1f"),
                "Video": st.column_config.LinkColumn(display_text="Watch"),
            },
        )

st.markdown("---")

st.subheader("Case studies")
st.caption("Three compact examples pulled from the current local snapshot.")

case_df = case_studies_cached(limit=3)
case_columns = st.columns(3, gap="large")

if case_df.empty:
    st.info("No case studies available yet.")
else:
    for column, row in zip(case_columns, case_df.itertuples(index=False), strict=False):
        with column:
            st.markdown(
                f"""
                <div class="mini-card">
                    <h4>{row.game_name}</h4>
                    <p>
                        <strong>Standout creator:</strong> {row.standout_creator}<br>
                        <strong>Peak growth at detection:</strong> +{row.max_growth:.1f}%<br>
                        <strong>Current CCU at detection:</strong> {int(row.current_ccu):,}<br>
                        <strong>Creator videos linked:</strong> {int(row.creator_videos)} across {int(row.creators)} creators<br>
                        <strong>Stickiness:</strong> {row.stickiness_pct if row.stickiness_pct is not None else 'N/A'}<br>
                        <strong>Decay days:</strong> {row.decay_days if row.decay_days is not None else 'N/A'}
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if row.standout_mechanic and row.standout_mechanic != "-":
                st.caption(f"Example mechanic signal: {row.standout_mechanic}")
            st.caption(
                f"Window: {row.first_detected_at:%Y-%m-%d} to {row.last_detected_at:%Y-%m-%d}"
            )
