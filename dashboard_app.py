from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd
import streamlit as st

from mechanic_detector import MechanicSpike, detect_mechanic_spikes, get_historical_spikes

def _utc_string(dt: datetime | None) -> str:
    if dt is None:
        return '-'
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


def _query_dataframe(query: str, params: tuple = ()) -> pd.DataFrame:
    """Execute query via DuckDB and return a pandas DataFrame (pure Python conversion)."""
    with duckdb.connect(str(DB_PATH), read_only=True) as db:
        result = db.execute(query, params)
        columns = [col[0] for col in result.description]
        rows = result.fetchall()
    return pd.DataFrame(rows, columns=columns)


DB_PATH = Path(__file__).resolve().with_name("early_shift.db")

st.set_page_config(
    page_title="Early Shift Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(ttl=300)
def load_top_movers(growth_threshold: float, limit: int) -> pd.DataFrame:
    query = """
        WITH latest_snapshot AS (
            SELECT universe_id,
                   name,
                   ccu,
                   timestamp,
                   ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
            FROM games
        ),
        filtered_latest AS (
            SELECT universe_id, name, ccu, timestamp
            FROM latest_snapshot
            WHERE row_num = 1
        ),
        week_ago_snapshot AS (
            SELECT universe_id,
                   ccu,
                   timestamp,
                   ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
            FROM games
            WHERE timestamp <= current_timestamp - INTERVAL 7 DAY
        ),
        filtered_week_ago AS (
            SELECT universe_id, ccu, timestamp
            FROM week_ago_snapshot
            WHERE row_num = 1
        )
        SELECT cur.universe_id,
               COALESCE(meta.name, cur.name) AS game_name,
               cur.ccu AS current_ccu,
               prev.ccu AS week_ago_ccu,
               ROUND(((cur.ccu - prev.ccu) * 100.0) / NULLIF(prev.ccu, 0), 1) AS growth_percent,
               cur.timestamp AS current_timestamp
        FROM filtered_latest cur
        JOIN filtered_week_ago prev ON prev.universe_id = cur.universe_id
        LEFT JOIN game_metadata meta ON meta.universe_id = cur.universe_id
        WHERE prev.ccu > 0
          AND ((cur.ccu - prev.ccu) * 1.0) / NULLIF(prev.ccu, 0) >= ?
        ORDER BY growth_percent DESC
        LIMIT ?
    """
    return _query_dataframe(query, (growth_threshold, limit))


@st.cache_data(ttl=300)
def load_recent_videos(hours: int) -> pd.DataFrame:
    query = """
        SELECT channel_title,
               title,
               'https://youtube.com/watch?v=' || video_id AS video_url,
               view_count,
               published_at
        FROM youtube_videos
        WHERE published_at >= current_timestamp - (? * INTERVAL 1 HOUR)
        ORDER BY published_at DESC
    """
    return _query_dataframe(query, (hours,))


@st.cache_data(ttl=180)
def detect_spikes_cached(
    lookback_hours: int, growth_threshold: float
) -> pd.DataFrame:
    spikes: Iterable[MechanicSpike] = detect_mechanic_spikes(
        db_path=str(DB_PATH),
        lookback_hours=lookback_hours,
        growth_threshold=growth_threshold,
    )
    rows = [
        {
            "Game": spike.game_name,
            "Growth %": round(spike.growth_percent, 1),
            "Current CCU": spike.current_ccu,
            "Mechanic": spike.mechanic,
            "YouTube": spike.video_url,
            "Channel": spike.channel_title,
            "Published": _utc_string(spike.published_at),
            "Detected": _utc_string(spike.detected_at),
        }
        for spike in spikes
    ]
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_historical_spikes(limit: int) -> pd.DataFrame:
    spikes = get_historical_spikes(db_path=str(DB_PATH), limit=limit)
    rows = [
        {
            "Game": spike.game_name,
            "Growth %": round(spike.growth_percent, 1),
            "Current CCU": spike.current_ccu,
            "Mechanic": spike.mechanic,
            "YouTube": spike.video_url,
            "Channel": spike.channel_title,
            "Published": _utc_string(spike.published_at),
            "Detected": _utc_string(spike.detected_at),
        }
        for spike in spikes
    ]
    return pd.DataFrame(rows)


st.title("Early Shift – Mechanic Spike Monitor")
st.caption(
    "Live view of Roblox market signals: CCU trends, creator chatter, and mechanic spike candidates."
)

st.sidebar.header("Filters")
threshold_pct = st.sidebar.slider("Growth threshold (%)", 10, 150, 25, step=5)
lookback_hours = st.sidebar.slider("Video lookback (hours)", 12, 96, 48, step=12)
top_limit = st.sidebar.slider("Top movers to display", 10, 100, 25, step=5)
history_limit = st.sidebar.slider("Historical spikes to display", 10, 200, 50, step=10)
refresh_now = st.sidebar.button("Refresh data caches")

if refresh_now:
    load_top_movers.clear()
    load_recent_videos.clear()
    detect_spikes_cached.clear()
    load_historical_spikes.clear()
    st.toast("Caches cleared – data will refresh on next load.")

threshold_ratio = threshold_pct / 100.0

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top CCU Movers")
    movers_df = load_top_movers(threshold_ratio, top_limit)
    if movers_df.empty:
        st.info("No games meet the current growth threshold.")
    else:
        st.dataframe(
            movers_df,
            use_container_width=True,
            hide_index=True,
        )

with col2:
    st.subheader("Recent Creator Mentions")
    videos_df = load_recent_videos(lookback_hours)
    if videos_df.empty:
        st.info("No creator videos in the selected window.")
    else:
        st.dataframe(
            videos_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "video_url": st.column_config.LinkColumn("YouTube", display_text="Watch"),
                "view_count": st.column_config.NumberColumn("Views", format="%d"),
            },
        )

st.markdown("---")
st.subheader("Mechanic Spike Candidates")
spike_df = detect_spikes_cached(lookback_hours, threshold_ratio)
if spike_df.empty:
    st.info(
        "No mechanic spikes detected for the current CCU and video thresholds."
    )
else:
    st.dataframe(
        spike_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "YouTube": st.column_config.LinkColumn(display_text="Open"),
            "Current CCU": st.column_config.NumberColumn(format="%d"),
            "Growth %": st.column_config.NumberColumn(format="%.1f%%"),
        },
    )

st.markdown("---")
st.caption(
    "Data source: DuckDB snapshots (games, metadata, YouTube) updated via Early Shift collectors."
)

