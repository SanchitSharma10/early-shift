from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Iterable, Dict, Any
import os

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from constants import DEFAULT_DB_PATH
from db_manager import get_db_connection
from mechanic_detector import MechanicSpike, detect_mechanic_spikes, get_historical_spikes
from queries import TRENDING_GAMES_QUERY
from check_my_game import search_youtube_for_game, get_game_ccu_status
import asyncio

DB_PATH = os.getenv("DB_PATH", DEFAULT_DB_PATH)
from check_my_game import search_youtube_for_game, get_game_ccu_status
import asyncio


def _utc_string(dt: datetime | None) -> str:
    """Format datetime as UTC string."""
    if dt is None:
        return '-'
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


def _time_ago(dt: datetime | None) -> str:
    """Format datetime as relative time (e.g., '2h ago')."""
    if dt is None:
        return '-'
    
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    now = datetime.now(timezone.utc)
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    elif seconds < 86400:
        return f"{int(seconds // 3600)}h ago"
    else:
        days = int(seconds // 86400)
        return f"{days}d ago"


def _signal_strength(video_count: int, growth_percent: float) -> str:
    """Calculate signal strength emoji based on video count and growth."""
    # Strong signal: 15+ videos OR 100%+ growth with 8+ videos
    if video_count >= 15 or (growth_percent >= 100 and video_count >= 8):
        return "🔥🔥🔥"
    # Medium signal: 8+ videos OR 50%+ growth with 5+ videos
    elif video_count >= 8 or (growth_percent >= 50 and video_count >= 5):
        return "🔥🔥"
    # Weak signal: everything else
    else:
        return "🔥"


def _query_dataframe(query: str, params: tuple = ()) -> pd.DataFrame:
    """Execute query via DuckDB and return a pandas DataFrame."""
    with get_db_connection(DB_PATH, read_only=True) as db:
        result = db.execute(query, params)
        columns = [col[0] for col in result.description]
        rows = result.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _calculate_stickiness_index(db, universe_id: int) -> float | None:
    """Calculate historical stickiness for a single game."""
    result = db.execute(
        """
        WITH daily_ccu AS (
            SELECT 
                CAST(timestamp AS DATE) as date,
                MAX(ccu) as peak_ccu
            FROM games
            WHERE universe_id = ?
              AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 60 DAY
              AND timestamp <= CURRENT_TIMESTAMP - INTERVAL 14 DAY
            GROUP BY CAST(timestamp AS DATE)
        ),
        peaks AS (
            SELECT 
                date,
                peak_ccu,
                LAG(peak_ccu, 1) OVER (ORDER BY date) as prev_ccu
            FROM daily_ccu
        ),
        spike_retention AS (
            SELECT 
                p.peak_ccu,
                d.peak_ccu as ccu_7d_later,
                CASE 
                    WHEN p.peak_ccu > 0 AND d.peak_ccu IS NOT NULL 
                    THEN d.peak_ccu * 1.0 / p.peak_ccu 
                    ELSE NULL 
                END as retention_ratio
            FROM peaks p
            LEFT JOIN daily_ccu d
              ON d.date = CAST(p.date + INTERVAL 7 DAY AS DATE)
            WHERE p.prev_ccu IS NOT NULL 
              AND p.peak_ccu > p.prev_ccu * 1.2
              AND d.peak_ccu IS NOT NULL
        )
        SELECT AVG(retention_ratio) as avg_stickiness
        FROM spike_retention
        WHERE retention_ratio IS NOT NULL
        """,
        (universe_id,),
    ).fetchone()
    if result and result[0] is not None:
        return min(float(result[0]), 1.0)
    return None


def _calculate_decay_days(db, universe_id: int) -> float | None:
    """Calculate median decay time (days) for historical spikes."""
    result = db.execute(
        """
        WITH daily_ccu AS (
            SELECT 
                CAST(timestamp AS DATE) as date,
                MAX(ccu) as peak_ccu
            FROM games
            WHERE universe_id = ?
              AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 60 DAY
              AND timestamp <= CURRENT_TIMESTAMP - INTERVAL 14 DAY
            GROUP BY CAST(timestamp AS DATE)
        ),
        spikes AS (
            SELECT 
                date,
                peak_ccu,
                LAG(peak_ccu, 1) OVER (ORDER BY date) as prev_ccu
            FROM daily_ccu
        ),
        decay_points AS (
            SELECT 
                s.date as spike_date,
                s.peak_ccu as spike_ccu,
                MIN(d.date) as decay_date
            FROM spikes s
            JOIN daily_ccu d
              ON d.date > s.date
             AND d.date <= CAST(s.date + INTERVAL 14 DAY AS DATE)
             AND d.peak_ccu <= s.peak_ccu * 0.5
            WHERE s.prev_ccu IS NOT NULL
              AND s.peak_ccu > s.prev_ccu * 1.2
            GROUP BY s.date, s.peak_ccu
        ),
        decay_values AS (
            SELECT 
                DATEDIFF('day', spike_date, decay_date) as decay_days
            FROM decay_points
            WHERE decay_date IS NOT NULL
        )
        SELECT median(decay_days) as median_decay
        FROM decay_values
        """,
        (universe_id,),
    ).fetchone()
    if result and result[0] is not None:
        return float(result[0])
    return None


def _compute_spike_quality(stickiness: float | None, decay_days: float | None) -> float | None:
    """Score spike quality using stickiness and decay."""
    scores = []
    weights = []
    if stickiness is not None:
        scores.append(min(max(stickiness, 0.0), 1.0))
        weights.append(0.7)
    if decay_days is not None:
        decay_score = min(max(decay_days / 7.0, 0.0), 1.0)
        scores.append(decay_score)
        weights.append(0.3)
    if not scores:
        return None
    weighted = sum(score * weight for score, weight in zip(scores, weights))
    return round(weighted / sum(weights) * 100, 1)


@st.cache_data(ttl=300)
def get_retention_stats(universe_ids: tuple[int, ...]) -> dict[int, dict[str, float | None]]:
    """Fetch stickiness and decay stats for a set of games."""
    if not universe_ids:
        return {}
    stats: dict[int, dict[str, float | None]] = {}
    with get_db_connection(DB_PATH, read_only=True) as db:
        for universe_id in universe_ids:
            stickiness = _calculate_stickiness_index(db, int(universe_id))
            decay_days = _calculate_decay_days(db, int(universe_id))
            stats[int(universe_id)] = {
                "stickiness_index": stickiness,
                "decay_days": decay_days,
            }
    return stats


@st.cache_data(ttl=180)
def get_investigation_queue_status() -> Dict[str, Any]:
    """Get investigation activity status."""
    # Get total investigations
    query_total = """
        SELECT COUNT(*) FROM investigation_log
    """
    
    # Get today's investigations
    query_today = """
        SELECT COUNT(*) 
        FROM investigation_log
        WHERE investigated_at >= current_date
    """
    
    # Get total videos found
    query_videos = """
        SELECT SUM(videos_found) 
        FROM investigation_log
    """
    
    # Get last poll time
    query_last_poll = """
        SELECT MAX(timestamp) as last_poll
        FROM games
    """
    
    with get_db_connection(DB_PATH, read_only=True) as db:
        total_investigations = db.execute(query_total).fetchone()[0]
        today_investigations = db.execute(query_today).fetchone()[0]
        total_videos = db.execute(query_videos).fetchone()[0] or 0
        last_poll = db.execute(query_last_poll).fetchone()[0]
    
    return {
        'total_investigations': total_investigations,
        'today_investigations': today_investigations,
        'total_videos': total_videos,
        'last_poll': last_poll
    }


@st.cache_data(ttl=300)
def get_ccu_timeseries(universe_id: int, days: int = 7) -> pd.DataFrame:
    """Get CCU time series for a specific game."""
    # Convert numpy.int64 to Python int for DuckDB compatibility
    universe_id = int(universe_id)
    days = int(days)
    query = """
        SELECT 
            timestamp,
            ccu
        FROM games
        WHERE universe_id = ?
          AND timestamp >= current_timestamp - (? * INTERVAL 1 DAY)
        ORDER BY timestamp ASC
    """
    return _query_dataframe(query, (universe_id, days))


@st.cache_data(ttl=300)
def load_decay_curve(universe_id: int, window_days: int = 7) -> pd.DataFrame:
    """Get median decay curve after historical spikes."""
    universe_id = int(universe_id)
    window_days = int(window_days)
    query = """
        WITH daily_ccu AS (
            SELECT 
                CAST(timestamp AS DATE) as date,
                MAX(ccu) as peak_ccu
            FROM games
            WHERE universe_id = ?
              AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 60 DAY
              AND timestamp <= CURRENT_TIMESTAMP - INTERVAL 7 DAY
            GROUP BY CAST(timestamp AS DATE)
        ),
        spikes AS (
            SELECT 
                date as spike_date,
                peak_ccu,
                LAG(peak_ccu, 1) OVER (ORDER BY date) as prev_ccu
            FROM daily_ccu
        ),
        spike_window AS (
            SELECT 
                s.spike_date,
                s.peak_ccu,
                d.date,
                DATEDIFF('day', s.spike_date, d.date) as day_offset,
                d.peak_ccu as day_ccu
            FROM spikes s
            JOIN daily_ccu d
              ON d.date >= s.spike_date
             AND d.date <= CAST(s.spike_date + (? * INTERVAL 1 DAY) AS DATE)
            WHERE s.prev_ccu IS NOT NULL
              AND s.peak_ccu > s.prev_ccu * 1.2
        ),
        ratios AS (
            SELECT 
                day_offset,
                day_ccu * 1.0 / peak_ccu as ratio
            FROM spike_window
            WHERE peak_ccu > 0
        )
        SELECT 
            day_offset,
            median(ratio) as median_ratio,
            COUNT(*) as samples
        FROM ratios
        GROUP BY day_offset
        ORDER BY day_offset
    """
    return _query_dataframe(query, (universe_id, window_days))


@st.cache_data(ttl=300)
def load_top_movers(growth_threshold: float, limit: int) -> pd.DataFrame:
    """Load top movers with additional metrics."""
    query = TRENDING_GAMES_QUERY
    query += " AND ((cur.ccu - prev.ccu) * 1.0) / NULLIF(prev.ccu, 0) >= ?"
    query += " ORDER BY growth_percent DESC LIMIT ?"
    df = _query_dataframe(query, (growth_threshold, limit))
    
    # Convert timestamp column to string to avoid React serialization issues
    if 'current_timestamp' in df.columns:
        df['current_timestamp'] = df['current_timestamp'].apply(_time_ago)
    
    return df


@st.cache_data(ttl=300)
def load_creator_dependency(lookback_hours: int) -> pd.DataFrame:
    """Assess creator concentration per game to flag fragile vs healthy exposure."""
    lookback_hours = int(lookback_hours)
    query = """
        WITH recent_spikes AS (
            SELECT universe_id, game_name, channel_title
            FROM mechanic_spikes
            WHERE detected_at >= current_timestamp - (? * INTERVAL 1 HOUR)
        ),
        creator_counts AS (
            SELECT universe_id, game_name, channel_title, COUNT(*) as creator_spikes
            FROM recent_spikes
            GROUP BY universe_id, game_name, channel_title
        ),
        game_totals AS (
            SELECT universe_id, game_name,
                   SUM(creator_spikes) as total_spikes,
                   COUNT(*) as creator_count,
                   MAX(creator_spikes) as top_spikes
            FROM creator_counts
            GROUP BY universe_id, game_name
        ),
        top_creators AS (
            SELECT c.universe_id,
                   c.game_name,
                   c.channel_title as top_creator,
                   c.creator_spikes as top_spikes,
                   g.total_spikes,
                   g.creator_count
            FROM creator_counts c
            JOIN game_totals g
              ON g.universe_id = c.universe_id
             AND g.top_spikes = c.creator_spikes
        )
        SELECT t.universe_id,
               t.game_name,
               t.top_creator,
               t.top_spikes,
               t.total_spikes,
               t.creator_count,
               ROUND(t.top_spikes * 100.0 / NULLIF(t.total_spikes, 0), 1) as top_share_pct,
               meta.genre as genre
        FROM top_creators t
        LEFT JOIN game_metadata meta ON meta.universe_id = t.universe_id
        WHERE t.total_spikes >= 3
        ORDER BY top_share_pct DESC, total_spikes DESC
        LIMIT 50
    """
    df = _query_dataframe(query, (lookback_hours,))
    if df.empty:
        return df
    def classify(row):
        if row["creator_count"] <= 1 or row["top_share_pct"] >= 60:
            return "Fragile"
        if row["creator_count"] >= 3 and row["top_share_pct"] <= 50:
            return "Healthy"
        return "Mixed"
    df["exposure_status"] = df.apply(classify, axis=1)
    return df


@st.cache_data(ttl=300)
def load_competitive_alerts(lookback_hours: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load genre-based competitive alerts for custom tracked games."""
    lookback_hours = int(lookback_hours)
    custom_query = """
        SELECT cg.universe_id,
               COALESCE(meta.name, cg.game_name) as game_name,
               meta.genre as genre
        FROM custom_games cg
        LEFT JOIN game_metadata meta ON meta.universe_id = cg.universe_id
        WHERE cg.is_active = TRUE
    """
    spikes_query = """
        SELECT ms.universe_id,
               ms.game_name,
               ms.channel_title,
               ms.growth_percent,
               ms.detected_at,
               meta.genre as genre
        FROM mechanic_spikes ms
        LEFT JOIN game_metadata meta ON meta.universe_id = ms.universe_id
        WHERE ms.detected_at >= current_timestamp - (? * INTERVAL 1 HOUR)
    """
    custom_df = _query_dataframe(custom_query)
    spikes_df = _query_dataframe(spikes_query, (lookback_hours,))
    if custom_df.empty or spikes_df.empty:
        return custom_df, spikes_df
    custom_df = custom_df.dropna(subset=["genre"])
    if custom_df.empty:
        return custom_df, spikes_df
    custom_genres = set(custom_df["genre"].dropna().unique())
    custom_ids = set(custom_df["universe_id"].dropna().astype(int).tolist())
    spikes_df = spikes_df[spikes_df["genre"].isin(custom_genres)]
    if not spikes_df.empty:
        spikes_df = spikes_df[~spikes_df["universe_id"].isin(custom_ids)]
        genre_map = (
            custom_df.groupby("genre")["game_name"]
            .apply(lambda names: ", ".join(sorted(set(names))))
            .to_dict()
        )
        spikes_df["your_games"] = spikes_df["genre"].map(genre_map)
    return custom_df, spikes_df


@st.cache_data(ttl=300)
def load_player_flow() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Load player flow analysis - gainers and losers."""
    gainers_query = """
        WITH latest_snapshot AS (
            SELECT universe_id, name, ccu, timestamp,
                   ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
            FROM games
        ),
        filtered_latest AS (
            SELECT universe_id, name, ccu, timestamp FROM latest_snapshot WHERE row_num = 1
        ),
        prior_snapshot AS (
            SELECT universe_id, ccu, timestamp,
                   ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
            FROM games
            WHERE timestamp <= current_timestamp - INTERVAL 12 HOUR
              AND timestamp >= current_timestamp - INTERVAL 48 HOUR
        ),
        filtered_prior AS (
            SELECT universe_id, ccu, timestamp FROM prior_snapshot WHERE row_num = 1
        )
        SELECT COALESCE(meta.name, cur.name) AS game_name,
               cur.ccu AS current_ccu,
               prev.ccu AS previous_ccu,
               (cur.ccu - prev.ccu) AS ccu_change,
               ROUND(((cur.ccu - prev.ccu) * 100.0) / NULLIF(prev.ccu, 0), 1) AS growth_pct
        FROM filtered_latest cur
        JOIN filtered_prior prev ON prev.universe_id = cur.universe_id
        LEFT JOIN game_metadata meta ON meta.universe_id = cur.universe_id
        WHERE prev.ccu > 0
          AND cur.ccu > prev.ccu
          AND (cur.ccu - prev.ccu) >= 500
        ORDER BY ccu_change DESC
        LIMIT 10
    """
    
    losers_query = """
        WITH latest_snapshot AS (
            SELECT universe_id, name, ccu, timestamp,
                   ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
            FROM games
        ),
        filtered_latest AS (
            SELECT universe_id, name, ccu, timestamp FROM latest_snapshot WHERE row_num = 1
        ),
        prior_snapshot AS (
            SELECT universe_id, ccu, timestamp,
                   ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
            FROM games
            WHERE timestamp <= current_timestamp - INTERVAL 12 HOUR
              AND timestamp >= current_timestamp - INTERVAL 48 HOUR
        ),
        filtered_prior AS (
            SELECT universe_id, ccu, timestamp FROM prior_snapshot WHERE row_num = 1
        )
        SELECT COALESCE(meta.name, cur.name) AS game_name,
               cur.ccu AS current_ccu,
               prev.ccu AS previous_ccu,
               (prev.ccu - cur.ccu) AS ccu_change,
               ROUND(((prev.ccu - cur.ccu) * 100.0) / NULLIF(prev.ccu, 0), 1) AS loss_pct
        FROM filtered_latest cur
        JOIN filtered_prior prev ON prev.universe_id = cur.universe_id
        LEFT JOIN game_metadata meta ON meta.universe_id = cur.universe_id
        WHERE prev.ccu > 0
          AND cur.ccu < prev.ccu
          AND (prev.ccu - cur.ccu) >= 500
        ORDER BY ccu_change DESC
        LIMIT 10
    """
    
    gainers_df = _query_dataframe(gainers_query)
    losers_df = _query_dataframe(losers_query)
    
    # Calculate correlation stats
    total_gained = gainers_df['ccu_change'].sum() if not gainers_df.empty else 0
    total_lost = losers_df['ccu_change'].sum() if not losers_df.empty else 0
    
    stats = {
        'total_gained': int(total_gained),
        'total_lost': int(total_lost),
        'net_flow': int(total_gained - total_lost),
        'correlation_pct': min(total_gained, total_lost) / max(total_gained, total_lost, 1) * 100
    }
    
    return gainers_df, losers_df, stats


@st.cache_data(ttl=300)
def load_recent_videos(hours: int) -> pd.DataFrame:
    """Load recent YouTube videos."""
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
    df = _query_dataframe(query, (hours,))
    
    # Add time_ago column
    if not df.empty:
        df['time_ago'] = df['published_at'].apply(_time_ago)
    
    return df


@st.cache_data(ttl=180)
def detect_spikes_cached(
    lookback_hours: int, growth_threshold: float
) -> pd.DataFrame:
    """Detect mechanic spikes with enhanced metrics."""
    spikes: Iterable[MechanicSpike] = detect_mechanic_spikes(
        db_path=DB_PATH,
        lookback_hours=lookback_hours,
        growth_threshold=growth_threshold,
        persist=False,
    )
    
    # Count videos per game and get metadata
    video_counts = {}
    metadata = {}
    now = datetime.now(timezone.utc)
    with get_db_connection(DB_PATH, read_only=True) as db:
        for spike in spikes:
            query = """
                SELECT COUNT(DISTINCT video_id)
                FROM youtube_videos
                WHERE channel_title = ? OR title LIKE ?
            """
            count = db.execute(query, (spike.channel_title, f"%{spike.game_name}%")).fetchone()[0]
            video_counts[spike.game_name] = count
            
            # Get analytics from game_metadata
            meta_query = """
                SELECT up_votes, down_votes, game_updated_at, visits, favorited_count
                FROM game_metadata
                WHERE universe_id = ?
            """
            meta_row = db.execute(meta_query, (spike.universe_id,)).fetchone()
            if meta_row:
                up, down, updated_at, visits, fav = meta_row
                total_votes = (up or 0) + (down or 0)
                like_ratio = (up or 0) / total_votes if total_votes > 0 else None
                update_hours = None
                if updated_at:
                    if updated_at.tzinfo is None:
                        updated_at = updated_at.replace(tzinfo=timezone.utc)
                    update_hours = (now - updated_at).total_seconds() / 3600
                fav_ratio = (fav / visits * 100) if visits and fav else None
                metadata[spike.universe_id] = {
                    "like_ratio": like_ratio,
                    "update_hours": update_hours,
                    "fav_ratio": fav_ratio,
                }
    
    retention_stats = get_retention_stats(tuple(int(s.universe_id) for s in spikes))

    rows = []
    for spike in spikes:
        video_count = video_counts.get(spike.game_name, 1)
        meta = metadata.get(spike.universe_id, {})
        like_pct = f"{meta['like_ratio']*100:.0f}%" if meta.get('like_ratio') else "-"
        update_str = "-"
        if meta.get('update_hours'):
            h = meta['update_hours']
            update_str = f"{int(h//24)}d" if h >= 24 else f"{int(h)}h"
        retention = retention_stats.get(int(spike.universe_id), {})
        stickiness = retention.get("stickiness_index")
        decay_days = retention.get("decay_days")
        quality = _compute_spike_quality(stickiness, decay_days)
        stickiness_display = stickiness * 100 if stickiness is not None else None
        rows.append({
            "Universe ID": int(spike.universe_id),
            "Game": spike.game_name,
            "Growth %": round(spike.growth_percent, 1),
            "Current CCU": spike.current_ccu,
            "Signal": _signal_strength(video_count, spike.growth_percent),
            "Videos": video_count,
            "Like": like_pct,
            "Updated": update_str,
            "Stickiness": stickiness_display,
            "Decay Days": decay_days,
            "Spike Quality": quality,
            "Mechanic": spike.mechanic,
            "YouTube": spike.video_url,
            "Channel": spike.channel_title,
            "Published": _time_ago(spike.published_at),
        })
    
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_historical_spikes(limit: int) -> pd.DataFrame:
    """Load historical spikes from database."""
    spikes = get_historical_spikes(db_path=DB_PATH, limit=limit)
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
def get_performance_metrics() -> Dict[str, Any]:
    """Calculate retrospective performance metrics."""
    query = """
        WITH spike_peaks AS (
            SELECT 
                ms.game_name,
                ms.detected_at,
                ms.growth_percent as detected_growth,
                MAX(g.ccu) as peak_ccu,
                MAX(((g.ccu - prev.ccu) * 100.0) / NULLIF(prev.ccu, 0)) as peak_growth
            FROM mechanic_spikes ms
            JOIN games g ON g.universe_id = (
                SELECT universe_id FROM games WHERE name = ms.game_name LIMIT 1
            )
            LEFT JOIN games prev ON prev.universe_id = g.universe_id 
                AND prev.timestamp = g.timestamp - INTERVAL 7 DAY
            WHERE g.timestamp >= ms.detected_at
              AND g.timestamp <= ms.detected_at + INTERVAL 48 HOUR
            GROUP BY ms.game_name, ms.detected_at, ms.growth_percent
        )
        SELECT 
            COUNT(*) as total_detections,
            AVG(peak_growth - detected_growth) as avg_additional_growth,
            COUNT(CASE WHEN peak_growth > detected_growth * 1.5 THEN 1 END) as continued_growth_count
        FROM spike_peaks
    """
    
    with get_db_connection(DB_PATH, read_only=True) as db:
        result = db.execute(query).fetchone()
    
    return {
        'total_detections': result[0] or 0,
        'avg_additional_growth': round(result[1] or 0, 1),
        'continued_growth_pct': round((result[2] or 0) / max(result[0], 1) * 100, 1)
    }


@st.cache_data(ttl=300)
def load_creator_impact() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load creator impact analysis - which creators correlate with spikes."""
    import math
    
    # Top creators by spike correlation with lag calculation
    creator_query = """
        SELECT 
            channel_title as creator,
            COUNT(*) as spike_appearances,
            COUNT(DISTINCT game_name) as games_covered,
            ROUND(AVG(growth_percent), 1) as avg_growth,
            ROUND(MEDIAN(growth_percent), 1) as median_growth,
            MAX(growth_percent) as max_growth,
            MIN(detected_at) as first_detection,
            MAX(detected_at) as last_detection,
            ROUND(MEDIAN(EXTRACT(EPOCH FROM (detected_at - published_at)) / 3600), 1) as median_lag_hours
        FROM mechanic_spikes
        GROUP BY channel_title
        HAVING COUNT(*) >= 2
        ORDER BY spike_appearances DESC
        LIMIT 25
    """
    
    # Creator-game details for drill-down
    detail_query = """
        SELECT 
            channel_title as creator,
            game_name,
            growth_percent,
            current_ccu,
            mechanic,
            video_url,
            detected_at,
            published_at,
            ROUND(EXTRACT(EPOCH FROM (detected_at - published_at)) / 3600, 1) as lag_hours
        FROM mechanic_spikes
        ORDER BY detected_at DESC
    """
    
    # Get total videos per creator to calculate reliability
    reliability_query = """
        SELECT 
            channel_title as creator,
            COUNT(DISTINCT video_id) as total_videos
        FROM youtube_videos
        GROUP BY channel_title
    """
    
    creators_df = _query_dataframe(creator_query)
    details_df = _query_dataframe(detail_query)
    reliability_df = _query_dataframe(reliability_query)
    volatility_df = pd.DataFrame()
    if not details_df.empty:
        volatility_df = (
            details_df.groupby("creator")["growth_percent"]
            .agg(["std"])
            .reset_index()
            .rename(columns={"std": "growth_std"})
        )
    
    # Add creator type classification and enhanced metrics
    if not creators_df.empty:
        # Merge reliability data
        creators_df = creators_df.merge(reliability_df, on='creator', how='left')
        creators_df['total_videos'] = creators_df['total_videos'].fillna(0).astype(int)
        
        # Calculate reliability: % of videos that correlate with spikes
        creators_df['reliability'] = (
            creators_df['spike_appearances'] / creators_df['total_videos'].replace(0, 1) * 100
        ).round(1).clip(upper=100)  # Cap at 100%

        if not volatility_df.empty:
            creators_df = creators_df.merge(volatility_df, on="creator", how="left")
        if "growth_std" not in creators_df.columns:
            creators_df["growth_std"] = 0.0
        else:
            creators_df["growth_std"] = creators_df["growth_std"].fillna(0.0)
        creators_df["volatility_pct"] = (
            creators_df["growth_std"] / creators_df["median_growth"].abs().replace(0, 1) * 100
        ).round(1).clip(upper=100)
        creators_df["risk_score"] = (
            (100 - creators_df["reliability"].clip(lower=0, upper=100)) * 0.5 +
            creators_df["volatility_pct"] * 0.5
        ).round(1)
        def risk_label(score: float) -> str:
            if score <= 33:
                return "Low"
            if score <= 66:
                return "Medium"
            return "High"
        creators_df["risk_label"] = creators_df["risk_score"].apply(risk_label)
        
        # Creator type classification
        creators_df['creator_type'] = creators_df.apply(
            lambda row: '🎯 Specialist' if row['games_covered'] == 1 
                       else ('🌐 Diverse' if row['games_covered'] >= 4 else '📊 Mixed'),
            axis=1
        )
        
        # Better impact score: rewards diversity, uses median growth
        # Formula: median_growth × sqrt(games_covered) × log2(spikes + 1)
        creators_df['impact_score'] = (
            creators_df['median_growth'] * 
            creators_df['games_covered'].apply(lambda x: math.sqrt(x)) * 
            creators_df['spike_appearances'].apply(lambda x: math.log2(x + 1))
        ).round(1)
        
        # Confidence indicator based on sample size
        def confidence_level(n):
            if n < 3:
                return '⚠️ Low'
            elif n <= 7:
                return '📊 Medium'
            else:
                return '✅ High'
        
        creators_df['confidence'] = creators_df['spike_appearances'].apply(confidence_level)
        
        # Format lag for display
        def format_lag(hours):
            if pd.isna(hours) or hours is None:
                return '-'
            if hours < 1:
                return '<1h'
            elif hours < 24:
                return f'{int(hours)}h'
            else:
                return f'{int(hours/24)}d'
        
        creators_df['lag_display'] = creators_df['median_lag_hours'].apply(format_lag)
    
    return creators_df, details_df, reliability_df


@st.cache_data(ttl=120)
def check_game_cached(game_name: str) -> dict:
    """Check a specific game's CCU and YouTube coverage."""
    ccu_status = get_game_ccu_status(game_name)
    
    # Run async YouTube search in sync context
    loop = asyncio.new_event_loop()
    videos = loop.run_until_complete(search_youtube_for_game(game_name))
    loop.close()
    
    video_count = len(videos)
    
    if video_count >= 8:
        signal, signal_emoji = "STRONG", "🔥🔥🔥"
    elif video_count >= 3:
        signal, signal_emoji = "MEDIUM", "🔥🔥"
    elif video_count >= 1:
        signal, signal_emoji = "WEAK", "🔥"
    else:
        signal, signal_emoji = "NONE", "❄️"
    
    return {
        "ccu": ccu_status,
        "videos": videos,
        "video_count": video_count,
        "signal": signal,
        "signal_emoji": signal_emoji,
    }


# ============================================================================
# STREAMLIT APP LAYOUT
# ============================================================================

st.set_page_config(
    page_title="Early Shift Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Early Shift – Mechanic Spike Monitor")

if os.getenv("DEMO_MODE", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
    demo_updated_at = os.getenv("DEMO_UPDATED_AT", "unknown").strip() or "unknown"
    st.info(f"Demo data - updated {demo_updated_at}. Live alerts run in Discord.")

# Get investigation status for header
inv_status = get_investigation_queue_status()
last_poll_time = _time_ago(inv_status['last_poll'])

st.caption(
    f"🔄 Last poll: {last_poll_time} • "
    f"Live view of Roblox market signals: CCU trends, creator chatter, and mechanic spike candidates."
)

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

st.sidebar.header("Filters")
threshold_pct = st.sidebar.slider("Growth threshold (%)", 10, 150, 25, step=5)
lookback_hours = st.sidebar.slider("Video lookback (hours)", 12, 96, 48, step=12)
top_limit = st.sidebar.slider("Top movers to display", 10, 100, 25, step=5)
history_limit = st.sidebar.slider("Historical spikes to display", 10, 200, 50, step=10)
refresh_now = st.sidebar.button("Refresh data caches")

if refresh_now:
    st.cache_data.clear()
    st.toast("Caches cleared – data will refresh on next load.")

threshold_ratio = threshold_pct / 100.0

# ============================================================================
# CHECK MY GAME (Sidebar)
# ============================================================================

st.sidebar.markdown("---")
st.sidebar.header("🔍 Check My Game")
check_game_input = st.sidebar.text_input(
    "Enter game name",
    placeholder="e.g. Dress To Impress",
    help="Search for YouTube coverage and CCU status of any Roblox game"
)
check_game_btn = st.sidebar.button("Check Game", type="primary", use_container_width=True)

# ============================================================================
# INVESTIGATION ACTIVITY STATUS
# ============================================================================

st.subheader("Investigation Activity")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("🔍 Investigations Today", inv_status['today_investigations'])
with col2:
    st.metric("📊 Total Investigations", inv_status['total_investigations'])
with col3:
    st.metric("🎥 Total Videos Found", inv_status['total_videos'])

st.markdown("---")

# ============================================================================
# CHECK MY GAME RESULTS (if searched)
# ============================================================================

if check_game_btn and check_game_input:
    st.subheader(f"🔍 Results for: {check_game_input}")
    
    with st.spinner("Searching YouTube and checking CCU..."):
        result = check_game_cached(check_game_input)
    
    col1, col2, col3, col4 = st.columns(4)
    
    ccu = result["ccu"]
    if ccu["found"]:
        with col1:
            game_display = ccu.get("game_name", "")[:25]
            st.metric("Game Found", game_display)
        with col2:
            st.metric("Current CCU", f"{ccu['current_ccu']:,}" if ccu.get('current_ccu') else "N/A")
        with col3:
            growth = ccu.get('growth_pct')
            st.metric("Growth (3d)", f"{growth:+.1f}%" if growth else "N/A")
        with col4:
            st.metric("YouTube Signal", f"{result['signal_emoji']} {result['signal']}")
    else:
        st.warning(f"Game '{check_game_input}' not found in CCU database.")
        with col1:
            st.metric("Videos Found", result['video_count'])
        with col2:
            st.metric("Signal", f"{result['signal_emoji']} {result['signal']}")
    
    if result["videos"]:
        st.markdown("**Recent YouTube Coverage:**")
        for video in result["videos"][:5]:
            col1, col2 = st.columns([4, 1])
            with col1:
                title = video['title'][:60] + "..." if len(video['title']) > 60 else video['title']
                st.markdown(f"📺 **{title}**")
                st.caption(f"by {video['channel']}")
            with col2:
                if video.get('url'):
                    st.link_button("Watch", video['url'], use_container_width=True)
    else:
        st.info("No YouTube videos found in the last 72 hours.")
    
    # Recommendation
    is_growing = ccu.get("is_growing", False) if ccu["found"] else False
    video_count = result["video_count"]
    
    if is_growing and video_count >= 5:
        st.success("🚀 **High momentum** - Creators are covering this game and CCU is spiking!")
    elif is_growing and video_count >= 1:
        st.success("📈 **Growing with coverage** - Good time to push marketing")
    elif is_growing:
        st.info("📊 **Organic growth** - No YouTube signal yet")
    elif video_count >= 5:
        st.warning("🎬 **High YouTube activity** but CCU flat - Watch for delayed spike")
    else:
        st.info("😴 **Low activity** - No significant signals detected")
    
    st.markdown("---")

# ============================================================================
# PLAYER FLOW ANALYSIS
# ============================================================================

st.subheader("📊 Player Flow Analysis")
st.caption("Where are players moving? (12-48h comparison window)")

gainers_df, losers_df, flow_stats = load_player_flow()

# Stats row
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("CCU Gained (Top 10)", f"+{flow_stats['total_gained']:,}")
with col2:
    st.metric("CCU Lost (Top 10)", f"-{flow_stats['total_lost']:,}")
with col3:
    st.metric("Net Flow", f"{flow_stats['net_flow']:+,}")
with col4:
    st.metric("Correlation", f"{flow_stats['correlation_pct']:.0f}%")

# Two columns for gainers and losers
col1, col2 = st.columns(2)

with col1:
    st.markdown("**🟢 Games GAINING Players**")
    if gainers_df.empty:
        st.info("No significant gainers (min 500 CCU)")
    else:
        display_df = gainers_df[['game_name', 'ccu_change', 'growth_pct']].copy()
        display_df.columns = ['Game', 'CCU Gained', 'Growth %']
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "CCU Gained": st.column_config.NumberColumn(format="+%d"),
                "Growth %": st.column_config.NumberColumn(format="+%.1f%%"),
            }
        )

with col2:
    st.markdown("**🔴 Games LOSING Players**")
    if losers_df.empty:
        st.info("No significant losers (min 500 CCU)")
    else:
        display_df = losers_df[['game_name', 'ccu_change', 'loss_pct']].copy()
        display_df.columns = ['Game', 'CCU Lost', 'Loss %']
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "CCU Lost": st.column_config.NumberColumn(format="-%d"),
                "Loss %": st.column_config.NumberColumn(format="-%.1f%%"),
            }
        )

st.markdown("---")

# ============================================================================
# TOP MOVERS & RECENT VIDEOS
# ============================================================================

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
        # Display with time_ago instead of full timestamp
        display_df = videos_df[['channel_title', 'title', 'video_url', 'view_count', 'time_ago']]
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "video_url": st.column_config.LinkColumn("YouTube", display_text="Watch"),
                "view_count": st.column_config.NumberColumn("Views", format="%d"),
                "time_ago": "Published"
            },
        )

st.markdown("---")

# ============================================================================
# MECHANIC SPIKE CANDIDATES
# ============================================================================

st.subheader("Mechanic Spike Candidates")
spike_df = detect_spikes_cached(lookback_hours, threshold_ratio)

if spike_df.empty:
    st.info(
        "No mechanic spikes detected for the current CCU and video thresholds."
    )
else:
    unique_games = spike_df[["Game", "Universe ID"]].drop_duplicates()
    unique_games = unique_games.sort_values(["Game", "Universe ID"])
    unique_games["label"] = (
        unique_games["Game"] + " [" + unique_games["Universe ID"].astype(str) + "]"
    )
    game_options = ["All games"] + unique_games["label"].tolist()
    selected_game = st.selectbox(
        "Filter by game",
        options=game_options,
        index=0,
        key="mechanic_spike_game_filter",
    )
    filtered_df = spike_df
    if selected_game != "All games":
        selected_id = int(
            unique_games.set_index("label").loc[selected_game, "Universe ID"]
        )
        filtered_df = spike_df[spike_df["Universe ID"] == selected_id]
    display_df = filtered_df.drop(columns=["Universe ID"], errors="ignore")
    stickiness_count = int(filtered_df["Stickiness"].notna().sum())
    decay_count = int(filtered_df["Decay Days"].notna().sum())
    st.caption(
        f"Stickiness available: {stickiness_count}/{len(filtered_df)} • "
        f"Decay days available: {decay_count}/{len(filtered_df)}"
    )
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "YouTube": st.column_config.LinkColumn(display_text="Open"),
            "Current CCU": st.column_config.NumberColumn(format="%d"),
            "Growth %": st.column_config.NumberColumn(format="%.1f%%"),
            "Signal": "🔥",
            "Videos": st.column_config.NumberColumn(format="%d"),
            "Stickiness": st.column_config.NumberColumn(format="%.0f%%"),
            "Decay Days": st.column_config.NumberColumn(format="%.1f"),
            "Spike Quality": st.column_config.NumberColumn(format="%.1f"),
        },
    )

    with st.expander("Decay Curve (median)"):
        decay_games = spike_df[["Game", "Universe ID"]].drop_duplicates()
        decay_games = decay_games.sort_values(["Game", "Universe ID"])
        decay_games["label"] = (
            decay_games["Game"] + " [" + decay_games["Universe ID"].astype(str) + "]"
        )
        selected_label = st.selectbox(
            "Game for decay curve",
            options=decay_games["label"].tolist(),
            index=0,
        )
        selected_id = int(
            decay_games.set_index("label").loc[selected_label, "Universe ID"]
        )
        curve_df = load_decay_curve(selected_id, window_days=7)
        if curve_df.empty or len(curve_df) < 2:
            st.info("Not enough historical spikes to build a decay curve yet.")
        else:
            curve_df["median_pct"] = (curve_df["median_ratio"] * 100).round(1)
            min_samples = int(curve_df["samples"].min())
            max_samples = int(curve_df["samples"].max())
            st.caption(
                f"Samples per day: {min_samples}-{max_samples} spikes"
            )
            fig = px.line(
                curve_df,
                x="day_offset",
                y="median_pct",
                markers=True,
                labels={"day_offset": "Days After Spike", "median_pct": "Median CCU (%)"},
            )
            fig.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ============================================================================
# CREATOR DEPENDENCE
# ============================================================================

st.subheader("Creator Dependence")
st.caption("Which games rely on a single creator vs. diversified exposure.")

dependence_df = load_creator_dependency(lookback_hours)
if dependence_df.empty:
    st.info("Not enough spike data to assess creator dependence yet.")
else:
    display_df = dependence_df[[
        "game_name", "genre", "top_creator", "creator_count", "top_share_pct",
        "total_spikes", "exposure_status"
    ]].copy()
    display_df.columns = [
        "Game", "Genre", "Top Creator", "Creators", "Top Share %", "Spikes", "Status"
    ]
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Top Share %": st.column_config.NumberColumn(format="%.1f"),
            "Spikes": st.column_config.NumberColumn(format="%d"),
        },
    )

st.markdown("---")

# ============================================================================
# COMPETITIVE ALERTS
# ============================================================================

st.subheader("Competitive Alerts")
st.caption("Genre-based spikes that might impact your tracked games.")

custom_games_df, competitive_df = load_competitive_alerts(lookback_hours)
if custom_games_df.empty:
    st.info("Add a game to custom tracking to enable competitive alerts.")
elif competitive_df.empty:
    st.info("No competitive spikes detected in your genres for this window.")
else:
    display_df = competitive_df[[
        "genre", "game_name", "growth_percent", "channel_title", "detected_at", "your_games"
    ]].copy()
    display_df["detected_at"] = display_df["detected_at"].apply(_time_ago)
    display_df.columns = ["Genre", "Game", "Growth %", "Creator", "Detected", "Your Games"]
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Growth %": st.column_config.NumberColumn(format="+%.1f%%"),
        },
    )

st.markdown("---")

# ============================================================================
# CREATOR IMPACT ANALYSIS
# ============================================================================

st.subheader("🎬 Creator Impact Analysis")
st.caption("Which creators' videos correlate with CCU spikes? Find high-impact partners.")

creators_df, creator_details_df, _ = load_creator_impact()

if creators_df.empty:
    st.info("Not enough data yet. Need more spike detections to analyze creator patterns.")
else:
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Creators Tracked", len(creators_df))
    with col2:
        high_confidence = len(creators_df[creators_df['spike_appearances'] >= 8])
        st.metric("✅ High Confidence", high_confidence, help="8+ spike appearances")
    with col3:
        diverse_count = len(creators_df[creators_df['games_covered'] >= 4])
        st.metric("🌐 Diverse Creators", diverse_count, help="Cover 4+ different games")
    with col4:
        avg_reliability = creators_df['reliability'].mean()
        st.metric("Avg Reliability", f"{avg_reliability:.1f}%", help="% of videos that correlate with spikes")
    
    # Main creator table with new columns
    st.markdown("**Top Creators by Spike Correlation**")
    display_creators = creators_df[[
        'creator', 'spike_appearances', 'games_covered', 'median_growth', 
        'reliability', 'lag_display', 'creator_type', 'impact_score', 'confidence',
        'risk_label', 'risk_score'
    ]].copy()
    display_creators.columns = [
        'Creator', 'Spikes', 'Games', 'Median Growth', 
        'Reliability', 'Lag to Lift', 'Type', 'Impact', 'Confidence', 'Risk', 'Risk Score'
    ]
    
    st.dataframe(
        display_creators,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Spikes": st.column_config.NumberColumn(format="%d"),
            "Games": st.column_config.NumberColumn(format="%d"),
            "Median Growth": st.column_config.NumberColumn(format="+%.1f%%"),
            "Reliability": st.column_config.NumberColumn(format="%.1f%%", help="% of videos that correlate with spikes"),
            "Lag to Lift": st.column_config.TextColumn(help="Median time from video to CCU spike"),
            "Impact": st.column_config.NumberColumn(format="%.1f", help="median_growth × √(games) × log₂(spikes+1)"),
            "Confidence": st.column_config.TextColumn(help="Based on sample size: <3=Low, 3-7=Medium, 8+=High"),
            "Risk": st.column_config.TextColumn(help="Low/Medium/High based on volatility and reliability"),
            "Risk Score": st.column_config.NumberColumn(format="%.1f", help="Higher = less predictable creator impact"),
        }
    )
    
    # Formula explanation
    with st.expander("ℹ️ How metrics are calculated"):
        st.markdown("""
        - **Reliability**: `spike_appearances / total_videos` - What % of this creator's videos coincide with a CCU spike
        - **Lag to Lift**: Median hours between video publish time and spike detection
        - **Impact Score**: `median_growth × √(games_covered) × log₂(spikes + 1)` - Rewards diverse coverage, penalizes spam
        - **Confidence**: Based on sample size - more spikes = more reliable signal
        - **Risk Score**: Combines volatility and reliability (higher = less predictable)
        """)
    
    # Creator drill-down
    st.markdown("**🔍 Creator Details**")
    selected_creator = st.selectbox(
        "Select a creator to see their coverage:",
        options=creators_df['creator'].tolist(),
        index=0
    )
    
    if selected_creator:
        creator_games = creator_details_df[creator_details_df['creator'] == selected_creator]
        creator_stats = creators_df[creators_df['creator'] == selected_creator].iloc[0]
        
        if not creator_games.empty:
            # Show creator summary stats
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Reliability", f"{creator_stats['reliability']:.1f}%")
            with col2:
                st.metric("Lag to Lift", creator_stats['lag_display'])
            with col3:
                st.metric("Impact Score", f"{creator_stats['impact_score']:.1f}")
            with col4:
                st.metric("Confidence", creator_stats['confidence'])
            with col5:
                st.metric("Risk", f"{creator_stats['risk_label']} ({creator_stats['risk_score']:.1f})")
            
            st.markdown(f"**Games covered by {selected_creator}:**")
            
            # Include lag_hours in details table
            display_cols = ['game_name', 'growth_percent', 'current_ccu', 'lag_hours', 'mechanic', 'video_url']
            display_details = creator_games[[c for c in display_cols if c in creator_games.columns]].copy()
            display_details.columns = ['Game', 'Growth %', 'CCU', 'Lag (h)', 'Mechanic', 'Video'][:len(display_details.columns)]
            
            st.dataframe(
                display_details,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Growth %": st.column_config.NumberColumn(format="+%.1f%%"),
                    "CCU": st.column_config.NumberColumn(format="%,d"),
                    "Lag (h)": st.column_config.NumberColumn(format="%.1f", help="Hours from video to spike"),
                    "Video": st.column_config.LinkColumn(display_text="Watch"),
                }
            )
            
            # Action hints based on stats
            median_growth = float(
                creator_stats.get('median_growth', creator_games['growth_percent'].median()) or 0.0
            )
            game_count = int(
                creator_stats.get('games_covered', creator_games['game_name'].nunique()) or 0
            )
            reliability = float(creator_stats.get('reliability', 0.0) or 0.0)
            spike_count = int(creator_stats.get('spike_appearances', len(creator_games)) or 0)
            
            st.markdown("**💡 Action Hint:**")
            st.caption(f"Sample size: {spike_count} spike(s)")
            if spike_count < 3:
                st.warning("⚠️ **Low sample** - Not enough spikes to trust this signal yet.")
            elif reliability >= 30 and median_growth >= 100 and game_count >= 3 and spike_count >= 5:
                st.success("🚀 **High-value partnership** - High reliability + high growth + diverse coverage. Strong sponsorship candidate.")
            elif reliability >= 25 and median_growth >= 100:
                st.success("🔥 **Growth driver** - Videos reliably correlate with big spikes. Worth reaching out.")
            elif game_count >= 4 and reliability >= 20:
                st.info("🌐 **Broad reach** - Covers many games with decent reliability. Good for awareness campaigns.")
            elif game_count == 1 and reliability >= 30:
                st.info(f"🎯 **Specialist** - Focused on {creator_games.iloc[0]['game_name']}. Ideal if that's your genre.")
            elif reliability < 15:
                st.warning("⚠️ **Low reliability** - Few videos correlate with spikes. May be coincidental coverage.")
            else:
                st.info("📊 **Monitor** - Moderate signal. Track for more data before deciding.")

st.markdown("---")

# ============================================================================
# PERFORMANCE METRICS
# ============================================================================

st.subheader("Detection Performance")

perf_metrics = get_performance_metrics()

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "Total Detections (All Time)",
        perf_metrics['total_detections'],
        help="Total mechanic spikes detected by Early Shift"
    )

with col2:
    st.metric(
        "Avg Additional Growth After Detection",
        f"+{perf_metrics['avg_additional_growth']}%",
        help="Average CCU growth in 48h after spike detection"
    )

with col3:
    st.metric(
        "Spikes That Continued Growing",
        f"{perf_metrics['continued_growth_pct']}%",
        help="Percentage of detected spikes that grew 50%+ more after detection"
    )

st.markdown("---")

# ============================================================================
# TIME SERIES CHART (if game selected)
# ============================================================================

if not movers_df.empty:
    st.subheader("CCU Trend Analysis")
    
    # Let user select a game to view time series
    selected_game = st.selectbox(
        "Select game to view 7-day CCU trend:",
        options=movers_df['game_name'].tolist(),
        index=0
    )
    
    if selected_game:
        # Get universe_id for selected game
        selected_universe_id = movers_df[movers_df['game_name'] == selected_game]['universe_id'].iloc[0]
        
        # Load time series data
        ts_df = get_ccu_timeseries(selected_universe_id, days=7)
        
        if not ts_df.empty:
            # Create line chart with Plotly
            fig = px.line(
                ts_df,
                x='timestamp',
                y='ccu',
                title=f"{selected_game} - 7 Day CCU Trend",
                labels={'ccu': 'Concurrent Users', 'timestamp': 'Time'}
            )
            
            fig.update_layout(
                hovermode='x unified',
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No time series data available for this game.")

st.markdown("---")
st.caption(
    "Data source: DuckDB snapshots (games, metadata, YouTube) updated via Early Shift collectors."
)


# NOTE: Check My Game feature - see PATCH_check_my_game.py for full integration


# NOTE: Check My Game feature - see PATCH_check_my_game.py for full integration
