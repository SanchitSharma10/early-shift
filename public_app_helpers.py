from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from check_my_game import get_game_ccu_status, search_youtube_for_game
from db_manager import get_db_connection


def _resolve_public_db_path() -> str:
    env_path = os.getenv("DB_PATH")
    if env_path:
        return env_path

    project_root = Path(__file__).parent
    demo_db = project_root / "early_shift_demo.db"
    if demo_db.exists():
        return str(demo_db)

    primary_db = project_root / "early_shift.db"
    return str(primary_db)


DB_PATH = _resolve_public_db_path()


def _query_dataframe(query: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    with get_db_connection(DB_PATH, read_only=True) as db:
        result = db.execute(query, params)
        columns = [column[0] for column in result.description]
        rows = result.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _table_exists(table_name: str) -> bool:
    query = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
    """
    frame = _query_dataframe(query, (table_name,))
    return bool(frame.iloc[0, 0]) if not frame.empty else False


def clean_label(text: str | None) -> str:
    if not text:
        return "-"
    cleaned = re.sub(r"^\s*(\[[^\]]+\]\s*)+", "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or text.strip()


def time_ago(value: datetime | None) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - value
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def get_public_snapshot() -> dict[str, Any]:
    retention_count_expr = (
        "(SELECT COUNT(*) FROM game_retention_metrics) AS retention_profiles"
        if _table_exists("game_retention_metrics")
        else "0 AS retention_profiles"
    )
    query = f"""
        SELECT
            (SELECT COUNT(*) FROM games) AS ccu_rows,
            (SELECT COUNT(DISTINCT universe_id) FROM games) AS tracked_games,
            (SELECT COUNT(*) FROM youtube_videos) AS youtube_videos,
            (SELECT COUNT(DISTINCT channel_id) FROM youtube_videos WHERE channel_id IS NOT NULL) AS tracked_channels,
            (SELECT COUNT(*) FROM mechanic_spikes) AS creator_linked_spikes,
            {retention_count_expr},
            (SELECT MIN(timestamp) FROM games) AS first_ccu_at,
            (SELECT MAX(timestamp) FROM games) AS last_ccu_at,
            (SELECT MIN(detected_at) FROM mechanic_spikes) AS first_spike_at,
            (SELECT MAX(detected_at) FROM mechanic_spikes) AS last_spike_at
    """
    row = _query_dataframe(query).iloc[0].to_dict()
    return row


def load_public_creator_signals(limit: int = 10) -> pd.DataFrame:
    query = """
        WITH spike_videos AS (
            SELECT
                channel_title AS creator,
                COUNT(*) AS spike_detections,
                COUNT(DISTINCT video_url) AS spike_videos,
                COUNT(DISTINCT game_name) AS games_covered,
                ROUND(MEDIAN(growth_percent), 1) AS median_growth,
                ROUND(MEDIAN(EXTRACT(EPOCH FROM (detected_at - published_at)) / 3600.0), 1) AS median_lag_hours
            FROM mechanic_spikes
            GROUP BY channel_title
            HAVING COUNT(DISTINCT video_url) >= 2
        ),
        creator_totals AS (
            SELECT
                channel_title AS creator,
                COUNT(DISTINCT video_id) AS total_videos
            FROM youtube_videos
            GROUP BY channel_title
        )
        SELECT
            sv.creator,
            sv.spike_detections,
            sv.spike_videos,
            sv.games_covered,
            sv.median_growth,
            ROUND(sv.spike_videos * 100.0 / NULLIF(ct.total_videos, 0), 1) AS hit_rate_pct,
            sv.median_lag_hours
        FROM spike_videos sv
        LEFT JOIN creator_totals ct ON ct.creator = sv.creator
        ORDER BY sv.spike_videos DESC, sv.median_growth DESC, sv.games_covered DESC
        LIMIT ?
    """
    return _query_dataframe(query, (limit,))


def load_public_recent_detections(limit: int = 10) -> pd.DataFrame:
    retention_join = (
        "LEFT JOIN game_retention_metrics grm ON grm.universe_id = ms.universe_id"
        if _table_exists("game_retention_metrics")
        else ""
    )
    stickiness_select = (
        "ROUND(grm.stickiness_index * 100.0, 1) AS stickiness_pct,"
        if _table_exists("game_retention_metrics")
        else "NULL AS stickiness_pct,"
    )
    decay_select = (
        "ROUND(grm.decay_days, 1) AS decay_days,"
        if _table_exists("game_retention_metrics")
        else "NULL AS decay_days,"
    )
    query = f"""
        WITH ranked AS (
            SELECT
                ms.game_name,
                ms.channel_title,
                ROUND(ms.growth_percent, 1) AS growth_percent,
                ms.current_ccu,
                {stickiness_select}
                {decay_select}
                ROUND(EXTRACT(EPOCH FROM (ms.detected_at - ms.published_at)) / 3600.0, 1) AS lag_hours,
                ms.detected_at,
                ms.video_url,
                ROW_NUMBER() OVER (PARTITION BY ms.game_name ORDER BY ms.detected_at DESC, ms.growth_percent DESC) AS row_num
            FROM mechanic_spikes ms
            {retention_join}
        )
        SELECT
            game_name,
            channel_title,
            growth_percent,
            current_ccu,
            stickiness_pct,
            decay_days,
            lag_hours,
            detected_at,
            video_url
        FROM ranked
        WHERE row_num = 1
        ORDER BY detected_at DESC, growth_percent DESC
        LIMIT ?
    """
    frame = _query_dataframe(query, (limit,))
    if not frame.empty:
        frame["game_name"] = frame["game_name"].map(clean_label)
    return frame


def load_detection_timeline(days: int = 14) -> pd.DataFrame:
    days = max(int(days), 1)
    query = f"""
        WITH latest AS (
            SELECT MAX(detected_at) AS max_detected_at
            FROM mechanic_spikes
        )
        SELECT
            CAST(ms.detected_at AS DATE) AS detected_day,
            COUNT(*) AS detections,
            ROUND(MEDIAN(ms.growth_percent), 1) AS median_growth
        FROM mechanic_spikes ms, latest
        WHERE ms.detected_at >= latest.max_detected_at - INTERVAL {days} DAY
        GROUP BY CAST(ms.detected_at AS DATE)
        ORDER BY detected_day
    """
    return _query_dataframe(query)


def load_public_case_studies(limit: int = 3) -> pd.DataFrame:
    retention_join = (
        "LEFT JOIN game_retention_metrics grm ON grm.universe_id = grouped.universe_id"
        if _table_exists("game_retention_metrics")
        else ""
    )
    stickiness_select = (
        "ROUND(grm.stickiness_index * 100.0, 1) AS stickiness_pct,"
        if _table_exists("game_retention_metrics")
        else "NULL AS stickiness_pct,"
    )
    decay_select = (
        "ROUND(grm.decay_days, 1) AS decay_days,"
        if _table_exists("game_retention_metrics")
        else "NULL AS decay_days,"
    )
    query = f"""
        WITH latest AS (
            SELECT MAX(detected_at) AS max_detected_at
            FROM mechanic_spikes
        ),
        grouped AS (
            SELECT
                ms.universe_id,
                ms.game_name,
                COUNT(*) AS detections,
                COUNT(DISTINCT ms.channel_title) AS creators,
                COUNT(DISTINCT ms.video_url) AS creator_videos,
                ROUND(MEDIAN(ms.growth_percent), 1) AS median_growth,
                ROUND(MAX(ms.growth_percent), 1) AS max_growth,
                MAX(ms.current_ccu) AS current_ccu,
                ARG_MAX(ms.channel_title, ms.growth_percent) AS standout_creator,
                ARG_MAX(ms.mechanic, ms.growth_percent) AS standout_mechanic,
                MIN(ms.detected_at) AS first_detected_at,
                MAX(ms.detected_at) AS last_detected_at
            FROM mechanic_spikes ms, latest
            WHERE ms.detected_at >= latest.max_detected_at - INTERVAL 45 DAY
            GROUP BY ms.universe_id, ms.game_name
            HAVING COUNT(*) >= 2
        )
        SELECT
            grouped.game_name,
            grouped.detections,
            grouped.creators,
            grouped.creator_videos,
            grouped.median_growth,
            grouped.max_growth,
            grouped.current_ccu,
            grouped.standout_creator,
            grouped.standout_mechanic,
            {stickiness_select}
            {decay_select}
            grouped.first_detected_at,
            grouped.last_detected_at
        FROM grouped
        {retention_join}
        ORDER BY grouped.max_growth DESC, grouped.detections DESC, grouped.creators DESC
        LIMIT ?
    """
    frame = _query_dataframe(query, (limit,))
    if not frame.empty:
        frame["game_name"] = frame["game_name"].map(clean_label)
        frame["standout_mechanic"] = frame["standout_mechanic"].map(clean_label)
    return frame


def run_public_game_check(game_name: str, max_results: int = 5) -> dict[str, Any]:
    normalized_name = game_name.strip()
    if not normalized_name:
        return {
            "query": "",
            "ccu": {"found": False, "game_name": "", "current_ccu": None, "baseline_ccu": None, "growth_pct": None, "is_growing": False},
            "videos": [],
            "video_count": 0,
            "signal": "NONE",
            "signal_emoji": "*",
            "recommendation": "Enter a Roblox game to inspect creator coverage and current momentum.",
        }

    ccu_status = get_game_ccu_status(normalized_name)
    loop = asyncio.new_event_loop()
    try:
        videos = loop.run_until_complete(search_youtube_for_game(normalized_name, max_results=max_results))
    finally:
        loop.close()

    video_count = len(videos)
    if video_count >= 8:
        signal, signal_emoji = "STRONG", "+++"
    elif video_count >= 3:
        signal, signal_emoji = "MEDIUM", "++"
    elif video_count >= 1:
        signal, signal_emoji = "WEAK", "+"
    else:
        signal, signal_emoji = "NONE", "-"

    if ccu_status.get("is_growing") and video_count >= 5:
        recommendation = "High momentum: creator coverage is active and CCU is already moving."
    elif ccu_status.get("is_growing") and video_count >= 1:
        recommendation = "Growing with coverage: worth checking for a partnership or timing window."
    elif ccu_status.get("is_growing"):
        recommendation = "Organic growth: CCU is moving without much creator support yet."
    elif video_count >= 5:
        recommendation = "Coverage is active but CCU is flat: watch for delayed lift or weak conversion."
    elif video_count >= 1:
        recommendation = "Some recent creator attention, but no strong momentum signal yet."
    else:
        recommendation = "Low activity right now: no meaningful creator signal in the last 72 hours."

    for video in videos:
        video["channel"] = clean_label(video.get("channel"))

    return {
        "query": normalized_name,
        "ccu": ccu_status,
        "videos": videos,
        "video_count": video_count,
        "signal": signal,
        "signal_emoji": signal_emoji,
        "recommendation": recommendation,
    }
