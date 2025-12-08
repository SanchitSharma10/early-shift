"""Mechanic spike detector for Early Shift.
Combines CCU growth data with creator video chatter to surface
potentially viral mechanics for Roblox studios."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List

from rapidfuzz import fuzz

from constants import (
    DEFAULT_DB_PATH,
    FUZZ_THRESHOLD,
    GROWTH_THRESHOLD,
    KEYWORD_HINTS,
    MENTION_LOOKBACK_HOURS,
    Tables,
)
from db_manager import get_db_connection
from queries import get_growth_candidates
from schema import SchemaManager

logger = logging.getLogger(__name__)


@dataclass
class MechanicSpike:
    universe_id: int
    game_name: str
    current_ccu: int
    week_ago_ccu: int
    growth_percent: float
    published_at: datetime
    mechanic: str
    video_title: str
    video_url: str
    channel_title: str
    detected_at: datetime


MECHANIC_REGEX = re.compile(
    r"(?i)(?:new|secret|update|introducing|added|unlock|mechanic|feature|quest|code)[:\-\s]*(.*)"
)


def _extract_mechanic(title: str) -> str:
    match = MECHANIC_REGEX.search(title)
    if match:
        candidate = match.group(1).strip()
        return candidate[:120]
    return title[:120]



def _persist_spikes(db, spikes: List[MechanicSpike]) -> None:
    """Save detected spikes to database."""
    if not spikes:
        return
    
    SchemaManager._ensure_mechanic_spikes_table(db)
    
    for spike in spikes:
        db.execute(f"""
            INSERT INTO {Tables.MECHANIC_SPIKES} (
                universe_id, game_name, current_ccu, week_ago_ccu,
                growth_percent, published_at, mechanic, video_title,
                video_url, channel_title, detected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            spike.universe_id,
            spike.game_name,
            spike.current_ccu,
            spike.week_ago_ccu,
            spike.growth_percent,
            spike.published_at,
            spike.mechanic,
            spike.video_title,
            spike.video_url,
            spike.channel_title,
            spike.detected_at
        ))
    db.commit()


def get_historical_spikes(
    db_path: str = DEFAULT_DB_PATH,
    limit: int = 50
) -> List[MechanicSpike]:
    """Retrieve historical spikes from database."""
    with get_db_connection(db_path, read_only=True) as db:
        SchemaManager._ensure_mechanic_spikes_table(db)
        
        rows = db.execute(f"""
            SELECT universe_id, game_name, current_ccu, week_ago_ccu,
                   growth_percent, published_at, mechanic, video_title,
                   video_url, channel_title, detected_at
            FROM {Tables.MECHANIC_SPIKES}
            ORDER BY detected_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        
        return [
            MechanicSpike(
                universe_id=row[0],
                game_name=row[1],
                current_ccu=row[2],
                week_ago_ccu=row[3],
                growth_percent=row[4],
                published_at=row[5],
                mechanic=row[6],
                video_title=row[7],
                video_url=row[8],
                channel_title=row[9],
                detected_at=row[10]
            )
            for row in rows
        ]


def _fetch_recent_videos(db, since: datetime) -> List[dict]:
    """Fetch recent videos from database."""
    query = f"""
        SELECT video_id,
               channel_title,
               title,
               published_at,
               view_count
        FROM {Tables.YOUTUBE_VIDEOS}
        WHERE published_at >= ?
    """
    return [
        {
            "video_id": row[0],
            "channel_title": row[1],
            "title": row[2],
            "published_at": row[3],
            "view_count": row[4],
        }
        for row in db.execute(query, (since,)).fetchall()
    ]


def _video_matches_game(game_name: str, video_title: str) -> bool:
    score = fuzz.partial_ratio(game_name.lower(), video_title.lower())
    if score >= FUZZ_THRESHOLD:
        return True
    for keyword in KEYWORD_HINTS:
        if keyword in video_title.lower() and game_name.lower() in video_title.lower():
            return True
    return False


def detect_mechanic_spikes(
    db_path: str = DEFAULT_DB_PATH,
    lookback_hours: int = MENTION_LOOKBACK_HOURS,
    growth_threshold: float = GROWTH_THRESHOLD,
    persist: bool = True,
) -> List[MechanicSpike]:
    """
    Detect mechanic spikes by correlating CCU growth with YouTube mentions.
    
    Args:
        db_path: Path to the database
        lookback_hours: Hours to look back for video mentions
        growth_threshold: Minimum growth rate threshold (as decimal)
        persist: Whether to persist results to database
        
    Returns:
        List of detected MechanicSpike objects
    """
    with get_db_connection(db_path) as db:
        candidates = get_growth_candidates(db, growth_threshold)
        if not candidates:
            return []

        since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        videos = _fetch_recent_videos(db, since)
        
        spikes: List[MechanicSpike] = []

        for candidate in candidates:
            for video in videos:
                title = video["title"] or ""
                if not title:
                    continue
                if not _video_matches_game(candidate.game_name, title):
                    continue
                mechanic = _extract_mechanic(title)
                spikes.append(
                    MechanicSpike(
                        universe_id=candidate.universe_id,
                        game_name=candidate.game_name,
                        current_ccu=candidate.current_ccu,
                        week_ago_ccu=candidate.week_ago_ccu,
                        growth_percent=candidate.growth_percent,
                        published_at=video["published_at"],
                        mechanic=mechanic,
                        video_title=title,
                        video_url=f"https://youtube.com/watch?v={video['video_id']}",
                        channel_title=video["channel_title"],
                        detected_at=datetime.now(timezone.utc),
                    )
                )
        
        if persist and spikes:
            _persist_spikes(db, spikes)
        
        return spikes


def format_spikes_table(spikes: Iterable[MechanicSpike]) -> str:
    spikes = list(spikes)
    if not spikes:
        return "No mechanic spikes detected in the selected window."

    headers = [
        "Game",
        "Growth",
        "Current CCU",
        "Mechanic",
        "Source",
        "Published",
    ]
    rows = [
        headers,
        ["-" * len(h) for h in headers],
    ]
    for spike in spikes:
        rows.append(
            [
                spike.game_name,
                f"{spike.growth_percent:.1f}%",
                f"{spike.current_ccu:,}",
                spike.mechanic,
                spike.video_url,
                spike.published_at.strftime("%Y-%m-%d %H:%M"),
            ]
        )
    col_widths = [max(len(row[idx]) for row in rows) for idx in range(len(headers))]
    lines: List[str] = []
    for row in rows:
        padded = [cell.ljust(col_widths[idx]) for idx, cell in enumerate(row)]
        lines.append(" | ".join(padded))
    return "\n".join(lines)


if __name__ == "__main__":
    spikes = detect_mechanic_spikes()
    logger.info(format_spikes_table(spikes))
    logger.info(f"{len(spikes)} spikes detected and persisted to database.")
