"""Mechanic spike detector for Early Shift.
Combines CCU growth data with creator video chatter to surface
potentially viral mechanics for Roblox studios."""
from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Iterable, List

import duckdb
from rapidfuzz import fuzz

GROWTH_THRESHOLD = 0.25  # 25% growth
MENTION_LOOKBACK_HOURS = 48
KEYWORD_HINTS = [
    "new",
    "update",
    "secret",
    "mechanic",
    "code",
    "feature",
    "quest",
    "event",
]
FUZZ_THRESHOLD = 82


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



def _ensure_spikes_table(db: duckdb.DuckDBPyConnection) -> None:
    """Create mechanic_spikes table if it doesn't exist."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS mechanic_spikes (
            universe_id INTEGER NOT NULL,
            game_name TEXT,
            current_ccu INTEGER,
            week_ago_ccu INTEGER,
            growth_percent DOUBLE,
            published_at TIMESTAMP,
            mechanic TEXT,
            video_title TEXT,
            video_url TEXT,
            channel_title TEXT,
            detected_at TIMESTAMP NOT NULL
        )
        """
    )
    db.commit()



def _persist_spikes(db: duckdb.DuckDBPyConnection, spikes: List[MechanicSpike]) -> None:
    """Save detected spikes to database."""
    if not spikes:
        return
    
    _ensure_spikes_table(db)
    
    for spike in spikes:
        db.execute("""
            INSERT INTO mechanic_spikes (
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
    db_path: str = "early_shift.db",
    limit: int = 50
) -> List[MechanicSpike]:
    """Retrieve historical spikes from database."""
    db = duckdb.connect(db_path)
    _ensure_spikes_table(db)
    
    rows = db.execute("""
        SELECT universe_id, game_name, current_ccu, week_ago_ccu,
               growth_percent, published_at, mechanic, video_title,
               video_url, channel_title, detected_at
        FROM mechanic_spikes
        ORDER BY detected_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    
    db.close()
    
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


def _read_growth_candidates(
    db: duckdb.DuckDBPyConnection, growth_threshold: float
) -> List[dict]:
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
               ((cur.ccu - prev.ccu) * 1.0) / NULLIF(prev.ccu, 0) AS growth_rate,
               cur.timestamp AS current_timestamp
        FROM filtered_latest cur
        JOIN filtered_week_ago prev ON prev.universe_id = cur.universe_id
        LEFT JOIN game_metadata meta ON meta.universe_id = cur.universe_id
        WHERE prev.ccu > 0
    """
    rows = db.execute(query).fetchall()
    candidates: List[dict] = []
    for row in rows:
        growth_rate = row[4]
        if growth_rate is None or growth_rate < growth_threshold:
            continue
        candidates.append(
            {
                "universe_id": row[0],
                "game_name": row[1] or "Unknown",
                "current_ccu": int(row[2] or 0),
                "week_ago_ccu": int(row[3] or 0),
                "growth_percent": float(growth_rate * 100.0),
                "current_timestamp": row[5],
            }
        )
    return candidates


def _fetch_recent_videos(db: duckdb.DuckDBPyConnection, since: datetime) -> List[dict]:
    query = """
        SELECT video_id,
               channel_title,
               title,
               published_at,
               view_count
        FROM youtube_videos
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
    db_path: str = "early_shift.db",
    lookback_hours: int = MENTION_LOOKBACK_HOURS,
    growth_threshold: float = GROWTH_THRESHOLD,
    persist: bool = True,
) -> List[MechanicSpike]:
    db = duckdb.connect(db_path)
    candidates = _read_growth_candidates(db, growth_threshold)
    if not candidates:
        db.close()
        return []

    since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    videos = _fetch_recent_videos(db, since)
    
    spikes: List[MechanicSpike] = []

    for candidate in candidates:
        for video in videos:
            title = video["title"] or ""
            if not title:
                continue
            if not _video_matches_game(candidate["game_name"], title):
                continue
            mechanic = _extract_mechanic(title)
            spikes.append(
                MechanicSpike(
                    universe_id=candidate["universe_id"],
                    game_name=candidate["game_name"],
                    current_ccu=candidate["current_ccu"],
                    week_ago_ccu=candidate["week_ago_ccu"],
                    growth_percent=candidate["growth_percent"],
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
    
    db.close()
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
    print(format_spikes_table(spikes))
    print(f"\n{len(spikes)} spikes detected and persisted to database.")
