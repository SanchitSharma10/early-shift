"""
queries.py - Shared SQL queries for Early Shift
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List

import duckdb

from constants import Tables


@dataclass
class TrendingGame:
    """Represents a game with significant growth."""
    universe_id: int
    game_name: str
    current_ccu: int
    week_ago_ccu: int
    growth_percent: float
    growth_rate: float
    peak_ccu: int
    timestamp: datetime
    
    @classmethod
    def from_db_row(cls, row: tuple) -> "TrendingGame":
        """Create TrendingGame from database row."""
        return cls(
            universe_id=row[0],
            game_name=row[1] or "Unknown",
            current_ccu=row[2] or 0,
            week_ago_ccu=row[3] or 0,
            growth_percent=float(row[4] or 0.0),
            growth_rate=float(row[5] or 0.0),
            peak_ccu=row[6] or row[2] or 0,
            timestamp=row[7],
        )


# SQL query to find trending games with significant growth
TRENDING_GAMES_QUERY = f"""
    WITH latest_snapshot AS (
        SELECT universe_id,
               name,
               ccu,
               timestamp,
               ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
        FROM {Tables.GAMES}
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
        FROM {Tables.GAMES}
        WHERE timestamp <= current_timestamp - INTERVAL 7 DAY
    ),
    filtered_week_ago AS (
        SELECT universe_id, ccu, timestamp
        FROM week_ago_snapshot
        WHERE row_num = 1
    ),
    peak_7d AS (
        SELECT universe_id,
               MAX(ccu) AS peak_ccu
        FROM {Tables.GAMES}
        WHERE timestamp >= current_timestamp - INTERVAL 7 DAY
        GROUP BY universe_id
    )
    SELECT cur.universe_id,
           COALESCE(meta.name, cur.name) AS game_name,
           cur.ccu AS current_ccu,
           prev.ccu AS week_ago_ccu,
           ((cur.ccu - prev.ccu) * 100.0) / NULLIF(prev.ccu, 0) AS growth_percent,
           ((cur.ccu - prev.ccu) * 1.0) / NULLIF(prev.ccu, 0) AS growth_rate,
           COALESCE(peak.peak_ccu, cur.ccu) AS peak_ccu,
           cur.timestamp AS current_timestamp
    FROM filtered_latest cur
    JOIN filtered_week_ago prev ON prev.universe_id = cur.universe_id
    LEFT JOIN {Tables.GAME_METADATA} meta ON meta.universe_id = cur.universe_id
    LEFT JOIN peak_7d peak ON peak.universe_id = cur.universe_id
    WHERE prev.ccu > 0
"""


def get_trending_games(
    db: duckdb.DuckDBPyConnection,
    growth_threshold: float = 0.0,
    limit: int | None = None
) -> List[TrendingGame]:
    """
    Query trending games with significant growth.
    
    Args:
        db: Database connection
        growth_threshold: Minimum growth rate (as decimal, e.g., 0.25 for 25%)
        limit: Maximum number of results to return
        
    Returns:
        List of TrendingGame objects
    """
    query = TRENDING_GAMES_QUERY
    
    if growth_threshold > 0:
        query += f" AND ((cur.ccu - prev.ccu) * 1.0) / NULLIF(prev.ccu, 0) >= {growth_threshold}"
    
    query += " ORDER BY growth_percent DESC"
    
    if limit:
        query += f" LIMIT {limit}"
    
    rows = db.execute(query).fetchall()
    return [TrendingGame.from_db_row(row) for row in rows]


@dataclass
class GrowthCandidate:
    """Represents a game candidate for mechanic spike detection."""
    universe_id: int
    game_name: str
    current_ccu: int
    week_ago_ccu: int
    growth_percent: float
    current_timestamp: datetime
    
    @classmethod
    def from_db_row(cls, row: tuple) -> "GrowthCandidate":
        """Create GrowthCandidate from database row."""
        growth_rate = row[4]
        return cls(
            universe_id=row[0],
            game_name=row[1] or "Unknown",
            current_ccu=int(row[2] or 0),
            week_ago_ccu=int(row[3] or 0),
            growth_percent=float(growth_rate * 100.0) if growth_rate else 0.0,
            current_timestamp=row[5],
        )


# SQL query for growth candidates (without peak CCU calculation)
GROWTH_CANDIDATES_QUERY = f"""
    WITH latest_snapshot AS (
        SELECT universe_id,
               name,
               ccu,
               timestamp,
               ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
        FROM {Tables.GAMES}
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
        FROM {Tables.GAMES}
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
    LEFT JOIN {Tables.GAME_METADATA} meta ON meta.universe_id = cur.universe_id
    WHERE prev.ccu > 0
"""


def get_growth_candidates(
    db: duckdb.DuckDBPyConnection,
    growth_threshold: float
) -> List[GrowthCandidate]:
    """
    Query games with growth above threshold.
    
    Args:
        db: Database connection
        growth_threshold: Minimum growth rate (as decimal, e.g., 0.25 for 25%)
        
    Returns:
        List of GrowthCandidate objects
    """
    rows = db.execute(GROWTH_CANDIDATES_QUERY).fetchall()
    candidates = []
    
    for row in rows:
        growth_rate = row[4]
        if growth_rate is None or growth_rate < growth_threshold:
            continue
        candidates.append(GrowthCandidate.from_db_row(row))
    
    return candidates