"""
roproxy_client.py - Poll Roblox universe CCU data via RoProxy
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import aiohttp
import duckdb

FALLBACK_UNIVERSES: List[int] = [
    994732206, 245662005, 383310974, 47545, 210851291, 4924922222, 185655149,
    10977891899, 5902977743, 8737602446, 1537690962, 1182249705, 142823291,
    1400147734, 920587237, 8149070699, 3351674303, 5569431581, 6381829480,
    4872321990, 4520749081, 13822889, 9498006165, 488667523, 286090429,
    3823781113, 447452406, 2010620636, 2013640567, 3291301470, 511316432,
    2216618303, 3145447021, 5926001758, 3192707582, 275420544, 263761432,
    92012076, 1377239466, 6284583030, 2988862959, 2583109575, 1540764883,
    5763726676, 3019923553, 5938036553
]

POPULAR_ENDPOINTS: List[str] = [
    "https://games.roproxy.com/v1/discovery/universes",
    "https://games.roblox.com/v1/discovery/universes",
]

DEFAULT_CACHE_HOURS = 4
DEFAULT_CONCURRENCY = 12


@dataclass
class UniverseSnapshot:
    universe_id: int
    name: str
    ccu: int
    root_place_id: Optional[int]
    description: Optional[str]
    creator_id: Optional[int]
    creator_name: Optional[str]
    genre: Optional[str]
    visits: Optional[int]


class RoProxyClient:
    """Poll Roblox CCU data and persist snapshots to DuckDB."""

    def __init__(
        self,
        db_path: str = "early_shift.db",
        cache_hours: int = DEFAULT_CACHE_HOURS,
        max_concurrency: int = DEFAULT_CONCURRENCY,
    ) -> None:
        self.db = duckdb.connect(db_path)
        self.base_url = "https://games.roproxy.com/v1/games"
        self.cache_path = Path(db_path).with_suffix(".top_universes.json")
        self.cache_ttl = timedelta(hours=cache_hours)
        self.http_timeout = aiohttp.ClientTimeout(total=20)
        self.max_concurrency = max(1, max_concurrency)
        self._init_db()

    def _init_db(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                universe_id BIGINT,
                name TEXT,
                ccu INTEGER,
                timestamp TIMESTAMP,
                PRIMARY KEY (universe_id, timestamp)
            )
            """
        )
        columns = {row[1] for row in self.db.execute("PRAGMA table_info('games')").fetchall()}
        if "game_id" in columns and "universe_id" not in columns:
            self.db.execute("ALTER TABLE games RENAME COLUMN game_id TO universe_id")
        self.db.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS name TEXT")
        self.db.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS ccu INTEGER")
        self.db.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS timestamp TIMESTAMP")

        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS game_metadata (
                universe_id BIGINT PRIMARY KEY,
                name TEXT,
                root_place_id BIGINT,
                creator_id BIGINT,
                creator_name TEXT,
                description TEXT,
                genre TEXT,
                visits BIGINT,
                last_seen_ccu INTEGER,
                updated_at TIMESTAMP
            )
            """
        )
        self.db.commit()

    async def _fetch_top_universes(self, limit: int) -> List[int]:
        universe_ids: List[int] = []
        seen: set[int] = set()
        headers = {
            "User-Agent": "EarlyShiftBot/1.0 (+https://github.com/SanchitSharma10)",
            "Accept": "application/json",
        }
        async with aiohttp.ClientSession(timeout=self.http_timeout, headers=headers) as session:
            for endpoint in POPULAR_ENDPOINTS:
                cursor: Optional[str] = None
                try:
                    while len(universe_ids) < limit:
                        params = {
                            "SortType": "Popular",
                            "Limit": min(100, limit - len(universe_ids)),
                        }
                        if cursor:
                            params["Cursor"] = cursor
                        async with session.get(endpoint, params=params) as resp:
                            if resp.status != 200:
                                break
                            payload = await resp.json()
                        data = payload.get("data") or payload.get("universes")
                        if not data:
                            break
                        for entry in data:
                            universe_id = entry.get("id") or entry.get("universeId")
                            if not universe_id:
                                continue
                            uid = int(universe_id)
                            if uid in seen:
                                continue
                            seen.add(uid)
                            universe_ids.append(uid)
                            if len(universe_ids) >= limit:
                                break
                        cursor = payload.get("nextPageCursor") or payload.get("nextPageToken")
                        if not cursor:
                            break
                except Exception as exc:
                    print(f"Warning: unable to fetch discovery data from {endpoint}: {exc}")
                if universe_ids:
                    break
        return universe_ids

    def _load_cached_universes(self) -> Optional[List[int]]:
        if not self.cache_path.exists():
            return None
        try:
            cached = json.loads(self.cache_path.read_text())
        except json.JSONDecodeError:
            return None
        timestamp = cached.get("generated_at")
        if not timestamp:
            return None
        generated_at = datetime.fromisoformat(timestamp)
        if datetime.utcnow() - generated_at > self.cache_ttl:
            return None
        return [int(uid) for uid in cached.get("universe_ids", [])]

    def _save_cached_universes(self, universe_ids: Iterable[int]) -> None:
        payload = {
            "generated_at": datetime.utcnow().isoformat(),
            "universe_ids": list(dict.fromkeys(int(uid) for uid in universe_ids)),
        }
        self.cache_path.write_text(json.dumps(payload))

    async def get_top_universe_ids(self, limit: int = 500) -> List[int]:
        cached = self._load_cached_universes()
        if cached:
            return cached[:limit]
        universe_ids = await self._fetch_top_universes(limit)
        if universe_ids:
            self._save_cached_universes(universe_ids)
            return universe_ids
        print("Warning: using fallback universe list; discovery APIs unavailable.")
        self._save_cached_universes(FALLBACK_UNIVERSES)
        return FALLBACK_UNIVERSES[:limit]

    async def _fetch_universe_snapshot(
        self,
        session: aiohttp.ClientSession,
        universe_id: int,
    ) -> UniverseSnapshot:
        url = f"{self.base_url}?universeIds={universe_id}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    data = payload.get("data")
                    if data:
                        game = data[0]
                        creator = game.get("creator") or {}
                        return UniverseSnapshot(
                            universe_id=universe_id,
                            name=game.get("name", "Unknown"),
                            ccu=game.get("playing", 0),
                            root_place_id=game.get("rootPlaceId"),
                            description=game.get("description"),
                            creator_id=creator.get("id"),
                            creator_name=creator.get("name"),
                            genre=game.get("genre"),
                            visits=game.get("visits"),
                        )
        except Exception as exc:
            print(f"Error fetching universe {universe_id}: {exc}")
        return UniverseSnapshot(
            universe_id=universe_id,
            name="Unknown",
            ccu=0,
            root_place_id=None,
            description=None,
            creator_id=None,
            creator_name=None,
            genre=None,
            visits=None,
        )

    def _upsert_metadata(self, snapshot: UniverseSnapshot) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO game_metadata (
                universe_id, name, root_place_id, creator_id, creator_name,
                description, genre, visits, last_seen_ccu, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.universe_id,
                snapshot.name,
                snapshot.root_place_id,
                snapshot.creator_id,
                snapshot.creator_name,
                snapshot.description,
                snapshot.genre,
                snapshot.visits,
                snapshot.ccu,
                datetime.utcnow(),
            ),
        )

    async def poll_top_games(self, universe_ids: Sequence[int]) -> None:
        unique_ids = list(dict.fromkeys(int(uid) for uid in universe_ids))
        if not unique_ids:
            print("No universe IDs supplied; skipping poll.")
            return

        timestamp = datetime.utcnow()
        async with aiohttp.ClientSession(timeout=self.http_timeout) as session:
            semaphore = asyncio.Semaphore(self.max_concurrency)

            async def fetch(uid: int) -> UniverseSnapshot:
                async with semaphore:
                    return await self._fetch_universe_snapshot(session, uid)

            snapshots = await asyncio.gather(*(fetch(uid) for uid in unique_ids))

        for snapshot in snapshots:
            self._upsert_metadata(snapshot)
            self.db.execute(
                "DELETE FROM games WHERE universe_id = ? AND timestamp = ?",
                (snapshot.universe_id, timestamp),
            )
            self.db.execute(
                "INSERT INTO games (universe_id, name, ccu, timestamp) VALUES (?, ?, ?, ?)",
                (snapshot.universe_id, snapshot.name, snapshot.ccu, timestamp),
            )
        self.db.commit()
        print(f"[OK] Polled {len(unique_ids)} universes at {timestamp.isoformat()}Z")


if __name__ == "__main__":
    client = RoProxyClient()
    universes = asyncio.run(client.get_top_universe_ids(limit=5))
    asyncio.run(client.poll_top_games(universes))
