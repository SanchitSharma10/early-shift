"""
main.py - Orchestrates Early Shift monitoring.
"""

import asyncio
from datetime import datetime
from typing import Dict, List

import aiohttp
import duckdb

from notion_writer import NotionWriter
from roproxy_client import RoProxyClient


class EarlyShift:
    """Main orchestrator for game trend monitoring."""

    def __init__(self, db_path: str = "early_shift.db") -> None:
        self.db_path = db_path
        self.client = RoProxyClient(db_path)
        self.writer = NotionWriter()
        self.db = duckdb.connect(self.db_path)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS studios (
                studio_id TEXT PRIMARY KEY,
                name TEXT,
                notion_token TEXT,
                notion_database_id TEXT,
                ntfy_topic TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # Backfill columns for older installs that stored webhook URLs.
        self.db.execute("ALTER TABLE studios ADD COLUMN IF NOT EXISTS notion_token TEXT")
        self.db.execute("ALTER TABLE studios ADD COLUMN IF NOT EXISTS notion_database_id TEXT")
        self.db.execute("ALTER TABLE studios ADD COLUMN IF NOT EXISTS ntfy_topic TEXT")
        self.db.commit()

    def get_trending_games(self) -> List[Dict[str, object]]:
        with open("diff_detector.sql", "r", encoding="utf-8") as handle:
            sql = handle.read()

        rows = self.db.execute(sql).fetchall()
        games: List[Dict[str, object]] = []
        for row in rows:
            games.append(
                {
                    "universe_id": row[0],
                    "game_name": row[1] or "Unknown",
                    "current_ccu": row[2] or 0,
                    "week_ago_ccu": row[3] or 0,
                    "growth_percent": float(row[4] or 0.0),
                    "growth_rate": float(row[5] or 0.0),
                    "peak_ccu": row[6] or row[2] or 0,
                    "timestamp": row[7],
                }
            )
        return games

    def get_subscribed_studios(self) -> List[Dict[str, object]]:
        rows = self.db.execute(
            "SELECT studio_id, name, notion_token, notion_database_id, ntfy_topic FROM studios"
        ).fetchall()
        return [
            {
                "studio_id": row[0],
                "name": row[1],
                "notion_token": row[2],
                "notion_database_id": row[3],
                "ntfy_topic": row[4],
            }
            for row in rows
        ]

    async def send_ntfy_alerts(self, games: List[Dict[str, object]], studios: List[Dict[str, object]]) -> None:
        async with aiohttp.ClientSession() as session:
            for studio in studios:
                topic = (studio.get("ntfy_topic") or "").strip()
                if not topic:
                    continue
                for game in games:
                    payload = {
                        "topic": topic,
                        "title": f"{game['game_name']} up {game['growth_percent']:.1f}%",
                        "message": (
                            f"Current CCU {game['current_ccu']:,} (peak {game['peak_ccu']:,} last 7d). "
                            f"Week-ago baseline {game['week_ago_ccu']:,}."
                        ),
                        "priority": 5,
                    }
                    try:
                        async with session.post("https://ntfy.sh", json=payload) as resp:
                            if resp.status >= 400:
                                text = await resp.text()
                                print(
                                    f"ntfy alert failure for {studio['name']} ({topic}): "
                                    f"{resp.status} {text.strip()}"
                                )
                    except Exception as exc:
                        print(f"ntfy alert error for {studio['name']} ({topic}): {exc}")

    async def run_monitoring_cycle(self) -> None:
        print(f"\n[Early Shift] Monitoring cycle started {datetime.utcnow().isoformat()}Z")

        universe_ids = await self.client.get_top_universe_ids(limit=500)
        await self.client.poll_top_games(universe_ids)

        trending = self.get_trending_games()
        print(f"[Early Shift] Found {len(trending)} trending games")

        if not trending:
            print("[Early Shift] No eligible games this cycle\n")
            return

        studios = self.get_subscribed_studios()
        if not studios:
            print("[Early Shift] No studios configured; skipping notifications\n")
            return

        await self.writer.notify_studios(trending, studios)
        await self.send_ntfy_alerts(trending, studios)

        print(f"[Early Shift] Cycle complete {datetime.utcnow().isoformat()}Z\n")

    async def run_forever(self) -> None:
        while True:
            await self.run_monitoring_cycle()
            await asyncio.sleep(4 * 60 * 60)


def add_test_studio(db_path: str = "early_shift.db") -> None:
    db = duckdb.connect(db_path)
    db.execute(
        """
        INSERT OR REPLACE INTO studios (studio_id, name, notion_token, notion_database_id, ntfy_topic)
        VALUES ('test-studio', 'Test Studio', 'YOUR_NOTION_TOKEN', 'YOUR_DATABASE_ID', 'early-shift-test')
        """
    )
    db.commit()
    db.close()
    print("Test studio inserted. Update credentials before using production alerts.")


if __name__ == "__main__":
    early_shift = EarlyShift()
    asyncio.run(early_shift.run_monitoring_cycle())