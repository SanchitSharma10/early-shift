"""
main.py - Orchestrates Early Shift monitoring.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List

import aiohttp

from constants import DEFAULT_DB_PATH, MONITORING_INTERVAL_HOURS, NTFY_URL, Tables
from db_manager import DatabaseManager
from notion_writer import NotionWriter
from queries import TrendingGame, get_trending_games
from roproxy_client import RoProxyClient
from schema import SchemaManager

logger = logging.getLogger(__name__)


class EarlyShift:
    """Main orchestrator for game trend monitoring."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH) -> None:
        self.db_path = db_path
        self.client = RoProxyClient(db_path)
        self.writer = NotionWriter()
        self.db_manager = DatabaseManager(db_path)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Ensure all required database tables exist."""
        SchemaManager.ensure_all_tables(self.db_manager.db)

    def get_trending_games(self) -> List[Dict[str, object]]:
        """Get trending games using shared query logic."""
        trending = get_trending_games(self.db_manager.db, growth_threshold=0.0)
        return [self._trending_game_to_dict(game) for game in trending]

    @staticmethod
    def _trending_game_to_dict(game: TrendingGame) -> Dict[str, object]:
        """Convert TrendingGame to dictionary for backward compatibility."""
        return {
            "universe_id": game.universe_id,
            "game_name": game.game_name,
            "current_ccu": game.current_ccu,
            "week_ago_ccu": game.week_ago_ccu,
            "growth_percent": game.growth_percent,
            "growth_rate": game.growth_rate,
            "peak_ccu": game.peak_ccu,
            "timestamp": game.timestamp,
        }

    def get_subscribed_studios(self) -> List[Dict[str, object]]:
        """Get list of subscribed studios."""
        rows = self.db_manager.db.execute(
            f"SELECT studio_id, name, notion_token, notion_database_id, ntfy_topic FROM {Tables.STUDIOS}"
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
        """Send ntfy.sh alerts for trending games."""
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
                        async with session.post(NTFY_URL, json=payload) as resp:
                            if resp.status >= 400:
                                text = await resp.text()
                                logger.warning(
                                    f"ntfy alert failure for {studio['name']} ({topic}): "
                                    f"{resp.status} {text.strip()}"
                                )
                    except Exception as exc:
                        logger.error(f"ntfy alert error for {studio['name']} ({topic}): {exc}")

    async def run_monitoring_cycle(self) -> None:
        """Run a single monitoring cycle."""
        logger.info(f"Monitoring cycle started {datetime.utcnow().isoformat()}Z")

        universe_ids = await self.client.get_top_universe_ids(limit=500)
        await self.client.poll_top_games(universe_ids)

        trending = self.get_trending_games()
        logger.info(f"Found {len(trending)} trending games")

        if not trending:
            logger.info("No eligible games this cycle")
            return

        studios = self.get_subscribed_studios()
        if not studios:
            logger.info("No studios configured; skipping notifications")
            return

        await self.writer.notify_studios(trending, studios)
        await self.send_ntfy_alerts(trending, studios)

        logger.info(f"Cycle complete {datetime.utcnow().isoformat()}Z")

    async def run_forever(self) -> None:
        """Run monitoring cycles continuously."""
        while True:
            await self.run_monitoring_cycle()
            await asyncio.sleep(MONITORING_INTERVAL_HOURS * 60 * 60)
    
    def close(self) -> None:
        """Clean up resources."""
        self.db_manager.close()


def add_test_studio(db_path: str = DEFAULT_DB_PATH) -> None:
    """Add a test studio for development/testing."""
    with DatabaseManager(db_path) as db_manager:
        SchemaManager.ensure_all_tables(db_manager.db)
        db_manager.db.execute(
            f"""
            INSERT OR REPLACE INTO {Tables.STUDIOS} (studio_id, name, notion_token, notion_database_id, ntfy_topic)
            VALUES ('test-studio', 'Test Studio', 'YOUR_NOTION_TOKEN', 'YOUR_DATABASE_ID', 'early-shift-test')
            """
        )
        db_manager.db.commit()
    logger.info("Test studio inserted. Update credentials before using production alerts.")


if __name__ == "__main__":
    early_shift = EarlyShift()
    asyncio.run(early_shift.run_monitoring_cycle())