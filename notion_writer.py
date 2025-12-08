"""
notion_writer.py - Writes trending games to Notion databases.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List

from notion_client import Client
from notion_client.errors import APIResponseError

from exceptions import NotionAPIError

logger = logging.getLogger(__name__)


class NotionWriter:
    """Writes game trends to each studio's Notion database using the official client."""

    def __init__(self) -> None:
        self._clients: Dict[str, Client] = {}

    def _client_for(self, token: str) -> Client:
        if token not in self._clients:
            self._clients[token] = Client(auth=token)
        return self._clients[token]

    async def send_to_notion(self, studio: Dict[str, object], game: Dict[str, object]) -> None:
        """Send a trending game notification to a studio's Notion database."""
        token = (studio.get("notion_token") or "").strip()
        database_id = (studio.get("notion_database_id") or "").strip()
        studio_name = studio.get("name", "Unknown Studio")

        if not token or not database_id:
            logger.warning(f"Notion credentials missing for {studio_name}; skipping notification.")
            return

        client = self._client_for(token)
        properties = {
            "Game": {
                "title": [
                    {"text": {"content": str(game.get("game_name") or "Unknown Game")}}
                ]
            },
            "Universe ID": {"number": int(game.get("universe_id") or 0)},
            "Growth Percent": {"number": float(game.get("growth_percent") or 0.0)},
            "Growth Rate": {"number": float(game.get("growth_rate") or 0.0)},
            "Current CCU": {"number": int(game.get("current_ccu") or 0)},
            "Peak CCU (7d)": {"number": int(game.get("peak_ccu") or 0)},
            "Week Ago CCU": {"number": int(game.get("week_ago_ccu") or 0)},
            "Alert Time": {"date": {"start": datetime.utcnow().isoformat() + "Z"}},
        }

        try:
            await asyncio.to_thread(
                client.pages.create,
                parent={"database_id": database_id},
                properties=properties,
            )
            logger.info(
                f"Notion update sent to {studio_name}: {game.get('game_name')} +"
                f"{float(game.get('growth_percent') or 0.0):.1f}%"
            )
        except APIResponseError as api_err:
            logger.error(f"Notion API error for {studio_name}: {api_err.status} {api_err.message}")
            raise NotionAPIError(
                f"Failed to create Notion page for {studio_name}: {api_err.message}",
                status_code=api_err.status
            ) from api_err
        except Exception as exc:
            logger.error(f"Unexpected Notion error for {studio_name}: {exc}")
            raise

    async def notify_studios(
        self,
        trending_games: List[Dict[str, object]],
        studios: List[Dict[str, object]],
    ) -> None:
        """Send notifications for all trending games to all studios."""
        tasks = []
        for game in trending_games:
            for studio in studios:
                tasks.append(self.send_to_notion(studio, game))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    logger.info("This module is intended to be used via EarlyShift.")