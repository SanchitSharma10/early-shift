"""
notion_writer.py - Writes trending games to Notion databases.
"""

import asyncio
from datetime import datetime
from typing import Dict, List

from notion_client import Client
from notion_client.errors import APIResponseError


class NotionWriter:
    """Writes game trends to each studio's Notion database using the official client."""

    def __init__(self) -> None:
        self._clients: Dict[str, Client] = {}

    def _client_for(self, token: str) -> Client:
        if token not in self._clients:
            self._clients[token] = Client(auth=token)
        return self._clients[token]

    async def send_to_notion(self, studio: Dict[str, object], game: Dict[str, object]) -> None:
        token = (studio.get("notion_token") or "").strip()
        database_id = (studio.get("notion_database_id") or "").strip()
        studio_name = studio.get("name", "Unknown Studio")

        if not token or not database_id:
            print(f"Notion credentials missing for {studio_name}; skipping notification.")
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
            print(
                f"Notion update sent to {studio_name}: {game.get('game_name')} +"
                f"{float(game.get('growth_percent') or 0.0):.1f}%"
            )
        except APIResponseError as api_err:
            print(
                f"Notion API error for {studio_name}: {api_err.status} {api_err.message}"
            )
        except Exception as exc:
            print(f"Unexpected Notion error for {studio_name}: {exc}")

    async def notify_studios(
        self,
        trending_games: List[Dict[str, object]],
        studios: List[Dict[str, object]],
    ) -> None:
        tasks = []
        for game in trending_games:
            for studio in studios:
                tasks.append(self.send_to_notion(studio, game))
        if tasks:
            await asyncio.gather(*tasks)


if __name__ == "__main__":
    print("This module is intended to be used via EarlyShift.")