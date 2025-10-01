"""YouTube collector for Early Shift.
Fetches recent videos for curated Roblox creators and stores
metadata in DuckDB for downstream mechanic detection."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

import duckdb
import requests
from dotenv import load_dotenv

# Load environment variables from local .env if present
load_dotenv(Path(__file__).with_name('.env'))

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# Hard-coded list of high-signal Roblox channels (channel IDs)
CREATOR_CHANNEL_IDS: List[str] = [
    "UCa2J9M0nsrQJ6GxKF5g8Pow",  # DeeterPlays
    "UCg-OUfS9y4Yh0YQBT8aJ6VQ",  # Laughability
    "UCb3Y0PXmR2d0w1X7eW3FhVQ",  # TanqR
    "UCqJnJ2C-V8GJZz7GzE5bnVQ",  # RussoPlays
    "UCn3l2Z6Y4ZC1d7T_y2cYwUw",  # DigitoSIM
    "UCp1R0kRBdUhEW_k8fKcoeFQ",  # DV Plays
    "UCFz5mJ1GScNwmuredmw6C5g",  # TeraBrite Games
    "UC5p0TQ3uO9cwvx6YQg9nEuw",  # KreekCraft
    "UCbsP5BL1zJ09GZbK9gk0rVQ",  # Calvin Vu
    "UC8_Up8ZYfSNb-iw5yKFZ7VQ",  # Conor3D
    "UCUnZ7K2_7qIbkQGi01P8hBw",  # LaughClip
    "UCi_5N6f0GO6zkRgLpmj12og",  # ItzVortex
    "UCQvZh3_ZrW6Q2VZ0x5uMPRQ",  # Parlo
    "UC2ClR5B2wSx8s4Z45xRyk-Q",  # DV Plays Roblox
    "UCZ5d5qb8xxOxALG7KX9Yw_g",  # DeeterPlays Clips
    "UC8S4rDRZn6Z_StJ-hh7ph8g",  # iamSanna
    "UCW3fsT0W48sL6-fdt5nBOIQ",  # MeganPlays
    "UCVyfo6o3v9wJ1gFwS7h3u3w",  # Glitch
    "UCtoxt3OAz_3t9skoJczAbfw",  # BuildIntoGames
    "UCzl4mgxgw9KLSVkKXzbyVQA",  # Lonnie
]

SEARCH_ENDPOINT = "https://www.googleapis.com/youtube/v3/search"
VIDEOS_ENDPOINT = "https://www.googleapis.com/youtube/v3/videos"


@dataclass
class VideoRecord:
    video_id: str
    channel_id: str
    channel_title: str
    title: str
    description: str
    published_at: datetime
    view_count: int
    like_count: int
    fetched_at: datetime


def _require_api_key() -> str:
    if not YOUTUBE_API_KEY:
        raise RuntimeError(
            "YOUTUBE_API_KEY environment variable is required to run the collector."
        )
    return YOUTUBE_API_KEY


def fetch_recent_video_ids(channel_id: str, max_results: int = 5) -> List[dict]:
    """Return raw search items for the most recent channel uploads."""

    api_key = _require_api_key()
    params = {
        "key": api_key,
        "part": "snippet",
        "channelId": channel_id,
        "order": "date",
        "type": "video",
        "maxResults": max_results,
    }
    response = requests.get(SEARCH_ENDPOINT, params=params, timeout=15)
    response.raise_for_status()
    payload = response.json()
    return payload.get("items", [])


def fetch_video_statistics(video_ids: Iterable[str]) -> dict[str, dict]:
    """Fetch statistics for the provided video IDs."""

    video_ids = list(video_ids)
    if not video_ids:
        return {}

    api_key = _require_api_key()
    params = {
        "key": api_key,
        "part": "statistics,contentDetails,snippet",
        "id": ",".join(video_ids),
        "maxResults": len(video_ids),
    }
    response = requests.get(VIDEOS_ENDPOINT, params=params, timeout=15)
    response.raise_for_status()
    payload = response.json()
    result: dict[str, dict] = {}
    for item in payload.get("items", []):
        result[item["id"]] = item
    return result


def collect_creator_videos(channel_id: str, max_results: int = 5) -> List[VideoRecord]:
    """Fetch and normalize the most recent videos for a channel."""

    search_items = fetch_recent_video_ids(channel_id, max_results=max_results)
    video_ids = [item["id"].get("videoId") for item in search_items if item.get("id")]
    stats_map = fetch_video_statistics(video_ids)
    records: List[VideoRecord] = []
    fetched_at = datetime.now(timezone.utc)

    for item in search_items:
        video_id = item.get("id", {}).get("videoId")
        if not video_id:
            continue
        snippet = item.get("snippet", {})
        stats = stats_map.get(video_id, {})
        published_at_str = snippet.get("publishedAt")
        try:
            published_at = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
        except Exception:
            published_at = fetched_at

        statistics = stats.get("statistics", {})
        records.append(
            VideoRecord(
                video_id=video_id,
                channel_id=snippet.get("channelId", channel_id),
                channel_title=snippet.get("channelTitle", "Unknown"),
                title=snippet.get("title", ""),
                description=snippet.get("description", ""),
                published_at=published_at,
                view_count=int(statistics.get("viewCount", 0) or 0),
                like_count=int(statistics.get("likeCount", 0) or 0),
                fetched_at=fetched_at,
            )
        )
    return records


def ensure_schema(db: duckdb.DuckDBPyConnection) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS youtube_videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT,
            channel_title TEXT,
            title TEXT,
            description TEXT,
            published_at TIMESTAMP,
            view_count BIGINT,
            like_count BIGINT,
            fetched_at TIMESTAMP
        )
        """
    )
    db.commit()


def store_records(records: Iterable[VideoRecord], db_path: str = "early_shift.db") -> int:
    records = list(records)
    if not records:
        return 0
    db = duckdb.connect(db_path)
    ensure_schema(db)
    db.executemany(
        """
        INSERT OR REPLACE INTO youtube_videos (
            video_id, channel_id, channel_title, title,
            description, published_at, view_count, like_count, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r.video_id,
                r.channel_id,
                r.channel_title,
                r.title,
                r.description,
                r.published_at,
                r.view_count,
                r.like_count,
                r.fetched_at,
            )
            for r in records
        ],
    )
    db.commit()
    db.close()
    return len(records)


def run_collection(channels: Iterable[str] = CREATOR_CHANNEL_IDS, max_results: int = 5) -> None:
    total = 0
    for channel_id in channels:
        try:
            records = collect_creator_videos(channel_id, max_results=max_results)
            inserted = store_records(records)
            print(f"[OK] Stored {inserted} videos for channel {channel_id}")
            total += inserted
        except Exception as exc:
            print(f"[WARN] Failed to collect {channel_id}: {exc}")
    print(f"[DONE] Stored/updated {total} videos")


if __name__ == "__main__":
    run_collection()
