"""YouTube collector for Early Shift.
Fetches recent videos for curated Roblox creators and stores
metadata in DuckDB for downstream mechanic detection."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence

import requests

from config import get_config
from constants import (
    CHANNELS_FILE,
    DATA_DIR,
    DEFAULT_DB_PATH,
    DEFAULT_YOUTUBE_CHANNELS,
    YOUTUBE_SEARCH_ENDPOINT,
    YOUTUBE_VIDEOS_ENDPOINT,
    Tables,
)
from db_manager import get_db_connection
from exceptions import YouTubeAPIError, ConfigurationError
from schema import SchemaManager

logger = logging.getLogger(__name__)


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
    """Get YouTube API key from configuration."""
    config = get_config()
    try:
        config.validate_youtube_api_key()
        return config.youtube_api_key
    except ValueError as e:
        raise ConfigurationError(str(e)) from e


def load_channels(path: Path | None = None) -> List[dict]:
    """Load channel configuration from JSON; fall back to defaults."""
    path = path or CHANNELS_FILE

    channels: List[dict] = []
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            raw = payload.get("channels", payload)
            if isinstance(raw, list):
                for entry in raw:
                    if isinstance(entry, dict):
                        chan_id = entry.get("id") or entry.get("channel_id")
                        name = entry.get("name") or entry.get("channel_title", chan_id)
                    else:
                        chan_id = str(entry)
                        name = chan_id
                    if not chan_id or chan_id.upper().startswith("REPLACE"):
                        continue
                    channels.append({"name": name, "id": chan_id})
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse channels file {path}: {e}")

    if not channels:
        channels = DEFAULT_YOUTUBE_CHANNELS.copy()
    return channels


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
    try:
        response = requests.get(YOUTUBE_SEARCH_ENDPOINT, params=params, timeout=15)
        response.raise_for_status()
        payload = response.json()
        return payload.get("items", [])
    except requests.RequestException as e:
        raise YouTubeAPIError(f"Failed to fetch videos for channel {channel_id}",
                             status_code=getattr(e.response, 'status_code', None)) from e


def fetch_video_statistics(video_ids: Iterable[str]) -> dict[str, dict]:
    """Fetch statistics for the provided video IDs."""
    video_ids = [vid for vid in video_ids if vid]
    if not video_ids:
        return {}

    api_key = _require_api_key()
    params = {
        "key": api_key,
        "part": "statistics,contentDetails,snippet",
        "id": ",".join(video_ids),
        "maxResults": len(video_ids),
    }
    try:
        response = requests.get(YOUTUBE_VIDEOS_ENDPOINT, params=params, timeout=15)
        response.raise_for_status()
        payload = response.json()
        result: dict[str, dict] = {}
        for item in payload.get("items", []):
            result[item["id"]] = item
        return result
    except requests.RequestException as e:
        raise YouTubeAPIError(f"Failed to fetch video statistics",
                             status_code=getattr(e.response, 'status_code', None)) from e


def collect_creator_videos(channel_id: str, max_results: int = 5) -> List[VideoRecord]:
    """Fetch and normalize the most recent videos for a channel."""

    search_items = fetch_recent_video_ids(channel_id, max_results=max_results)
    video_ids = [item.get("id", {}).get("videoId") for item in search_items if item.get("id")]
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


def store_records(records: Iterable[VideoRecord], db_path: str = DEFAULT_DB_PATH) -> int:
    """Store video records to the database."""
    records = list(records)
    if not records:
        return 0
    
    with get_db_connection(db_path) as db:
        SchemaManager._ensure_youtube_table(db)
        db.executemany(
            f"""
            INSERT OR REPLACE INTO {Tables.YOUTUBE_VIDEOS} (
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
    return len(records)


def select_channel_batch(
    channels: Sequence[dict],
    batch_size: int | None,
    batch_index: int,
) -> List[dict]:
    if not batch_size or batch_size <= 0 or batch_size >= len(channels):
        return list(channels)
    batches = [channels[i : i + batch_size] for i in range(0, len(channels), batch_size)]
    batch_index = batch_index % len(batches)
    return batches[batch_index]


def run_collection(
    channels: Sequence[dict],
    max_results: int = 5,
    batch_size: int | None = None,
    batch_index: int = 0,
) -> None:
    """Run video collection for the specified channels."""
    selected = select_channel_batch(channels, batch_size, batch_index)
    total = 0
    for channel in selected:
        channel_id = channel["id"]
        channel_name = channel.get("name", channel_id)
        try:
            records = collect_creator_videos(channel_id, max_results=max_results)
            inserted = store_records(records)
            logger.info(f"Stored {inserted} videos for {channel_name} ({channel_id})")
            total += inserted
        except YouTubeAPIError as exc:
            logger.error(f"YouTube API error for {channel_name} ({channel_id}): {exc}")
        except Exception as exc:
            logger.warning(f"Failed to collect {channel_name} ({channel_id}): {exc}")
    logger.info(f"Stored/updated {total} videos from {len(selected)} channels")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect Roblox creator videos.")
    parser.add_argument("--channels-file", type=Path, help="Optional JSON file with channel list")
    parser.add_argument("--max-results", type=int, default=5, help="Videos per channel to fetch")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Number of channels per run (0 = all)",
    )
    parser.add_argument(
        "--batch-index",
        type=int,
        default=0,
        help="Batch index to run when batch-size is set",
    )
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)
    channels = load_channels(args.channels_file)
    source_path = args.channels_file or CHANNELS_FILE
    if source_path.exists():
        logger.info(f"Loaded {len(channels)} channels from {source_path}")
    else:
        logger.info(f"Loaded {len(channels)} channels from defaults")
    batch_size = args.batch_size if args.batch_size > 0 else None
    run_collection(channels, max_results=args.max_results, batch_size=batch_size, batch_index=args.batch_index)
