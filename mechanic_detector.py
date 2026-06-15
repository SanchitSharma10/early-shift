"""Mechanic spike detector for Early Shift.
Combines CCU growth data with creator video chatter to surface
potentially viral mechanics for Roblox studios."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List

from rapidfuzz import fuzz

try:
    from notifications import NotificationManager, SpikeAlert
    NOTIFICATIONS_AVAILABLE = True
except ImportError:
    NOTIFICATIONS_AVAILABLE = False

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
    mechanic_category: str  # Categorized mechanic type
    video_title: str
    video_url: str
    channel_title: str
    detected_at: datetime
    # New fields for confidence and causality
    confidence_score: float | None = None  # 0.0 to 1.0
    causality_type: str = "unclear"  # 'video_driven', 'update_driven', 'unclear'
    video_count: int = 1
    channel_tier: str = "small"  # 'mega', 'large', 'medium', 'small', 'micro'
    trend_phase: str = "unknown"  # 'emerging', 'growing', 'saturated', 'unknown'


# Channel tier thresholds (subscriber counts)
CHANNEL_TIERS = {
    "mega": 10_000_000,   # 10M+ subs: 3.0x weight
    "large": 1_000_000,   # 1M+ subs: 2.0x weight
    "medium": 100_000,    # 100K+ subs: 1.5x weight
    "small": 10_000,      # 10K+ subs: 1.0x weight
    "micro": 0,           # <10K subs: 0.5x weight
}

CHANNEL_TIER_WEIGHTS = {
    "mega": 3.0,
    "large": 2.0,
    "medium": 1.5,
    "small": 1.0,
    "micro": 0.5,
}


def get_channel_tier(subscriber_count: int | None) -> str:
    """Determine channel tier based on subscriber count."""
    if subscriber_count is None or subscriber_count < 10_000:
        return "micro"
    elif subscriber_count < 100_000:
        return "small"
    elif subscriber_count < 1_000_000:
        return "medium"
    elif subscriber_count < 10_000_000:
        return "large"
    else:
        return "mega"


def get_external_signal_boost(game_name: str, db, lookback_hours: int = 48) -> float:
    """
    Check external signals for additional confidence boost.
    
    Looks for Google Trends, RSS, or social signals matching the game.
    
    Returns:
        Boost multiplier (0.0 to 0.2) to add to confidence
    """
    try:
        from constants import Tables
        
        # Check for recent external signals mentioning this game
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        
        # Normalize game name for matching
        clean_name = _clean_game_name(game_name).lower()
        
        result = db.execute(f"""
            SELECT COUNT(*) as signal_count,
                   AVG(signal_value) as avg_signal
            FROM {Tables.EXTERNAL_SIGNALS}
            WHERE collected_at >= ?
              AND (
                  LOWER(signal_text) LIKE ?
                  OR LOWER(keyword) LIKE ?
                  OR LOWER(context) LIKE ?
              )
        """, (
            cutoff,
            f"%{clean_name}%",
            f"%{clean_name}%",
            f"%{clean_name}%",
        )).fetchone()
        
        if result and result[0] > 0:
            signal_count = result[0]
            # Boost: up to 0.2 for 5+ external signals
            boost = min(signal_count / 5.0 * 0.1, 0.2)
            return round(boost, 3)
        
    except Exception as e:
        logger.debug(f"External signal check failed: {e}")
    
    return 0.0


def calculate_confidence_score(
    growth_percent: float,
    video_count: int,
    channel_tier: str,
    has_mechanic_match: bool = True,
    external_signal_boost: float = 0.0,
) -> float:
    """
    Calculate confidence score for a spike detection.
    
    Formula: (growth_score × 0.30) + (video_score × 0.30) + (channel_score × 0.20) + (mechanic_score × 0.10) + external_boost
    
    Returns:
        Float between 0.0 and 1.0
    """
    # Growth score: cap at 100% growth = 1.0
    growth_score = min(growth_percent / 100.0, 1.0)
    
    # Video score: 10+ videos = 1.0
    video_score = min(video_count / 10.0, 1.0)
    
    # Channel score: based on tier weight normalized
    channel_weight = CHANNEL_TIER_WEIGHTS.get(channel_tier, 1.0)
    channel_score = channel_weight / 3.0  # Normalize to 0-1 (max tier weight is 3.0)
    
    # Mechanic score: 1.0 if specific mechanic extracted, 0.5 if generic
    mechanic_score = 1.0 if has_mechanic_match else 0.5
    
    confidence = (
        growth_score * 0.30 +
        video_score * 0.30 +
        channel_score * 0.20 +
        mechanic_score * 0.10 +
        external_signal_boost  # Up to 0.2 additional boost
    )
    
    return round(min(confidence, 1.0), 3)


def _normalize_datetime(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware in UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def determine_causality(
    spike_detected_at: datetime,
    video_published_times: List[datetime],
    spike_window_hours: int = 4,
) -> str:
    """
    Determine if videos likely caused the spike or reacted to it.
    
    Args:
        spike_detected_at: When the CCU spike was detected
        video_published_times: List of video publication timestamps
        spike_window_hours: Hours before spike to consider as "causal"
    
    Returns:
        'video_driven' if videos preceded spike
        'update_driven' if videos followed spike (reaction)
        'unclear' if mixed or insufficient data
    """
    if not video_published_times:
        return "unclear"
    
    # Normalize all datetimes to UTC-aware
    spike_detected_at = _normalize_datetime(spike_detected_at)
    video_published_times = [_normalize_datetime(t) for t in video_published_times]
    
    spike_threshold = spike_detected_at - timedelta(hours=spike_window_hours)
    
    videos_before = sum(1 for t in video_published_times if t < spike_threshold)
    videos_after = sum(1 for t in video_published_times if t >= spike_threshold)
    
    total = len(video_published_times)
    
    if videos_before > videos_after and videos_before >= total * 0.6:
        return "video_driven"
    elif videos_after > videos_before and videos_after >= total * 0.6:
        return "update_driven"
    else:
        return "unclear"


def classify_trend_phase(
    video_count: int,
    earliest_video_at: datetime | None,
    detected_at: datetime,
) -> str:
    """
    Classify trend phase from mention volume + age.

    Rules:
      - emerging: <=5 videos and <24h old
      - growing: <=20 videos and <48h old
      - saturated: >20 videos or >=48h old
    """
    if earliest_video_at is None:
        return "unknown"

    detected_at = _normalize_datetime(detected_at)
    earliest_video_at = _normalize_datetime(earliest_video_at)
    age_hours = max((detected_at - earliest_video_at).total_seconds() / 3600.0, 0.0)

    if video_count <= 5 and age_hours < 24:
        return "emerging"
    if video_count <= 20 and age_hours < 48:
        return "growing"
    return "saturated"


MECHANIC_PATTERNS = [
    re.compile(
        r"(?i)\b(?:new|secret|update|introducing|added|unlock(?:ing)?|mechanic|feature|quest|code|rework|revamp|event|season|act|chapter|episode|boss|mode|map|weapon|pet|fusion|merge|system)\b[:\-\s]*(.*)"
    ),
    re.compile(
        r"(?i)\b(?:v\d+|version\s*\d+)\b[:\-\s]*(.*)"
    ),
]

# Specific mechanic extraction patterns for better accuracy
SPECIFIC_MECHANIC_PATTERNS = [
    re.compile(r"(?i)(fusion|merge)\s+(\w+(?:\s+\w+)?)(?:\s+(?:system|update|event))?"),
    re.compile(r"(?i)(\w+)\s+(event|update|season)\s*[\:\-]?\s*(.*)"),
    re.compile(r"(?i)new\s+(\w+(?:\s+\w+)?)\s+(code|pet|boss|weapon|map|mode)"),
    re.compile(r"(?i)(christmas|winter|halloween|easter|summer)\s+(event|update)"),
    re.compile(r"(?i)(\w+)\s+(rework|revamp|buff|nerf)"),
]

MECHANIC_STOPWORDS = {
    "roblox",
    "update",
    "new",
    "secret",
    "free",
    "items",
    "item",
    "guide",
    "tutorial",
    "official",
    "trailer",
    "leaks",
    "leak",
    "patch",
    "sneak",
    "peek",
    "the",
    "a",
    "an",
    "to",
    "of",
    "for",
    "with",
    "and",
    "or",
    "vs",
    "in",
    "on",
    "is",
    "are",
}

# Mechanic categories for cross-game pattern detection
MECHANIC_CATEGORIES = {
    "pet_system": ["pet", "pets", "hatch", "egg", "breed", "evolve", "shiny", "mythic", "legendary pet"],
    "fusion_merge": ["fusion", "merge", "combine", "fuse", "craft", "crafting"],
    "tycoon": ["tycoon", "factory", "business", "money", "cash", "millionaire", "billionaire"],
    "obby": ["obby", "obstacle", "parkour", "tower of", "climb", "escape"],
    "simulator": ["simulator", "sim", "grind", "rebirth", "prestige", "reborn"],
    "tower_defense": ["tower defense", "td", "defend", "waves", "turret", "towers"],
    "battle_pvp": ["battle", "pvp", "fight", "arena", "deathmatch", "vs", "1v1"],
    "story_quest": ["quest", "story", "chapter", "mission", "campaign", "lore"],
    "gacha_luck": ["gacha", "spin", "lucky", "chance", "rare", "luck", "roll", "crate"],
    "codes_rewards": ["code", "codes", "redeem", "free", "reward", "giveaway"],
    "seasonal_event": ["christmas", "xmas", "winter", "halloween", "event", "seasonal", "holiday", "easter", "summer"],
    "trading": ["trade", "trading", "value", "worth", "demand", "limited"],
    "speed_racing": ["speed", "race", "racing", "fast", "car", "vehicle"],
    "horror": ["horror", "scary", "ghost", "monster", "survive", "night"],
    "roleplay": ["roleplay", "rp", "life", "school", "hospital", "city"],
    "brainrot_meme": ["skibidi", "sigma", "aura", "ohio", "grimace", "mewing", "gyatt", "rizz"],
}


def categorize_mechanic(text: str) -> str:
    """Categorize extracted mechanic into a known type.
    
    Returns:
        Category name or 'other' if no match found.
    """
    if not text:
        return "other"
    
    text_lower = text.lower()
    
    # Check each category
    for category, keywords in MECHANIC_CATEGORIES.items():
        for keyword in keywords:
            if keyword in text_lower:
                return category
    
    return "other"


# Common event prefixes to strip (case-insensitive)
EVENT_PREFIXES = [
    r'xmas\s*event',
    r'christmas\s*event', 
    r'winter\s*update',
    r'holiday\s*event',
    r'new\s*update',
    r'update\s*\d*',
    r'event',
]

def _clean_game_name(name: str) -> str:
    """Clean game name for better fuzzy matching - remove emojis, brackets, event prefixes."""
    # Remove all bracketed content like [UPDATE], [🎁DAY 4], etc.
    clean = re.sub(r'\[.*?\]', '', name)
    # Remove emojis and special characters (keep only alphanumeric, spaces, hyphens, apostrophes)
    clean = re.sub(r'[^\w\s\-\'"]', '', clean)
    # Remove common event prefixes
    for prefix in EVENT_PREFIXES:
        clean = re.sub(rf'^{prefix}\s*', '', clean, flags=re.IGNORECASE)
    # Clean up whitespace
    clean = ' '.join(clean.split()).strip()
    return clean if clean else name  # Fall back to original if cleaning removes everything


def _strip_game_name(text: str, game_name: str | None) -> str:
    if not text or not game_name:
        return text
    clean_name = _clean_game_name(game_name)
    if not clean_name:
        return text
    return re.sub(re.escape(clean_name), "", text, flags=re.IGNORECASE).strip()


def _clean_phrase(text: str) -> str:
    cleaned = re.sub(r"[\[\]\(\)\{\}]", " ", text)
    cleaned = re.split(r"[|•—\-]", cleaned)[0]
    cleaned = cleaned.replace(":", " ")
    cleaned = " ".join(cleaned.split()).strip()
    if not cleaned:
        return cleaned
    words = [w for w in re.split(r"\s+", cleaned) if w]
    filtered = [w for w in words if w.lower() not in MECHANIC_STOPWORDS]
    if filtered:
        cleaned = " ".join(filtered)
    return cleaned.strip(" .!?\"'")[:120]


def _extract_mechanic(
    title: str,
    description: str | None = None,
    game_name: str | None = None,
) -> tuple[str, bool]:
    """
    Extract mechanic from video title/description.
    
    Returns:
        Tuple of (mechanic_string, is_specific_match)
        - mechanic_string: Extracted mechanic text
        - is_specific_match: True if matched a specific pattern (higher confidence)
    """
    title = title or ""
    description = description or ""

    sources = [title, description[:200] if description else ""]
    
    # First try specific patterns for better accuracy
    for source in sources:
        if not source:
            continue
        source_stripped = _strip_game_name(source, game_name)
        for pattern in SPECIFIC_MECHANIC_PATTERNS:
            match = pattern.search(source_stripped)
            if match:
                # Join all non-None groups
                groups = [g for g in match.groups() if g]
                if groups:
                    candidate = _clean_phrase(" ".join(groups))
                    if candidate:
                        return candidate, True  # Specific match
    
    # Fall back to general patterns
    for source in sources:
        if not source:
            continue
        source_stripped = _strip_game_name(source, game_name)
        for pattern in MECHANIC_PATTERNS:
            match = pattern.search(source_stripped)
            if match:
                candidate = _clean_phrase(match.group(1))
                if candidate:
                    return candidate, False  # Generic match

    fallback = _clean_phrase(_strip_game_name(title, game_name))
    return (fallback if fallback else title[:120], False)


def _persist_spikes(db, spikes: List[MechanicSpike]) -> None:
    """Save detected spikes to database with confidence and causality data."""
    if not spikes:
        return
    
    SchemaManager._ensure_mechanic_spikes_table(db)
    
    # Ensure all new columns exist
    _ensure_category_column(db)
    _ensure_confidence_columns(db)
    
    for spike in spikes:
        db.execute(f"""
            INSERT INTO {Tables.MECHANIC_SPIKES} (
                universe_id, game_name, current_ccu, week_ago_ccu,
                growth_percent, published_at, mechanic, mechanic_category,
                video_title, video_url, channel_title, detected_at,
                confidence_score, causality_type, video_count, channel_tier, trend_phase
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            spike.universe_id,
            spike.game_name,
            spike.current_ccu,
            spike.week_ago_ccu,
            spike.growth_percent,
            spike.published_at,
            spike.mechanic,
            spike.mechanic_category,
            spike.video_title,
            spike.video_url,
            spike.channel_title,
            spike.detected_at,
            spike.confidence_score,
            spike.causality_type,
            spike.video_count,
            spike.channel_tier,
            spike.trend_phase,
        ))
    db.commit()


def _ensure_confidence_columns(db) -> None:
    """Add confidence and causality columns if they don't exist."""
    columns = {
        row[1]
        for row in db.execute(
            f"PRAGMA table_info('{Tables.MECHANIC_SPIKES}')"
        ).fetchall()
    }
    
    if "confidence_score" not in columns:
        db.execute(f"""
            ALTER TABLE {Tables.MECHANIC_SPIKES}
            ADD COLUMN confidence_score DOUBLE
        """)
        logger.info("Added confidence_score column to mechanic_spikes table")
    
    if "causality_type" not in columns:
        db.execute(f"""
            ALTER TABLE {Tables.MECHANIC_SPIKES}
            ADD COLUMN causality_type VARCHAR(20) DEFAULT 'unclear'
        """)
        logger.info("Added causality_type column to mechanic_spikes table")
    
    if "video_count" not in columns:
        db.execute(f"""
            ALTER TABLE {Tables.MECHANIC_SPIKES}
            ADD COLUMN video_count INTEGER DEFAULT 1
        """)
        logger.info("Added video_count column to mechanic_spikes table")
    
    if "channel_tier" not in columns:
        db.execute(f"""
            ALTER TABLE {Tables.MECHANIC_SPIKES}
            ADD COLUMN channel_tier VARCHAR(20) DEFAULT 'small'
        """)
        logger.info("Added channel_tier column to mechanic_spikes table")

    if "trend_phase" not in columns:
        db.execute(f"""
            ALTER TABLE {Tables.MECHANIC_SPIKES}
            ADD COLUMN trend_phase VARCHAR(20) DEFAULT 'unknown'
        """)
        logger.info("Added trend_phase column to mechanic_spikes table")
    
    db.commit()


def _ensure_category_column(db) -> None:
    """Add mechanic_category column if it doesn't exist."""
    columns = {
        row[1]
        for row in db.execute(
            f"PRAGMA table_info('{Tables.MECHANIC_SPIKES}')"
        ).fetchall()
    }
    if "mechanic_category" not in columns:
        db.execute(f"""
            ALTER TABLE {Tables.MECHANIC_SPIKES}
            ADD COLUMN mechanic_category VARCHAR(50) DEFAULT 'other'
        """)
        db.commit()
        logger.info("Added mechanic_category column to mechanic_spikes table")


def get_historical_spikes(
    db_path: str = DEFAULT_DB_PATH,
    limit: int = 50
) -> List[MechanicSpike]:
    """Retrieve historical spikes from database."""
    with get_db_connection(db_path, read_only=True) as db:
        table_exists = db.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE lower(table_name) = lower(?)
            """,
            [Tables.MECHANIC_SPIKES],
        ).fetchone()[0]
        if not table_exists:
            return []
        
        # Check if mechanic_category column exists
        columns = {
            row[1]
            for row in db.execute(
                f"PRAGMA table_info('{Tables.MECHANIC_SPIKES}')"
            ).fetchall()
        }
        has_category = "mechanic_category" in columns
        has_confidence = "confidence_score" in columns
        has_causality = "causality_type" in columns
        has_video_count = "video_count" in columns
        has_channel_tier = "channel_tier" in columns
        has_trend_phase = "trend_phase" in columns
        
        if has_category:
            select_category = "mechanic_category"
        else:
            select_category = "'other' as mechanic_category"
        
        # Build select for optional confidence columns
        select_confidence = "confidence_score" if has_confidence else "NULL as confidence_score"
        select_causality = "causality_type" if has_causality else "'unclear' as causality_type"
        select_video_count = "video_count" if has_video_count else "1 as video_count"
        select_channel_tier = "channel_tier" if has_channel_tier else "'small' as channel_tier"
        select_trend_phase = "trend_phase" if has_trend_phase else "'unknown' as trend_phase"

        rows = db.execute(f"""
            SELECT universe_id, game_name, current_ccu, week_ago_ccu,
                   growth_percent, published_at, mechanic, {select_category},
                   video_title, video_url, channel_title, detected_at,
                   {select_confidence}, {select_causality}, {select_video_count}, {select_channel_tier}, {select_trend_phase}
            FROM {Tables.MECHANIC_SPIKES}
            ORDER BY COALESCE(confidence_score, 0) DESC, detected_at DESC
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
                mechanic_category=row[7],
                video_title=row[8],
                video_url=row[9],
                channel_title=row[10],
                detected_at=row[11],
                confidence_score=row[12],
                causality_type=row[13] or "unclear",
                video_count=row[14] or 1,
                channel_tier=row[15] or "small",
                trend_phase=row[16] or "unknown",
            )
            for row in rows
        ]


def _get_cached_channel_tier(channel_id: str | None, db) -> str:
    """Get channel tier from cache, fall back to 'small'."""
    if not channel_id:
        return "small"
    
    try:
        result = db.execute("""
            SELECT tier FROM channel_tiers WHERE channel_id = ?
        """, (channel_id,)).fetchone()
        
        return result[0] if result else "small"
    except:
        return "small"


def _fetch_recent_videos(db, since: datetime) -> List[dict]:
    """Fetch recent videos from database with channel tier info."""
    columns = {
        row[1]
        for row in db.execute(
            f"PRAGMA table_info('{Tables.YOUTUBE_VIDEOS}')"
        ).fetchall()
    }
    if "description" in columns:
        select_description = "description"
    else:
        select_description = "NULL as description"

    query = f"""
        SELECT video_id,
               channel_id,
               channel_title,
               title,
               {select_description},
               published_at,
               view_count
        FROM {Tables.YOUTUBE_VIDEOS}
        WHERE published_at >= ?
    """
    
    videos = []
    for row in db.execute(query, (since,)).fetchall():
        if _is_spam_video(row[3]):
            continue
        channel_id = row[1]
        # Get cached tier for this channel
        channel_tier = _get_cached_channel_tier(channel_id, db)
        
        videos.append({
            "video_id": row[0],
            "channel_id": channel_id,
            "channel_title": row[2],
            "title": row[3],
            "description": row[4],
            "published_at": row[5],
            "view_count": row[6],
            "channel_tier": channel_tier,
        })
    
    return videos


SPAM_TITLE_RE = re.compile(
    r"\b(scripts?|pastebin|exploits?|hacks?|cheats?|aimbot|auto\s*farms?|"
    r"free\s+robux|cod(?:es?)|c[oó]digos?)\b",
    re.IGNORECASE,
)


def _is_spam_video(title: str | None) -> bool:
    """Codes/script/exploit listing videos chase already-popular games and add attribution noise."""
    return bool(title and SPAM_TITLE_RE.search(title))


def _video_matches_game(game_name: str, video_title: str) -> bool:
    # Clean game name for better matching (removes emojis, brackets, etc.)
    clean_name = _clean_game_name(game_name).lower()
    video_lower = video_title.lower()
    
    score = fuzz.partial_ratio(clean_name, video_lower)
    if score >= FUZZ_THRESHOLD:
        return True
    
    # Also try matching on individual significant words from the game name
    # e.g., "Tower Defense Simulator" should match on "Tower Defense"
    words = clean_name.split()
    if len(words) >= 2:
        # Try first two words as a phrase
        phrase = ' '.join(words[:2])
        if phrase in video_lower:
            return True
    
    for keyword in KEYWORD_HINTS:
        if keyword in video_lower and clean_name in video_lower:
            return True
    return False


async def _send_spike_notifications(spikes: List[MechanicSpike]) -> None:
    """Send notifications for detected spikes."""
    if not NOTIFICATIONS_AVAILABLE:
        logger.debug("Notifications module not available")
        return
    
    manager = NotificationManager()
    
    # Group spikes by game to avoid spam
    games_notified = set()
    
    for spike in spikes:
        if spike.game_name in games_notified:
            continue
        
        # Count videos for this game
        video_count = sum(1 for s in spikes if s.game_name == spike.game_name)
        
        alert = SpikeAlert(
            game_name=spike.game_name,
            current_ccu=spike.current_ccu,
            growth_percent=spike.growth_percent,
            video_count=video_count,
            top_channel=spike.channel_title,
            top_video_url=spike.video_url,
            detected_at=spike.detected_at
        )
        
        try:
            results = await manager.send_spike_alert(alert)
            if any(v for v in results.values() if v):
                logger.info(f"Sent notification for {spike.game_name}")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
        
        games_notified.add(spike.game_name)


def detect_mechanic_spikes(
    db_path: str = DEFAULT_DB_PATH,
    lookback_hours: int = MENTION_LOOKBACK_HOURS,
    growth_threshold: float = GROWTH_THRESHOLD,
    persist: bool = True,
    notify: bool = True,
) -> List[MechanicSpike]:
    """
    Detect mechanic spikes by correlating CCU growth with YouTube mentions.
    
    Args:
        db_path: Path to the database
        lookback_hours: Hours to look back for video mentions
        growth_threshold: Minimum growth rate threshold (as decimal)
        persist: Whether to persist results to database
        
    Returns:
        List of detected MechanicSpike objects with confidence scores and causality
    """
    with get_db_connection(db_path) as db:
        candidates = get_growth_candidates(db, growth_threshold)
        if not candidates:
            return []

        since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        videos = _fetch_recent_videos(db, since)
        
        # Group spikes by game for aggregation
        game_spikes: Dict[int, List[dict]] = {}
        detected_at = datetime.now(timezone.utc)

        for candidate in candidates:
            for video in videos:
                title = video["title"] or ""
                if not title:
                    continue
                if not _video_matches_game(candidate.game_name, title):
                    continue
                
                mechanic, is_specific = _extract_mechanic(
                    title,
                    description=video.get("description"),
                    game_name=candidate.game_name,
                )
                # Also categorize based on full context (title + game name)
                category = categorize_mechanic(f"{title} {candidate.game_name}")
                
                # Get channel tier from video (pre-fetched from cache)
                channel_tier = video.get("channel_tier", "small")
                
                spike_data = {
                    "universe_id": candidate.universe_id,
                    "game_name": candidate.game_name,
                    "current_ccu": candidate.current_ccu,
                    "week_ago_ccu": candidate.week_ago_ccu,
                    "growth_percent": candidate.growth_percent,
                    "published_at": video["published_at"],
                    "mechanic": mechanic,
                    "mechanic_category": category,
                    "video_title": title,
                    "video_url": f"https://youtube.com/watch?v={video['video_id']}",
                    "channel_title": video["channel_title"],
                    "channel_tier": channel_tier,
                    "is_specific_match": is_specific,
                }
                
                if candidate.universe_id not in game_spikes:
                    game_spikes[candidate.universe_id] = []
                game_spikes[candidate.universe_id].append(spike_data)
        
        # Create aggregated spikes with confidence and causality
        spikes: List[MechanicSpike] = []
        
        for universe_id, spike_list in game_spikes.items():
            # Aggregate by game - take the best spike (highest tier channel)
            best_spike = max(spike_list, key=lambda s: CHANNEL_TIER_WEIGHTS.get(s["channel_tier"], 1.0))
            
            # Calculate video count and video times for causality
            video_count = len(spike_list)
            video_times = [s["published_at"] for s in spike_list]
            earliest_video_time = min(video_times) if video_times else None
            
            # Determine best channel tier across all videos
            best_tier = max(
                [s["channel_tier"] for s in spike_list],
                key=lambda t: CHANNEL_TIER_WEIGHTS.get(t, 1.0)
            )
            
            # Check if any video had a specific mechanic match
            has_specific_match = any(s["is_specific_match"] for s in spike_list)
            
            # Get external signal boost
            external_boost = get_external_signal_boost(best_spike["game_name"], db)
            
            # Calculate confidence score with external signal boost
            confidence = calculate_confidence_score(
                growth_percent=best_spike["growth_percent"],
                video_count=video_count,
                channel_tier=best_tier,
                has_mechanic_match=has_specific_match,
                external_signal_boost=external_boost,
            )
            
            # Determine causality
            causality = determine_causality(
                spike_detected_at=detected_at,
                video_published_times=video_times,
            )

            trend_phase = classify_trend_phase(
                video_count=video_count,
                earliest_video_at=earliest_video_time,
                detected_at=detected_at,
            )
            
            spikes.append(
                MechanicSpike(
                    universe_id=best_spike["universe_id"],
                    game_name=best_spike["game_name"],
                    current_ccu=best_spike["current_ccu"],
                    week_ago_ccu=best_spike["week_ago_ccu"],
                    growth_percent=best_spike["growth_percent"],
                    published_at=best_spike["published_at"],
                    mechanic=best_spike["mechanic"],
                    mechanic_category=best_spike["mechanic_category"],
                    video_title=best_spike["video_title"],
                    video_url=best_spike["video_url"],
                    channel_title=best_spike["channel_title"],
                    detected_at=detected_at,
                    confidence_score=confidence,
                    causality_type=causality,
                    video_count=video_count,
                    channel_tier=best_tier,
                    trend_phase=trend_phase,
                )
            )
        
        # Sort by confidence score (highest first)
        spikes.sort(key=lambda s: s.confidence_score or 0, reverse=True)
        
        if persist and spikes:
            _persist_spikes(db, spikes)
        
        # Note: Notifications are handled by main.py's send_discord_alerts()
        # This avoids asyncio.run() conflicts when called from async context
        
        return spikes


def format_spikes_table(spikes: Iterable[MechanicSpike], include_confidence: bool = True) -> str:
    spikes = list(spikes)
    if not spikes:
        return "No mechanic spikes detected in the selected window."

    if include_confidence:
        headers = [
            "Game",
            "Growth",
            "CCU",
            "Confidence",
            "Causality",
            "Videos",
            "Phase",
            "Category",
            "Mechanic",
        ]
    else:
        headers = [
            "Game",
            "Growth",
            "CCU",
            "Category",
            "Mechanic",
            "Published",
        ]
    
    rows = [
        headers,
        ["-" * len(h) for h in headers],
    ]
    for spike in spikes:
        if include_confidence:
            # Confidence emoji indicator
            conf = spike.confidence_score or 0
            conf_display = f"{conf:.0%}"
            if conf >= 0.7:
                conf_emoji = "🔥"
            elif conf >= 0.5:
                conf_emoji = "⚡"
            else:
                conf_emoji = "📊"
            
            # Causality indicator
            causality_map = {
                "video_driven": "🎥→📈",
                "update_driven": "📝→🎥",
                "unclear": "❓",
            }
            causality_display = causality_map.get(spike.causality_type, "❓")
            
            rows.append(
                [
                    spike.game_name[:25],
                    f"{spike.growth_percent:.1f}%",
                    f"{spike.current_ccu:,}",
                    f"{conf_emoji} {conf_display}",
                    causality_display,
                    str(spike.video_count),
                    spike.trend_phase,
                    spike.mechanic_category[:12],
                    spike.mechanic[:30],
                ]
            )
        else:
            rows.append(
                [
                    spike.game_name[:30],
                    f"{spike.growth_percent:.1f}%",
                    f"{spike.current_ccu:,}",
                    spike.mechanic_category,
                    spike.mechanic[:40],
                    spike.published_at.strftime("%Y-%m-%d %H:%M"),
                ]
            )
    col_widths = [max(len(row[idx]) for row in rows) for idx in range(len(headers))]
    lines: List[str] = []
    for row in rows:
        padded = [cell.ljust(col_widths[idx]) for idx, cell in enumerate(row)]
        lines.append(" | ".join(padded))
    return "\n".join(lines)


def get_category_summary(db_path: str = DEFAULT_DB_PATH, days: int = 7) -> dict:
    """Get summary of mechanic categories over the past N days.
    
    Returns:
        Dict mapping category -> count of spikes
    """
    with get_db_connection(db_path, read_only=True) as db:
        _ensure_category_column(db)
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        rows = db.execute(f"""
            SELECT 
                COALESCE(mechanic_category, 'other') as category,
                COUNT(*) as count,
                COUNT(DISTINCT universe_id) as unique_games,
                AVG(growth_percent) as avg_growth
            FROM {Tables.MECHANIC_SPIKES}
            WHERE detected_at >= ?
            GROUP BY COALESCE(mechanic_category, 'other')
            ORDER BY count DESC
        """, (cutoff,)).fetchall()
        
        return {
            row[0]: {
                "count": row[1],
                "unique_games": row[2],
                "avg_growth": row[3]
            }
            for row in rows
        }


if __name__ == "__main__":
    spikes = detect_mechanic_spikes()
    logger.info(format_spikes_table(spikes))
    logger.info(f"{len(spikes)} spikes detected and persisted to database.")
