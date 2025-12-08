"""
constants.py - Centralized constants for Early Shift
"""

from pathlib import Path

# Database
DEFAULT_DB_PATH = "early_shift.db"

# Table Names
class Tables:
    GAMES = "games"
    GAME_METADATA = "game_metadata"
    STUDIOS = "studios"
    YOUTUBE_VIDEOS = "youtube_videos"
    MECHANIC_SPIKES = "mechanic_spikes"
    CUSTOM_GAMES = "custom_games"

# Thresholds
GROWTH_THRESHOLD = 0.25  # 25% growth
MENTION_LOOKBACK_HOURS = 48
FUZZ_THRESHOLD = 82

# API Configuration
DEFAULT_CACHE_HOURS = 4
DEFAULT_CONCURRENCY = 12

# YouTube
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

# Intervals
MONITORING_INTERVAL_HOURS = 4
CACHE_TTL_HOURS = 4

# Paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
CHANNELS_FILE = DATA_DIR / "youtube_channels.json"

# API Endpoints
YOUTUBE_SEARCH_ENDPOINT = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEOS_ENDPOINT = "https://www.googleapis.com/youtube/v3/videos"
ROPROXY_BASE_URL = "https://games.roproxy.com/v1/games"
ROPROXY_DISCOVERY_ENDPOINTS = [
    "https://games.roproxy.com/v1/discovery/universes",
    "https://games.roblox.com/v1/discovery/universes",
]
NTFY_URL = "https://ntfy.sh"

# Fallback Data
FALLBACK_UNIVERSES = [
    994732206, 245662005, 383310974, 47545, 210851291, 4924922222, 185655149,
    10977891899, 5902977743, 8737602446, 1537690962, 1182249705, 142823291,
    1400147734, 920587237, 8149070699, 3351674303, 5569431581, 6381829480,
    4872321990, 4520749081, 13822889, 9498006165, 488667523, 286090429,
    3823781113, 447452406, 2010620636, 2013640567, 3291301470, 511316432,
    2216618303, 3145447021, 5926001758, 3192707582, 275420544, 263761432,
    92012076, 1377239466, 6284583030, 2988862959, 2583109575, 1540764883,
    5763726676, 3019923553, 5938036553
]

# Default YouTube Channels
DEFAULT_YOUTUBE_CHANNELS = [
    {"name": "Laughability", "id": "UCn4RkjqDN4UE0Uy3y1-K6_w"},
    {"name": "TanqR", "id": "UCquKkmifC6eDU-bbKxqjJgw"},
    {"name": "RussoPlays", "id": "UCEMOZ_fY37Eeu7vIAynxKUg"},
    {"name": "DigitoSIM", "id": "UCKr0RAl4snaKO0vspEJSvoA"},
    {"name": "TeraBrite Games", "id": "UChHJHjFVD3apDH6v5aQ8SfA"},
    {"name": "KreekCraft", "id": "UCxsk7hqE_CwZWGEJEkGanbA"},
    {"name": "Calvin Vu", "id": "UCQAfJ12iiKIyq6ulQGL-2-g"},
    {"name": "Conor3D", "id": "UCPnGatTXxMGFtys-uUBlNqA"},
    {"name": "LaughClip", "id": "UCCPJsdOITJlv7U7Zju1ltqA"},
    {"name": "ItzVortex", "id": "UCnHjN7aOxyVqYzijXvA9VQQ"},
    {"name": "DV Plays", "id": "UCt4FfINxDa0AMZ6pGr3BFdQ"},
    {"name": "DeeterPlays", "id": "UCEGGYLCdQdce1vA9qmuU4IA"},
    {"name": "iamSanna", "id": "UCzQMI4gtB50HUYAZIa2z-Hg"},
    {"name": "MeganPlays", "id": "UCwwSHAjJb3E0GqMucvUU-FQ"},
    {"name": "Glitch", "id": "UCn_FAXem2-e3HQvmK-mOH4g"},
    {"name": "BuildIntoGames", "id": "UCbplrphSrY6kRCj6tpY3v-w"},
    {"name": "Lonnie", "id": "UCetEdVo7Cq9qE8xAGLtvG0g"},
]