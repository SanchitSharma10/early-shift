"""
constants.py - Centralized constants for Early Shift
"""

from pathlib import Path

# Paths (define early so we can use PROJECT_ROOT for DB path)
PROJECT_ROOT = Path(__file__).parent

# Database - use absolute path to avoid CWD issues
DEFAULT_DB_PATH = str(PROJECT_ROOT / "early_shift.db")

# Table Names
class Tables:
    GAMES = "games"
    GAME_METADATA = "game_metadata"
    STUDIOS = "studios"
    YOUTUBE_VIDEOS = "youtube_videos"
    MECHANIC_SPIKES = "mechanic_spikes"
    CUSTOM_GAMES = "custom_games"
    ALERT_LOG = "alert_log"
    EXTERNAL_SIGNALS = "external_signals"
    GAME_KEYWORD_INDEX = "game_keyword_index"
    KEYWORD_PATTERNS = "keyword_patterns"

# Thresholds
GROWTH_THRESHOLD = 0.25  # 25% growth
ALERT_GROWTH_THRESHOLD = GROWTH_THRESHOLD  # Alert gating default
MENTION_LOOKBACK_HOURS = 48
FUZZ_THRESHOLD = 70  # Lowered from 82 to catch more matches

# Alerting
DEFAULT_ALERT_COOLDOWN_HOURS = 24

# API Configuration
DEFAULT_CACHE_HOURS = 4
DEFAULT_CONCURRENCY = 12
DEFAULT_ROPROXY_BATCH_DELAY_SECONDS = 2.5
DEFAULT_ROPROXY_BACKOFF_BASE_SECONDS = 5.0
DEFAULT_ROPROXY_BACKOFF_MAX_SECONDS = 30.0
DEFAULT_ROPROXY_MAX_RETRIES = 5

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

# YouTube polling
DEFAULT_YOUTUBE_CHANNEL_COOLDOWN_HOURS = 12
UPCOMING_CHANNEL_VIDEO_THRESHOLD = 5

# External signals
DEFAULT_EXTERNAL_SIGNALS_INTERVAL_HOURS = 6
DEFAULT_GOOGLE_TRENDS_GEO = "US"
DEFAULT_GOOGLE_TRENDS_TIMEFRAME = "now 7-d"

SOCIAL_CULTURE_KEYWORDS = [
    "brainrot",
    "skibidi",
    "sigma",
    "aura",
    "rizz",
    "gyat",
    "fanum tax",
    "mewing",
    "mogging",
    "grimace shake",
    "ohio meme",
    "npc",
    "sus",
    "goofy",
    "griddy",
]

EXTERNAL_SIGNAL_KEYWORDS = SOCIAL_CULTURE_KEYWORDS
DEFAULT_RSS_FEEDS = [
    {"name": "Roblox Blog", "url": "https://blog.roblox.com/feed/"},
    {"name": "DevForum", "url": "https://devforum.roblox.com/latest.rss"},
]

# Intervals
MONITORING_INTERVAL_HOURS = 4
CACHE_TTL_HOURS = 4
DEFAULT_TOP_UNIVERSE_LIMIT = 500

# Paths (already defined at top)
DATA_DIR = PROJECT_ROOT / "data"
CHANNELS_FILE = DATA_DIR / "youtube_channels.json"

# API Endpoints
YOUTUBE_SEARCH_ENDPOINT = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEOS_ENDPOINT = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_CHANNELS_ENDPOINT = "https://www.googleapis.com/youtube/v3/channels"
ROBLOX_GAMES_ENDPOINT = "https://games.roblox.com/v1/games"
ROBLOX_PLACE_TO_UNIVERSE_ENDPOINT = "https://apis.roblox.com/universes/v1/places/{place_id}/universe"
ROBLOX_DISCOVERY_ENDPOINTS = [
    "https://games.roblox.com/v1/discovery/universes",
]
ROBLOX_VOTES_ENDPOINT = "https://games.roblox.com/v1/games/votes"
NTFY_URL = "https://ntfy.sh"

# Compatibility aliases while the rest of the codebase still uses the older names.
ROPROXY_BASE_URL = ROBLOX_GAMES_ENDPOINT
ROPROXY_DISCOVERY_ENDPOINTS = ROBLOX_DISCOVERY_ENDPOINTS

# Fallback Data - 200+ universe IDs from database + Roblox Charts
# Updated Jan 2026 - Combined from existing tracking + popular games
FALLBACK_UNIVERSES = [
    # === TOP GAMES (Most Popular) ===
    994732206,   # Blox Fruits
    1686885941,  # Brookhaven RP
    383310974,   # Adopt Me!
    5750914919,  # Fisch
    5203828273,  # Dress To Impress
    6035872082,  # RIVALS
    3808081382,  # The Strongest Battlegrounds
    2440500124,  # DOORS
    4777817887,  # Blade Ball
    4730278139,  # untitled boxing game
    703124385,   # Tower of Hell
    245662005,   # Jailbreak
    111958650,   # Arsenal
    210851291,   # Build A Boat For Treasure
    47545,       # Work at a Pizza Place
    65241,       # Natural Disaster Survival
    
    # === FROM DATABASE (Tracked Games) ===
    13822889, 66654135, 73885730, 88070565, 92012076, 113491250, 142823291,
    185655149, 263761432, 275420544, 286090429, 321778215, 447452406, 488667523,
    511316432, 601130232, 648454481, 903807016, 920587237, 1008451066, 1054526971,
    1176784616, 1182249705, 1202096104, 1289954547, 1377239466, 1400147734,
    1511883870, 1516533665, 1537690962, 1540764883, 1720936166, 1831550657,
    2010620636, 2013640567, 2073329983, 2216618303, 2324662457, 2380077519,
    2583109575, 2619619496, 2668101271, 2711375305, 2988862959, 2992873140,
    3019923553, 3047037061, 3145447021, 3192707582, 3240075297, 3291301470,
    3317679266, 3351674303, 3508322461, 3647333358, 3823781113, 4053293514,
    4342047058, 4348829796, 4391829435, 4452297356, 4509896324, 4520749081,
    4568630521, 4658598196, 4839802560, 4872321990, 4924922222, 4949420752,
    5166944221, 5361032378, 5385674359, 5421899973, 5569032992, 5569431581,
    5578556129, 5763726676, 5829613337, 5902977743, 5926001758, 5938036553,
    5995470825, 6275120808, 6284583030, 6325068386, 6331902150, 6381829480,
    6701277882, 6739698191, 6931042565, 6945584306, 7008097940, 7018190066,
    7028566528, 7069852763, 7072674902, 7082423811, 7094518649, 7264587281,
    7326934954, 7344582593, 7436755782, 7640282930, 7658676858, 7671049560,
    7709344486, 7750955984, 7883776681, 7983308985, 8149070699, 8181391950,
    8316902627, 8377806270, 8507699752, 8520148363, 8539298853, 8620685718,
    8639693426, 8737602446, 8795154789, 8834587397, 8844400854, 8950496606,
    8964336233, 9029463012, 9145063979, 9181563985, 9498006165, 10977891899,
    
    # === ADDITIONAL POPULAR GAMES ===
    2753915549,  # Anime Defenders
    6284583030,  # King Legacy
    4922745621,  # Bedwars
    3527629287,  # Murder Mystery 2
    1537690962,  # Bee Swarm Simulator
    4490140733,  # Anime Adventures
    2474168535,  # Pet Simulator X
    5985232436,  # Shindo Life
    4872321990,  # Da Hood
    3213915814,  # Lumber Tycoon 2
    6596711860,  # All Star Tower Defense
    4615322269,  # Anime Fighting Simulator X
    5608623798,  # A Dusty Trip
    5017849855,  # MM2
    4693417007,  # Breaking Point
    5060552905,  # Arm Wrestle Simulator
    3956818381,  # Creatures of Sonaria
    2809202155,  # Piggy
    4616652839,  # Dragon Ball Z Final Stand
    3414853460,  # World // Zero
    5774715103,  # Evade
    2768379856,  # Deepwoken
    1962086868,  # My Restaurant
    6501436079,  # Combat Warriors
    4541429666,  # Super Golf
    4390380541,  # RoBeats
    2985834498,  # Funky Friday
    2534724415,  # Royale High
    6069955633,  # Gym League
    6193858302,  # Saitama Battlegrounds
    3589359498,  # Flicker
    4792368631,  # Pressure
    5608514991,  # Word Bomb
    5436387845,  # Obby But You're a Ball
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
