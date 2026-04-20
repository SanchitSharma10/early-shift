#!/usr/bin/env python3
"""
add_game.py - Add custom games for Early Shift tracking

Usage:
    python add_game.py <universe_id_or_url> [--name "Game Name"] [--owner "Owner"] [--contact "email@example.com"]
    
Examples:
    python add_game.py 123456789
    python add_game.py https://www.roblox.com/games/123456789/My-Game
    python add_game.py 123456789 --name "My Cool Game" --owner "John" --contact "john@studio.com"
    python add_game.py --list  # Show all custom games
    python add_game.py --remove 123456789  # Remove a game
"""

import argparse
import asyncio
import re
import sys
from datetime import datetime

import aiohttp
import duckdb

from constants import (
    DEFAULT_DB_PATH,
    ROBLOX_GAMES_ENDPOINT,
    ROBLOX_PLACE_TO_UNIVERSE_ENDPOINT,
    Tables,
)
from schema import SchemaManager

# Windows console encoding fix
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def extract_universe_id(input_str: str) -> int | None:
    """Extract universe ID from URL or direct input."""
    # Direct numeric input
    if input_str.isdigit():
        return int(input_str)
    
    # URL pattern: /games/PLACE_ID/... or universes/UNIVERSE_ID
    place_match = re.search(r'roblox\.com/games/(\d+)', input_str)
    if place_match:
        # This is a place ID, we need to convert to universe ID
        return int(place_match.group(1))  # We'll handle conversion later
    
    universe_match = re.search(r'universes?/(\d+)', input_str)
    if universe_match:
        return int(universe_match.group(1))
    
    return None


async def get_universe_from_place(place_id: int) -> dict | None:
    """Convert place ID to universe ID and get game info."""
    url = ROBLOX_PLACE_TO_UNIVERSE_ENDPOINT.format(place_id=place_id)
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    universe_id = data.get("universeId")
                    if universe_id:
                        # Now get the game details
                        return await get_game_info(universe_id)
        except Exception as e:
            print(f"Error converting place ID: {e}")
    return None


async def get_game_info(universe_id: int, retries: int = 3) -> dict | None:
    """Fetch game info from Roblox API."""
    url = f"{ROBLOX_GAMES_ENDPOINT}?universeIds={universe_id}"
    
    for attempt in range(retries):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        games = data.get("data", [])
                        if games:
                            game = games[0]
                            return {
                                "universe_id": universe_id,
                                "name": game.get("name", "Unknown"),
                                "playing": game.get("playing", 0),
                                "visits": game.get("visits", 0),
                                "creator": game.get("creator", {}).get("name", "Unknown"),
                            }
                    elif resp.status == 429:
                        # Rate limited - wait and retry
                        if attempt < retries - 1:
                            await asyncio.sleep(2 * (attempt + 1))
                            continue
            except Exception as e:
                print(f"Error fetching game info (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(1)
    return None


def add_custom_game(
    db: duckdb.DuckDBPyConnection,
    universe_id: int,
    game_name: str,
    owner_name: str = None,
    owner_contact: str = None,
    webhook_url: str = None,
    notes: str = None,
) -> bool:
    """Add a custom game to tracking."""
    try:
        # Check if already exists
        existing = db.execute(
            f"SELECT universe_id, game_name FROM {Tables.CUSTOM_GAMES} WHERE universe_id = ?",
            [universe_id]
        ).fetchone()
        
        if existing:
            print(f"⚠️  Game already tracked: {existing[1]} (ID: {existing[0]})")
            # Reactivate if inactive
            db.execute(
                f"UPDATE {Tables.CUSTOM_GAMES} SET is_active = TRUE WHERE universe_id = ?",
                [universe_id]
            )
            db.commit()
            return False
        
        db.execute(
            f"""
            INSERT INTO {Tables.CUSTOM_GAMES} 
            (universe_id, game_name, owner_name, owner_contact, webhook_url, notes, is_active, added_at)
            VALUES (?, ?, ?, ?, ?, ?, TRUE, ?)
            """,
            [universe_id, game_name, owner_name, owner_contact, webhook_url, notes, datetime.utcnow()]
        )
        db.commit()
        return True
    except Exception as e:
        print(f"Error adding game: {e}")
        return False


def list_custom_games(db: duckdb.DuckDBPyConnection) -> None:
    """List all custom tracked games."""
    games = db.execute(
        f"""
        SELECT universe_id, game_name, owner_name, is_active, added_at 
        FROM {Tables.CUSTOM_GAMES}
        ORDER BY added_at DESC
        """
    ).fetchall()
    
    if not games:
        print("No custom games being tracked.")
        return
    
    print("\n📊 Custom Tracked Games:")
    print("-" * 80)
    print(f"{'ID':<12} {'Game Name':<30} {'Owner':<15} {'Active':<8} {'Added'}")
    print("-" * 80)
    
    for game in games:
        status = "✅" if game[3] else "❌"
        owner = game[2] or "-"
        added = game[4].strftime("%Y-%m-%d") if game[4] else "-"
        print(f"{game[0]:<12} {game[1][:28]:<30} {owner[:13]:<15} {status:<8} {added}")
    
    print(f"\nTotal: {len(games)} games")


def remove_custom_game(db: duckdb.DuckDBPyConnection, universe_id: int) -> bool:
    """Remove (deactivate) a custom game."""
    # Check if it exists first
    existing = db.execute(
        f"SELECT universe_id FROM {Tables.CUSTOM_GAMES} WHERE universe_id = ?",
        [universe_id]
    ).fetchone()
    
    if not existing:
        return False
    
    db.execute(
        f"UPDATE {Tables.CUSTOM_GAMES} SET is_active = FALSE WHERE universe_id = ?",
        [universe_id]
    )
    db.commit()
    return True


async def main():
    parser = argparse.ArgumentParser(description="Add custom games to Early Shift tracking")
    parser.add_argument("game", nargs="?", help="Universe ID or Roblox game URL")
    parser.add_argument("--name", help="Game name (auto-detected if not provided)")
    parser.add_argument("--owner", help="Owner/studio name")
    parser.add_argument("--contact", help="Contact email or Discord")
    parser.add_argument("--webhook", help="Webhook URL for notifications")
    parser.add_argument("--notes", help="Additional notes")
    parser.add_argument("--list", action="store_true", help="List all custom games")
    parser.add_argument("--remove", type=int, help="Remove a game by universe ID")
    
    args = parser.parse_args()
    
    # Connect to database (same location as main.py)
    db = duckdb.connect(DEFAULT_DB_PATH)
    SchemaManager.ensure_all_tables(db)
    
    # Handle list command
    if args.list:
        list_custom_games(db)
        db.close()
        return
    
    # Handle remove command
    if args.remove:
        if remove_custom_game(db, args.remove):
            print(f"✅ Removed game {args.remove} from tracking")
        else:
            print(f"❌ Game {args.remove} not found")
        db.close()
        return
    
    # Need a game ID for adding
    if not args.game:
        parser.print_help()
        db.close()
        return
    
    # Extract and validate universe ID
    raw_id = extract_universe_id(args.game)
    if not raw_id:
        print(f"❌ Could not parse game ID from: {args.game}")
        db.close()
        return
    
    print(f"🔍 Looking up game...")
    
    # Try to get game info - first assume it's a place ID (from URL)
    game_info = None
    if "roblox.com/games/" in args.game:
        # It's a place ID, need to convert
        game_info = await get_universe_from_place(raw_id)
    
    if not game_info:
        # Try as universe ID directly
        game_info = await get_game_info(raw_id)
    
    if not game_info:
        print(f"❌ Could not find game with ID: {raw_id}")
        print("   Make sure the game exists and is publicly accessible.")
        db.close()
        return
    
    universe_id = game_info["universe_id"]
    game_name = args.name or game_info["name"]
    
    print(f"\n📎 Game Found:")
    print(f"   Name: {game_name}")
    print(f"   Universe ID: {universe_id}")
    print(f"   Current Players: {game_info['playing']:,}")
    print(f"   Total Visits: {game_info['visits']:,}")
    print(f"   Creator: {game_info['creator']}")
    
    # Add to database
    if add_custom_game(
        db,
        universe_id=universe_id,
        game_name=game_name,
        owner_name=args.owner,
        owner_contact=args.contact,
        webhook_url=args.webhook,
        notes=args.notes,
    ):
        print(f"\n✅ Added '{game_name}' to Early Shift tracking!")
        print(f"   It will be monitored in the next polling cycle.")
    
    db.close()


if __name__ == "__main__":
    asyncio.run(main())
