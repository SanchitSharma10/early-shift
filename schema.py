"""
schema.py - Centralized database schema management
"""

import duckdb

from constants import Tables


class SchemaManager:
    """Manages database schema creation and migrations."""
    
    @staticmethod
    def ensure_all_tables(db: duckdb.DuckDBPyConnection) -> None:
        """
        Ensure all required tables exist with the correct schema.
        
        Args:
            db: Database connection
        """
        SchemaManager._ensure_games_tables(db)
        SchemaManager._ensure_studios_table(db)
        SchemaManager._ensure_youtube_table(db)
        SchemaManager._ensure_mechanic_spikes_table(db)
        SchemaManager._ensure_custom_games_table(db)
        db.commit()
    
    @staticmethod
    def _ensure_games_tables(db: duckdb.DuckDBPyConnection) -> None:
        """Create games and game_metadata tables."""
        # Main games table for CCU snapshots
        db.execute(f"""
            CREATE TABLE IF NOT EXISTS {Tables.GAMES} (
                universe_id BIGINT,
                name TEXT,
                ccu INTEGER,
                timestamp TIMESTAMP,
                PRIMARY KEY (universe_id, timestamp)
            )
        """)
        
        # Handle legacy column name migration
        columns = {row[1] for row in db.execute(f"PRAGMA table_info('{Tables.GAMES}')").fetchall()}
        if "game_id" in columns and "universe_id" not in columns:
            db.execute(f"ALTER TABLE {Tables.GAMES} RENAME COLUMN game_id TO universe_id")
        
        # Ensure all columns exist
        db.execute(f"ALTER TABLE {Tables.GAMES} ADD COLUMN IF NOT EXISTS name TEXT")
        db.execute(f"ALTER TABLE {Tables.GAMES} ADD COLUMN IF NOT EXISTS ccu INTEGER")
        db.execute(f"ALTER TABLE {Tables.GAMES} ADD COLUMN IF NOT EXISTS timestamp TIMESTAMP")
        
        # Game metadata table
        db.execute(f"""
            CREATE TABLE IF NOT EXISTS {Tables.GAME_METADATA} (
                universe_id BIGINT PRIMARY KEY,
                name TEXT,
                root_place_id BIGINT,
                creator_id BIGINT,
                creator_name TEXT,
                description TEXT,
                genre TEXT,
                visits BIGINT,
                last_seen_ccu INTEGER,
                updated_at TIMESTAMP
            )
        """)
    
    @staticmethod
    def _ensure_studios_table(db: duckdb.DuckDBPyConnection) -> None:
        """Create studios table."""
        db.execute(f"""
            CREATE TABLE IF NOT EXISTS {Tables.STUDIOS} (
                studio_id TEXT PRIMARY KEY,
                name TEXT,
                notion_token TEXT,
                notion_database_id TEXT,
                ntfy_topic TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Backfill columns for older installations
        db.execute(f"ALTER TABLE {Tables.STUDIOS} ADD COLUMN IF NOT EXISTS notion_token TEXT")
        db.execute(f"ALTER TABLE {Tables.STUDIOS} ADD COLUMN IF NOT EXISTS notion_database_id TEXT")
        db.execute(f"ALTER TABLE {Tables.STUDIOS} ADD COLUMN IF NOT EXISTS ntfy_topic TEXT")
    
    @staticmethod
    def _ensure_youtube_table(db: duckdb.DuckDBPyConnection) -> None:
        """Create youtube_videos table."""
        db.execute(f"""
            CREATE TABLE IF NOT EXISTS {Tables.YOUTUBE_VIDEOS} (
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
        """)
    
    @staticmethod
    def _ensure_mechanic_spikes_table(db: duckdb.DuckDBPyConnection) -> None:
        """Create mechanic_spikes table."""
        db.execute(f"""
            CREATE TABLE IF NOT EXISTS {Tables.MECHANIC_SPIKES} (
                universe_id INTEGER NOT NULL,
                game_name TEXT,
                current_ccu INTEGER,
                week_ago_ccu INTEGER,
                growth_percent DOUBLE,
                published_at TIMESTAMP,
                mechanic TEXT,
                video_title TEXT,
                video_url TEXT,
                channel_title TEXT,
                detected_at TIMESTAMP NOT NULL
            )
        """)
        
        # Outcome tracking columns for measuring precision
        db.execute(f"ALTER TABLE {Tables.MECHANIC_SPIKES} ADD COLUMN IF NOT EXISTS sustained_growth_72h BOOLEAN")
        db.execute(f"ALTER TABLE {Tables.MECHANIC_SPIKES} ADD COLUMN IF NOT EXISTS manually_validated BOOLEAN")
        db.execute(f"ALTER TABLE {Tables.MECHANIC_SPIKES} ADD COLUMN IF NOT EXISTS validation_notes TEXT")
        db.execute(f"ALTER TABLE {Tables.MECHANIC_SPIKES} ADD COLUMN IF NOT EXISTS outcome_updated_at TIMESTAMP")
    
    @staticmethod
    def _ensure_custom_games_table(db: duckdb.DuckDBPyConnection) -> None:
        """Create custom_games table for developer-added games."""
        db.execute(f"""
            CREATE TABLE IF NOT EXISTS {Tables.CUSTOM_GAMES} (
                universe_id BIGINT PRIMARY KEY,
                game_name TEXT,
                owner_name TEXT,
                owner_contact TEXT,
                webhook_url TEXT,
                notes TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)