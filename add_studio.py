"""
add_studio.py - Helper to add studios to Early Shift
"""

import argparse
from typing import Optional

import duckdb

DB_PATH = "early_shift.db"


def ensure_schema(db: duckdb.DuckDBPyConnection) -> None:
    """Ensure the studios table and expected columns exist."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS studios (
            studio_id TEXT PRIMARY KEY,
            name TEXT,
            notion_token TEXT,
            notion_database_id TEXT,
            ntfy_topic TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # Backfill columns when the table already existed with older schema
    db.execute("ALTER TABLE studios ADD COLUMN IF NOT EXISTS notion_token TEXT")
    db.execute("ALTER TABLE studios ADD COLUMN IF NOT EXISTS notion_database_id TEXT")
    db.execute("ALTER TABLE studios ADD COLUMN IF NOT EXISTS ntfy_topic TEXT")


def add_studio(name: str, notion_token: str, database_id: str, ntfy_topic: Optional[str] = None) -> None:
    """Register a studio and its delivery preferences."""
    studio_id = name.lower().strip().replace(" ", "-")

    db = duckdb.connect(DB_PATH)
    ensure_schema(db)

    db.execute(
        """
        INSERT OR REPLACE INTO studios (studio_id, name, notion_token, notion_database_id, ntfy_topic)
        VALUES (?, ?, ?, ?, ?)
        """,
        (studio_id, name, notion_token, database_id, ntfy_topic),
    )
    db.commit()
    db.close()

    print(f"? Added studio: {name}")
    print(f"   Studio ID: {studio_id}")
    print("   Notion credentials stored securely")
    if ntfy_topic:
        print(f"   Ntfy topic: {ntfy_topic}")
    print("\n?? Send invoice for $99/month to begin monitoring!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add a studio to Early Shift")
    parser.add_argument("--name", required=True, help="Studio name")
    parser.add_argument("--token", required=True, help="Notion API token")
    parser.add_argument("--database", required=True, help="Notion database ID")
    parser.add_argument("--ntfy-topic", help="Optional ntfy.sh topic for mobile alerts")

    args = parser.parse_args()
    add_studio(args.name, args.token, args.database, args.ntfy_topic)
