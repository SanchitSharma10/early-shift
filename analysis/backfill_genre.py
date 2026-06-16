"""Backfill modern genre taxonomy (genre_l1/genre_l2) into game_metadata.

The games.roblox.com/v1/games endpoint already returns these fields; the
collector historically stored only the legacy `genre` (83% "All").
"""
import time

import duckdb
import requests

BATCH = 50  # games.roblox.com/v1/games rejects more than 50 universeIds per call
URL = "https://games.roblox.com/v1/games"

con = duckdb.connect("early_shift.db")
con.execute("ALTER TABLE game_metadata ADD COLUMN IF NOT EXISTS genre_l1 VARCHAR")
con.execute("ALTER TABLE game_metadata ADD COLUMN IF NOT EXISTS genre_l2 VARCHAR")

ids = [r[0] for r in con.execute(
    "SELECT universe_id FROM game_metadata ORDER BY universe_id").fetchall()]
print(f"backfilling genre for {len(ids)} games in batches of {BATCH}")

updated = 0
for i in range(0, len(ids), BATCH):
    chunk = ids[i:i + BATCH]
    for attempt in range(4):
        try:
            resp = requests.get(URL, params={"universeIds": ",".join(map(str, chunk))},
                                timeout=30)
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"  rate limited, sleeping {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            print(f"  retry {attempt + 1}: {e}")
            time.sleep(5)
    else:
        print(f"  batch {i // BATCH} failed, skipping")
        continue
    rows = [(d.get("genre_l1"), d.get("genre_l2"), d["id"])
            for d in resp.json().get("data", [])]
    con.executemany(
        "UPDATE game_metadata SET genre_l1 = ?, genre_l2 = ? WHERE universe_id = ?", rows)
    updated += len(rows)
    print(f"  batch {i // BATCH + 1}/{(len(ids) + BATCH - 1) // BATCH}: {len(rows)} games")
    time.sleep(0.8)

print(f"\nupdated {updated} games")
print("\n--- genre_l1 distribution ---")
for row in con.execute("""
    SELECT COALESCE(genre_l1, '(null)'), COUNT(*) FROM game_metadata
    GROUP BY 1 ORDER BY 2 DESC LIMIT 15
""").fetchall():
    print(f"  {row[0]}: {row[1]}")
con.close()
