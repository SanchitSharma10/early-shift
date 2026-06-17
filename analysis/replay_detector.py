"""Replay the mechanic detector over the Feb 8 - Jun 10 2026 CCU panel.

Uses the production matching rules imported from mechanic_detector
(_clean_game_name / _video_matches_game, FUZZ_THRESHOLD=70) and the same
thresholds (>=25% growth vs ~7 days prior, videos published within 48h).

Detections are written to analysis/backfill_detections.csv — the production
mechanic_spikes table is NOT modified. backtest.py unions this CSV in when
present.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import pandas as pd

from mechanic_detector import _is_spam_video, _video_matches_game

DB = "early_shift.db"
GROWTH_THRESHOLD = 0.25
MIN_BASELINE_CCU = 50
COOLDOWN_DAYS = 7
REPLAY_START = pd.Timestamp("2026-02-08")   # day after the last live detection
REPLAY_END = pd.Timestamp("2026-06-09")     # last day with a full +1d of data

con = duckdb.connect(DB, read_only=True)

# daily CCU panel with 2 weeks of pre-history for baselines
panel = con.execute(f"""
    SELECT universe_id, DATE_TRUNC('day', timestamp) AS day, MEDIAN(ccu) AS ccu
    FROM games
    WHERE timestamp BETWEEN '2026-01-25' AND '2026-06-10'
    GROUP BY 1, 2
""").df()
panel["day"] = pd.to_datetime(panel["day"])

names = dict(con.execute("""
    SELECT universe_id, ANY_VALUE(name) FROM game_metadata
    WHERE name IS NOT NULL GROUP BY 1
""").fetchall())
fallback = dict(con.execute("""
    SELECT universe_id, ANY_VALUE(name) FROM games
    WHERE name IS NOT NULL GROUP BY 1
""").fetchall())

videos = con.execute("""
    SELECT published_at, title, channel_title
    FROM youtube_videos
    WHERE published_at BETWEEN '2026-02-05' AND '2026-06-11'
      AND title IS NOT NULL
""").df()
n_before = len(videos)
videos = videos[~videos["title"].apply(_is_spam_video)].copy()
print(f"spam filter removed {n_before - len(videos)} of {n_before} videos "
      f"({(n_before - len(videos)) / n_before:.1%})")
videos["published_at"] = pd.to_datetime(videos["published_at"])
videos["pub_day"] = videos["published_at"].dt.normalize()
videos_by_day = {d: g for d, g in videos.groupby("pub_day")}

def candidate_videos(day):
    """Videos published within the 48h ending at the end of `day`."""
    frames = [videos_by_day[d] for d in (day - pd.Timedelta(days=1), day)
              if d in videos_by_day]
    return pd.concat(frames) if frames else None

detections = []
n_spikes = 0
for uid, g in panel.groupby("universe_id"):
    name = names.get(uid) or fallback.get(uid)
    if not name:
        continue
    s = g.set_index("day")["ccu"].sort_index()
    last_det = None
    for day, ccu in s.items():
        if day < REPLAY_START or day > REPLAY_END:
            continue
        prior = s[(s.index >= day - pd.Timedelta(days=8)) &
                  (s.index <= day - pd.Timedelta(days=6))]
        if not len(prior) or prior.median() <= MIN_BASELINE_CCU:
            continue
        week_ago = prior.median()
        growth = (ccu - week_ago) / week_ago
        if growth < GROWTH_THRESHOLD:
            continue
        n_spikes += 1
        if last_det is not None and (day - last_det).days < COOLDOWN_DAYS:
            continue
        cand = candidate_videos(day)
        if cand is None:
            continue
        hits = cand[cand["title"].apply(lambda t: _video_matches_game(name, t))]
        if not len(hits):
            continue
        last_det = day
        detections.append({
            "universe_id": uid,
            "game_name": name,
            "det_at": day,
            "det_ccu": int(ccu),
            "week_ago_ccu": int(week_ago),
            "growth_percent": growth * 100,
            "video_titles": " || ".join(hits["title"].head(10)),
            "channels": " || ".join(hits["channel_title"].fillna("").head(10)),
            "n_videos": len(hits),
        })

out = pd.DataFrame(detections).sort_values("det_at")
out.to_csv("analysis/backfill_detections.csv", index=False)
print(f"replay window: {REPLAY_START.date()} .. {REPLAY_END.date()}")
print(f"spike game-days >=25% growth: {n_spikes}")
print(f"creator-linked detections (after cooldown): {len(out)}")
print(f"distinct games: {out['universe_id'].nunique() if len(out) else 0}")
if len(out):
    print(f"median growth at detection: {out['growth_percent'].median():.1f}%")
    print("\nby month:")
    print(out.set_index("det_at").resample("MS")["universe_id"].count().to_string())
