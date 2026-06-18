"""Backtest Early Shift's creator-linked spike detections against matched controls.

Methodology
-----------
Detection episodes: mechanic_spikes rows are deduplicated to (game, batch) events,
then to "episodes" with a 7-day per-game cooldown (repeated alerts for the same
ongoing spike count once).

For each episode:
  baseline   = median CCU in [det-14d, det-7d]  (pre-spike level)
  det_ccu    = current_ccu recorded at detection
  ccu_72h    = median CCU in [det+2d, det+4d]
  ccu_7d     = median CCU in [det+6d, det+8d]
  lift       = det_ccu - baseline
  retention(t) = (ccu_t - baseline) / lift      (1.0 = fully held, 0 = round trip)
  sustained(t) = ccu_t >= baseline * 1.10       (still >=10% above pre-spike level)

Controls: spike days in the same CCU panel where a game grew >=25% vs 7 days
prior but had NO detection within +/-7 days. Same outcome math, same cooldown.
Each detection episode is compared to controls in the same log10-CCU band
(+/-0.5) and same calendar fortnight.

Match quality: an episode is a "named match" if any evidence video title
fuzzy-contains the game name (partial_ratio >= 80). Metrics are reported for
all episodes and for named matches separately.
"""
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from rapidfuzz import fuzz

DB = "early_shift.db"
GROWTH_THRESHOLD = 0.25
SUSTAIN_MARGIN = 1.10
COOLDOWN_DAYS = 7

con = duckdb.connect(DB, read_only=True)

# ---------------------------------------------------------------- detections
events = con.execute("""
    SELECT universe_id,
           ANY_VALUE(game_name) AS game_name,
           DATE_TRUNC('hour', detected_at) AS batch,
           MIN(detected_at) AS det_at,
           ANY_VALUE(current_ccu) AS det_ccu,
           ANY_VALUE(week_ago_ccu) AS week_ago_ccu,
           ANY_VALUE(growth_percent) AS growth_percent,
           LIST(video_title) AS video_titles,
           LIST(channel_title) AS channels
    FROM mechanic_spikes
    GROUP BY universe_id, DATE_TRUNC('hour', detected_at)
    ORDER BY universe_id, det_at
""").df()

# union replayed detections (Feb-Jun backfill) if present
backfill_path = Path("analysis/backfill_detections.csv")
if backfill_path.exists():
    bf = pd.read_csv(backfill_path, parse_dates=["det_at"])
    bf["video_titles"] = bf["video_titles"].fillna("").str.split(" || ", regex=False)
    bf["channels"] = bf["channels"].fillna("").str.split(" || ", regex=False)
    bf["batch"] = bf["det_at"].copy()
    bf_aligned = bf[list(events.columns)].copy()
    # duckdb emits datetime64[us], read_csv emits [ns]; align or concat breaks
    for col in ("batch", "det_at"):
        events[col] = pd.to_datetime(events[col]).astype("datetime64[ns]")
        bf_aligned[col] = pd.to_datetime(bf_aligned[col]).astype("datetime64[ns]")
    events = pd.concat([events.copy(), bf_aligned], ignore_index=True)
    events = events.sort_values(["universe_id", "det_at"]).reset_index(drop=True)
    print(f"unioned {len(bf)} backfill detections from {backfill_path}")

# collapse to episodes: 7-day cooldown per game
episodes = []
last_seen = {}
for _, row in events.iterrows():
    uid, t = row["universe_id"], row["det_at"]
    if uid in last_seen and (t - last_seen[uid]).total_seconds() < COOLDOWN_DAYS * 86400:
        last_seen[uid] = t
        continue
    last_seen[uid] = t
    episodes.append(row)
episodes = pd.DataFrame(episodes).reset_index(drop=True)

def named_match(row):
    name = (row["game_name"] or "").lower()
    if not name:
        return False
    return any(fuzz.partial_ratio(name, (vt or "").lower()) >= 80
               for vt in row["video_titles"])

episodes["named_match"] = episodes.apply(named_match, axis=1)

# ---------------------------------------------------------------- CCU panel
# Both windows are derived from the detections themselves:
#   panel must cover [first detection - 21d, last detection + 21d] so every
#   episode has baseline (-14..-7d) and outcome (+14d) coverage;
#   controls must come from the detection calendar window so they share
#   season/platform conditions with the episodes.
det_min = pd.Timestamp(events["det_at"].min()).normalize()
det_max = pd.Timestamp(events["det_at"].max()).normalize()
panel_lo = (det_min - pd.Timedelta(days=21)).date()
panel_hi = (det_max + pd.Timedelta(days=21)).date()
print(f"detections: {det_min.date()} .. {det_max.date()}  |  panel: {panel_lo} .. {panel_hi}")

panel = con.execute(f"""
    SELECT universe_id, DATE_TRUNC('day', timestamp) AS day, MEDIAN(ccu) AS ccu
    FROM games
    WHERE timestamp BETWEEN '{panel_lo}' AND '{panel_hi}'
    GROUP BY 1, 2
""").df()
panel["day"] = pd.to_datetime(panel["day"])
series = {uid: g.set_index("day")["ccu"].sort_index()
          for uid, g in panel.groupby("universe_id")}

def window_median(uid, t, lo_days, hi_days):
    s = series.get(uid)
    if s is None:
        return np.nan
    w = s[(s.index >= t + pd.Timedelta(days=lo_days)) &
          (s.index <= t + pd.Timedelta(days=hi_days))]
    return w.median() if len(w) else np.nan

def measure(uid, t, det_ccu):
    base = window_median(uid, t, -14, -7)
    c72 = window_median(uid, t, 2, 4)
    c7d = window_median(uid, t, 6, 8)
    out = {"baseline": base, "ccu_72h": c72, "ccu_7d": c7d}
    if np.isnan(base) or base <= 0 or det_ccu is None or det_ccu <= base:
        out.update(retention_72h=np.nan, retention_7d=np.nan,
                   sustained_72h=np.nan, sustained_7d=np.nan)
        return out
    lift = det_ccu - base
    out["retention_72h"] = float((c72 - base) / lift) if not np.isnan(c72) else np.nan
    out["retention_7d"] = float((c7d - base) / lift) if not np.isnan(c7d) else np.nan
    # cast to float: numpy bools in object columns sum as logical OR, corrupting rates
    out["sustained_72h"] = float(c72 >= base * SUSTAIN_MARGIN) if not np.isnan(c72) else np.nan
    out["sustained_7d"] = float(c7d >= base * SUSTAIN_MARGIN) if not np.isnan(c7d) else np.nan
    return out

meas = episodes.apply(lambda r: pd.Series(measure(r["universe_id"], pd.Timestamp(r["det_at"]).normalize(), r["det_ccu"])), axis=1)
episodes = pd.concat([episodes, meas], axis=1)

# ---------------------------------------------------------------- controls
detected_games_dates = {(r["universe_id"], pd.Timestamp(r["det_at"]).normalize())
                        for _, r in events.iterrows()}
det_by_game = events.groupby("universe_id")["det_at"].apply(list).to_dict()

controls = []
for uid, s in series.items():
    s = s.sort_index()
    last_ctrl = None
    for day, ccu in s.items():
        prior = s[(s.index >= day - pd.Timedelta(days=8)) &
                  (s.index <= day - pd.Timedelta(days=6))]
        if not len(prior) or prior.median() <= 50:   # ignore tiny games
            continue
        base7 = prior.median()
        growth = (ccu - base7) / base7
        if growth < GROWTH_THRESHOLD:
            continue
        # exclude if this game had a detection within +/-7 days
        near_det = any(abs((day - pd.Timestamp(d).normalize()).days) <= 7
                       for d in det_by_game.get(uid, []))
        if near_det:
            continue
        if last_ctrl is not None and (day - last_ctrl).days < COOLDOWN_DAYS:
            continue
        last_ctrl = day
        controls.append({"universe_id": uid, "det_at": day, "det_ccu": ccu,
                         "growth_percent": growth * 100})
controls = pd.DataFrame(controls)
cmeas = controls.apply(lambda r: pd.Series(measure(r["universe_id"], r["det_at"], r["det_ccu"])), axis=1)
controls = pd.concat([controls, cmeas], axis=1)

# restrict controls to the detection calendar window
controls = controls[(controls["det_at"] >= det_min) & (controls["det_at"] <= det_max)]

# ---------------------------------------------------------------- matching
def summarize(df, label):
    d = df.dropna(subset=["retention_72h"])
    d7 = df.dropna(subset=["retention_7d"])
    print(f"\n=== {label} ===")
    print(f"  episodes measurable: 72h={len(d)}, 7d={len(d7)}")
    if len(d):
        print(f"  sustained at 72h (>=10% above baseline): {d['sustained_72h'].mean():.1%}")
        print(f"  median lift retention at 72h: {d['retention_72h'].median():.2f}")
    if len(d7):
        print(f"  sustained at 7d: {d7['sustained_7d'].mean():.1%}")
        print(f"  median lift retention at 7d: {d7['retention_7d'].median():.2f}")

valid = episodes.dropna(subset=["baseline"])
valid = valid[valid["det_ccu"] > valid["baseline"]]

summarize(valid, "ALL detection episodes")
summarize(valid[valid["named_match"]], "NAMED-MATCH episodes (video title names the game)")
summarize(valid[~valid["named_match"]], "UNNAMED episodes (weaker attribution)")
summarize(controls, "CONTROL spikes (>=25% growth, no creator detection)")

# by-genre cut (modern taxonomy backfilled into game_metadata)
genres = dict(con.execute("""
    SELECT universe_id, ANY_VALUE(genre_l1) FROM game_metadata
    WHERE genre_l1 IS NOT NULL AND genre_l1 != '' GROUP BY 1
""").fetchall())
gd = valid.dropna(subset=["retention_7d"]).copy()
gd["genre_l1"] = gd["universe_id"].map(genres)
print("\n=== detection episodes by genre (n >= 15) ===")
for g, grp in sorted(gd.groupby("genre_l1"), key=lambda kv: -len(kv[1])):
    if len(grp) >= 15:
        print(f"  {g:25s} n={len(grp):3d}  sustained@7d={grp['sustained_7d'].mean():5.1%}"
              f"  median retention@7d={grp['retention_7d'].median():.2f}")

# size + time matched comparison
matched_rows = []
for _, ep in valid.dropna(subset=["retention_7d"]).iterrows():
    band = np.log10(max(ep["baseline"], 1))
    t = pd.Timestamp(ep["det_at"]).normalize()
    cand = controls.dropna(subset=["retention_7d"])
    cand = cand[(np.abs(np.log10(cand["baseline"].clip(lower=1)) - band) <= 0.5) &
                (np.abs((cand["det_at"] - t).dt.days) <= 7)]
    if len(cand):
        matched_rows.append({
            "episode_retention_7d": ep["retention_7d"],
            "episode_sustained_7d": ep["sustained_7d"],
            "ctrl_retention_7d": cand["retention_7d"].median(),
            "ctrl_sustained_7d": cand["sustained_7d"].mean(),
            "n_controls": len(cand),
        })
matched = pd.DataFrame(matched_rows)
print(f"\n=== MATCHED comparison (size band +/-0.5 log10, +/-7 days) ===")
print(f"  episodes with matches: {len(matched)}")
if len(matched):
    print(f"  detection sustained@7d: {matched['episode_sustained_7d'].mean():.1%}"
          f"  vs matched controls: {matched['ctrl_sustained_7d'].mean():.1%}")
    print(f"  detection median retention@7d: {matched['episode_retention_7d'].median():.2f}"
          f"  vs matched controls: {matched['ctrl_retention_7d'].median():.2f}")

# ---------------------------------------------------------------- export
episodes.drop(columns=["video_titles", "channels"]).to_csv("analysis/backtest_episodes.csv", index=False)
controls.to_csv("analysis/backtest_controls.csv", index=False)
print("\nWrote analysis/backtest_episodes.csv and analysis/backtest_controls.csv")
