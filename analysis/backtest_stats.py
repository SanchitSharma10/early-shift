"""Follow-up stats: paired significance test + detection lead time vs CCU peak."""
import duckdb
import numpy as np
import pandas as pd
from math import comb

ep = pd.read_csv("analysis/backtest_episodes.csv", parse_dates=["det_at"])
ctrl = pd.read_csv("analysis/backtest_controls.csv", parse_dates=["det_at"])

# ---------------------------------------------------------- paired sign test
rows = []
for _, e in ep.dropna(subset=["retention_7d"]).iterrows():
    band = np.log10(max(e["baseline"], 1))
    cand = ctrl.dropna(subset=["retention_7d"])
    cand = cand[(np.abs(np.log10(cand["baseline"].clip(lower=1)) - band) <= 0.5) &
                (np.abs((cand["det_at"] - e["det_at"]).dt.days) <= 7)]
    if len(cand):
        rows.append(e["retention_7d"] - cand["retention_7d"].median())
diffs = pd.Series(rows)
pos = int((diffs > 0).sum())
n = int((diffs != 0).count())
# two-sided binomial sign test, p = P(X <= min(pos, n-pos) or >= max(...)) under p=0.5
k = min(pos, n - pos)
p_two = sum(comb(n, i) for i in range(0, k + 1)) / 2**n * 2
print(f"paired episodes: {n}")
print(f"episode beats matched control retention@7d: {pos}/{n} ({pos/n:.1%})")
print(f"median paired difference: {diffs.median():+.3f}")
print(f"sign test two-sided p ~ {min(p_two, 1):.3f}")

# ---------------------------------------------------------- lead time to peak
con = duckdb.connect("early_shift.db", read_only=True)
panel_lo = (ep["det_at"].min() - pd.Timedelta(days=21)).date()
panel_hi = (ep["det_at"].max() + pd.Timedelta(days=21)).date()
panel = con.execute(f"""
    SELECT universe_id, DATE_TRUNC('day', timestamp) AS day, MEDIAN(ccu) AS ccu
    FROM games WHERE timestamp BETWEEN '{panel_lo}' AND '{panel_hi}'
    GROUP BY 1, 2
""").df()
panel["day"] = pd.to_datetime(panel["day"])
series = {uid: g.set_index("day")["ccu"].sort_index() for uid, g in panel.groupby("universe_id")}

leads = []
for _, e in ep.iterrows():
    s = series.get(e["universe_id"])
    if s is None:
        continue
    t = pd.Timestamp(e["det_at"]).normalize()
    # look back a full week so "fired before the peak" isn't true by construction
    w = s[(s.index >= t - pd.Timedelta(days=7)) & (s.index <= t + pd.Timedelta(days=14))]
    if len(w) < 5:
        continue
    peak_day = w.idxmax()
    fut = s[(s.index > t) & (s.index <= t + pd.Timedelta(days=14))]
    fut_peak = fut.max() if len(fut) else np.nan
    leads.append({"game": e["game_name"], "det": t.date(),
                  "lead_days": (peak_day - t).days,
                  "peak_ccu": w.max(), "det_ccu": e["det_ccu"],
                  "future_headroom": (fut_peak - e["det_ccu"]) / e["det_ccu"]})
leads = pd.DataFrame(leads)
print(f"\nlead-time sample: {len(leads)} episodes")
print(f"detection fired BEFORE the 14-day peak: {(leads['lead_days'] > 0).mean():.1%}")
print(f"median lead time to peak: {leads['lead_days'].median():.1f} days")
print(f"median FUTURE CCU headroom after detection: {leads['future_headroom'].median():+.1%}")
print(f"future peak exceeds detection CCU: {(leads['future_headroom'] > 0).mean():.1%}")
print("\nlead day distribution:")
print(leads["lead_days"].value_counts().sort_index().to_string())

# day-of-week sanity for the 72h artifact question
print("\ndetection day-of-week distribution:")
print(pd.Timestamp(0))  # noop keep imports happy
print(ep["det_at"].dt.day_name().value_counts().to_string())

leads.to_csv("analysis/backtest_leadtime.csv", index=False)
print("\nWrote analysis/backtest_leadtime.csv")
