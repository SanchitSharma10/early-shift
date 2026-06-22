"""Event-study chart: CCU trajectories around detection, detections vs control spikes.

Left panel:  median CCU relative to pre-spike baseline, day -7 .. +14.
Right panel: median share of the spike lift still retained, day 0 .. +14.
Bands are bootstrap 90% CIs of the median. Output: analysis/event_study.png
plus the underlying series in analysis/event_study_series.csv.
"""
import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

rng = np.random.default_rng(7)
OFFSETS = range(-7, 15)

ep = pd.read_csv("analysis/backtest_episodes.csv", parse_dates=["det_at"])
ctrl = pd.read_csv("analysis/backtest_controls.csv", parse_dates=["det_at"])

con = duckdb.connect("early_shift.db", read_only=True)
panel = con.execute("""
    SELECT universe_id, DATE_TRUNC('day', timestamp) AS day, MEDIAN(ccu) AS ccu
    FROM games WHERE timestamp BETWEEN '2025-11-30' AND '2026-06-30'
    GROUP BY 1, 2
""").df()
panel["day"] = pd.to_datetime(panel["day"])
series = {uid: g.set_index("day")["ccu"].sort_index()
          for uid, g in panel.groupby("universe_id")}

def trajectories(df):
    """Per-event arrays of CCU/baseline and lift share, indexed by day offset."""
    rel, lift = [], []
    for _, r in df.iterrows():
        if not (r["baseline"] > 0 and r["det_ccu"] > r["baseline"]):
            continue
        s = series.get(r["universe_id"])
        if s is None:
            continue
        t = pd.Timestamp(r["det_at"]).normalize()
        rel_row, lift_row = {}, {}
        for o in OFFSETS:
            v = s.get(t + pd.Timedelta(days=o), np.nan)
            rel_row[o] = v / r["baseline"]
            if o >= 0:
                lift_row[o] = (v - r["baseline"]) / (r["det_ccu"] - r["baseline"])
        rel.append(rel_row)
        lift.append(lift_row)
    return pd.DataFrame(rel), pd.DataFrame(lift)

def med_ci(df, n_boot=500):
    med = df.median()
    lo, hi = {}, {}
    for col in df.columns:
        v = df[col].dropna().to_numpy()
        if len(v) < 10:
            lo[col] = hi[col] = np.nan
            continue
        boots = np.median(rng.choice(v, size=(n_boot, len(v))), axis=1)
        lo[col], hi[col] = np.percentile(boots, [5, 95])
    return med, pd.Series(lo), pd.Series(hi)

ep_rel, ep_lift = trajectories(ep)
ct_rel, ct_lift = trajectories(ctrl)
print(f"episodes plotted: {len(ep_rel)}, controls plotted: {len(ct_rel)}")

C_DET, C_CTRL = "#d6336c", "#5f6b7a"
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))
fig.suptitle("Creator-linked CCU spikes vs matched uncovered spikes (Dec 2025 – Jun 2026)",
             fontsize=13, fontweight="bold")

for df, color, label in [(ep_rel, C_DET, f"Creator-linked detections (n={len(ep_rel)})"),
                         (ct_rel, C_CTRL, f"Control spikes, no detected coverage (n={len(ct_rel)})")]:
    med, lo, hi = med_ci(df)
    x = list(df.columns)
    ax1.plot(x, med, color=color, lw=2.2, label=label)
    ax1.fill_between(x, lo, hi, color=color, alpha=0.18)
ax1.axvline(0, color="black", lw=0.8, ls="--")
ax1.axhline(1.0, color="black", lw=0.8, ls=":")
ax1.annotate("alert fires", xy=(0, 1.0), xycoords=("data", "axes fraction"),
             xytext=(8, -14), textcoords="offset points", fontsize=9, va="top")
ax1.set_xlabel("days relative to detection")
ax1.set_ylabel("median CCU / pre-spike baseline")
ax1.set_title("CCU relative to pre-spike baseline")
ax1.legend(loc="upper right", fontsize=9)
ax1.grid(alpha=0.25)

for df, color, label in [(ep_lift, C_DET, "Creator-linked detections"),
                         (ct_lift, C_CTRL, "Control spikes")]:
    med, lo, hi = med_ci(df)
    x = list(df.columns)
    ax2.plot(x, med, color=color, lw=2.2, label=label)
    ax2.fill_between(x, lo, hi, color=color, alpha=0.18)
ax2.axhline(0, color="black", lw=0.8, ls=":")
ax2.axhline(1.0, color="black", lw=0.6, ls=":", alpha=0.5)
ax2.set_xlabel("days after detection")
ax2.set_ylabel("median share of spike lift retained")
ax2.set_title("Lift retention after the alert")
ax2.legend(loc="upper right", fontsize=9)
ax2.grid(alpha=0.25)

fig.text(0.5, 0.005,
         "Baseline = median CCU days −14..−7. Controls: ≥25% weekly growth, no detected creator video, same window. "
         "Bands: bootstrap 90% CI of the median. The ~7-day wave in both cohorts is weekend seasonality; the gap between them is the effect. "
         "Source: early_shift.db",
         ha="center", fontsize=8, color="#555")
fig.tight_layout(rect=(0, 0.03, 1, 1))
fig.savefig("analysis/event_study.png", dpi=150)
print("wrote analysis/event_study.png")

# export the plotted series for reuse (case study, dashboard)
rows = []
for name, df_rel, df_lift in [("detection", ep_rel, ep_lift), ("control", ct_rel, ct_lift)]:
    med, lo, hi = med_ci(df_rel)
    for o in df_rel.columns:
        rows.append({"cohort": name, "metric": "ccu_over_baseline", "day": o,
                     "median": med[o], "ci_lo": lo[o], "ci_hi": hi[o]})
    med, lo, hi = med_ci(df_lift)
    for o in df_lift.columns:
        rows.append({"cohort": name, "metric": "lift_retained", "day": o,
                     "median": med[o], "ci_lo": lo[o], "ci_hi": hi[o]})
pd.DataFrame(rows).to_csv("analysis/event_study_series.csv", index=False)
print("wrote analysis/event_study_series.csv")
