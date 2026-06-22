# Early Shift Detection Backtest

*Updated 2026-06-10 (spam filter + genre taxonomy applied). Scripts:
`analysis/replay_detector.py` (Feb–Jun detection backfill),
`analysis/backtest.py` (outcomes + matched controls + genre cut),
`analysis/backtest_stats.py` (paired test + lead time),
`analysis/backfill_genre.py` (genre_l1/l2 from the games API).
Per-episode data: `backtest_episodes.csv`, `backtest_controls.csv`,
`backtest_leadtime.csv`, `backfill_detections.csv`.*

## Sample

Two detection sources, identical rules (≥25% CCU growth vs ~7 days prior +
creator video within 48h, fuzzy title match ≥70, codes/script/exploit spam
videos excluded — 15.9% of the video pool):

- **Live detections** (as deployed): Dec 21, 2025 – Feb 7, 2026 → 144 events.
- **Replayed detections**: the same ruleset replayed over the Feb 8 – Jun 9
  CCU panel → 913 events across 408 games. The rules predate this period,
  so the replay is an out-of-sample test of the ruleset. Replay output lives
  in `backfill_detections.csv`; the production `mechanic_spikes` table is
  untouched.

After per-game 7-day cooldowns and coverage requirements: **689 episodes with
measurable 7-day outcomes** vs **904 measurable control spikes** (≥25% growth,
no detected creator coverage, same calendar window), matched by size
(±0.5 log10 baseline CCU) and time (±7 days).

Outcome math per episode: baseline = median CCU days −14..−7;
retention(t) = (CCU_t − baseline) / (CCU_det − baseline);
sustained(t) = CCU_t ≥ 110% of baseline.

## Findings

### 1. Creator-linked spikes retain lift better than uncovered spikes (p ≈ 0.002)

| | Detections | Matched controls |
| --- | --- | --- |
| Sustained @72h | 61.0% | 48.3% |
| Median lift retention @72h | 0.40 | 0.19 |
| Sustained @7d | **64.7%** | **55.9%** |
| Median lift retention @7d | **0.50** | **0.37** |

Paired comparison across 657 episode/control pairs: detections beat their
matched control on 7-day retention in 56.0% of pairs, median paired
difference **+0.108 retention**, two-sided sign test **p ≈ 0.002**.

![Event study: creator-linked spikes vs matched uncovered spikes](event_study.png)

The event study (`event_study.py`, series in `event_study_series.csv`) shows
the effect visually: both cohorts spike to ~1.45× baseline at detection with
near-identical pre-trends, then the creator-linked cohort holds a persistent
retention gap through day 14. The ~7-day wave in both curves is weekend
seasonality and co-moves across cohorts — the vertical gap between them is
the effect.

History, honestly: the first pass (live Dec–Feb sample only, n=64 pairs)
showed **no** significant difference. That null was underpowered and
contaminated by a scheduler artifact (76% of live detections fired on
Fridays, so the 72h horizon always landed on the Monday trough). With daily
replay cadence and a 10× sample the effect is positive and significant, and
it survives removing codes/script spam videos. Both results are reported
because the second supersedes the first for identifiable reasons, not because
we kept testing until something worked.

### 2. The early-warning claim holds

On 954 lead-time-measurable episodes:

| Metric | Value |
| --- | --- |
| Future 14-day peak exceeds detection-time CCU | **69.9%** |
| Median additional CCU headroom after the alert | **+18.2%** |
| Median lead time to local peak | **2.0 days** |

A studio acting on an alert still had upside ahead in ~70% of cases.

### 3. Retention differs sharply by genre (modern taxonomy, n ≥ 15)

| Genre | Episodes | Sustained @7d | Median retention @7d |
| --- | --- | --- | --- |
| RPG | 45 | 75.6% | 0.54 |
| Strategy | 63 | 74.6% | 0.63 |
| Adventure | 26 | 73.1% | 0.56 |
| Simulation | 179 | 68.2% | 0.62 |
| Party & Casual | 31 | 64.5% | 0.51 |
| Action | 85 | 62.4% | 0.42 |
| Roleplay & Avatar Sim | 51 | 60.8% | 0.39 |
| Survival | 90 | 60.0% | 0.34 |
| Shooter | 25 | 52.0% | 0.33 |
| Sports & Racing | 53 | 45.3% | 0.15 |

Creator-driven lift in RPG/Strategy/Simulation games tends to stick;
Sports & Racing spikes are mostly flash traffic (median retention 0.15).
This is the actionable layer for studios: the same alert means different
things in different genres.

### 4. Remaining caveats

- **Control contamination biases the effect DOWN**: controls are "no
  *detected* coverage" — only ~151 channels are tracked, so some controls had
  coverage we can't see.
- **Correlation, not causation**: creators may select games that were going
  to hold their growth anyway. Matching controls for size, timing, and spike
  magnitude — not for everything a creator can see.
- **Replay granularity**: backfilled detections are daily (live ones were
  intra-day). Lead-time medians are conservative at this resolution.
- The "mechanic" extraction subsystem is NOT validated (491/568 live
  detections classify as "other"); nothing in this report depends on it.

## What to say publicly

> "Across 689 creator-linked spike episodes backtested against size- and
> time-matched control spikes (Dec 2025 – Jun 2026, codes/script spam
> excluded), creator-covered spikes retained significantly more of their CCU
> lift at 7 days (median retention 0.50 vs 0.37, paired sign test p ≈ 0.002).
> Alerts fired with a median 2-day lead and ~18% median CCU upside remaining.
> Retention varies sharply by genre: RPG and Strategy spikes hold (~75%
> sustained), Sports & Racing spikes are mostly flash traffic (45%). An
> earlier, smaller sample showed no retention effect; the difference is
> documented and attributable to sample size and a scheduling artifact."

## Next steps, in value order

1. Backfill outcome columns (`sustained_growth_72h`) in `mechanic_spikes`.
2. Game-centric video search to reduce control contamination.
3. Keep the detector running daily so the live sample grows past the replay.
