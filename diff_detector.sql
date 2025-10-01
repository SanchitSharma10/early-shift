-- diff_detector.sql - Detects games with >=25% weekly growth and tracks peak CCU

WITH latest_snapshot AS (
    SELECT universe_id,
           name,
           ccu,
           timestamp,
           ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
    FROM games
),
filtered_latest AS (
    SELECT universe_id, name, ccu, timestamp
    FROM latest_snapshot
    WHERE row_num = 1
),
week_ago_snapshot AS (
    SELECT universe_id,
           ccu,
           timestamp,
           ROW_NUMBER() OVER (PARTITION BY universe_id ORDER BY timestamp DESC) AS row_num
    FROM games
    WHERE timestamp <= datetime('now', '-7 days')
),
filtered_week_ago AS (
    SELECT universe_id, ccu, timestamp
    FROM week_ago_snapshot
    WHERE row_num = 1
),
peak_window AS (
    SELECT universe_id,
           MAX(ccu) AS peak_ccu
    FROM games
    WHERE timestamp >= datetime('now', '-7 days')
    GROUP BY universe_id
)
SELECT
    cur.universe_id,
    COALESCE(meta.name, cur.name) AS game_name,
    cur.ccu AS current_ccu,
    prev.ccu AS week_ago_ccu,
    ROUND(((cur.ccu - prev.ccu) * 100.0) / NULLIF(prev.ccu, 0), 1) AS growth_percent,
    ((cur.ccu - prev.ccu) * 1.0) / NULLIF(prev.ccu, 0) AS growth_rate,
    COALESCE(peak.peak_ccu, cur.ccu) AS peak_ccu,
    cur.timestamp AS current_time
FROM filtered_latest cur
JOIN filtered_week_ago prev
  ON prev.universe_id = cur.universe_id
LEFT JOIN peak_window peak
  ON peak.universe_id = cur.universe_id
LEFT JOIN game_metadata meta
  ON meta.universe_id = cur.universe_id
WHERE prev.ccu > 0
  AND ((cur.ccu - prev.ccu) * 100.0 / prev.ccu) >= 25.0
ORDER BY growth_percent DESC;