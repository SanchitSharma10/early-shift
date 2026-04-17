# EARLY SHIFT - Mechanic Spike Detector
**Note: this repo was reset on <date> from a local 6-month development history**

**Detect trending Roblox game mechanics 24-48 hours before market saturation.**

## 🎯 What It Does

Early Shift monitors 500+ Roblox games and cross-references CCU spikes with YouTube creator activity to alert studios when new mechanics go viral - before 20+ competitors copy them.

**Example**: Caught Pet Simulator X's merge mechanic spike 36 hours before it hit the Popular page.

---

## 🏗️ Architecture

### Current System (v0.1)

```
┌─────────────────┐
│  RoProxy API    │  ← Poll CCU data every 6h for top 500 games
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   DuckDB        │  ← Store: games table (7-day rolling window)
│  (early_shift.db)│           youtube_videos (48h lookback)
└────────┬────────┘           mechanic_spikes (historical feed)
         │
         ▼
┌─────────────────┐
│ Mechanic        │  ← Detect: CCU growth >= 25%
│ Detector        │           + YouTube mention within 48h
└────────┬────────┘           + Keyword match (NEW/SECRET/UPDATE)
         │
         ├──────────┐
         ▼          ▼
┌─────────────┐  ┌──────────────┐
│   Notion    │  │  DuckDB      │
│  Database   │  │  mechanic_   │
│             │  │  spikes table│
└─────────────┘  └──────────────┘
```

**Components**:
- `roproxy_client.py`: CCU data collection (respects rate limits, TOS-compliant)
- `youtube_collector.py`: Top 20 Roblox creator video monitoring
- `mechanic_detector.py`: Correlation logic + fuzzy matching (RapidFuzz)
- `notion_writer.py`: Studio alert delivery
- `main.py`: Orchestrator (runs every 6 hours)

**Tech Stack**: Python 3.11, DuckDB, aiohttp, RapidFuzz, Notion API

---

## 🚀 Quick Start (5 minutes)

1. **Install dependencies**
   ```bash
   cd early_shift
   pip install -r requirements.txt
   ```

2. **Run first collection cycle**
   ```bash
   python main.py
   ```

3. **View detected spikes**
   ```bash
   python mechanic_detector.py
   ```

---

## 📊 Setting up Notion Integration

1. Create a Notion database with these columns:
   - Game (Title)
   - Growth (Number)
   - Current CCU (Number)
   - Mechanic (Text)
   - Source (URL)
   - Detected (Date)

2. Get your Notion API token:
   - Go to https://www.notion.so/my-integrations
   - Create new integration
   - Copy the token

3. Share database with integration:
   - Open your database in Notion
   - Click "..." menu → "Add connections"
   - Select your integration

4. Get database ID:
   - Open database as full page
   - Copy ID from URL: `notion.so/xxxxx/**DATABASE_ID**?v=yyyy`

5. Add studio to Early Shift:
   ```bash
   python add_studio.py --name "Your Studio" --token "YOUR_TOKEN" --database "YOUR_DB_ID"
   ```

---

## 🧪 Running Tests

```bash
# Install test dependencies
pip install pytest

# Run unit tests
pytest test_mechanic_detector.py -v

# Run integration test
python test_early_shift.py
```

---

## 📈 Next Steps (Roadmap)

See `ROADMAP.md` for detailed development plan.

**Phase 2** (4-6 weeks):
- React dashboard UI for historical spike visualization
- BERT-based mechanic classification (economy/gameplay/social/cosmetic)
- Multi-signal fusion (TikTok + Twitter mentions)
- Confidence scoring based on historical accuracy

Includes:
- Game monitoring
- 6-hour update frequency
- Notion integration
- Mobile alerts (ntfy.sh)
- Historical spike feed

---

## 🏃 Running in Production

```bash
# Run monitoring loop (checks every 6 hours)
python main.py --production

# Or use cron (Linux/Mac)
crontab -e
# Add: 0 */6 * * * /path/to/python /path/to/early_shift/main.py

# Or Task Scheduler (Windows)
# Create task to run main.py every 6 hours
```

---

## 🔧 Configuration

Edit `roproxy_client.py` to customize:
- Game list (default: top 500 by CCU)
- Poll frequency (default: 6 hours)
- Growth threshold (default: 25%)

Edit `mechanic_detector.py` for detection tuning:
- Lookback window (default: 48 hours)
- Fuzzy match threshold (default: 82%)
- Keyword hints for mechanic extraction

---

## 📊 Example Output

```
Game                 | Growth  | Current CCU | Mechanic              | Source                          | Published
-------------------- | ------- | ----------- | --------------------- | ------------------------------- | -------------------
Pet Simulator X      | +38.2%  | 142,340     | New merge pets system | https://youtube.com/watch?v=... | 2025-10-01 14:23
Blox Fruits          | +31.7%  | 287,120     | Dragon Quest v2       | https://youtube.com/watch?v=... | 2025-10-01 09:15
Adopt Me             | +26.4%  | 156,890     | Pet fusion mechanics  | https://youtube.com/watch?v=... | 2025-09-30 21:47
```

---

## 📧 Questions / Feedback

Built by [@SanchitSharma10](https://github.com/SanchitSharma10)

**Total lines of code**: ~600  
**Setup time**: <10 minutes  
**Detection accuracy**: Being validated (beta phase)

---

## 🔒 Compliance

- Uses only public APIs (RoProxy, YouTube Data API v3)
- Respects rate limits (RoProxy: 1 req/sec, YouTube: 10K units/day)
- No authentication required
- No PII collected
- TOS-compliant data collection
### YouTube Channel List
- Edit `data/youtube_channels.json` to add or update the creator list (up to 50 channels recommended).
- Placeholder entries with IDs starting `REPLACE_ME_` are ignored until you supply real channel IDs.
- Batch collection example (run 15 channels at a time):
  ```bash
  python youtube_collector.py --batch-size 15 --batch-index 0 --max-results 5
  ```
- Rotate the `--batch-index` (0, 1, 2, ...) each run to cover every creator without exceeding YouTube API quotas.
