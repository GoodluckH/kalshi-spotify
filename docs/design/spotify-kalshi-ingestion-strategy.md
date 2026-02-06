# Spotify #1 Nowcast System (Kalshi) — Design Doc

## 1) Kalshi Market Rules & Settlement

Everything in this system is built around how Kalshi settles these contracts. Get this wrong and nothing else matters.

### 1.1 Settlement source
- **Source**: [charts.spotify.com](https://charts.spotify.com) (web charts only, NOT the mobile app)
- **Chart**: Daily Top Songs USA
- **Rule format**: _"If [Song] is #1 on the Daily Top Songs USA chart on the chart dated [Date], then the market resolves to Yes."_
- **Disclaimer**: Markets are not endorsed by Spotify.

### 1.2 Key timestamps (all UTC)

```
Streaming day (e.g., Feb 6):
  00:00 UTC ──────────────────────────── 23:59 UTC
  │  Spotify counts all US streams during this window  │
  └────────────────────────────────────────────────────┘

Trading window:
  Market opens (morning of Feb 6) ──── 04:59 UTC Feb 7 (close)
                                        │
                                        └─ This is 11:59 PM EST on Feb 6

Chart publication:
  Typically publishes by ~22:00 UTC Feb 7 (6 PM EST next day)
  Expiration backstop: 15:00 UTC ~2 weeks later (e.g., Feb 20)

Settlement:
  Within 5 minutes of Kalshi determining the outcome
  (settlement_timer_seconds = 300)
```

### 1.3 What this means for us
1. **Trading closes ~5 hours after streaming ends.** From 00:00 UTC (midnight) to 04:59 UTC, the streaming day is over but you can still trade. This is a window where you have near-complete information (a full day of signals) and can still place bets.
2. **The chart publishes AFTER trading closes.** You never see the official result before the market closes. The entire game is estimation.
3. **USA vs Global are separate markets** with different tickers and potentially different #1 songs. We target USA (`KXSPOTIFYD`).

## 2) Goal & Problem Statement

Build a reliable data ingestion and scoring system that estimates the current day's Spotify Daily Top Song (USA) before the chart publishes, to identify mispriced contracts on Kalshi.

Spotify's chart is a **lagging** metric — streams happen today, the chart publishes tomorrow afternoon. The system assembles a **proxy estimate** from real-time and near-real-time signals to price the outcome before the market does.

## 3) Infrastructure

The data volume is tiny (~200 chart rows/day, a handful of scraper outputs per hour). The stack should reflect that.

### 3.1 Stack

| Layer | Choice | Why |
|-------|--------|-----|
| Compute | **Railway** (or a $5/mo Hetzner VPS) | Simple deploys, cron support, no ops |
| Database | **Postgres** (single instance on Railway) | Handles everything — chart history, scraper output, scores, market prices |
| Scheduling | **Cron** (Railway cron or system crontab) | No orchestration framework needed at this scale |
| Language | **Python** | Best library ecosystem for scraping + Spotify API + Kalshi API |
| Alerts | **Discord or Telegram webhook** | Push notifications when the model spots a trade |
| Error tracking | **Sentry free tier** | Know when a scraper breaks |

### 3.2 What we do NOT need
- **Message queues** (Redis Streams, Kafka) — scrapers write directly to Postgres. There's no throughput problem to solve.
- **Time-series databases** (TimescaleDB, ClickHouse) — regular Postgres with timestamps and indexes handles this fine.
- **Object storage** (R2, B2) — chart data is small JSON/CSV, it lives in the database.
- **DAG orchestrators** (Prefect, Dagster) — cron + a simple Python runner is sufficient.
- **Separate analytics DB** — Postgres handles both OLTP and the lightweight analytics queries we need.

### 3.3 Architecture

```
┌─────────────────────────────────────────────────┐
│                  Cron Scheduler                  │
│  (hourly, every 4h, daily — depends on source)  │
└──────────────┬──────────────────────────────────┘
               │
               ▼
┌──────────────────────────┐
│      Data Collectors     │
│                          │
│  spotify_playlist.py     │  ← Spotify Web API (reliable)
│  apple_charts.py         │  ← Scrape (fragile)
│  shazam_charts.py        │  ← Semi-public API (moderate)
│  tiktok_trending.py      │  ← Scrape Creative Center (fragile)
│  youtube_charts.py       │  ← Scrape (moderate)
│  new_releases.py         │  ← Spotify API (reliable)
│  kalshi_prices.py        │  ← Kalshi API (reliable)
│  official_chart.py       │  ← charts.spotify.com (reliable)
└──────────────┬───────────┘
               │  write directly
               ▼
┌──────────────────────────┐
│     Postgres Database    │
│                          │
│  charts_history          │  ← historical daily charts + stream counts
│  playlist_snapshots      │  ← TTH position changes over time
│  cross_platform_ranks    │  ← Apple, Shazam, TikTok, YouTube
│  artist_profiles         │  ← label, historical peak, avg first-day streams
│  kalshi_market_prices    │  ← contract prices over time
│  track_mapping           │  ← ISRC + cross-platform ID resolution
│  scores                  │  ← model output per track per day
└──────────────┬───────────┘
               │
               ▼
┌──────────────────────────┐
│     Scoring Engine       │
│  (runs after each pull)  │
│                          │
│  incumbent prior         │
│  + signal adjustments    │
│  → probability estimate  │
│  → compare to Kalshi     │
│  → trade / no-trade      │
└──────────────┬───────────┘
               │
               ▼
┌──────────────────────────┐
│     Execution Layer      │
│                          │
│  Kalshi API client       │
│  Discord/Telegram alert  │
└──────────────────────────┘
```

## 4) Data Sources

### 4.1 Source reliability tiers

Every source falls into one of two categories. This matters for engineering — API sources get simple HTTP clients, scrape sources get browser automation or HTML parsers that WILL break and need monitoring.

| Source | Method | Reliability | Update frequency | Signal value |
|--------|--------|-------------|-----------------|--------------|
| Spotify Web API (playlists) | Official API | High | Hourly polls | High |
| Spotify official chart | Scrape charts.spotify.com | High | Daily (next day) | Ground truth |
| Kalshi API | Official API | High | Continuous | High (market consensus) |
| Kworb.net | Scrape | Moderate | Daily | High (historical stream counts) |
| Apple Music charts | Scrape web page | Low-moderate | Updates few times/day | High |
| Shazam charts | Undocumented API | Moderate | Updates few times/day | Medium |
| TikTok Creative Center | Scrape / reverse-engineer | Low | ~Daily snapshots | Medium |
| YouTube Music charts | Scrape | Moderate | Updates few times/day | Medium |
| New release calendars | Spotify API + scrape | Moderate | Daily | Medium (priors) |

**Cut from scope:**
- ~~Last.fm scrobbles~~ — tiny, demographically skewed user base. Not predictive for US Spotify #1.
- ~~SoundCloud Trending~~ — minimal overlap with Spotify chart movers.
- ~~Radio spins (SiriusXM/Mediabase)~~ — paywalled and lagging.

### 4.2 Polling schedule

```
Every hour:
  - Spotify "Today's Top Hits" playlist snapshot (API)
  - Apple Music US Top 100 (scrape)
  - Shazam US Top 200 (scrape)
  - Kalshi market prices for active contracts (API)

Every 4-6 hours:
  - TikTok Creative Center trending songs (scrape)
  - YouTube Music US Top 100 (scrape)

Daily (early morning UTC):
  - Previous day's official Spotify chart + stream counts (charts.spotify.com)
  - New releases on Spotify (API — check what dropped today)
  - Kworb.net daily chart archive (scrape, for stream count history)

Daily (around 00:00 UTC — start of new chart day):
  - "Day-turn" job: finalize prior day's signals, compute opening estimate for new day
```

### 4.3 Track entity resolution

Matching a song across platforms is the hardest data engineering problem here.

**Strategy (in priority order):**
1. **ISRC** (International Standard Recording Code) — the universal track identifier. Spotify API exposes it. If Apple/Shazam expose it, match on this. Most reliable.
2. **Spotify track ID as canonical** — everything maps to a Spotify track ID. Use Spotify search API as the resolver.
3. **Fuzzy match fallback** — normalize title (strip "(feat. X)", "(Official Video)", etc.) + normalize artist name → fuzzy match. Accept only high-confidence matches (>0.9 similarity).
4. **Manual override table** — for known mismatches, maintain a small lookup table.

Store all mappings in `track_mapping` table so you only resolve once per track.

## 5) Core Algorithm

### 5.1 The incumbent prior (most important signal)

On any given day, the **strongest predictor of today's #1 is yesterday's #1**. Most days are boring — the same song holds #1 for days or weeks. The model starts here.

```
Base probability:
  If song was #1 yesterday:
    p_base = f(streak_length, day_of_week, is_new_release_friday)

  Typical priors:
    - #1 for 1 day, it's Tuesday:    p ≈ 0.80
    - #1 for 7 days, it's Tuesday:   p ≈ 0.75
    - #1 for 1 day, it's Friday:     p ≈ 0.45 (new releases compete)
    - #1 for 30 days, it's Friday:   p ≈ 0.35 (decaying + fresh competition)
```

These priors come from **historical backtest** on the chart data. Before building anything else, compute these from the Kworb historical data.

### 5.2 Signal adjustments

Each signal adjusts the base probability up or down.

**A) Spotify Editorial Placement (high weight)**
- Track position in "Today's Top Hits" (37M+ followers). Position #1 on TTH ≈ massive stream boost.
- Signal: did the incumbent move down on TTH? Did a challenger move up?
- Quantify: position change → estimated stream delta (calibrate from historical data).

**B) Apple Music chart velocity (high weight)**
- Apple Music US Top 100 updates several times per day and correlates well with Spotify.
- Signal: rank delta over the last 6-12 hours. A song jumping 20+ spots on Apple is likely surging on Spotify too.

**C) Shazam momentum (medium weight)**
- Shazam is a leading indicator — people Shazam songs before they stream them.
- Signal: Shazam rank delta. Most useful for breakout detection, less useful for incumbent stability.

**D) TikTok virality (medium weight, Friday-biased)**
- A song trending on TikTok can explode on Spotify within 24-48 hours.
- Signal: presence in TikTok trending + velocity. More useful for identifying potential disruptors than for confirming incumbents.

**E) New release features (high weight on Fridays)**
- Major artist dropping a new album/single on Friday? Check:
  - Artist's historical first-day stream count (from `artist_profiles`)
  - Label (major label releases get more editorial push)
  - Whether the track was added to TTH or other major playlists
- This is your original idea — scout releases and research the artist's track record.

**F) Day-of-week effects (context signal)**
- Fridays: high volatility, new releases dominate
- Sat-Sun: streams dip overall, but relative ranks are sticky
- Mon-Thu: low volatility, incumbent advantage is strongest

### 5.3 Scoring approach

Start with a **weighted rules engine**. Do NOT jump to ML — you don't have enough labeled training data yet, and rules are explainable and debuggable.

```
For each candidate song:
  p_estimate = incumbent_prior                          # start with base rate
             + w1 * tth_position_signal                 # editorial boost/decay
             + w2 * apple_velocity_signal               # cross-platform momentum
             + w3 * shazam_velocity_signal              # breakout detection
             + w4 * tiktok_signal                       # viral potential
             + w5 * new_release_artist_strength_signal   # release day factor

  Clamp p_estimate to [0.01, 0.99]
  Normalize across all candidates so probabilities sum to 1
```

Calibrate weights by backtesting against historical chart data.

### 5.4 Trade decision

```
For each candidate song with Kalshi contract:
  model_probability = p_estimate
  market_probability = kalshi_yes_price  (e.g., $0.70 = 70%)

  edge = model_probability - market_probability

  If edge > threshold (e.g., 0.10):
    → BUY YES (you think it's more likely than market does)

  If edge < -threshold:
    → BUY NO / sell (you think it's less likely than market does)

  Otherwise:
    → No trade (edge too small to overcome fees + uncertainty)
```

### 5.5 Scenario playbook

| Scenario | Key signals | Strategy |
|----------|------------|----------|
| **Boring Tuesday** | Incumbent held #1 for 5 days, no new releases | Incumbent prior is ~80%. If market prices it at 65%, buy YES. |
| **Friday new release** | Major artist drops album, gets TTH #1 slot | Check artist's historical first-day streams. If they typically debut at #1 with 8M+ streams, and market has them at 40%, buy YES. |
| **Mid-week viral surge** | Song jumps 30 spots on Apple, appears on Shazam top 10 | Challenger has momentum. If market hasn't moved yet, buy YES on challenger and/or NO on incumbent. |
| **Close race** | Top 2 songs within ~5% of each other on Apple | High uncertainty. Either avoid or hedge by buying YES on both at favorable prices. |

## 6) Reliability & Failure Handling

### 6.1 Scraper resilience
- Every scraper writes to a `scraper_health` table: `{source, last_success, last_failure, consecutive_failures}`.
- If a scraper fails 3 consecutive times, fire a Sentry alert + Discord notification.
- Scoring engine checks source freshness: if Apple Music data is >6 hours stale, reduce its weight to zero and rely on remaining signals.

### 6.2 Source fallback hierarchy
- If Apple Music scraper breaks → lean more on Shazam + Spotify TTH
- If Shazam breaks → lean more on Apple Music + TTH
- If TikTok breaks → it's a bonus signal anyway, score without it
- If Spotify API breaks → this is critical. Alert immediately. Fall back to scraping charts.spotify.com directly.
- If Kalshi API breaks → you can't trade anyway. Alert and wait.

### 6.3 Data integrity
- Schema versioning: every scraper output is stored with a `schema_version` field. When a source changes its HTML/API format, bump the version and update the parser.
- Idempotent writes: re-running a scraper for the same timestamp overwrites (upsert), doesn't duplicate.
- All timestamps stored in UTC. No timezone conversion at write time — only at display/comparison time.

## 7) Day-Turn Lifecycle

Aligned to the Kalshi market lifecycle for the Feb 6 chart date:

```
~06:00 UTC Feb 6     Market opens for Feb 6 chart
                     → Run opening estimate (incumbent prior + any overnight signals)
                     → Check if any overnight releases dropped

~12:00 UTC Feb 6     Mid-day signal check
                     → Fresh Apple Music, Shazam, TTH snapshots
                     → Update estimate, compare to Kalshi prices
                     → Alert if edge detected

~18:00 UTC Feb 6     Afternoon signal check
                     → Another round of cross-platform data
                     → US is fully awake, streaming patterns visible
                     → Update estimate

~00:00 UTC Feb 7     Streaming day ends
                     → Final signal collection
                     → Compute end-of-day estimate with full day's data

~04:00 UTC Feb 7     Last trading window
                     → Trading closes at 04:59 UTC
                     → This is the highest-information, lowest-time window
                     → If model has strong conviction, execute final trades

~22:00 UTC Feb 7     Chart publishes (typical)
                     → Record actual result
                     → Compare to model estimate (track accuracy)
                     → Feed back into weight calibration
```

## 8) Historical Data Strategy (Phase 0)

Before building any signals or scoring, you need historical ground truth.

### 8.1 Backfill
- **Source**: Kworb.net daily US chart archives (has stream counts going back years).
- **Goal**: 1-2 years of daily #1 data with stream counts.
- **Derived tables**:
  - `incumbent_transition_rates`: P(stays #1 | streak=N, day_of_week=D)
  - `artist_profiles`: avg first-day streams, peak chart position, label, genre
  - `day_of_week_effects`: avg #1 stream count by day, avg #1 turnover rate by day
  - `friday_release_outcomes`: for each major Friday release, did it take #1? How many streams?

### 8.2 Ongoing collection
- Every day after the chart publishes: append to `charts_history` with stream counts.
- Continuously update `artist_profiles` as new data comes in.

## 9) Roadmap

**Phase 0 — Historical foundation (week 1)**
- Backfill 1-2 years of US daily chart data from Kworb
- Build `artist_profiles` and `incumbent_transition_rates` tables
- Compute baseline priors (incumbent win rate by day-of-week, streak length)

**Phase 1 — Core loop (weeks 2-3)**
- Spotify Web API integration (TTH playlist snapshots, new release detection)
- Apple Music chart scraper
- Kalshi API integration (market price ingestion)
- Official chart scraper (charts.spotify.com, for daily ground truth)
- Scoring engine v1 (incumbent prior + editorial + Apple velocity)
- Discord/Telegram alerts when edge > threshold
- Manual trading based on alerts

**Phase 2 — Expand signals (weeks 4-5)**
- Shazam chart scraper
- TikTok Creative Center scraper
- YouTube Music chart scraper
- Add these signals to scoring engine
- Backtest scoring weights against Phase 0 historical data

**Phase 3 — Automate & refine (weeks 6+)**
- Automated order placement via Kalshi API
- Position sizing based on edge magnitude and bankroll
- Simple dashboard (can be a CLI table or basic web UI)
- Track model accuracy over time, adjust weights
- Consider ML-based scoring if rule-based approach plateaus

## 10) Constraints & Decisions

- **Market scope**: US Spotify Daily chart only (`KXSPOTIFYD` series).
- **Optimization target**: Expected value (EV). Take fewer, higher-confidence trades.
- **Data sources**: Free/public only. No paid data providers.
- **Stack**: Python + Postgres + cron. No queues, no orchestrators, no separate analytics DBs.
- **Settlement source**: charts.spotify.com web charts (not mobile app).
- **All timestamps in UTC** — Spotify chart day is UTC, Kalshi close times are UTC.

## 11) Open Questions

- **Kalshi liquidity**: How thick are the order books? Thin markets mean slippage and may limit position sizes. Need to monitor.
- **Kalshi fee structure**: What are the per-contract fees? This sets the minimum edge threshold for a trade to be profitable.
- **Apple Music scraping stability**: No official API. How often does the page structure change? Need to monitor and have a backup parser.
- **ISRC coverage**: Do Apple Music and Shazam expose ISRCs in their public-facing data? If not, fuzzy matching is the bottleneck.
- **Chart corrections**: Does Spotify ever revise a chart after publication? If so, which version does Kalshi use?
