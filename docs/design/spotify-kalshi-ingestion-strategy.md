# Spotify #1 Nowcast System (Kalshi) — Design Doc

## 1) Goal & Problem Statement
Build a reliable real-time ingestion and scoring system that **nowcasts** the next day’s Spotify Daily Top Song (US or Global, depending on the market) to gain an edge in Kalshi markets. Spotify’s chart is a **lagging, filtered** metric released the following morning, so the system must assemble a **proxy index** from multiple real-time signals.

## 2) Non-AWS Infrastructure Stack (Railway-first)
You asked to avoid AWS. Below are **modern, reliable alternatives** that fit Railway and are simple to operate.

### 2.1 Suggested Platform Architecture
- **Compute:** Railway services
  - Python or Go ingestion workers
  - Cron-based jobs for pollers
- **Queue / Buffer:**
  - **Upstash Redis** (Redis Streams + consumer groups)
  - **Kafka on Confluent Cloud** (if you want higher throughput)
- **Storage (time-series):**
  - **TimescaleDB** (on Railway Postgres + Timescale extension)
  - **ClickHouse Cloud** (excellent for analytics/velocity queries)
- **Object storage:**
  - **Cloudflare R2** (cheap, S3-compatible)
  - **Backblaze B2** (also S3-compatible)
- **Scheduling / Orchestration:**
  - Railway cron + simple “day-turn” runner
  - Optional: **Prefect Cloud** or **Dagster Cloud** if you want DAGs
- **Observability:**
  - **Grafana Cloud** (metrics + dashboards)
  - **BetterStack** or **Sentry** (errors + logs)

### 2.2 Why this works without AWS
- Railway handles compute and deployment with minimal ops.
- Upstash gives you a managed queue with low overhead.
- TimescaleDB/ClickHouse handle velocity queries needed to rank songs based on relative momentum.

## 3) Data Architecture (Ingestion → Normalization → Scoring)

### 3.1 High-level pipeline
```
Pollers (cron jobs) → Queue (Redis Streams / Kafka) → Normalizer → Time-series DB → Scoring Engine → “Buy/Sell” signals
```

### 3.2 Poller types
- **High-frequency pollers** (every 5–15 minutes):
  - Spotify editorial playlist changes (TTH etc.)
  - Apple Music Top 100 updates
  - TikTok trend songs
- **Medium-frequency pollers** (hourly):
  - YouTube Music charts
  - Shazam charts
  - Last.fm scrobbles
- **Low-frequency pollers** (daily):
  - New releases + label/artist metadata
  - Press/news signals

### 3.3 Normalization Layer
Normalize all inputs to **Spotify track IDs**:
- Title/artist fuzzy match + Spotify search API
- Store canonical metadata (ISRC if available)

## 4) Core Algorithm / Strategy
This system isn’t about “predicting charts” directly. It **nowcasts** tomorrow’s Spotify ranking using proxy momentum.

### 4.1 Signal categories
**A) Editorial Placement (High Weight)**
- Spotify “Today’s Top Hits” playlist position changes.
- Heavily weighted because editorial placement drives massive daily streams.

**B) Apple Music Velocity (High Weight)**
- Apple Music charts update frequently and correlate well with Spotify trends.
- Use **rank velocity**: positions gained per hour.

**C) Viral Signals (Medium Weight)**
- TikTok trending audio list (songs that spike here often surge on Spotify).
- Shazam chart movements (strong for “breakout” songs).

**D) Real-time sampling (Medium Weight)**
- Last.fm scrobble velocity (sampled but real-time).

**E) Prior / Release Features (Medium Weight)**
- Artist/label historical chart performance.
- Release timing + album hype + pre-save signals.

### 4.2 Scoring approach
Start with a **weighted rules engine** (fast, explainable), then evolve to ML if needed.

Example score:
```
score = w1 * SpotifyEditorialBoost
      + w2 * AppleMusicVelocity
      + w3 * TikTokVelocity
      + w4 * LastFmVelocity
      + w5 * PriorArtistStrength
```

### 4.3 Scenario strategy
- **Friday Debut**: prioritize editorial changes + Apple Music jumps.
- **Mid-week Viral Climber**: prioritize TikTok + Shazam + Apple Music jumps.

## 5) Additional Data Sources (Beyond initial list)
These increase coverage and reduce blind spots:

### 5.1 Shazam Charts
Shazam often leads Spotify when a song is exploding virally.
- Especially effective for mid-week or organic breakout tracks.

### 5.2 YouTube Music / YouTube Trending
YouTube’s music trending charts are a good proxy for global virality.

### 5.3 SoundCloud Trending
Indicates underground or early breakout, especially for hip-hop.

### 5.4 Music Industry News & Release Calendars
Good for **release priors** rather than immediate real-time signals.
- **Hits Daily Double** (industry sales + buzz)
- **Billboard** (industry reporting + upcoming release headlines)
- **Hypebot** (industry + streaming news)
- **Music Business Worldwide** (label/artist announcements)

### 5.5 SiriusXM / Radio Spins (optional)
Radio drives legacy pop and country streams later in the day.

## 6) Reliability & Anti-Fragility
To be “reliable,” this system must continue operating when sources break.

- Multiple sources for each signal class.
- Rate limiting & retries with exponential backoff.
- Schema versioning for scrapers.
- Source health monitoring and fallback scoring rules.
- Cache “last known” signal and decay over time.

## 7) Day-Turn / Market Alignment
- This system targets the **US Spotify Daily chart** (Kalshi settlement basis).
- Align calculations with **Spotify chart cutoff** (typically midnight US Eastern).
- Run a “day-turn” job at cutoff to finalize daily metrics.
- Ensure all timestamps are normalized to cutoff timezone.

## 8) Trading/Execution Strategy (Kalshi)
The strategy is **speed + signal clarity** with **expected value (EV)** as the default objective:

- **Objective:** maximize EV over pure hit rate (take fewer, higher-confidence trades when the market is mispriced).
- Detect editorial playlist changes within minutes.
- Identify Apple Music / TikTok velocity spikes before market reacts.
- Use score thresholds to trigger buys only when the implied probability is meaningfully below your model’s probability.
- Hedge with top 2–3 candidates if volatility is high and pricing is favorable.

If you later decide to optimize for a different objective (e.g., win rate for reputation or low variance), it’s a parameter change to the thresholding logic, not a re-architecture.

## 9) Roadmap
**Phase 1:**
- Apple Music, Spotify TTH playlist, TikTok trending, Last.fm

**Phase 2:**
- Shazam, YouTube trending, industry news ingestion

**Phase 3:**
- ML-based scoring with historical backtests

## 10) Open Questions (Resolved + Constraints)
- **Market scope:** US Spotify Daily chart only.
- **Optimization target:** default to **expected value (EV)**; adjust to win rate if you want lower variance.
- **Third-party data:** assume **no paid sources** for now; prioritize free public signals.

## 11) Free / Low-Cost Data Sources (Priority List)
If you want reliable signals without paying for data, prioritize these:
- **Apple Music Charts** (public chart pages / RSS).
- **Spotify Web API** (editorial playlist positions).
- **TikTok Creative Center** (trending songs).
- **Shazam Charts** (public charts).
- **YouTube Music / Trending** (public charts).
- **Last.fm API** (scrobble sampling).
- **Industry news & release calendars** (free headlines + schedules):
  - Hits Daily Double
  - Billboard
  - Hypebot
  - Music Business Worldwide
