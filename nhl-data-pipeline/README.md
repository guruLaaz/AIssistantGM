# NHL Data Pipeline

Daily pipeline for fetching, storing, and reporting NHL player statistics and fantasy league data.

## Setup

```bash
pip install -r requirements.txt
```

For Fantrax features (league sync, news scraping), also install Playwright:

```bash
pip install playwright
playwright install chromium
```

## Usage

### Full daily pipeline

```bash
python pipeline.py
```

Runs all default steps in order: rosters → schedules → gamelogs → seasontotals → injuries → fantrax-league.

### Individual steps

```bash
python pipeline.py --step rosters        # Fetch all 32 team rosters
python pipeline.py --step schedules      # Fetch team schedules
python pipeline.py --step gamelogs       # Fetch player game logs (bulk, Stats API)
python pipeline.py --step seasontotals   # Fetch season totals (bulk, Stats API)
python pipeline.py --step injuries       # Fetch Rotowire injury report
python pipeline.py --step fantrax-league # Sync fantasy league teams, standings, rosters
python pipeline.py --step backfill-news  # Scrape Fantrax player news (Playwright)
python pipeline.py --step stats          # Alias: gamelogs + seasontotals
```

> `backfill-news` is **not** included in the default pipeline run. It requires Playwright and must be invoked explicitly.

### Reports

```bash
python pipeline.py --summary            # Print data summary
python pipeline.py --freshness          # Check data staleness (>48h warning)
```

### Options

```bash
python pipeline.py --season 20252026    # Override season (default: auto-detected)
python pipeline.py --verbose            # DEBUG-level logging
python pipeline.py --db path/to/db      # Override database path
```

## Pipeline steps

| Order | Step | Source | Tables updated |
|-------|------|--------|----------------|
| 1 | rosters | NHL Web API | `players` |
| 2 | schedules | NHL Web API | `team_games` |
| 3 | gamelogs | NHL Stats API (bulk) | `skater_stats`, `goalie_stats` |
| 4 | seasontotals | NHL Stats API (bulk) | `skater_stats`, `goalie_stats` |
| 5 | injuries | Rotowire JSON | `player_injuries` |
| 6 | fantrax-league | Fantrax API | `fantasy_teams`, `fantasy_standings`, `fantasy_roster_slots`, `fantasy_gp_per_position`, `fantrax_players` |

The `rosters` step also re-links unmatched news items to players (`player_news`).

Each step continues on failure so one broken step doesn't block the rest.

### On-demand steps

| Step | Source | Tables updated |
|------|--------|----------------|
| backfill-news | Fantrax news page (Playwright) | `player_news` |

`backfill-news` scrapes Rotowire-sourced player news from the Fantrax news page using Playwright with infinite scroll. Not part of the default pipeline.

## Data sources

- **NHL Web API** (`api-web.nhle.com/v1`) — rosters, schedules
- **NHL Stats API** (`api.nhle.com/stats/rest/en`) — game logs, season totals (bulk; includes hits/blocks)
- **Rotowire** (`rotowire.com`) — injury reports (JSON endpoint)
- **Fantrax** (`fantrax.com`) — fantasy league data and player news
  - League API (`fantrax.com/fxpa/req`) — standings, team rosters, fantasy points
  - News page (`fantrax.com/news/nhl/player-news`) — aggregates Rotowire player news
  - Requires cookie-based authentication via Playwright browser login (see Configuration)

## Configuration

Fantrax features require a `.env` file in the project root:

```
FANTRAX_USERNAME=your_email@example.com
FANTRAX_PASSWORD=your_password
FANTRAX_LEAGUE_ID=your_league_id
FANTRAX_COOKIE_FILE=fantraxloggedin.cookie   # optional, this is the default
```

On first run, Playwright logs into Fantrax and saves session cookies to the cookie file. Subsequent runs reuse the cookies until they expire.

The interactive assistant requires an Anthropic API key:

```
ANTHROPIC_API_KEY=sk-ant-...
ASSISTANT_MODEL=claude-opus-4-6            # optional (default: claude-sonnet-4-20250514)
BRAVE_SEARCH_API_KEY=BSA...               # optional, enables web_search tool
```

### Running the assistant

```bash
python -m assistant.main                      # Start the assistant
python -m assistant.main --run-pipeline-first  # Refresh data, then start
```

Launches an interactive chat loop powered by Claude with tool use. Select your fantasy team, then ask questions in plain English.

The assistant has **12 tools** it can call autonomously:

| Category | Tools |
|----------|-------|
| Roster | `get_my_roster`, `get_roster_analysis` |
| Players | `get_player_stats`, `compare_players`, `get_player_trends` |
| Free agents | `search_free_agents`, `get_pickup_recommendations` |
| League | `get_league_standings`, `get_schedule_analysis` |
| News & injuries | `get_news_briefing`, `get_injuries` |
| Search | `web_search` |

Pickup recommendations pair the best free agents with your weakest rostered players by position, showing the FP/G upgrade for each swap. Salary data is included for all players (rostered and free agents).

Conversation context is automatically trimmed when approaching the 100k token limit.

## Database

SQLite at `db/nhl_data.db` with 13 tables:

- `players` — NHL roster players (id, name, team, position, rotowire_id)
- `skater_stats` — per-game and season total stats for skaters
- `goalie_stats` — per-game and season total stats for goalies
- `team_games` — team schedule / game history
- `player_news` — player news items (Rotowire and Fantrax sources)
- `player_injuries` — injury report data
- `pipeline_log` — step execution timestamps (for freshness checks)
- `fantasy_teams` — Fantrax league teams (id, league_id, name, short_name, logo_url)
- `fantasy_standings` — league standings per team (rank, W/L, points, FP for/against, claims remaining, etc.)
- `fantasy_roster_slots` — player roster assignments per fantasy team (player_name, position, salary, fantasy points)
- `fantasy_gp_per_position` — actual fantasy games-played per position group (F/D/G) per team, from Fantrax
- `fantrax_players` — all ~8,400 players with Fantrax ID, name, team, position, and NHL salary
- `line_combinations` — even-strength lines, PP units, PK units per team (from DailyFaceoff via Rotowire)

## Data quirks & known limitations

- **Hits/blocks available everywhere.** Both per-game and season total rows now include hits and blocks via the NHL Stats API. Per-game gamelogs are fetched in monthly chunks (Oct-Apr) to stay under the Stats API's 10K-row cap. Season-level hits/blocks in the assistant come from the season totals row.
- **TOI stored as seconds.** Time-on-ice is stored as integer seconds in the DB, not `"MM:SS"` strings. Use `utils/time.py` (`toi_to_seconds`, `seconds_to_toi`) for conversion.
- **Fantrax ≠ NHL player IDs.** Fantrax player IDs are opaque strings (e.g. `048w3`), NHL player IDs are integers (e.g. `8479638`). Linking is done via player name matching with accent normalization.
- **Mixed news sources.** The `player_news` table stores news from both Rotowire and Fantrax. Fantrax items use `fx_`-prefixed IDs in the `rotowire_news_id` column.

## Cron

Run daily at 6 AM:

```
0 6 * * * cd /path/to/nhl-data-pipeline && python pipeline.py >> logs/cron.log 2>&1
```

## Tests

```bash
python -m pytest tests/ -v                  # Unit tests
python -m pytest tests/ -v --integration    # Include integration tests
```

## Project structure

```
nhl-data-pipeline/
├── assistant/
│   ├── __init__.py
│   ├── client.py              # Claude API client with tool-use loop
│   ├── formatters.py          # Terminal-friendly output formatters
│   ├── main.py                # CLI entry point for the assistant
│   ├── player_match.py        # Fuzzy player name resolution (Fantrax ↔ NHL)
│   ├── queries.py             # Data query layer (roster, free agents, trends)
│   ├── scoring.py             # Fantasy point calculations
│   ├── system_prompt.txt      # System prompt template for Claude
│   └── tools.py               # Claude tool definitions and dispatch
├── db/
│   ├── __init__.py
│   ├── schema.py              # Database initialization and player upsert
│   └── nhl_data.db            # SQLite database
├── fetchers/
│   ├── __init__.py
│   ├── fantrax_league.py      # Fantrax league/standings/roster fetcher
│   ├── fantrax_news.py        # Fantrax player news scraper (Playwright)
│   ├── nhl_api.py             # NHL Web API + Stats API fetcher
│   └── rotowire.py            # Rotowire injuries fetcher
├── utils/
│   ├── __init__.py
│   └── time.py                # TOI conversion utilities
├── tests/
│   ├── __init__.py
│   ├── conftest.py            # Pytest config (integration marker)
│   ├── test_fantrax_league.py
│   ├── test_fantrax_news.py
│   ├── test_nhl_api.py
│   ├── test_pipeline.py
│   ├── test_player_match.py
│   ├── test_queries.py
│   ├── test_rotowire.py
│   ├── test_schema.py
│   ├── test_scoring.py
│   ├── test_time.py
│   └── test_advanced_queries.py
├── docs/
│   └── rotowire_endpoints.md  # Rotowire API endpoint notes
├── logs/
│   └── pipeline.log           # Pipeline execution log
├── league_rules.md            # Fantrax league rules reference
├── pipeline.py                # Daily pipeline orchestrator + CLI
├── requirements.txt
└── README.md
```
