# NHL Data Pipeline

Daily pipeline for fetching, storing, and reporting NHL player statistics.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

### Full daily pipeline

```bash
python pipeline.py
```

### Individual steps

```bash
python pipeline.py --step rosters       # Fetch all 32 team rosters
python pipeline.py --step schedules     # Fetch team schedules
python pipeline.py --step gamelogs      # Fetch player game logs
python pipeline.py --step seasontotals  # Fetch season totals from landing pages
python pipeline.py --step news          # Fetch Rotowire RSS news
python pipeline.py --step injuries      # Fetch Rotowire injury report
python pipeline.py --step stats         # Alias: gamelogs + seasontotals
```

### Reports

```bash
python pipeline.py --summary            # Print data summary
python pipeline.py --freshness          # Check data staleness (>48h warning)
```

### Options

```bash
python pipeline.py --season 20242025    # Override season (default: auto-detected)
python pipeline.py --verbose            # DEBUG-level logging
python pipeline.py --db path/to/db      # Override database path
```

## Pipeline steps

| Order | Step | Source | Tables updated |
|-------|------|--------|----------------|
| 1 | rosters | NHL Web API | `players` |
| 2 | schedules | NHL Web API | `team_games` |
| 3 | gamelogs | NHL Web API | `skater_stats`, `goalie_stats` |
| 4 | seasontotals | NHL Web API | `skater_stats`, `goalie_stats` |
| 5 | news | Rotowire RSS | `player_news` |
| 6 | injuries | Rotowire JSON | `player_injuries` |

Each step continues on failure so one broken step doesn't block the rest.

## Data sources

- **NHL Web API** (`api-web.nhle.com/v1`) — rosters, game logs, season totals, schedules
- **Rotowire** (`rotowire.com`) — player news (RSS), injury reports (JSON)

## Database

SQLite at `db/nhl_data.db` with tables:

- `players` — NHL roster players (id, name, team, position)
- `skater_stats` — per-game and season total stats for skaters
- `goalie_stats` — per-game and season total stats for goalies
- `team_games` — team schedule / game history
- `player_news` — Rotowire RSS news items
- `player_injuries` — injury report data
- `pipeline_log` — pipeline execution timestamps (for freshness checks)

## Cron

Run daily at 6 AM:

```
0 6 * * * cd /path/to/nhl-data-pipeline && python pipeline.py >> logs/cron.log 2>&1
```

## Tests

```bash
python -m pytest tests/ -v
```

## Project structure

```
nhl-data-pipeline/
├── db/
│   ├── schema.py          # Database initialization and player upsert
│   └── nhl_data.db        # SQLite database
├── fetchers/
│   ├── nhl_api.py         # NHL Web API fetcher
│   └── rotowire.py        # Rotowire news/injuries fetcher
├── utils/
│   └── time.py            # TOI conversion utilities
├── tests/
│   ├── test_schema.py     # DB schema tests
│   ├── test_nhl_api.py    # NHL API fetcher tests
│   ├── test_rotowire.py   # Rotowire fetcher tests
│   ├── test_time.py       # TOI conversion tests
│   └── test_pipeline.py   # Pipeline orchestrator tests
├── pipeline.py            # Daily pipeline orchestrator + CLI
├── logs/
│   └── pipeline.log       # Pipeline execution log
├── requirements.txt
└── README.md
```
