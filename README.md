# AIssistantGM

AI-powered fantasy hockey assistant that aggregates NHL data from multiple sources and provides intelligent roster analysis through a conversational Claude-powered interface.

## What It Does

- **Daily data pipeline** fetches rosters, game logs, season stats, injuries, and fantasy league data from the NHL API, Rotowire, and Fantrax
- **Interactive AI assistant** answers questions about your fantasy team in plain English -- roster analysis, player comparisons, trade targets, waiver pickups, and more
- **SQLite storage** keeps everything local and fast with 10 normalized tables

## Quick Start

```bash
cd nhl-data-pipeline
pip install -r requirements.txt
```

Set up your `.env` (copy from `.env.example`):

```
FANTRAX_USERNAME=your_email@example.com
FANTRAX_PASSWORD=your_password
FANTRAX_LEAGUE_ID=your_league_id
ANTHROPIC_API_KEY=sk-ant-...
```

Run the pipeline and start the assistant:

```bash
python pipeline.py                           # Fetch all data
python -m assistant.main                     # Start the AI assistant
python -m assistant.main --run-pipeline-first # Refresh data, then chat
```

## Project Structure

```
AIssistantGM/
├── nhl-data-pipeline/        # Active development
│   ├── assistant/            # Claude-powered interactive assistant
│   ├── fetchers/             # Data source integrations (NHL, Fantrax, Rotowire)
│   ├── db/                   # SQLite schema and database
│   ├── utils/                # Shared utilities
│   ├── tests/                # pytest test suite
│   ├── pipeline.py           # Daily pipeline orchestrator
│   └── README.md             # Detailed pipeline & assistant docs
│
└── v1/                       # Original Fantrax CLI (archived)
    └── README.md
```

## Pipeline

Runs 6 steps in order, each resilient to individual failures:

| Step | Source | Data |
|------|--------|------|
| rosters | NHL Web API | All 32 team rosters |
| schedules | NHL Web API | Team game schedules |
| gamelogs | NHL Stats API | Per-game player stats |
| seasontotals | NHL Stats API | Season aggregate stats |
| injuries | Rotowire | Current injury report |
| fantrax-league | Fantrax API | Fantasy teams, standings, rosters |

```bash
python pipeline.py --step rosters      # Run a single step
python pipeline.py --summary           # Data summary
python pipeline.py --freshness         # Check data staleness
```

Schedule daily with cron:

```
0 6 * * * cd /path/to/nhl-data-pipeline && python pipeline.py >> logs/cron.log 2>&1
```

## AI Assistant

The assistant uses Claude with 12 specialized tools:

| Category | Tools |
|----------|-------|
| Roster | `get_my_roster`, `get_roster_analysis` |
| Players | `get_player_stats`, `compare_players`, `get_player_trends` |
| Free agents | `search_free_agents` |
| League | `get_league_standings`, `get_schedule_analysis` |
| News & injuries | `get_news_briefing`, `get_injuries` |
| Advanced | `get_trade_targets`, `get_roster_moves` |

Ask things like:
- "Who should I drop to pick up a streamer this week?"
- "Compare McDavid and Draisaitl over the last 14 days"
- "Any buy-low trade targets available?"
- "Show me my roster analysis"

## Tests

```bash
cd nhl-data-pipeline
python -m pytest tests/ -v                  # Unit tests
python -m pytest tests/ -v --integration    # Include integration tests
```

## Tech Stack

- **Python** with `requests`, `rich`, `playwright`
- **Anthropic Claude API** for the AI assistant
- **SQLite** for local data storage
- **NHL Web API + Stats API**, **Rotowire**, **Fantrax** as data sources

## v1 (Archived)

The original Fantrax CLI lives in `v1/`. To run it:

```bash
pip install -e v1/
fantrax --help
```

See [v1/README.md](v1/README.md) for details.
