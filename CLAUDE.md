# CLAUDE.md

This file provides context for Claude Code when working on this project.

> **Note:** See [README.md](README.md) for project overview, installation, usage examples, and troubleshooting.

## Project Structure

```
AIssistantGM/
├── aissistant_gm/                  # Main package
│   ├── __init__.py
│   └── fantrax/                    # Fantrax CLI module
│       ├── __init__.py
│       ├── __main__.py
│       ├── cli.py
│       ├── config.py
│       ├── auth.py
│       ├── cache.py
│       ├── database.py
│       ├── display.py
│       ├── stats.py
│       ├── sync.py
│       ├── commands/
│       │   └── (teams.py, roster.py, players.py, sync.py, news.py)
│       └── fantraxapi/             # Embedded API wrapper
│           ├── api.py
│           ├── exceptions.py
│           └── objs/
├── tests/
│   └── fantrax/                    # Unit tests
├── integration_tests/
│   └── fantrax/                    # Integration tests
└── pyproject.toml
```

## Tech Stack

- **Python 3.8+** (3.11+ recommended for fantraxapi compatibility)
- **typer** - CLI framework
- **rich** - Terminal formatting and tables
- **fantraxapi** - Embedded Fantrax API wrapper
- **selenium** + **webdriver-manager** - Browser authentication
- **sqlite3** - Local database caching
- **pytest** - Testing framework

## Key Conventions

### Code Style
- **Type hints** used throughout
- **Dataclasses** for data structures (Config, CacheResult)
- **Context managers** for database connections
- **snake_case** for functions, **PascalCase** for classes

### Naming
- Commands: `<entity>_command` (e.g., `teams_command`)
- Format functions: `format_<entity>_<format>()` (e.g., `format_teams_table`)
- Private functions: prefix with `_`
- CLI options: kebab-case (`--last-n-days`, `--no-cache`)

### Output Formats
Three formats supported consistently: `table` (Rich), `json`, `simple` (plain text)

### Database
- SQLite with schema versioning (current: v2)
- Tables recreated on schema version mismatch
- Cache freshness tracked per data type (standings: 12h, teams: 1 week, etc.)

## Important Patterns

### Authentication Flow
1. Check for cached cookies in `~/.fantrax_cookies_<league>.json`
2. If missing/invalid, launch headless Chrome via Selenium
3. Perform login, save cookies
4. Monkey-patch `api.request` to inject cookies automatically

### API Monkey-Patching
The embedded `fantraxapi` library has a bug with `scoringPeriodList`. See [teams.py:62-99](aissistant_gm/fantrax/commands/teams.py#L62-L99) for the monkey-patch that handles this.

### Rate Limiting
Minimum 1 second between API requests (configurable via `FANTRAX_MIN_REQUEST_INTERVAL`).

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| FANTRAX_USERNAME | Yes | Fantrax account email |
| FANTRAX_PASSWORD | Yes | Fantrax account password |
| FANTRAX_LEAGUE_ID | Yes | Default league ID |
| FANTRAX_DB_PATH | No | SQLite database path |
| FANTRAX_COOKIE_FILE | No | Cookie cache location |
| FANTRAX_CACHE_ENABLED | No | Enable/disable caching |
| FANTRAX_CACHE_MAX_AGE_HOURS | No | Default cache duration |
| FANTRAX_MIN_REQUEST_INTERVAL | No | Seconds between API calls |
| FANTRAX_FA_NEWS_LIMIT | No | Max FA players to sync news for (default: 500) |
| FANTRAX_FA_FETCH_LIMIT | No | Max free agents to fetch (default: 5000) |
| FANTRAX_SYNC_DAYS_SCORES | No | Days of daily scores to sync (default: 35) |
| FANTRAX_MAX_NEWS_PER_PLAYER | No | Max news items per player (default: 30) |
| FANTRAX_SELENIUM_TIMEOUT | No | Selenium wait timeout in seconds (default: 10) |
| FANTRAX_LOGIN_WAIT_TIME | No | Wait time after login in seconds (default: 5) |
| FANTRAX_BROWSER_WINDOW_SIZE | No | Chrome window size (default: 1920,1600) |
| FANTRAX_USER_AGENT | No | Browser user-agent string |

## Coding

### Do
- Add unit and integration tests any time we add a feature
- Add or modify unit and integration tests if needed any time we modify a feature
- Run unit tests and integration tests (excluding slow tests) after adding a new feature, or after large code modifications
- Ask before making architectural changes
- Ask for clarification if the requirement is ambiguous
- Use constants or configuration values instead of hardcoded values inside the code
- Use Rich console for output, not print()
- Follow existing patterns in display.py for new output formats
- Use type hints on function signatures

### Don't
- Don't make assumptions on the feature requirements
- Don't modify the monkey-patch in teams.py without understanding the upstream bug
- Don't commit .env or cookie files

### When stuck
- Ask a clarifying question or propose a short plan

### Testing Commands
- Unit tests: `pytest -m "not integration"`
- Integration tests: `pytest -m "integration" -m "not slow"`
- All non-slow: `pytest -m "not slow"`
