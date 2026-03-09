"""Infrastructure constants — timeouts, URLs, retries, pagination, AI client."""

# ---------------------------------------------------------------------------
# API URLs
# ---------------------------------------------------------------------------

NHL_API_BASE = "https://api-web.nhle.com/v1"
NHL_STATS_API_BASE = "https://api.nhle.com/stats/rest/en"
BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
FANTRAX_API_URL = "https://www.fantrax.com/fxpa/req"
MONEYPUCK_INJURIES_URL = "https://moneypuck.com/moneypuck/playerData/playerNews/current_injuries.csv"

# ---------------------------------------------------------------------------
# Timeouts & Retries
# ---------------------------------------------------------------------------

HTTP_TIMEOUT = 30          # default HTTP request timeout (seconds)
BRAVE_SEARCH_TIMEOUT = 10  # Brave Search API timeout (seconds)
DB_TIMEOUT = 30            # SQLite connection timeout (seconds)

NHL_RATE_LIMIT = 0.2           # seconds between NHL API requests
DAILYFACEOFF_RATE_LIMIT = 2.0  # seconds between DailyFaceoff requests
ROTOWIRE_SEARCH_DELAY = 0.5   # seconds between Rotowire player searches
FANTRAX_PAGE_DELAY = 1        # seconds between paginated salary fetches

BACKOFF_MAX_RETRIES = 4  # max retries on HTTP 429
BACKOFF_BASE = 1         # base backoff seconds; doubles each retry: 1, 2, 4, 8

# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

NHL_STATS_PAGE_SIZE = 100             # rows per page for Stats API
FANTRAX_SALARY_PAGE_SIZE = "500"      # string — Fantrax API requirement
FANTRAX_MAX_SALARY_PAGES = 20         # safety cap for paginated salary fetches
BACKFILL_MAX_SCROLLS = 5000           # max scroll iterations for news backfill
WEB_SEARCH_MIN_RESULTS = 1            # min results for Brave Search
WEB_SEARCH_MAX_RESULTS = 10           # max results for Brave Search

# ---------------------------------------------------------------------------
# AI Client
# ---------------------------------------------------------------------------

MAX_TOKENS = 16_000
DEFAULT_MODEL = "claude-haiku-4-5-20251001"
DEEP_MODEL = "claude-opus-4-6"
DEFAULT_THINKING_BUDGET = 4_096
