"""Fantasy league logic constants — scoring, GP limits, thresholds."""

# ---------------------------------------------------------------------------
# Scoring Weights
# ---------------------------------------------------------------------------

SKATER_SCORING = {"goals": 1.0, "assists": 1.0, "blocks": 0.1, "hits": 0.1}
GOALIE_SCORING = {
    "goals": 1.0,
    "assists": 1.0,
    "wins": 2.0,
    "shutouts": 1.0,
    "ot_losses": 1.0,
    "losses": 0.0,
}

# ---------------------------------------------------------------------------
# League Structure
# ---------------------------------------------------------------------------

GP_LIMITS = {"F": 984, "D": 492, "G": 82}  # roster slots × 82 games
SALARY_CAP = 109_825_000                     # NHL cap ($95.5M) + 15%
FORWARD_POSITIONS = {"C", "L", "R"}          # individual position codes for "F"
IR_SLOT_STATUS = "3"                         # Fantrax status_id for IR slot
IR_SLOT_LIMIT = 1                            # league has 1 IR slot per team

# ---------------------------------------------------------------------------
# Analysis Tuning
# ---------------------------------------------------------------------------

REGRESSION_K = 25            # Bayesian regression: 50/50 weight at K games played
DEFAULT_MIN_GAMES = 10       # minimum GP for free-agent search
DEFAULT_FA_LIMIT = 20        # default free-agent results limit
DEFAULT_RECENT_GAMES = 5     # game log entries to show
DEFAULT_SCHEDULE_DAYS = 14   # schedule lookahead (days)
DEFAULT_NEWS_LIMIT = 15      # news briefing items
NEWS_RECENCY_DAYS = 42       # news window for player context (days)
STALE_SCROLL_THRESHOLD = 3   # consecutive stale scrolls before stopping

# ---------------------------------------------------------------------------
# Trend Detection
# ---------------------------------------------------------------------------

RECENT_GAMES_WINDOW = 14         # rolling window for recent FP/G trend
PERIPHERAL_STATS_WINDOW = 30     # games for peripheral stats (hits+blocks) calculation

HOT_THRESHOLD_MULTIPLIER = 1.20  # L14 FP/G >= 120% of season → "hot"
COLD_THRESHOLD_MULTIPLIER = 0.80 # L14 FP/G <= 80% of season → "cold"
TREND_HOT_THRESHOLD_7_DAY = 1.25 # L7 FP/G >= 125% of season → "hot" (stricter)
TREND_COLD_THRESHOLD_7_DAY = 0.75  # L7 FP/G <= 75% of season → "cold" (stricter)

# ---------------------------------------------------------------------------
# Roster & GP Warnings
# ---------------------------------------------------------------------------

GP_WARNING_THRESHOLD = 85  # % of GP limit before "nearly full" warning

# ---------------------------------------------------------------------------
# Goalie Analysis
# ---------------------------------------------------------------------------

GOALIE_MAX_GAP_GAMES = 6  # consecutive missed team games before assuming injured

# ---------------------------------------------------------------------------
# Trade Recommendations
# ---------------------------------------------------------------------------

TRADE_SEND_PLAYER_APPEARANCE_CAP = 2  # max times a send player appears per opponent

# ---------------------------------------------------------------------------
# Injury-Based Game Estimation
# ---------------------------------------------------------------------------

INJURY_SEASON_ENDING_DAYS = 60  # days out → 0 estimated games (season-ending)
INJURY_MODERATE_DAYS = 30       # days out → reduce estimated games by days_out/3

# ---------------------------------------------------------------------------
# Cross-Position Pickups
# ---------------------------------------------------------------------------

CROSS_POSITION_GP_THRESHOLD = 75  # GP% threshold to enable cross-position pickups

# ---------------------------------------------------------------------------
# Playable Ice Time Thresholds (seconds per game)
# ---------------------------------------------------------------------------

FORWARD_PLAYABLE_TOI_PER_GAME = 900    # 15 min/game for forwards
DEFENSEMAN_PLAYABLE_TOI_PER_GAME = 1080  # 18 min/game for defensemen

# ---------------------------------------------------------------------------
# Trade Target Line Deployment Filters
# ---------------------------------------------------------------------------

# Only consider trade targets on these even-strength lines (no 4th liners)
TRADE_TARGET_ELIGIBLE_EV_LINES = {1, 2, 3}
# Must be on a power-play unit (PP1 or PP2) OR on an eligible EV line
TRADE_TARGET_ELIGIBLE_PP_UNITS = {1, 2}

# ---------------------------------------------------------------------------
# Drop Candidate / Value Above Replacement
# ---------------------------------------------------------------------------

DROP_CANDIDATES_COUNT = 3                 # drop candidates to return per FA result
DROP_FPG_CEILING = {"F": 0.9, "D": 0.8}  # never suggest dropping above this L14 FP/G
VERDICT_STRONG_THRESHOLD = 0.5            # net FP/G >= this = "strong"
VERDICT_MARGINAL_THRESHOLD = 0.2          # net FP/G >= this = "marginal"
MIN_ROSTER_FALLBACK = {"F": 12, "D": 6, "G": 2}  # safe default when no schedule data
