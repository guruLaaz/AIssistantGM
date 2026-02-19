# Cron (daily full):  0 6 * * *   cd /path/to/nhl-data-pipeline && python pipeline.py >> logs/cron.log 2>&1
"""Daily NHL data pipeline orchestrator.

Runs all data-fetching steps (rosters, schedules, game logs, season totals,
injuries), logs progress, and generates summary reports.
"""

from __future__ import annotations

import argparse
import logging
import time as time_module
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from db.schema import get_db, init_db
from fetchers.nhl_api import (
    ALL_TEAMS,
    DEFAULT_RATE_LIMIT,
    calculate_games_benched,
    fetch_all_rosters,
    fetch_goalie_game_log,
    fetch_player_landing,
    fetch_skater_game_log,
    fetch_team_schedule,
    save_goalie_stats,
    save_skater_stats,
    save_team_schedule,
)
from fetchers.fantrax_league import sync_fantrax_league
from fetchers.fantrax_news import backfill_fantrax_news
from fetchers.rotowire import (
    backfill_news_player_ids,
    fetch_injuries,
    save_injuries,
)

logger = logging.getLogger("pipeline")

DB_PATH = Path(__file__).parent / "db" / "nhl_data.db"
LOG_DIR = Path(__file__).parent / "logs"

PIPELINE_STEPS = [
    "rosters", "schedules", "gamelogs", "seasontotals", "injuries",
    "fantrax-league",
]
STEP_ALIASES: dict[str, list[str]] = {
    "stats": ["gamelogs", "seasontotals"],
}


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class StepResult:
    """Result of a single pipeline step execution."""

    name: str
    status: str  # "ok" | "error"
    duration_s: float
    detail: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def current_season(today: datetime | None = None) -> str:
    """Return the current NHL season string (e.g. '20252026').

    The NHL season starts in October. If the current month is July or later,
    the season that will start (or just started) uses the current year.
    Otherwise, the season started the previous year.
    """
    if today is None:
        today = datetime.now()
    if today.month >= 7:
        start_year = today.year
    else:
        start_year = today.year - 1
    return f"{start_year}{start_year + 1}"


def setup_logging(
    verbose: bool = False,
    log_dir: Path = LOG_DIR,
) -> logging.Logger:
    """Configure console + file logging.

    Args:
        verbose: If True, set DEBUG level; else INFO.
        log_dir: Directory for log files. Created if missing.

    Returns:
        Configured logger.
    """
    log_dir.mkdir(parents=True, exist_ok=True)

    log_level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    pl = logging.getLogger("pipeline")
    pl.setLevel(log_level)
    pl.handlers.clear()

    console = logging.StreamHandler()
    console.setLevel(log_level)
    console.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    pl.addHandler(console)

    fh = logging.FileHandler(log_dir / "pipeline.log")
    fh.setLevel(log_level)
    fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    pl.addHandler(fh)

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return pl


def _log_pipeline_step(conn, step_name: str, status: str) -> None:
    """Record a step execution timestamp in pipeline_log."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO pipeline_log (step, last_run_at, status) "
        "VALUES (?, ?, ?)",
        (step_name, now, status),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Step runners — one per pipeline step
# ---------------------------------------------------------------------------

def _run_rosters(conn, season):
    count, failed = fetch_all_rosters(conn)
    backfilled = backfill_news_player_ids(conn)
    return {"players_upserted": count, "teams_failed": failed, "news_backfilled": backfilled}


def _run_schedules(conn, season):
    total = 0
    for i, team in enumerate(ALL_TEAMS):
        try:
            games = fetch_team_schedule(team, season)
            save_team_schedule(conn, team, season, games)
            total += len(games)
        except Exception as e:
            logger.warning("Failed schedule for %s: %s", team, e)
        if i < len(ALL_TEAMS) - 1:
            time_module.sleep(DEFAULT_RATE_LIMIT)
    return {"games_saved": total}


def _run_gamelogs(conn, season):
    cursor = conn.execute("SELECT id, position FROM players")
    players = cursor.fetchall()
    skater_games = 0
    goalie_games = 0
    for player in players:
        pid = player["id"]
        pos = player["position"]
        try:
            if pos == "G":
                stats = fetch_goalie_game_log(pid, season)
                save_goalie_stats(conn, pid, season, stats)
                goalie_games += len(stats)
            else:
                stats = fetch_skater_game_log(pid, season)
                save_skater_stats(conn, pid, season, stats)
                skater_games += len(stats)
        except Exception as e:
            logger.warning("Failed game log for player %d: %s", pid, e)
        time_module.sleep(DEFAULT_RATE_LIMIT)
    return {"skater_games": skater_games, "goalie_games": goalie_games}


def _run_seasontotals(conn, season):
    cursor = conn.execute("SELECT id, position FROM players")
    players = cursor.fetchall()
    skaters_updated = 0
    goalies_updated = 0
    for player in players:
        pid = player["id"]
        pos = player["position"]
        try:
            landing = fetch_player_landing(pid)
            featured = landing.get("featuredStats", {})
            reg = featured.get("regularSeason", {})
            sub = reg.get("subSeason", {})
            if not sub:
                continue
            if pos == "G":
                stats = [{
                    "game_date": None,
                    "toi": 0,
                    "saves": int(sub.get("saves", 0)),
                    "goals_against": int(sub.get("goalsAgainst", 0)),
                    "shots_against": int(sub.get("shotsAgainst", 0)),
                    "wins": int(sub.get("wins", 0)),
                    "losses": int(sub.get("losses", 0)),
                    "ot_losses": int(sub.get("otLosses", 0)),
                    "shutouts": int(sub.get("shutouts", 0)),
                }]
                save_goalie_stats(conn, pid, season, stats, is_season_total=True)
                goalies_updated += 1
            else:
                stats = [{
                    "game_date": None,
                    "toi": 0,
                    "pp_toi": 0,
                    "goals": int(sub.get("goals", 0)),
                    "assists": int(sub.get("assists", 0)),
                    "points": int(sub.get("points", 0)),
                    "plus_minus": int(sub.get("plusMinus", 0)),
                    "pim": int(sub.get("pim", 0)),
                    "shots": int(sub.get("shots", 0)),
                    "hits": 0,
                    "blocks": 0,
                    "powerplay_goals": int(sub.get("powerPlayGoals", 0)),
                    "powerplay_points": int(sub.get("powerPlayPoints", 0)),
                    "shorthanded_goals": int(sub.get("shorthandedGoals", 0)),
                    "shorthanded_points": int(sub.get("shorthandedPoints", 0)),
                }]
                save_skater_stats(conn, pid, season, stats, is_season_total=True)
                skaters_updated += 1
        except Exception as e:
            logger.warning("Failed season totals for player %d: %s", pid, e)
        time_module.sleep(DEFAULT_RATE_LIMIT)
    return {"skaters_updated": skaters_updated, "goalies_updated": goalies_updated}


def _run_injuries(conn, season):
    items = fetch_injuries()
    upserted, unmatched = save_injuries(conn, items)
    return {"injuries_upserted": upserted, "unmatched": unmatched}


def _run_backfill_news(conn, season):
    return backfill_fantrax_news(conn)


def _run_fantrax_league(conn, season):
    return sync_fantrax_league(conn)


_STEP_RUNNERS: dict[str, Any] = {
    "rosters": _run_rosters,
    "schedules": _run_schedules,
    "gamelogs": _run_gamelogs,
    "seasontotals": _run_seasontotals,
    "injuries": _run_injuries,
    "backfill-news": _run_backfill_news,
    "fantrax-league": _run_fantrax_league,
}


# ---------------------------------------------------------------------------
# Core pipeline API
# ---------------------------------------------------------------------------

def run_step(step: str, db_path: Path, season: str) -> StepResult:
    """Run a single pipeline step by name.

    Args:
        step: Step name from PIPELINE_STEPS.
        db_path: Path to SQLite database.
        season: Season string (e.g. '20252026').

    Returns:
        StepResult with timing, status, and detail.

    Raises:
        ValueError: If step name is unknown.
    """
    if step not in _STEP_RUNNERS:
        raise ValueError(
            f"Unknown step: {step!r}. Valid steps: {list(_STEP_RUNNERS.keys())}"
        )

    init_db(db_path)
    conn = get_db(db_path)
    try:
        logger.info("Step: %s", step)
        start = time_module.monotonic()
        try:
            detail = _STEP_RUNNERS[step](conn, season)
            duration = time_module.monotonic() - start
            _log_pipeline_step(conn, step, "ok")
            logger.info("  OK (%.1fs)", duration)
            return StepResult(
                name=step, status="ok", duration_s=duration, detail=detail,
            )
        except Exception as e:
            duration = time_module.monotonic() - start
            _log_pipeline_step(conn, step, "error")
            logger.error("  FAILED: %s (%.1fs)", e, duration)
            return StepResult(
                name=step, status="error", duration_s=duration,
                detail={}, error=str(e),
            )
    finally:
        conn.close()


def run_pipeline(db_path: Path, season: str) -> list[StepResult]:
    """Run all pipeline steps in PIPELINE_STEPS order.

    Initialises the database, then executes each step. Continues on failure
    so that one broken step doesn't block the rest.

    Args:
        db_path: Path to SQLite database.
        season: Season string.

    Returns:
        List of StepResult, one per step.
    """
    init_db(db_path)
    conn = get_db(db_path)
    results: list[StepResult] = []
    try:
        for step_name in PIPELINE_STEPS:
            logger.info("Step: %s", step_name)
            start = time_module.monotonic()
            try:
                detail = _STEP_RUNNERS[step_name](conn, season)
                duration = time_module.monotonic() - start
                _log_pipeline_step(conn, step_name, "ok")
                results.append(StepResult(
                    name=step_name, status="ok",
                    duration_s=duration, detail=detail,
                ))
                logger.info("  OK (%.1fs)", duration)
            except Exception as e:
                duration = time_module.monotonic() - start
                _log_pipeline_step(conn, step_name, "error")
                results.append(StepResult(
                    name=step_name, status="error",
                    duration_s=duration, detail={}, error=str(e),
                ))
                logger.error("  FAILED: %s (%.1fs)", e, duration)
    finally:
        conn.close()
    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def generate_summary(db_path: Path, season: str) -> dict[str, Any]:
    """Generate a summary report from the database.

    Args:
        db_path: Path to SQLite database.
        season: Season string.

    Returns:
        Dict with skater_count, goalie_count, top_scorers, injury_count,
        games_benched.
    """
    init_db(db_path)
    conn = get_db(db_path)
    try:
        # Player counts
        skater_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM players WHERE position != 'G'"
        ).fetchone()["cnt"]

        goalie_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM players WHERE position = 'G'"
        ).fetchone()["cnt"]

        # Top 5 scorers from season totals
        rows = conn.execute(
            """
            SELECT p.full_name, p.team_abbrev, s.goals, s.assists, s.points
            FROM skater_stats s
            JOIN players p ON p.id = s.player_id
            WHERE s.season = ? AND s.is_season_total = 1
            ORDER BY s.points DESC, s.goals DESC
            LIMIT 5
            """,
            (season,),
        ).fetchall()
        top_scorers = [
            {
                "name": r["full_name"],
                "team": r["team_abbrev"],
                "goals": r["goals"],
                "assists": r["assists"],
                "points": r["points"],
            }
            for r in rows
        ]

        # Injury count
        injury_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM player_injuries"
        ).fetchone()["cnt"]

        # Games benched
        all_players = conn.execute(
            "SELECT id, full_name, team_abbrev FROM players"
        ).fetchall()
        games_benched_list: list[dict[str, Any]] = []
        for p in all_players:
            benched = calculate_games_benched(conn, p["id"], season)
            if benched is not None and benched > 0:
                team_gp = conn.execute(
                    "SELECT COUNT(*) as cnt FROM team_games "
                    "WHERE team = ? AND season = ?",
                    (p["team_abbrev"], season),
                ).fetchone()["cnt"]
                games_benched_list.append({
                    "name": p["full_name"],
                    "team": p["team_abbrev"],
                    "games_benched": benched,
                    "team_gp": team_gp,
                })

        return {
            "skater_count": skater_count,
            "goalie_count": goalie_count,
            "top_scorers": top_scorers,
            "injury_count": injury_count,
            "games_benched": games_benched_list,
        }
    finally:
        conn.close()


def check_freshness(db_path: Path) -> dict[str, Any]:
    """Check when each pipeline step was last run.

    Args:
        db_path: Path to SQLite database.

    Returns:
        Dict mapping step name to {last_updated, stale} where stale is True
        if data is >48 h old or has never been fetched.
    """
    init_db(db_path)
    conn = get_db(db_path)
    now = datetime.now(timezone.utc)
    threshold = timedelta(hours=48)
    result: dict[str, Any] = {}
    try:
        for step in PIPELINE_STEPS:
            row = conn.execute(
                "SELECT last_run_at FROM pipeline_log WHERE step = ?",
                (step,),
            ).fetchone()
            if row and row["last_run_at"]:
                last_updated = row["last_run_at"]
                try:
                    ts = datetime.fromisoformat(last_updated)
                    stale = (now - ts) > threshold
                except ValueError:
                    stale = True
            else:
                last_updated = None
                stale = True
            result[step] = {"last_updated": last_updated, "stale": stale}
    finally:
        conn.close()
    return result


# ---------------------------------------------------------------------------
# Formatting helpers (for CLI output)
# ---------------------------------------------------------------------------

def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.0f}s"


_STEP_LABELS = {
    "rosters": ("players_upserted", "players"),
    "schedules": ("games_saved", "games"),
    "gamelogs": (None, "game logs"),
    "seasontotals": (None, "players updated"),
    "injuries": ("injuries_upserted", "injuries"),
    "backfill-news": ("new_inserted", "articles"),
    "fantrax-league": ("teams_synced", "fantasy teams"),
}


def _format_detail(r: StepResult) -> str:
    d = r.detail
    if r.name == "gamelogs":
        total = d.get("skater_games", 0) + d.get("goalie_games", 0)
        return f"{total:,} game logs"
    if r.name == "seasontotals":
        total = d.get("skaters_updated", 0) + d.get("goalies_updated", 0)
        return f"{total:,} players updated"
    if r.name == "fantrax-league":
        return (
            f"{d.get('teams_synced', 0)} teams, "
            f"{d.get('standings_synced', 0)} standings, "
            f"{d.get('roster_slots_synced', 0)} roster slots"
        )
    key, label = _STEP_LABELS.get(r.name, (None, "items"))
    if key:
        return f"{d.get(key, 0):,} {label}"
    return str(d)


def _print_results(results: list[StepResult]) -> None:
    print("\n=== Pipeline Summary ===")
    total = 0.0
    for r in results:
        total += r.duration_s
        dur = _format_duration(r.duration_s)
        if r.status == "ok":
            print(f"  [OK] {r.name.capitalize()}: {_format_detail(r)} ({dur})")
        else:
            print(f"  [FAIL] {r.name.capitalize()}: FAILED - {r.error} ({dur})")
    print(f"  Total time: {_format_duration(total)}")


def _print_summary(summary: dict[str, Any]) -> None:
    print("\n=== Data Summary ===")
    print(f"  Players: {summary['skater_count']} skaters, "
          f"{summary['goalie_count']} goalies")
    print(f"  Injuries: {summary['injury_count']}")

    if summary["top_scorers"]:
        print("\n  Top 5 Scorers:")
        for i, s in enumerate(summary["top_scorers"], 1):
            print(f"    {i}. {s['name']} ({s['team']}) - "
                  f"{s['goals']}G {s['assists']}A {s['points']}P")

    if summary["games_benched"]:
        print(f"\n  Players with games benched: {len(summary['games_benched'])}")
        for b in summary["games_benched"][:10]:
            print(f"    {b['name']} ({b['team']}): "
                  f"{b['games_benched']} benched / {b['team_gp']} team GP")


def _print_freshness(freshness: dict[str, Any]) -> None:
    print("\n=== Data Freshness ===")
    for step, info in freshness.items():
        status = "STALE" if info["stale"] else "OK"
        ts = info["last_updated"] or "never"
        marker = "[!!]" if info["stale"] else "[OK]"
        print(f"  {marker} {step}: {ts} [{status}]")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Args:
        argv: Command-line arguments (None = sys.argv).

    Returns:
        0 on success, 1 on any step failure.
    """
    valid_steps = list(_STEP_RUNNERS.keys()) + list(STEP_ALIASES.keys())

    parser = argparse.ArgumentParser(
        description="NHL daily data pipeline",
    )
    parser.add_argument(
        "--step",
        choices=valid_steps,
        help="Run a single step (or alias like 'stats')",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print data summary report",
    )
    parser.add_argument(
        "--freshness",
        action="store_true",
        help="Check data freshness / staleness",
    )
    parser.add_argument(
        "--season",
        type=str,
        default=None,
        help="Season in 8-digit format (e.g. 20252026)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Database path (default: db/nhl_data.db)",
    )

    args = parser.parse_args(argv)

    season = args.season or current_season()
    db_path = Path(args.db) if args.db else DB_PATH

    setup_logging(verbose=args.verbose, log_dir=LOG_DIR)

    if args.summary:
        summary = generate_summary(db_path, season)
        _print_summary(summary)
        return 0

    if args.freshness:
        freshness = check_freshness(db_path)
        _print_freshness(freshness)
        return 0

    if args.step:
        step_names = STEP_ALIASES.get(args.step, [args.step])
        results = []
        for name in step_names:
            results.append(run_step(name, db_path, season))
        _print_results(results)
        return 0 if all(r.status == "ok" for r in results) else 1

    # Default: full pipeline
    results = run_pipeline(db_path, season)
    _print_results(results)
    return 0 if all(r.status == "ok" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
