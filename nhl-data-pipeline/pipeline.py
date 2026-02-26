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
    fetch_all_goalie_gamelogs_bulk,
    fetch_all_goalie_seasontotals_bulk,
    discover_missing_players,
    fetch_all_rosters,
    fetch_all_skater_gamelogs_bulk,
    fetch_all_skater_seasontotals_bulk,
    fetch_goalie_game_log,
    fetch_player_landing,
    fetch_skater_game_log,
    fetch_team_schedule,
    save_goalie_stats,
    save_skater_stats,
    save_team_schedule,
)
from assistant.scoring import calc_skater_fpts
from fetchers.fantrax_league import sync_fantrax_league
from fetchers.fantrax_news import backfill_fantrax_news
from fetchers.dailyfaceoff import fetch_all_lines
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
    "lines", "fantrax-league",
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
    discovered = discover_missing_players(conn, season)
    backfilled = backfill_news_player_ids(conn)
    return {
        "players_upserted": count,
        "teams_failed": failed,
        "discovered": discovered,
        "news_backfilled": backfilled,
    }


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
    # Get known player IDs for filtering
    known_ids = {
        row["id"] for row in conn.execute("SELECT id FROM players").fetchall()
    }

    # Bulk fetch skater gamelogs from Stats API
    skater_rows = fetch_all_skater_gamelogs_bulk(season)
    skater_games = 0
    for row in skater_rows:
        pid = row["player_id"]
        if pid not in known_ids:
            continue
        try:
            save_skater_stats(conn, pid, season, [row])
            skater_games += 1
        except Exception as e:
            logger.warning("Failed saving skater game log for %d: %s", pid, e)

    # Bulk fetch goalie gamelogs from Stats API
    goalie_rows = fetch_all_goalie_gamelogs_bulk(season)
    goalie_games = 0
    for row in goalie_rows:
        pid = row["player_id"]
        if pid not in known_ids:
            continue
        try:
            save_goalie_stats(conn, pid, season, [row])
            goalie_games += 1
        except Exception as e:
            logger.warning("Failed saving goalie game log for %d: %s", pid, e)

    return {"skater_games": skater_games, "goalie_games": goalie_games}


def _run_seasontotals(conn, season):
    # Get known player IDs for filtering
    known_ids = {
        row["id"] for row in conn.execute("SELECT id FROM players").fetchall()
    }

    # Bulk fetch skater season totals from Stats API
    skater_rows = fetch_all_skater_seasontotals_bulk(season)
    skaters_updated = 0
    for row in skater_rows:
        pid = row["player_id"]
        if pid not in known_ids:
            continue
        try:
            save_skater_stats(conn, pid, season, [row], is_season_total=True)
            skaters_updated += 1
        except Exception as e:
            logger.warning("Failed saving skater season totals for %d: %s", pid, e)

    # Bulk fetch goalie season totals from Stats API
    goalie_rows = fetch_all_goalie_seasontotals_bulk(season)
    goalies_updated = 0
    for row in goalie_rows:
        pid = row["player_id"]
        if pid not in known_ids:
            continue
        try:
            save_goalie_stats(conn, pid, season, [row], is_season_total=True)
            goalies_updated += 1
        except Exception as e:
            logger.warning("Failed saving goalie season totals for %d: %s", pid, e)

    return {"skaters_updated": skaters_updated, "goalies_updated": goalies_updated}


def _run_injuries(conn, season):
    items = fetch_injuries()
    upserted, unmatched = save_injuries(conn, items)
    return {"injuries_upserted": upserted, "unmatched": unmatched}


def _run_lines(conn, season):
    return fetch_all_lines(conn)


_BACKFILL_MAX_SCROLLS = 50


def _run_backfill_news(conn, season):
    return backfill_fantrax_news(conn, max_scrolls=_BACKFILL_MAX_SCROLLS)


def _run_fantrax_league(conn, season):
    return sync_fantrax_league(conn)


_STEP_RUNNERS: dict[str, Any] = {
    "rosters": _run_rosters,
    "schedules": _run_schedules,
    "gamelogs": _run_gamelogs,
    "seasontotals": _run_seasontotals,
    "injuries": _run_injuries,
    "lines": _run_lines,
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

        # Top 5 fantasy point producers (skaters)
        rows = conn.execute(
            """
            SELECT p.full_name, p.team_abbrev, p.position,
                   s.goals, s.assists, s.points, s.hits, s.blocks,
                   (SELECT COUNT(*) FROM skater_stats g
                    WHERE g.player_id = s.player_id
                      AND g.season = s.season
                      AND g.is_season_total = 0) as gp
            FROM skater_stats s
            JOIN players p ON p.id = s.player_id
            WHERE s.season = ? AND s.is_season_total = 1
            """,
            (season,),
        ).fetchall()
        scored = []
        for r in rows:
            fpts = calc_skater_fpts(
                goals=r["goals"] or 0, assists=r["assists"] or 0,
                blocks=r["blocks"] or 0, hits=r["hits"] or 0,
            )
            gp = r["gp"] or 0
            scored.append({
                "name": r["full_name"],
                "team": r["team_abbrev"],
                "position": r["position"],
                "goals": r["goals"],
                "assists": r["assists"],
                "hits": r["hits"],
                "blocks": r["blocks"],
                "fpts": round(fpts, 1),
                "fpg": round(fpts / gp, 2) if gp else 0.0,
                "gp": gp,
            })
        scored.sort(key=lambda x: x["fpts"], reverse=True)
        top_fantasy = scored[:5]

        # Injury count
        injury_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM player_injuries"
        ).fetchone()["cnt"]

        # News count and date range
        news_row = conn.execute(
            "SELECT COUNT(*) as cnt, MIN(published_at) as oldest, "
            "MAX(published_at) as newest FROM player_news"
        ).fetchone()
        news_count = news_row["cnt"]
        news_oldest = (news_row["oldest"] or "")[:10]
        news_newest = (news_row["newest"] or "")[:10]

        # Fantasy standings
        standings_rows = conn.execute(
            "SELECT ft.name, fs.rank, "
            "fs.points_for, fs.fantasy_points_per_game "
            "FROM fantasy_standings fs "
            "JOIN fantasy_teams ft ON fs.team_id = ft.id "
            "ORDER BY fs.rank"
        ).fetchall()
        standings = [
            {
                "name": r["name"],
                "rank": r["rank"],
                "points_for": r["points_for"],
                "fpg": r["fantasy_points_per_game"],
            }
            for r in standings_rows
        ]

        # Data freshness (inline, no separate call)
        freshness: dict[str, str | None] = {}
        for step in PIPELINE_STEPS:
            row = conn.execute(
                "SELECT last_run_at FROM pipeline_log WHERE step = ?",
                (step,),
            ).fetchone()
            freshness[step] = row["last_run_at"] if row and row["last_run_at"] else None

        return {
            "skater_count": skater_count,
            "goalie_count": goalie_count,
            "top_fantasy": top_fantasy,
            "injury_count": injury_count,
            "news_count": news_count,
            "news_oldest": news_oldest,
            "news_newest": news_newest,
            "standings": standings,
            "freshness": freshness,
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
    "lines": ("players_saved", "line assignments"),
    "backfill-news": ("new_inserted", "articles"),
    "fantrax-league": ("teams_synced", "fantasy teams"),
}


def _format_detail(r: StepResult) -> str:
    d = r.detail
    if r.name == "rosters":
        s = f"{d.get('players_upserted', 0):,} players"
        disc = d.get("discovered", 0)
        if disc:
            s += f" ({disc} discovered)"
        return s
    if r.name == "gamelogs":
        total = d.get("skater_games", 0) + d.get("goalie_games", 0)
        return f"{total:,} game logs"
    if r.name == "seasontotals":
        total = d.get("skaters_updated", 0) + d.get("goalies_updated", 0)
        return f"{total:,} players updated"
    if r.name == "lines":
        return (
            f"{d.get('players_saved', 0):,} line assignments "
            f"({d.get('unmatched', 0)} unmatched, "
            f"{d.get('teams_failed', 0)} teams failed)"
        )
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


def _safe_print(text: str) -> None:
    """Print text, replacing unencodable characters for Windows consoles."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("ascii", errors="replace").decode("ascii"))


def _print_summary(summary: dict[str, Any]) -> None:
    _safe_print("\n=== Data Summary ===")
    _safe_print(f"  Players: {summary['skater_count']} skaters, "
                f"{summary['goalie_count']} goalies")
    _safe_print(f"  Injuries: {summary['injury_count']}")
    oldest = summary.get("news_oldest", "")
    newest = summary.get("news_newest", "")
    date_range = f" ({oldest} to {newest})" if oldest and newest else ""
    _safe_print(f"  News: {summary.get('news_count', 0)} articles{date_range}")

    if summary.get("top_fantasy"):
        _safe_print("\n  Top 5 Fantasy Producers (Skaters):")
        for i, s in enumerate(summary["top_fantasy"], 1):
            _safe_print(f"    {i}. {s['name']} ({s['team']}, {s['position']}) - "
                        f"{s['fpts']} FP ({s['fpg']} FP/G) | "
                        f"{s['goals']}G {s['assists']}A {s['hits']}H {s['blocks']}B")

    if summary.get("standings"):
        _safe_print(f"\n  Fantasy Standings ({len(summary['standings'])} teams):")
        for s in summary["standings"]:
            _safe_print(f"    {s['rank']:>2}. {s['name']:<25} "
                        f"{s['points_for']:>7.1f} PF  "
                        f"{s['fpg']:.1f} FP/G")

    if summary.get("freshness"):
        _safe_print("\n  Data Freshness:")
        for step, ts in summary["freshness"].items():
            label = ts[:16].replace("T", " ") if ts else "never"
            _safe_print(f"    {step:<16} {label}")


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
    parser.add_argument(
        "--max-scrolls",
        type=int,
        default=50,
        help="Max scroll iterations for backfill-news (default: 50)",
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
        global _BACKFILL_MAX_SCROLLS
        _BACKFILL_MAX_SCROLLS = args.max_scrolls
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
