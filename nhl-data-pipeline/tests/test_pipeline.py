"""Tests for pipeline.py — daily pipeline orchestrator."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from db.schema import get_db, init_db, upsert_player
from fetchers.nhl_api import save_skater_stats, save_team_schedule
from pipeline import (
    PIPELINE_STEPS,
    StepResult,
    check_freshness,
    current_season,
    generate_summary,
    main,
    run_pipeline,
    run_step,
    setup_logging,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database connection."""
    init_db(db_path)
    return get_db(db_path)


# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------

def _insert_test_players(conn: sqlite3.Connection) -> None:
    """Insert 7 skaters and 2 goalies for testing."""
    skaters = [
        (1, "Connor McDavid", "Connor", "McDavid", "EDM", "C"),
        (2, "Leon Draisaitl", "Leon", "Draisaitl", "EDM", "C"),
        (3, "Nathan MacKinnon", "Nathan", "MacKinnon", "COL", "C"),
        (4, "Auston Matthews", "Auston", "Matthews", "TOR", "C"),
        (5, "Mitch Marner", "Mitch", "Marner", "TOR", "C"),
        (6, "Sidney Crosby", "Sidney", "Crosby", "PIT", "C"),
        (7, "Alex Ovechkin", "Alex", "Ovechkin", "WSH", "LW"),
    ]
    for pid, full, first, last, team, pos in skaters:
        upsert_player(conn, {
            "id": pid, "full_name": full, "first_name": first,
            "last_name": last, "team_abbrev": team, "position": pos,
        })
    goalies = [
        (100, "Stuart Skinner", "Stuart", "Skinner", "EDM", "G"),
        (101, "Linus Ullmark", "Linus", "Ullmark", "OTT", "G"),
    ]
    for pid, full, first, last, team, pos in goalies:
        upsert_player(conn, {
            "id": pid, "full_name": full, "first_name": first,
            "last_name": last, "team_abbrev": team, "position": pos,
        })


def _make_season_total(goals: int, assists: int) -> list[dict]:
    """Build a single-element season-total stat list."""
    pts = goals + assists
    return [{
        "game_date": None, "toi": 0,
        "goals": goals, "assists": assists, "points": pts,
        "plus_minus": 0, "pim": 0, "shots": 0,
        "hits": 0, "blocks": 0,
        "powerplay_goals": 0, "powerplay_points": 0,
        "shorthanded_goals": 0, "shorthanded_points": 0,
    }]


# =============================================================================
# current_season Tests (2 tests)
# =============================================================================

class TestCurrentSeason:
    """Tests for current_season helper."""

    def test_february_2026(self) -> None:
        """Feb 2026 -> '20252026' (season started Oct 2025)."""
        assert current_season(datetime(2026, 2, 17)) == "20252026"

    def test_july_2025(self) -> None:
        """Jul 2025 -> '20252026' (upcoming season starts Oct 2025)."""
        assert current_season(datetime(2025, 7, 1)) == "20252026"


# =============================================================================
# Pipeline Execution Tests (4 tests)
# =============================================================================

class TestRunPipeline:
    """Tests for run_pipeline orchestration."""

    def test_calls_all_steps_in_order(self, db_path: Path) -> None:
        """All 6 steps execute in PIPELINE_STEPS order."""
        call_order: list[str] = []

        def make_recorder(name: str):
            def recorder(conn, season):
                call_order.append(name)
                return {}
            return recorder

        with patch.dict(
            "pipeline._STEP_RUNNERS",
            {name: make_recorder(name) for name in PIPELINE_STEPS},
            clear=True,
        ):
            results = run_pipeline(db_path, "20252026")

        assert call_order == PIPELINE_STEPS
        assert len(results) == 6
        assert [r.name for r in results] == PIPELINE_STEPS

    def test_continues_on_step_failure(self, db_path: Path) -> None:
        """If one step raises, remaining steps still run."""
        call_order: list[str] = []

        def make_recorder(name: str):
            def recorder(conn, season):
                call_order.append(name)
                if name == "schedules":
                    raise RuntimeError("Schedules failed")
                return {}
            return recorder

        with patch.dict(
            "pipeline._STEP_RUNNERS",
            {name: make_recorder(name) for name in PIPELINE_STEPS},
            clear=True,
        ):
            results = run_pipeline(db_path, "20252026")

        assert call_order == PIPELINE_STEPS
        assert len(results) == 6
        assert results[1].name == "schedules"
        assert results[1].status == "error"
        assert "Schedules failed" in results[1].error
        for i, r in enumerate(results):
            if i != 1:
                assert r.status == "ok"

    def test_reports_status_per_step(self, db_path: Path) -> None:
        """Each StepResult has correct status and populated detail dict."""
        def make_runner(name: str):
            def runner(conn, season):
                if name == "news":
                    raise ValueError("News error")
                return {"count": 42}
            return runner

        with patch.dict(
            "pipeline._STEP_RUNNERS",
            {name: make_runner(name) for name in PIPELINE_STEPS},
            clear=True,
        ):
            results = run_pipeline(db_path, "20252026")

        for r in results:
            if r.name == "news":
                assert r.status == "error"
                assert r.error is not None
                assert r.detail == {}
            else:
                assert r.status == "ok"
                assert r.detail == {"count": 42}
                assert r.error is None

    def test_tracks_duration_per_step(self, db_path: Path) -> None:
        """Every StepResult.duration_s >= 0."""
        with patch.dict(
            "pipeline._STEP_RUNNERS",
            {name: (lambda conn, season: {}) for name in PIPELINE_STEPS},
            clear=True,
        ):
            results = run_pipeline(db_path, "20252026")

        for r in results:
            assert r.duration_s >= 0


# =============================================================================
# Single Step Execution Tests (3 tests)
# =============================================================================

class TestRunStep:
    """Tests for run_step individual execution."""

    @patch("pipeline.fetch_all_rosters")
    def test_rosters(self, mock_fetch: MagicMock, db_path: Path) -> None:
        """run_step('rosters') calls fetch_all_rosters and returns result."""
        mock_fetch.return_value = (25, [])

        result = run_step("rosters", db_path, "20252026")

        assert result.name == "rosters"
        assert result.status == "ok"
        assert result.detail["players_upserted"] == 25
        assert result.detail["teams_failed"] == []
        mock_fetch.assert_called_once()

    @patch("pipeline.save_news")
    @patch("pipeline.fetch_news")
    def test_news(
        self, mock_fetch: MagicMock, mock_save: MagicMock, db_path: Path
    ) -> None:
        """run_step('news') calls fetch_news + save_news."""
        mock_fetch.return_value = [{"mock": "item"}]
        mock_save.return_value = 1

        result = run_step("news", db_path, "20252026")

        assert result.name == "news"
        assert result.status == "ok"
        assert result.detail["news_added"] == 1
        mock_fetch.assert_called_once()
        mock_save.assert_called_once()

    def test_invalid_name_raises(self, db_path: Path) -> None:
        """run_step with unknown name raises ValueError."""
        with pytest.raises(ValueError, match="Unknown step"):
            run_step("bogus", db_path, "20252026")


# =============================================================================
# Summary Report Tests (5 tests)
# =============================================================================

class TestGenerateSummary:
    """Tests for generate_summary report."""

    def test_skater_goalie_counts(
        self, db_path: Path, db: sqlite3.Connection
    ) -> None:
        """Returns correct skater and goalie counts."""
        _insert_test_players(db)

        summary = generate_summary(db_path, "20252026")

        assert summary["skater_count"] == 7
        assert summary["goalie_count"] == 2

    def test_top_scorers_sorted(
        self, db_path: Path, db: sqlite3.Connection
    ) -> None:
        """Top 5 scorers from is_season_total=1, sorted by points desc."""
        _insert_test_players(db)
        # Insert season totals with known points (descending)
        points_data = [
            (1, 65, 65),   # McDavid: 130 pts
            (2, 60, 60),   # Draisaitl: 120 pts
            (3, 55, 55),   # MacKinnon: 110 pts
            (4, 50, 50),   # Matthews: 100 pts
            (5, 45, 45),   # Marner: 90 pts
            (6, 40, 40),   # Crosby: 80 pts
            (7, 35, 35),   # Ovechkin: 70 pts
        ]
        for pid, goals, assists in points_data:
            save_skater_stats(
                db, pid, "20252026",
                _make_season_total(goals, assists),
                is_season_total=True,
            )

        summary = generate_summary(db_path, "20252026")
        top = summary["top_scorers"]

        assert len(top) == 5
        assert top[0]["name"] == "Connor McDavid"
        assert top[0]["points"] == 130
        assert top[4]["points"] == 90
        # Verify sorted descending
        for i in range(len(top) - 1):
            assert top[i]["points"] >= top[i + 1]["points"]

    def test_games_benched(
        self, db_path: Path, db: sqlite3.Connection
    ) -> None:
        """Team GP=10, player GP=5 -> games_benched=5."""
        upsert_player(db, {
            "id": 1, "full_name": "Test Player",
            "team_abbrev": "EDM", "position": "C",
        })
        # Insert 10 team games
        for i in range(1, 11):
            db.execute(
                "INSERT INTO team_games (team, season, game_date) "
                "VALUES ('EDM', '20252026', ?)",
                (f"2026-01-{i:02d}",),
            )
        # Player played 5 games
        for i in range(1, 6):
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, toi) "
                "VALUES (1, ?, '20252026', 0, 1200)",
                (f"2026-01-{i:02d}",),
            )
        db.commit()

        summary = generate_summary(db_path, "20252026")
        benched = summary["games_benched"]

        assert len(benched) == 1
        assert benched[0]["games_benched"] == 5
        assert benched[0]["team_gp"] == 10

    def test_injury_count(
        self, db_path: Path, db: sqlite3.Connection
    ) -> None:
        """Accurate count of injuries."""
        _insert_test_players(db)
        for pid in [1, 2, 3]:
            db.execute(
                "INSERT INTO player_injuries "
                "(player_id, source, injury_type, status) "
                "VALUES (?, 'rotowire', 'Upper Body', 'Day-To-Day')",
                (pid,),
            )
        db.commit()

        summary = generate_summary(db_path, "20252026")

        assert summary["injury_count"] == 3

    def test_empty_database(self, db_path: Path) -> None:
        """Fresh DB: zero counts, empty lists, no crash."""
        init_db(db_path)

        summary = generate_summary(db_path, "20252026")

        assert summary["skater_count"] == 0
        assert summary["goalie_count"] == 0
        assert summary["top_scorers"] == []
        assert summary["injury_count"] == 0
        assert summary["games_benched"] == []


# =============================================================================
# Data Freshness Tests (3 tests)
# =============================================================================

class TestCheckFreshness:
    """Tests for check_freshness data staleness check."""

    def test_returns_all_timestamps(self, db_path: Path) -> None:
        """After pipeline run, every step has a non-None last_updated."""
        init_db(db_path)
        conn = get_db(db_path)
        now = datetime.now(timezone.utc).isoformat()
        for step in PIPELINE_STEPS:
            conn.execute(
                "INSERT OR REPLACE INTO pipeline_log "
                "(step, last_run_at, status) VALUES (?, ?, 'ok')",
                (step, now),
            )
        conn.commit()
        conn.close()

        freshness = check_freshness(db_path)

        for step in PIPELINE_STEPS:
            assert step in freshness
            assert freshness[step]["last_updated"] is not None
            assert freshness[step]["stale"] is False

    def test_warns_stale_over_48h(self, db_path: Path) -> None:
        """Step run 3 days ago is flagged stale=True."""
        init_db(db_path)
        conn = get_db(db_path)
        old_time = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        conn.execute(
            "INSERT INTO pipeline_log (step, last_run_at, status) "
            "VALUES ('rosters', ?, 'ok')",
            (old_time,),
        )
        conn.commit()
        conn.close()

        freshness = check_freshness(db_path)

        assert freshness["rosters"]["stale"] is True

    def test_handles_empty_tables(self, db_path: Path) -> None:
        """Fresh DB: all steps return last_updated=None and stale=True."""
        init_db(db_path)

        freshness = check_freshness(db_path)

        for step in PIPELINE_STEPS:
            assert freshness[step]["last_updated"] is None
            assert freshness[step]["stale"] is True


# =============================================================================
# Logging Tests (3 tests)
# =============================================================================

class TestSetupLogging:
    """Tests for setup_logging configuration."""

    def test_creates_log_directory(self, tmp_path: Path) -> None:
        """Creates logs/ directory if it doesn't exist."""
        log_dir = tmp_path / "logs"
        assert not log_dir.exists()

        setup_logging(log_dir=log_dir)

        assert log_dir.exists()

    def test_writes_to_pipeline_log_file(self, tmp_path: Path) -> None:
        """Log messages appear in logs/pipeline.log."""
        log_dir = tmp_path / "logs"
        pl = setup_logging(log_dir=log_dir)
        pl.info("Test message 12345")

        # Flush all handlers
        for handler in pl.handlers:
            handler.flush()

        log_file = log_dir / "pipeline.log"
        assert log_file.exists()
        content = log_file.read_text()
        assert "Test message 12345" in content

    def test_verbose_sets_debug_level(self, tmp_path: Path) -> None:
        """verbose=True sets logger to DEBUG level."""
        log_dir = tmp_path / "logs"

        pl = setup_logging(verbose=True, log_dir=log_dir)

        assert pl.level == logging.DEBUG


# =============================================================================
# CLI Tests (7 tests)
# =============================================================================

class TestCli:
    """Tests for main() CLI dispatch."""

    @patch("pipeline.run_pipeline")
    @patch("pipeline.setup_logging")
    def test_default_runs_full_pipeline(
        self, mock_logging: MagicMock, mock_run: MagicMock
    ) -> None:
        """No args runs full pipeline."""
        mock_run.return_value = []

        result = main([])

        mock_run.assert_called_once()
        assert result == 0

    @patch("pipeline.run_step")
    @patch("pipeline.setup_logging")
    def test_step_flag(
        self, mock_logging: MagicMock, mock_step: MagicMock
    ) -> None:
        """--step rosters calls run_step('rosters', ...)."""
        mock_step.return_value = StepResult(
            name="rosters", status="ok", duration_s=1.0, detail={},
        )

        result = main(["--step", "rosters"])

        mock_step.assert_called_once()
        assert mock_step.call_args[0][0] == "rosters"

    @patch("pipeline.run_step")
    @patch("pipeline.setup_logging")
    def test_step_stats_alias(
        self, mock_logging: MagicMock, mock_step: MagicMock
    ) -> None:
        """--step stats expands to gamelogs + seasontotals."""
        mock_step.return_value = StepResult(
            name="test", status="ok", duration_s=1.0, detail={},
        )

        main(["--step", "stats"])

        assert mock_step.call_count == 2
        step_names = [c[0][0] for c in mock_step.call_args_list]
        assert step_names == ["gamelogs", "seasontotals"]

    @patch("pipeline.generate_summary")
    @patch("pipeline.setup_logging")
    def test_summary_flag(
        self, mock_logging: MagicMock, mock_summary: MagicMock
    ) -> None:
        """--summary calls generate_summary."""
        mock_summary.return_value = {
            "skater_count": 0, "goalie_count": 0,
            "top_scorers": [], "injury_count": 0, "games_benched": [],
        }

        result = main(["--summary"])

        mock_summary.assert_called_once()
        assert result == 0

    @patch("pipeline.check_freshness")
    @patch("pipeline.setup_logging")
    def test_freshness_flag(
        self, mock_logging: MagicMock, mock_fresh: MagicMock
    ) -> None:
        """--freshness calls check_freshness."""
        mock_fresh.return_value = {
            step: {"last_updated": None, "stale": True}
            for step in PIPELINE_STEPS
        }

        result = main(["--freshness"])

        mock_fresh.assert_called_once()
        assert result == 0

    @patch("pipeline.run_pipeline")
    @patch("pipeline.setup_logging")
    def test_season_flag(
        self, mock_logging: MagicMock, mock_run: MagicMock
    ) -> None:
        """--season 20242025 passes season string to pipeline."""
        mock_run.return_value = []

        main(["--season", "20242025"])

        assert mock_run.call_args[0][1] == "20242025"

    @patch("pipeline.run_pipeline")
    @patch("pipeline.setup_logging")
    def test_verbose_flag(
        self, mock_logging: MagicMock, mock_run: MagicMock
    ) -> None:
        """--verbose calls setup_logging with verbose=True."""
        mock_run.return_value = []

        main(["--verbose"])

        mock_logging.assert_called_once()
        assert mock_logging.call_args[1]["verbose"] is True
