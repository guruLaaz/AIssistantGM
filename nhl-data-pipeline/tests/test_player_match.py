"""Tests for assistant/player_match.py — player name resolution."""

import sqlite3
from pathlib import Path

import pytest
from db.schema import init_db, get_db, upsert_player
from assistant.player_match import (
    resolve_player,
    resolve_fantrax_to_nhl,
    get_rostered_nhl_ids,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database connection with test players."""
    init_db(db_path)
    conn = get_db(db_path)

    upsert_player(conn, {
        "id": 8478402, "full_name": "Connor McDavid",
        "first_name": "Connor", "last_name": "McDavid",
        "team_abbrev": "EDM", "position": "C",
    })
    upsert_player(conn, {
        "id": 8471675, "full_name": "Sidney Crosby",
        "first_name": "Sidney", "last_name": "Crosby",
        "team_abbrev": "PIT", "position": "C",
    })
    upsert_player(conn, {
        "id": 8477934, "full_name": "Leon Draisaitl",
        "first_name": "Leon", "last_name": "Draisaitl",
        "team_abbrev": "EDM", "position": "C",
    })
    upsert_player(conn, {
        "id": 8480069, "full_name": "Cale Makar",
        "first_name": "Cale", "last_name": "Makar",
        "team_abbrev": "COL", "position": "D",
    })
    # Accented name
    upsert_player(conn, {
        "id": 8478550, "full_name": "Jes\u00fas Imaz",
        "first_name": "Jes\u00fas", "last_name": "Imaz",
        "team_abbrev": "MTL", "position": "LW",
    })
    # Goalie
    upsert_player(conn, {
        "id": 8477424, "full_name": "Juuse Saros",
        "first_name": "Juuse", "last_name": "Saros",
        "team_abbrev": "NSH", "position": "G",
    })

    return conn


class TestResolvePlayer:
    """Tests for resolve_player function."""

    def test_exact_match(self, db: sqlite3.Connection) -> None:
        """Exact full_name returns the correct player."""
        result = resolve_player(db, "Connor McDavid")
        assert result is not None
        assert result["id"] == 8478402
        assert result["full_name"] == "Connor McDavid"
        assert result["team_abbrev"] == "EDM"
        assert result["position"] == "C"

    def test_case_insensitive_match(self, db: sqlite3.Connection) -> None:
        """Case-insensitive match works."""
        result = resolve_player(db, "connor mcdavid")
        assert result is not None
        assert result["id"] == 8478402

    def test_mixed_case_match(self, db: sqlite3.Connection) -> None:
        """Mixed case match works."""
        result = resolve_player(db, "SIDNEY CROSBY")
        assert result is not None
        assert result["id"] == 8471675

    def test_accent_normalized_match(self, db: sqlite3.Connection) -> None:
        """Accent-stripped name matches accented player."""
        result = resolve_player(db, "Jesus Imaz")
        assert result is not None
        assert result["id"] == 8478550

    def test_partial_name_match(self, db: sqlite3.Connection) -> None:
        """Partial name LIKE match works."""
        result = resolve_player(db, "Draisaitl")
        assert result is not None
        assert result["id"] == 8477934

    def test_last_name_only_match(self, db: sqlite3.Connection) -> None:
        """Last name only resolves via LIKE."""
        result = resolve_player(db, "Makar")
        assert result is not None
        assert result["id"] == 8480069

    def test_no_match_returns_none(self, db: sqlite3.Connection) -> None:
        """Unrecognized name returns None."""
        result = resolve_player(db, "Wayne Gretzky")
        assert result is None

    def test_empty_string_returns_none(self, db: sqlite3.Connection) -> None:
        """Empty string returns None."""
        result = resolve_player(db, "")
        assert result is None

    def test_whitespace_only_returns_none(self, db: sqlite3.Connection) -> None:
        """Whitespace-only string returns None."""
        result = resolve_player(db, "   ")
        assert result is None

    def test_none_returns_none(self, db: sqlite3.Connection) -> None:
        """None input returns None."""
        result = resolve_player(db, None)
        assert result is None

    def test_result_has_correct_keys(self, db: sqlite3.Connection) -> None:
        """Result dict has exactly the expected keys."""
        result = resolve_player(db, "Connor McDavid")
        assert set(result.keys()) == {"id", "full_name", "team_abbrev", "position"}

    def test_leading_trailing_whitespace_stripped(self, db: sqlite3.Connection) -> None:
        """Leading/trailing whitespace is stripped before matching."""
        result = resolve_player(db, "  Connor McDavid  ")
        assert result is not None
        assert result["id"] == 8478402

    def test_goalie_resolved(self, db: sqlite3.Connection) -> None:
        """Goalies are resolved with position G."""
        result = resolve_player(db, "Juuse Saros")
        assert result is not None
        assert result["position"] == "G"


class TestResolveFantraxToNhl:
    """Tests for resolve_fantrax_to_nhl function."""

    def test_match_found(self, db: sqlite3.Connection) -> None:
        """Known player name returns NHL ID."""
        nhl_id = resolve_fantrax_to_nhl(db, "Connor McDavid")
        assert nhl_id == 8478402

    def test_no_match_returns_none(self, db: sqlite3.Connection) -> None:
        """Unknown name returns None."""
        nhl_id = resolve_fantrax_to_nhl(db, "Wayne Gretzky")
        assert nhl_id is None

    def test_case_insensitive(self, db: sqlite3.Connection) -> None:
        """Case-insensitive matching works through resolve_player."""
        nhl_id = resolve_fantrax_to_nhl(db, "connor mcdavid")
        assert nhl_id == 8478402


class TestGetRosteredNhlIds:
    """Tests for get_rostered_nhl_ids function."""

    def test_returns_correct_set(self, db: sqlite3.Connection) -> None:
        """Rostered players are resolved to NHL IDs."""
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short) "
            "VALUES ('team1', 'Connor McDavid', 'C')"
        )
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short) "
            "VALUES ('team1', 'Sidney Crosby', 'C')"
        )
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short) "
            "VALUES ('team2', 'Cale Makar', 'D')"
        )
        db.commit()

        ids = get_rostered_nhl_ids(db)
        assert ids == {8478402, 8471675, 8480069}

    def test_empty_roster_returns_empty_set(self, db: sqlite3.Connection) -> None:
        """No roster slots returns empty set."""
        ids = get_rostered_nhl_ids(db)
        assert ids == set()

    def test_unresolvable_names_skipped(self, db: sqlite3.Connection) -> None:
        """Players that can't be resolved are excluded from the set."""
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short) "
            "VALUES ('team1', 'Connor McDavid', 'C')"
        )
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short) "
            "VALUES ('team1', 'Fake Player', 'RW')"
        )
        db.commit()

        ids = get_rostered_nhl_ids(db)
        assert ids == {8478402}

    def test_null_player_names_skipped(self, db: sqlite3.Connection) -> None:
        """Rows with NULL player_name are skipped."""
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short) "
            "VALUES ('team1', NULL, 'C')"
        )
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short) "
            "VALUES ('team1', 'Connor McDavid', 'C')"
        )
        db.commit()

        ids = get_rostered_nhl_ids(db)
        assert ids == {8478402}

    def test_duplicate_names_deduplicated(self, db: sqlite3.Connection) -> None:
        """Same player on multiple teams only appears once."""
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short) "
            "VALUES ('team1', 'Connor McDavid', 'C')"
        )
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short) "
            "VALUES ('team2', 'Connor McDavid', 'C')"
        )
        db.commit()

        ids = get_rostered_nhl_ids(db)
        assert ids == {8478402}
