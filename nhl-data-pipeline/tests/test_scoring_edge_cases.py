"""Edge case tests for assistant/scoring.py.

Covers negative values, floats, large values, and type handling.
"""

from assistant.scoring import (
    calc_skater_fpts,
    calc_goalie_fpts,
    calc_skater_fpts_from_row,
    calc_goalie_fpts_from_row,
    _safe_get,
)


# ---------------------------------------------------------------------------
# _safe_get
# ---------------------------------------------------------------------------


class TestSafeGet:
    """Tests for the _safe_get internal helper."""

    def test_existing_key(self) -> None:
        assert _safe_get({"goals": 5}, "goals") == 5

    def test_missing_key(self) -> None:
        assert _safe_get({}, "goals") == 0

    def test_none_value(self) -> None:
        assert _safe_get({"goals": None}, "goals") == 0

    def test_zero_value(self) -> None:
        assert _safe_get({"goals": 0}, "goals") == 0

    def test_float_value(self) -> None:
        assert _safe_get({"goals": 2.5}, "goals") == 2.5

    def test_negative_value(self) -> None:
        """Negative values are returned as-is (not clamped)."""
        assert _safe_get({"goals": -3}, "goals") == -3


# ---------------------------------------------------------------------------
# calc_skater_fpts edge cases
# ---------------------------------------------------------------------------


class TestCalcSkaterFptsEdgeCases:
    """Edge cases for calc_skater_fpts."""

    def test_negative_goals(self) -> None:
        """Negative goals produce negative FP (no clamping)."""
        result = calc_skater_fpts(goals=-1, assists=0, blocks=0, hits=0)
        assert result == -1.0

    def test_float_values(self) -> None:
        """Float stats work correctly."""
        result = calc_skater_fpts(goals=0.5, assists=0.5, blocks=5.0, hits=5.0)
        assert abs(result - 2.0) < 1e-9

    def test_very_large_values(self) -> None:
        """Very large stat values don't overflow."""
        result = calc_skater_fpts(goals=1000, assists=1000, blocks=10000, hits=10000)
        assert result == 1000 + 1000 + 1000 + 1000

    def test_mixed_positive_negative(self) -> None:
        """Mixed positive and negative values."""
        result = calc_skater_fpts(goals=10, assists=-5, blocks=0, hits=0)
        assert result == 5.0


# ---------------------------------------------------------------------------
# calc_goalie_fpts edge cases
# ---------------------------------------------------------------------------


class TestCalcGoalieFptsEdgeCases:
    """Edge cases for calc_goalie_fpts."""

    def test_negative_wins(self) -> None:
        """Negative wins produce negative FP."""
        result = calc_goalie_fpts(goals=0, assists=0, wins=-1, shutouts=0, ot_losses=0)
        assert result == -2.0

    def test_float_values(self) -> None:
        """Float values work correctly."""
        result = calc_goalie_fpts(goals=0.0, assists=0.0, wins=0.5, shutouts=0.0, ot_losses=0.5)
        assert abs(result - 1.5) < 1e-9

    def test_very_large_values(self) -> None:
        result = calc_goalie_fpts(goals=0, assists=0, wins=500, shutouts=100, ot_losses=200)
        assert result == 500 * 2 + 100 + 200

    def test_all_ones(self) -> None:
        """All values = 1: 1 + 1 + 2 + 1 + 1 + 0 = 6."""
        result = calc_goalie_fpts(goals=1, assists=1, wins=1, shutouts=1, ot_losses=1, losses=1)
        assert result == 6.0


# ---------------------------------------------------------------------------
# from_row edge cases
# ---------------------------------------------------------------------------


class TestFromRowEdgeCases:
    """Edge cases for calc_*_fpts_from_row functions."""

    def test_skater_row_with_negative_values(self) -> None:
        row = {"goals": -2, "assists": 5, "blocks": 0, "hits": 0}
        assert calc_skater_fpts_from_row(row) == 3.0

    def test_skater_row_all_none(self) -> None:
        row = {"goals": None, "assists": None, "blocks": None, "hits": None}
        assert calc_skater_fpts_from_row(row) == 0.0

    def test_goalie_row_with_negative_values(self) -> None:
        row = {"wins": -1, "shutouts": 0, "ot_losses": 0, "losses": 0}
        assert calc_goalie_fpts_from_row(row) == -2.0

    def test_goalie_row_all_none(self) -> None:
        row = {
            "goals": None, "assists": None, "wins": None,
            "shutouts": None, "ot_losses": None, "losses": None,
        }
        assert calc_goalie_fpts_from_row(row) == 0.0

    def test_skater_row_float_stats(self) -> None:
        """Float values in row produce correct FP."""
        row = {"goals": 1.5, "assists": 2.5, "blocks": 10.0, "hits": 10.0}
        expected = 1.5 + 2.5 + 1.0 + 1.0
        assert abs(calc_skater_fpts_from_row(row) - expected) < 1e-9

    def test_skater_row_zero_value_not_confused_with_none(self) -> None:
        """Zero values should be used, not replaced with 0."""
        row = {"goals": 0, "assists": 0, "blocks": 0, "hits": 0}
        assert calc_skater_fpts_from_row(row) == 0.0

    def test_goalie_row_empty_dict(self) -> None:
        assert calc_goalie_fpts_from_row({}) == 0.0

    def test_skater_row_extra_keys_only(self) -> None:
        """Row with only extra keys (no scoring keys) returns 0."""
        row = {"player_id": 123, "game_date": "2025-10-10", "toi": 1200}
        assert calc_skater_fpts_from_row(row) == 0.0
