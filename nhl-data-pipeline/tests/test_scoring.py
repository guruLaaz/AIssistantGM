"""Tests for assistant/scoring.py — fantasy point calculations."""

from assistant.scoring import (
    SKATER_SCORING,
    GOALIE_SCORING,
    calc_skater_fpts,
    calc_goalie_fpts,
    calc_skater_fpts_from_row,
    calc_goalie_fpts_from_row,
)


class TestScoringConstants:
    """Tests for scoring constant dictionaries."""

    def test_skater_scoring_values(self) -> None:
        """Skater scoring matches league rules."""
        assert SKATER_SCORING == {"goals": 1.0, "assists": 1.0, "blocks": 0.1, "hits": 0.1}

    def test_goalie_scoring_values(self) -> None:
        """Goalie scoring matches league rules."""
        assert GOALIE_SCORING == {
            "goals": 1.0,
            "assists": 1.0,
            "wins": 2.0,
            "shutouts": 1.0,
            "ot_losses": 1.0,
            "losses": 0.0,
        }


class TestCalcSkaterFpts:
    """Tests for calc_skater_fpts function."""

    def test_basic_calculation(self) -> None:
        """Single goal = 1.0 FP."""
        assert calc_skater_fpts(goals=1, assists=0, blocks=0, hits=0) == 1.0

    def test_full_season_example(self) -> None:
        """30G + 40A + 100Blk + 150Hit = 30 + 40 + 10 + 15 = 95.0 FP."""
        result = calc_skater_fpts(goals=30, assists=40, blocks=100, hits=150)
        assert result == 95.0

    def test_all_zeros(self) -> None:
        """All zeros = 0.0 FP."""
        assert calc_skater_fpts(goals=0, assists=0, blocks=0, hits=0) == 0.0

    def test_only_blocks_and_hits(self) -> None:
        """10 blocks + 10 hits = 1.0 + 1.0 = 2.0 FP."""
        assert calc_skater_fpts(goals=0, assists=0, blocks=10, hits=10) == 2.0

    def test_single_game_multi_point(self) -> None:
        """2G + 1A + 3Blk + 5Hit = 2 + 1 + 0.3 + 0.5 = 3.8 FP."""
        result = calc_skater_fpts(goals=2, assists=1, blocks=3, hits=5)
        assert abs(result - 3.8) < 1e-9


class TestCalcGoalieFpts:
    """Tests for calc_goalie_fpts function."""

    def test_shutout_win(self) -> None:
        """Shutout win = W(2) + SHO(1) = 3.0 FP."""
        result = calc_goalie_fpts(goals=0, assists=0, wins=1, shutouts=1, ot_losses=0)
        assert result == 3.0

    def test_regular_win(self) -> None:
        """Regular win (no shutout) = 2.0 FP."""
        result = calc_goalie_fpts(goals=0, assists=0, wins=1, shutouts=0, ot_losses=0)
        assert result == 2.0

    def test_goalie_loss(self) -> None:
        """Regulation loss = 0.0 FP."""
        result = calc_goalie_fpts(goals=0, assists=0, wins=0, shutouts=0, ot_losses=0, losses=1)
        assert result == 0.0

    def test_overtime_loss(self) -> None:
        """OT loss = 1.0 FP."""
        result = calc_goalie_fpts(goals=0, assists=0, wins=0, shutouts=0, ot_losses=1)
        assert result == 1.0

    def test_goalie_with_goal_and_assist(self) -> None:
        """Goalie scores a goal and an assist in a win = 1 + 1 + 2 = 4.0 FP."""
        result = calc_goalie_fpts(goals=1, assists=1, wins=1, shutouts=0, ot_losses=0)
        assert result == 4.0

    def test_all_zeros(self) -> None:
        """All zeros = 0.0 FP."""
        result = calc_goalie_fpts(goals=0, assists=0, wins=0, shutouts=0, ot_losses=0, losses=0)
        assert result == 0.0

    def test_losses_default_zero(self) -> None:
        """Losses parameter defaults to 0."""
        result = calc_goalie_fpts(goals=0, assists=0, wins=0, shutouts=0, ot_losses=0)
        assert result == 0.0


class TestCalcSkaterFptsFromRow:
    """Tests for calc_skater_fpts_from_row function."""

    def test_standard_row(self) -> None:
        """Standard DB row with all keys present."""
        row = {"goals": 2, "assists": 1, "blocks": 5, "hits": 3}
        assert calc_skater_fpts_from_row(row) == 2 + 1 + 0.5 + 0.3

    def test_full_season_row(self) -> None:
        """30G + 40A + 100Blk + 150Hit = 95.0 FP."""
        row = {"goals": 30, "assists": 40, "blocks": 100, "hits": 150}
        assert calc_skater_fpts_from_row(row) == 95.0

    def test_missing_keys_default_to_zero(self) -> None:
        """Missing keys are treated as 0."""
        row = {"goals": 5}
        assert calc_skater_fpts_from_row(row) == 5.0

    def test_empty_row(self) -> None:
        """Empty dict returns 0.0."""
        assert calc_skater_fpts_from_row({}) == 0.0

    def test_none_values_treated_as_zero(self) -> None:
        """None values in row are treated as 0."""
        row = {"goals": None, "assists": 3, "blocks": None, "hits": None}
        assert calc_skater_fpts_from_row(row) == 3.0

    def test_extra_keys_ignored(self) -> None:
        """Extra keys in the row dict are ignored."""
        row = {
            "goals": 1, "assists": 1, "blocks": 0, "hits": 0,
            "player_id": 8478402, "game_date": "2025-11-01", "toi": 1200,
        }
        assert calc_skater_fpts_from_row(row) == 2.0


class TestCalcGoalieFptsFromRow:
    """Tests for calc_goalie_fpts_from_row function."""

    def test_standard_row(self) -> None:
        """Standard goalie row with a win."""
        row = {"goals": 0, "assists": 0, "wins": 1, "shutouts": 0, "ot_losses": 0, "losses": 0}
        assert calc_goalie_fpts_from_row(row) == 2.0

    def test_shutout_win_row(self) -> None:
        """Shutout win row = 3.0 FP."""
        row = {"goals": 0, "assists": 0, "wins": 1, "shutouts": 1, "ot_losses": 0, "losses": 0}
        assert calc_goalie_fpts_from_row(row) == 3.0

    def test_goalie_stats_row_without_goals_assists(self) -> None:
        """goalie_stats table doesn't have goals/assists columns — defaults to 0."""
        row = {"wins": 1, "shutouts": 0, "ot_losses": 0, "losses": 0}
        assert calc_goalie_fpts_from_row(row) == 2.0

    def test_none_values_treated_as_zero(self) -> None:
        """None values in row are treated as 0."""
        row = {"goals": None, "assists": None, "wins": 1, "shutouts": None, "ot_losses": None, "losses": None}
        assert calc_goalie_fpts_from_row(row) == 2.0

    def test_empty_row(self) -> None:
        """Empty dict returns 0.0."""
        assert calc_goalie_fpts_from_row({}) == 0.0

    def test_extra_keys_ignored(self) -> None:
        """Extra keys in the row dict are ignored."""
        row = {
            "wins": 1, "shutouts": 1, "ot_losses": 0, "losses": 0,
            "saves": 35, "goals_against": 0, "shots_against": 35, "toi": 3600,
        }
        assert calc_goalie_fpts_from_row(row) == 3.0

    def test_ot_loss_row(self) -> None:
        """OT loss from a goalie_stats row = 1.0 FP."""
        row = {"wins": 0, "shutouts": 0, "ot_losses": 1, "losses": 0}
        assert calc_goalie_fpts_from_row(row) == 1.0
