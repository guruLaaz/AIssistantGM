"""Tests for assistant/formatters.py — terminal output formatters."""

from assistant.formatters import (
    _line_tag,
    format_roster,
    format_free_agents,
    format_player_card,
    format_comparison,
    format_trends,
    format_standings,
    format_schedule,
    format_news,
    format_injuries,
    format_trade_targets,
    format_drop_candidates,
    format_roster_moves,
)


# ---------------------------------------------------------------------------
# format_roster
# ---------------------------------------------------------------------------


class TestFormatRoster:
    """Tests for format_roster."""

    def test_empty_list(self) -> None:
        assert format_roster([]) == "No players on roster."

    def test_single_skater(self) -> None:
        data = [{
            "player_name": "Connor McDavid", "position": "C",
            "games_played": 50, "goals": 30, "assists": 40,
            "hits": 20, "blocks": 10,
            "fantasy_points": 73.0, "fpts_per_game": 1.46, "injury": None,
        }]
        result = format_roster(data)
        assert "Connor McDavid" in result
        assert "30G 40A 20H 10B" in result
        assert "73.0" in result

    def test_goalie_row(self) -> None:
        data = [{
            "player_name": "Igor Shesterkin", "position": "G",
            "games_played": 40, "wins": 25, "losses": 10, "shutouts": 3,
            "fantasy_points": 56.0, "fpts_per_game": 1.40, "injury": None,
        }]
        result = format_roster(data)
        assert "25W 10L 3SO" in result

    def test_injured_player_tag(self) -> None:
        data = [{
            "player_name": "Test Player", "position": "C",
            "games_played": 10, "goals": 5, "assists": 5,
            "hits": 2, "blocks": 1,
            "fantasy_points": 10.3, "fpts_per_game": 1.03,
            "injury": {"status": "IR", "injury_type": "Knee"},
        }]
        result = format_roster(data)
        assert "[IR]" in result

    def test_none_injury_no_tag(self) -> None:
        data = [{
            "player_name": "Healthy", "position": "D",
            "games_played": 10, "goals": 1, "assists": 2,
            "hits": 5, "blocks": 8,
            "fantasy_points": 4.3, "fpts_per_game": 0.43, "injury": None,
        }]
        result = format_roster(data)
        assert "[" not in result.split("\n")[-1]  # no tag on last line

    def test_long_name_truncated(self) -> None:
        data = [{
            "player_name": "A" * 50, "position": "C",
            "games_played": 1, "goals": 0, "assists": 0,
            "hits": 0, "blocks": 0,
            "fantasy_points": 0.0, "fpts_per_game": 0.0, "injury": None,
        }]
        result = format_roster(data)
        lines = result.strip().split("\n")
        # Name column should be truncated, not overflow
        assert len(lines[-1]) < 120

    def test_none_player_name(self) -> None:
        """None player_name doesn't crash."""
        data = [{
            "player_name": None, "position": "C",
            "games_played": 0, "goals": 0, "assists": 0,
            "hits": 0, "blocks": 0,
            "fantasy_points": 0.0, "fpts_per_game": 0.0, "injury": None,
        }]
        result = format_roster(data)
        assert isinstance(result, str)

    def test_zero_stats(self) -> None:
        """All zeros renders without error."""
        data = [{
            "player_name": "Zero", "position": "LW",
            "games_played": 0, "goals": 0, "assists": 0,
            "hits": 0, "blocks": 0,
            "fantasy_points": 0.0, "fpts_per_game": 0.0, "injury": None,
        }]
        result = format_roster(data)
        assert "0G 0A 0H 0B" in result


# ---------------------------------------------------------------------------
# format_free_agents
# ---------------------------------------------------------------------------


class TestFormatFreeAgents:
    """Tests for format_free_agents."""

    def test_empty_list(self) -> None:
        assert format_free_agents([]) == "No free agents found."

    def test_single_skater(self) -> None:
        data = [{
            "player_name": "Leon Draisaitl", "team": "EDM",
            "position": "C", "games_played": 50,
            "goals": 25, "assists": 35, "hits": 40, "blocks": 15,
            "fpts_per_game": 1.31, "injury": None,
        }]
        result = format_free_agents(data)
        assert "Leon Draisaitl" in result
        assert "EDM" in result

    def test_goalie_free_agent(self) -> None:
        data = [{
            "player_name": "Test Goalie", "team": "NYR",
            "position": "G", "games_played": 30,
            "wins": 18, "shutouts": 2, "gaa": 2.45,
            "fpts_per_game": 1.20, "injury": None,
        }]
        result = format_free_agents(data)
        assert "18W 2SO 2.45GAA" in result

    def test_injury_tag_appended(self) -> None:
        data = [{
            "player_name": "Injured FA", "team": "CHI",
            "position": "C", "games_played": 20,
            "goals": 5, "assists": 5, "hits": 10, "blocks": 5,
            "fpts_per_game": 0.75,
            "injury": {"status": "IR", "injury_type": "Ankle"},
        }]
        result = format_free_agents(data)
        assert "[IR]" in result

    def test_none_team(self) -> None:
        """None team doesn't crash."""
        data = [{
            "player_name": "No Team", "team": None,
            "position": "D", "games_played": 10,
            "goals": 1, "assists": 2, "hits": 3, "blocks": 4,
            "fpts_per_game": 0.37, "injury": None,
        }]
        result = format_free_agents(data)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# format_player_card
# ---------------------------------------------------------------------------


class TestFormatPlayerCard:
    """Tests for format_player_card."""

    def test_none_data(self) -> None:
        assert format_player_card(None) == "Player not found."

    def test_empty_dict(self) -> None:
        assert format_player_card({}) == "Player not found."

    def test_skater_card(self) -> None:
        data = {
            "player": {"full_name": "Connor McDavid", "team_abbrev": "EDM", "position": "C"},
            "is_goalie": False,
            "season_stats": {
                "goals": 30, "assists": 40, "points": 70,
                "hits": 20, "blocks": 10, "shots": 200, "plus_minus": 15,
                "games_played": 50, "fantasy_points": 73.0, "fpts_per_game": 1.46,
            },
            "game_log": [],
            "injury": None,
            "news": [],
        }
        result = format_player_card(data)
        assert "Connor McDavid" in result
        assert "EDM" in result
        assert "G:30" in result
        assert "A:40" in result

    def test_goalie_card(self) -> None:
        data = {
            "player": {"full_name": "Juuse Saros", "team_abbrev": "NSH", "position": "G"},
            "is_goalie": True,
            "season_stats": {
                "wins": 20, "losses": 10, "ot_losses": 5, "shutouts": 3,
                "gaa": 2.45, "sv_pct": 0.915,
                "games_played": 35, "fantasy_points": 48.0, "fpts_per_game": 1.37,
            },
            "game_log": [],
            "injury": None,
            "news": [],
        }
        result = format_player_card(data)
        assert "W:20" in result
        assert "SO:3" in result
        assert "GAA:2.45" in result

    def test_with_injury_banner(self) -> None:
        data = {
            "player": {"full_name": "Test", "team_abbrev": "TOR", "position": "C"},
            "is_goalie": False,
            "season_stats": {"games_played": 10, "fantasy_points": 10.0, "fpts_per_game": 1.0},
            "game_log": [],
            "injury": {"injury_type": "Upper Body", "status": "Day-to-Day"},
            "news": [],
        }
        result = format_player_card(data)
        assert "INJURY:" in result
        assert "Upper Body" in result

    def test_with_game_log(self) -> None:
        data = {
            "player": {"full_name": "Test", "team_abbrev": "TOR", "position": "C"},
            "is_goalie": False,
            "season_stats": {"games_played": 10, "fantasy_points": 10.0, "fpts_per_game": 1.0},
            "game_log": [
                {"game_date": "2026-02-18", "goals": 2, "assists": 1,
                 "points": 3, "hits": 3, "blocks": 1, "shots": 5,
                 "fantasy_points": 3.4},
            ],
            "injury": None,
            "news": [],
        }
        result = format_player_card(data)
        assert "Recent Games:" in result
        assert "2026-02-18" in result

    def test_with_news(self) -> None:
        data = {
            "player": {"full_name": "Test", "team_abbrev": "TOR", "position": "C"},
            "is_goalie": False,
            "season_stats": {"games_played": 1, "fantasy_points": 1.0, "fpts_per_game": 1.0},
            "game_log": [],
            "injury": None,
            "news": [
                {"published_at": "2026-02-18", "headline": "Big Trade"},
            ],
        }
        result = format_player_card(data)
        assert "Recent News:" in result
        assert "Big Trade" in result

    def test_goalie_game_log(self) -> None:
        data = {
            "player": {"full_name": "Goalie", "team_abbrev": "NYR", "position": "G"},
            "is_goalie": True,
            "season_stats": {"games_played": 5, "fantasy_points": 8.0, "fpts_per_game": 1.6},
            "game_log": [
                {"game_date": "2026-02-18", "wins": 1, "losses": 0,
                 "ot_losses": 0, "shutouts": 1, "saves": 35,
                 "goals_against": 0, "fantasy_points": 3.0},
            ],
            "injury": None,
            "news": [],
        }
        result = format_player_card(data)
        assert "Recent Games:" in result
        assert "SV" in result  # header for saves


# ---------------------------------------------------------------------------
# format_comparison
# ---------------------------------------------------------------------------


class TestFormatComparison:
    """Tests for format_comparison."""

    def test_empty_list(self) -> None:
        assert format_comparison([]) == "No players to compare."

    def test_two_skaters(self) -> None:
        data = [
            {"player": {"full_name": "Player A"}, "is_goalie": False,
             "games_played": 50, "fantasy_points": 73.0, "fpts_per_game": 1.46,
             "goals": 30, "assists": 40, "points": 70, "hits": 20, "blocks": 10},
            {"player": {"full_name": "Player B"}, "is_goalie": False,
             "games_played": 48, "fantasy_points": 60.0, "fpts_per_game": 1.25,
             "goals": 20, "assists": 30, "points": 50, "hits": 15, "blocks": 8},
        ]
        result = format_comparison(data)
        assert "Player A" in result
        assert "Player B" in result
        assert "Goals" in result
        assert "Assists" in result

    def test_mixed_goalie_skater(self) -> None:
        data = [
            {"player": {"full_name": "Skater"}, "is_goalie": False,
             "games_played": 50, "fantasy_points": 50.0, "fpts_per_game": 1.0},
            {"player": {"full_name": "Goalie"}, "is_goalie": True,
             "games_played": 40, "fantasy_points": 48.0, "fpts_per_game": 1.2,
             "wins": 25, "losses": 10, "shutouts": 3, "gaa": 2.5},
        ]
        result = format_comparison(data)
        # When there's a goalie, goalie stats are shown
        assert "Wins" in result
        assert "GAA" in result

    def test_single_player(self) -> None:
        """Single player comparison still works."""
        data = [
            {"player": {"full_name": "Solo"}, "is_goalie": False,
             "games_played": 30, "fantasy_points": 30.0, "fpts_per_game": 1.0,
             "goals": 15, "assists": 10, "points": 25, "hits": 5, "blocks": 3},
        ]
        result = format_comparison(data)
        assert "Solo" in result

    def test_missing_stat_key_shows_dash(self) -> None:
        """Missing stat key shows dash."""
        data = [
            {"player": {"full_name": "Incomplete"}, "is_goalie": False,
             "games_played": 10, "fantasy_points": 5.0, "fpts_per_game": 0.5},
        ]
        result = format_comparison(data)
        assert "-" in result  # missing goals, assists etc


# ---------------------------------------------------------------------------
# format_trends
# ---------------------------------------------------------------------------


class TestFormatTrends:
    """Tests for format_trends."""

    def test_none_data(self) -> None:
        assert format_trends(None) == "Player not found."

    def test_empty_dict(self) -> None:
        assert format_trends({}) == "Player not found."

    def test_hot_trend(self) -> None:
        data = {
            "player": {"full_name": "Hot Player"},
            "windows": {
                "last_7": {"fpts_per_game": 2.5, "games": 7},
                "last_14": {"fpts_per_game": 2.0, "games": 14},
                "season": {"fpts_per_game": 1.5, "games": 50},
            },
            "trend": "hot",
        }
        result = format_trends(data)
        assert "HOT" in result

    def test_cold_trend(self) -> None:
        data = {
            "player": {"full_name": "Cold Player"},
            "windows": {
                "last_7": {"fpts_per_game": 0.5, "games": 7},
                "last_14": {"fpts_per_game": 0.8, "games": 14},
                "season": {"fpts_per_game": 1.5, "games": 50},
            },
            "trend": "cold",
        }
        result = format_trends(data)
        assert "COLD" in result

    def test_neutral_trend(self) -> None:
        data = {
            "player": {"full_name": "Stable Player"},
            "windows": {
                "last_7": {"fpts_per_game": 1.5, "games": 7},
                "last_14": {"fpts_per_game": 1.5, "games": 14},
                "season": {"fpts_per_game": 1.5, "games": 50},
            },
            "trend": "neutral",
        }
        result = format_trends(data)
        assert "Neutral" in result

    def test_empty_windows(self) -> None:
        """Empty windows still renders with zeros."""
        data = {
            "player": {"full_name": "New Player"},
            "windows": {},
            "trend": "neutral",
        }
        result = format_trends(data)
        assert "0.00" in result


# ---------------------------------------------------------------------------
# format_standings
# ---------------------------------------------------------------------------


class TestFormatStandings:
    """Tests for format_standings."""

    def test_empty_list(self) -> None:
        assert format_standings([]) == "No standings data."

    def test_single_team(self) -> None:
        data = [{
            "rank": 1, "team_name": "My Team",
            "games_played": 70, "points_for": 5000.5,
            "points_against": 4200.0, "fantasy_points_per_game": 71.4,
            "streak": "W3",
        }]
        result = format_standings(data)
        assert "My Team" in result
        assert "5000.5" in result
        assert "W3" in result

    def test_multiple_teams(self) -> None:
        data = [
            {"rank": 1, "team_name": "First", "games_played": 70,
             "points_for": 5000.0, "points_against": 4000.0,
             "fantasy_points_per_game": 71.4, "streak": "W5"},
            {"rank": 2, "team_name": "Second", "games_played": 70,
             "points_for": 4800.0, "points_against": 4200.0,
             "fantasy_points_per_game": 68.6, "streak": "L1"},
        ]
        result = format_standings(data)
        assert "First" in result
        assert "Second" in result

    def test_none_team_name_fallback(self) -> None:
        """Falls back to short_name if team_name is None."""
        data = [{
            "rank": 1, "team_name": None, "short_name": "MT",
            "games_played": 0, "points_for": 0.0,
            "points_against": 0.0, "fantasy_points_per_game": 0.0,
            "streak": "",
        }]
        result = format_standings(data)
        assert "MT" in result

    def test_missing_all_names(self) -> None:
        """Both team_name and short_name missing doesn't crash."""
        data = [{
            "rank": 1, "games_played": 0, "points_for": 0.0,
            "points_against": 0.0, "fantasy_points_per_game": 0.0,
            "streak": "",
        }]
        result = format_standings(data)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# format_schedule
# ---------------------------------------------------------------------------


class TestFormatSchedule:
    """Tests for format_schedule."""

    def test_none_data(self) -> None:
        assert format_schedule(None) == "No schedule data."

    def test_empty_dict(self) -> None:
        assert format_schedule({}) == "No schedule data."

    def test_games_listed(self) -> None:
        data = {
            "team": "EDM", "game_count": 2,
            "games": [
                {"game_date": "2026-02-20", "opponent": "CGY", "home_away": "home"},
                {"game_date": "2026-02-25", "opponent": "VAN", "home_away": "away"},
            ],
            "back_to_backs": [],
        }
        result = format_schedule(data)
        assert "EDM" in result
        assert "CGY" in result
        assert "2 games" in result

    def test_back_to_back_marked(self) -> None:
        data = {
            "team": "EDM", "game_count": 2,
            "games": [
                {"game_date": "2026-02-20", "opponent": "CGY", "home_away": "home"},
                {"game_date": "2026-02-21", "opponent": "VAN", "home_away": "away"},
            ],
            "back_to_backs": [("2026-02-20", "2026-02-21")],
        }
        result = format_schedule(data)
        assert "*" in result
        assert "back-to-back" in result

    def test_no_games(self) -> None:
        data = {"team": "EDM", "game_count": 0, "games": [], "back_to_backs": []}
        result = format_schedule(data)
        assert "0 games" in result


# ---------------------------------------------------------------------------
# format_news
# ---------------------------------------------------------------------------


class TestFormatNews:
    """Tests for format_news."""

    def test_empty_list(self) -> None:
        assert format_news([]) == "No recent news."

    def test_single_item(self) -> None:
        data = [{
            "player_name": "Connor McDavid",
            "headline": "Hat Trick Night",
            "published_at": "2026-02-18",
        }]
        result = format_news(data)
        assert "Connor McDavid" in result
        assert "Hat Trick Night" in result
        assert "2026-02-18" in result

    def test_none_player_name(self) -> None:
        data = [{
            "player_name": None,
            "headline": "Mystery",
            "published_at": "2026-02-18",
        }]
        result = format_news(data)
        assert "Unknown" in result

    def test_none_published_at(self) -> None:
        data = [{
            "player_name": "Test",
            "headline": "No Date",
            "published_at": None,
        }]
        result = format_news(data)
        assert "Test" in result

    def test_none_headline(self) -> None:
        data = [{
            "player_name": "Test",
            "headline": None,
            "published_at": "2026-02-18",
        }]
        result = format_news(data)
        assert "Test" in result


# ---------------------------------------------------------------------------
# format_injuries
# ---------------------------------------------------------------------------


class TestFormatInjuries:
    """Tests for format_injuries."""

    def test_empty_list(self) -> None:
        assert format_injuries([]) == "No injuries to report."

    def test_single_injury(self) -> None:
        data = [{
            "full_name": "Sidney Crosby", "team_abbrev": "PIT",
            "position": "C", "injury_type": "Upper Body",
            "status": "Day-to-Day", "updated_at": "2026-02-18",
        }]
        result = format_injuries(data)
        assert "Sidney Crosby" in result
        assert "PIT" in result
        assert "Upper Body" in result

    def test_none_fields(self) -> None:
        """None values for optional fields don't crash."""
        data = [{
            "full_name": None, "team_abbrev": "",
            "position": None, "injury_type": None,
            "status": None, "updated_at": None,
        }]
        result = format_injuries(data)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# format_trade_targets
# ---------------------------------------------------------------------------


class TestFormatTradeTargets:
    """Tests for format_trade_targets."""

    def test_empty_list(self) -> None:
        assert "No buy-low" in format_trade_targets([])

    def test_single_target(self) -> None:
        data = [{
            "player_name": "J.T. Miller", "owner_team_name": "Rival Team",
            "position": "C", "games_played": 50,
            "season_fpg": 1.20, "recent_7_fpg": 1.60,
            "trend_pct": 33.3, "owner_rank": 15,
        }]
        result = format_trade_targets(data)
        assert "J.T. Miller" in result
        assert "Rival Team" in result
        assert "+33%" in result

    def test_none_owner_rank(self) -> None:
        data = [{
            "player_name": "Test", "owner_team_name": "Team",
            "position": "D", "games_played": 30,
            "season_fpg": 1.0, "recent_7_fpg": 1.5,
            "trend_pct": 50.0, "owner_rank": None,
        }]
        result = format_trade_targets(data)
        assert "Test" in result


# ---------------------------------------------------------------------------
# format_drop_candidates
# ---------------------------------------------------------------------------


class TestFormatDropCandidates:
    """Tests for format_drop_candidates."""

    def test_empty_list(self) -> None:
        assert "No drop candidates" in format_drop_candidates([])

    def test_cold_streak_indicator(self) -> None:
        data = [{
            "player_name": "Cold Guy", "position": "LW",
            "games_played": 40, "season_fpg": 1.0,
            "recent_14_fpg": 0.5, "trend": "cold", "injury": None,
        }]
        result = format_drop_candidates(data)
        assert "COLD" in result

    def test_hot_indicator(self) -> None:
        data = [{
            "player_name": "Hot Guy", "position": "C",
            "games_played": 40, "season_fpg": 1.0,
            "recent_14_fpg": 1.5, "trend": "hot", "injury": None,
        }]
        result = format_drop_candidates(data)
        assert "HOT" in result

    def test_neutral_indicator(self) -> None:
        data = [{
            "player_name": "Neutral", "position": "D",
            "games_played": 40, "season_fpg": 1.0,
            "recent_14_fpg": 1.0, "trend": "neutral", "injury": None,
        }]
        result = format_drop_candidates(data)
        assert "Neut" in result

    def test_with_injury(self) -> None:
        data = [{
            "player_name": "Hurt", "position": "C",
            "games_played": 20, "season_fpg": 0.8,
            "recent_14_fpg": 0.0, "trend": "cold",
            "injury": {"status": "IR"},
        }]
        result = format_drop_candidates(data)
        assert "[IR]" in result


# ---------------------------------------------------------------------------
# format_roster_moves
# ---------------------------------------------------------------------------


class TestFormatRosterMoves:
    """Tests for format_roster_moves."""

    def test_empty_both(self) -> None:
        result = format_roster_moves([], [])
        assert "No clear drop candidates" in result
        assert "No clear pickup recommendations" in result

    def test_drops_only(self) -> None:
        drops = [{
            "player_name": "Drop Me", "position": "C",
            "season_fpg": 0.5, "recent_14_fpg": 0.3,
            "trend": "cold", "injury": None,
        }]
        result = format_roster_moves(drops, [])
        assert "Drop Me" in result
        assert "RECOMMENDED DROPS" in result
        assert "No clear pickup recommendations" in result

    def test_pickups_only(self) -> None:
        pickups = [{
            "pickup_name": "Pick Me", "pickup_position": "C",
            "pickup_fpg": 1.5, "drop_name": "Drop Me",
            "drop_fpg": 0.5, "fpg_upgrade": 1.0,
            "reason": "+1.00 FP/G upgrade",
        }]
        result = format_roster_moves([], pickups)
        assert "Pick Me" in result
        assert "RECOMMENDED PICKUPS" in result

    def test_both_populated(self) -> None:
        drops = [{
            "player_name": "Drop Me", "position": "C",
            "season_fpg": 0.5, "recent_14_fpg": 0.3,
            "trend": "cold", "injury": None,
        }]
        pickups = [{
            "pickup_name": "Pick Me", "pickup_position": "C",
            "pickup_fpg": 1.5, "drop_name": "Drop Me",
            "drop_fpg": 0.5, "fpg_upgrade": 1.0,
            "reason": "+1.00 FP/G upgrade",
        }]
        result = format_roster_moves(drops, pickups)
        assert "RECOMMENDED DROPS" in result
        assert "RECOMMENDED PICKUPS" in result
        assert "Pick Me" in result
        assert "Drop Me" in result

    def test_positive_upgrade_sign(self) -> None:
        pickups = [{
            "pickup_name": "Upgrade", "pickup_position": "D",
            "pickup_fpg": 2.0, "drop_name": "Downgrade",
            "drop_fpg": 1.0, "fpg_upgrade": 1.0,
            "reason": "upgrade",
        }]
        result = format_roster_moves([], pickups)
        assert "+1.00" in result

    def test_long_reason_displayed(self) -> None:
        """Reason column now supports up to 40 characters."""
        pickups = [{
            "pickup_name": "Pick", "pickup_position": "C",
            "pickup_fpg": 1.5, "drop_name": "Drop",
            "drop_fpg": 0.5, "fpg_upgrade": 1.0,
            "reason": "Cold streak, +1.00 FP/G | News: Big headline here",
        }]
        result = format_roster_moves([], pickups)
        # First 40 chars of reason should appear
        assert "Cold streak" in result
        assert "News:" in result

    def test_claims_banner_dict_format(self) -> None:
        """Claims remaining banner is shown when pickups is a dict."""
        pickup_data = {
            "claims_remaining": 5,
            "recommendations": [{
                "pickup_name": "Pick", "pickup_position": "C",
                "pickup_fpg": 1.5, "drop_name": "Drop",
                "drop_fpg": 0.5, "fpg_upgrade": 1.0,
                "reason": "upgrade",
            }],
        }
        result = format_roster_moves([], pickup_data)
        assert "CLAIMS REMAINING: 5/10" in result
        assert "Pick" in result

    def test_claims_scarce_warning(self) -> None:
        """Warning shown when claims_remaining <= 2."""
        pickup_data = {
            "claims_remaining": 2,
            "recommendations": [],
        }
        result = format_roster_moves([], pickup_data)
        assert "CLAIMS ARE SCARCE" in result

    def test_no_claims_banner_when_none(self) -> None:
        """No claims banner when claims_remaining is None."""
        pickup_data = {
            "claims_remaining": None,
            "recommendations": [],
        }
        result = format_roster_moves([], pickup_data)
        assert "CLAIMS REMAINING" not in result

    def test_legacy_list_format_still_works(self) -> None:
        """Backward compatibility: plain list still works."""
        pickups = [{
            "pickup_name": "Pick", "pickup_position": "C",
            "pickup_fpg": 1.5, "drop_name": "Drop",
            "drop_fpg": 0.5, "fpg_upgrade": 1.0,
            "reason": "upgrade",
        }]
        result = format_roster_moves([], pickups)
        assert "Pick" in result
        assert "CLAIMS REMAINING" not in result

    def test_gp_remaining_banner(self) -> None:
        """GP remaining banner is shown when gp_remaining is present."""
        pickup_data = {
            "claims_remaining": 5,
            "gp_remaining": {"F": 321, "D": 54, "G": 31},
            "recommendations": [],
        }
        result = format_roster_moves([], pickup_data)
        assert "GP Remaining:" in result
        assert "F=321" in result
        assert "D=54" in result
        assert "G=31" in result

    def test_total_value_columns(self) -> None:
        """Pickup table shows ~GP and TotV columns."""
        pickup_data = {
            "claims_remaining": 5,
            "gp_remaining": {"F": 321, "D": 54, "G": 31},
            "recommendations": [{
                "pickup_name": "Pick", "pickup_position": "C",
                "pickup_season_fpg": 1.0, "pickup_recent_fpg": 1.0,
                "drop_name": "Drop", "drop_position": "D",
                "drop_season_fpg": 0.5, "drop_recent_fpg": 0.3,
                "fpg_upgrade": 0.70, "est_games": 25,
                "total_value": 17.5,
                "reason": "Cross-pos: drop D, add F",
            }],
        }
        result = format_roster_moves([], pickup_data)
        assert "~GP" in result
        assert "TotV" in result
        assert "25" in result
        assert "+17.5" in result

    def test_no_gp_banner_when_none(self) -> None:
        """No GP remaining banner when gp_remaining is None."""
        pickup_data = {
            "claims_remaining": 5,
            "gp_remaining": None,
            "recommendations": [],
        }
        result = format_roster_moves([], pickup_data)
        assert "GP Remaining" not in result


# ---------------------------------------------------------------------------
# New formatter tests for TOI, trends, and news
# ---------------------------------------------------------------------------


class TestFormatPlayerCardToi:
    """Tests for TOI/PP display in format_player_card."""

    def test_toi_line_shown_for_skater(self) -> None:
        """Player card shows TOI and PP stats line for skaters."""
        data = {
            "player": {"full_name": "Test Skater", "position": "C",
                        "team_abbrev": "TST"},
            "is_goalie": False,
            "season_stats": {
                "goals": 10, "assists": 20, "points": 30,
                "hits": 50, "blocks": 25, "shots": 100,
                "plus_minus": 5, "toi": 4800, "pp_toi": 1200,
                "toi_per_game": 120,
                "powerplay_goals": 4, "powerplay_points": 12,
                "shorthanded_goals": 1, "shorthanded_points": 2,
                "games_played": 40, "fantasy_points": 37.5,
                "fpts_per_game": 0.94,
            },
            "game_log": [],
            "injury": None,
            "news": [],
        }
        result = format_player_card(data)
        assert "TOI/G: 2:00" in result
        assert "PP TOI: 20:00" in result
        assert "PPG:4" in result
        assert "PPP:12" in result
        assert "SHG:1" in result
        assert "SHP:2" in result

    def test_toi_column_in_game_log(self) -> None:
        """Game log header and rows include TOI column."""
        data = {
            "player": {"full_name": "Test Skater", "position": "C",
                        "team_abbrev": "TST"},
            "is_goalie": False,
            "season_stats": {
                "goals": 10, "assists": 20, "points": 30,
                "hits": 50, "blocks": 25, "shots": 100,
                "plus_minus": 5, "toi": 4800, "pp_toi": 1200,
                "powerplay_goals": 4, "powerplay_points": 12,
                "shorthanded_goals": 1, "shorthanded_points": 2,
                "games_played": 40, "fantasy_points": 37.5,
                "fpts_per_game": 0.94,
            },
            "game_log": [{
                "game_date": "2025-10-15", "goals": 1, "assists": 2,
                "points": 3, "hits": 3, "blocks": 1, "shots": 5,
                "toi": 1200, "fantasy_points": 3.4,
            }],
            "injury": None,
            "news": [],
        }
        result = format_player_card(data)
        assert "TOI" in result
        assert "20:00" in result  # 1200s = 20:00


class TestFormatTrends30Day:
    """Tests for 30-day window in format_trends."""

    def test_last_30_shown(self) -> None:
        """Trends output includes Last 30 window."""
        data = {
            "player": {"full_name": "Test Player"},
            "windows": {
                "last_7": {"fpts_per_game": 1.5, "games": 7},
                "last_14": {"fpts_per_game": 1.4, "games": 14},
                "last_30": {"fpts_per_game": 1.3, "games": 25},
                "season": {"fpts_per_game": 1.2, "games": 50},
            },
            "trend": "hot",
        }
        result = format_trends(data)
        assert "Last 30" in result
        assert "25 games" in result


class TestFormatComparisonToi:
    """Tests for TOI/G in format_comparison."""

    def test_toi_per_game_shown(self) -> None:
        """Comparison table shows TOI/G formatted as MM:SS."""
        data = [
            {
                "player": {"full_name": "Player A"},
                "is_goalie": False,
                "games_played": 40, "fantasy_points": 50.0,
                "fpts_per_game": 1.25, "goals": 10, "assists": 20,
                "points": 30, "hits": 50, "blocks": 25,
                "toi_per_game": 1200,  # 20:00
            },
            {
                "player": {"full_name": "Player B"},
                "is_goalie": False,
                "games_played": 40, "fantasy_points": 45.0,
                "fpts_per_game": 1.13, "goals": 8, "assists": 18,
                "points": 26, "hits": 40, "blocks": 20,
                "toi_per_game": 960,  # 16:00
            },
        ]
        result = format_comparison(data)
        assert "TOI/G" in result
        assert "20:00" in result
        assert "16:00" in result


class TestFormatTradeTargetsHighToi:
    """Tests for high-TOI underperformer tag in format_trade_targets."""

    def test_high_toi_tag(self) -> None:
        """High-TOI underperformer shows [TOI] badge with trend pct."""
        data = [{
            "player_name": "Slow Starter", "owner_team_name": "Other Team",
            "position": "LW", "games_played": 50,
            "season_fpg": 0.80, "recent_7_fpg": 0.0,
            "trend_pct": -15.0, "owner_rank": 10,
            "toi_per_game": 1200, "pp_toi": 300,
            "signal": "high_toi_underperformer",
        }]
        result = format_trade_targets(data)
        assert "[TOI]" in result
        assert "-15%" in result
        assert "20:00" in result  # 1200s = 20:00

    def test_trending_up_shows_pct(self) -> None:
        """Trending-up candidate shows +pct% as before."""
        data = [{
            "player_name": "Hot Player", "owner_team_name": "Other Team",
            "position": "C", "games_played": 50,
            "season_fpg": 1.20, "recent_7_fpg": 1.60,
            "trend_pct": 33.3, "owner_rank": 5,
            "toi_per_game": 1100, "pp_toi": 250,
            "signal": "trending_up",
        }]
        result = format_trade_targets(data)
        assert "+33%" in result
        assert "18:20" in result  # 1100s = 18:20


class TestFormatDropCandidatesNews:
    """Tests for news headline in format_drop_candidates."""

    def test_news_shown(self) -> None:
        """Drop candidate with recent_news shows truncated headline."""
        data = [{
            "player_name": "News Guy", "position": "C",
            "games_played": 40, "season_fpg": 1.0,
            "recent_14_fpg": 0.5, "trend": "cold", "injury": None,
            "recent_news": "Expected to return from injury next week",
        }]
        result = format_drop_candidates(data)
        assert "News:" in result
        assert "Expected to return" in result

    def test_no_news_no_extra_line(self) -> None:
        """Drop candidate without news has no News: line."""
        data = [{
            "player_name": "No News", "position": "D",
            "games_played": 40, "season_fpg": 1.0,
            "recent_14_fpg": 0.8, "trend": "neutral", "injury": None,
            "recent_news": None,
        }]
        result = format_drop_candidates(data)
        assert "News:" not in result


# ---------------------------------------------------------------------------
# _line_tag helper
# ---------------------------------------------------------------------------


class TestLineTag:
    """Tests for _line_tag helper."""

    def test_none_returns_empty(self) -> None:
        assert _line_tag(None) == ""

    def test_empty_dict_returns_empty(self) -> None:
        assert _line_tag({}) == ""

    def test_ev_only(self) -> None:
        assert _line_tag({"ev_line": 1}) == "L1"

    def test_pp_only(self) -> None:
        assert _line_tag({"pp_unit": 2}) == "PP2"

    def test_both_ev_and_pp(self) -> None:
        assert _line_tag({"ev_line": 1, "pp_unit": 1}) == "L1/PP1"

    def test_ev_line_4(self) -> None:
        assert _line_tag({"ev_line": 4}) == "L4"

    def test_none_values_ignored(self) -> None:
        assert _line_tag({"ev_line": None, "pp_unit": None}) == ""

    def test_zero_values_ignored(self) -> None:
        """Zero is falsy, so ev_line=0 should be treated as no line."""
        assert _line_tag({"ev_line": 0, "pp_unit": 0}) == ""


# ---------------------------------------------------------------------------
# Line info in format_player_card
# ---------------------------------------------------------------------------


class TestFormatPlayerCardLines:
    """Tests for line combination display in player card."""

    def test_line_context_shown(self) -> None:
        """Player card with line_context shows Lines, EV Linemates, PP Linemates."""
        data = {
            "player": {"full_name": "Connor McDavid", "team_abbrev": "EDM", "position": "C"},
            "is_goalie": False,
            "season_stats": {
                "games_played": 50, "fantasy_points": 73.0, "fpts_per_game": 1.46,
                "goals": 30, "assists": 40, "points": 70,
                "hits": 20, "blocks": 10, "shots": 200, "plus_minus": 15,
            },
            "game_log": [],
            "injury": None,
            "news": [],
            "line_context": {
                "ev_line": 1, "pp_unit": 1,
                "ev_linemates": ["Draisaitl", "Hyman"],
                "pp_linemates": ["Draisaitl", "Nugent-Hopkins", "Bouchard"],
            },
        }
        result = format_player_card(data)
        assert "Lines: Forward Line 1 | PP1" in result
        assert "EV Linemates: Draisaitl, Hyman" in result
        assert "PP Linemates: Draisaitl, Nugent-Hopkins, Bouchard" in result

    def test_defense_line_label(self) -> None:
        """Defenseman shows 'Defense Line X' instead of 'Forward Line X'."""
        data = {
            "player": {"full_name": "Cale Makar", "team_abbrev": "COL", "position": "D"},
            "is_goalie": False,
            "season_stats": {
                "games_played": 50, "fantasy_points": 60.0, "fpts_per_game": 1.20,
                "goals": 15, "assists": 35, "points": 50,
                "hits": 30, "blocks": 40, "shots": 180, "plus_minus": 10,
            },
            "game_log": [],
            "injury": None,
            "news": [],
            "line_context": {
                "ev_line": 1, "pp_unit": 1,
                "ev_linemates": ["Toews"],
                "pp_linemates": [],
            },
        }
        result = format_player_card(data)
        assert "Defense Line 1" in result

    def test_no_line_context_no_section(self) -> None:
        """Without line_context, no Lines section appears."""
        data = {
            "player": {"full_name": "Test", "team_abbrev": "TOR", "position": "C"},
            "is_goalie": False,
            "season_stats": {"games_played": 10, "fantasy_points": 10.0, "fpts_per_game": 1.0},
            "game_log": [],
            "injury": None,
            "news": [],
        }
        result = format_player_card(data)
        assert "Lines:" not in result
        assert "EV Linemates:" not in result

    def test_pp_only_context(self) -> None:
        """Player on PP but no EV line (unlikely but possible with goalies filtered out)."""
        data = {
            "player": {"full_name": "PP Specialist", "team_abbrev": "NYR", "position": "C"},
            "is_goalie": False,
            "season_stats": {"games_played": 10, "fantasy_points": 8.0, "fpts_per_game": 0.8},
            "game_log": [],
            "injury": None,
            "news": [],
            "line_context": {
                "ev_line": None, "pp_unit": 2,
                "ev_linemates": [],
                "pp_linemates": ["A", "B", "C"],
            },
        }
        result = format_player_card(data)
        assert "PP2" in result
        assert "Forward Line" not in result


# ---------------------------------------------------------------------------
# Line info in format_comparison
# ---------------------------------------------------------------------------


class TestFormatComparisonLines:
    """Tests for line combination row in player comparison."""

    def test_line_row_shown(self) -> None:
        """Comparison shows Line row when at least one player has line_context."""
        data = [
            {"player": {"full_name": "Player A"}, "is_goalie": False,
             "games_played": 50, "fantasy_points": 73.0, "fpts_per_game": 1.46,
             "goals": 30, "assists": 40, "points": 70, "hits": 20, "blocks": 10,
             "line_context": {"ev_line": 1, "pp_unit": 1}},
            {"player": {"full_name": "Player B"}, "is_goalie": False,
             "games_played": 48, "fantasy_points": 60.0, "fpts_per_game": 1.25,
             "goals": 20, "assists": 30, "points": 50, "hits": 15, "blocks": 8,
             "line_context": {"ev_line": 3, "pp_unit": None}},
        ]
        result = format_comparison(data)
        assert "Line" in result
        assert "L1/PP1" in result
        assert "L3" in result

    def test_no_line_row_when_no_context(self) -> None:
        """Line row is omitted when no players have line_context."""
        data = [
            {"player": {"full_name": "Player A"}, "is_goalie": False,
             "games_played": 50, "fantasy_points": 73.0, "fpts_per_game": 1.46,
             "goals": 30, "assists": 40, "points": 70, "hits": 20, "blocks": 10},
        ]
        result = format_comparison(data)
        lines = result.strip().split("\n")
        # No line with "Line" label at column position
        assert not any(line.startswith("Line") for line in lines)


# ---------------------------------------------------------------------------
# Line info in format_free_agents
# ---------------------------------------------------------------------------


class TestFormatFreeAgentsLines:
    """Tests for Line column in free agents table."""

    def test_line_column_shown(self) -> None:
        """Free agent with ev_line and pp_unit shows line tag."""
        data = [{
            "player_name": "Hot FA", "team": "TOR",
            "position": "C", "games_played": 40,
            "goals": 15, "assists": 25, "hits": 30, "blocks": 10,
            "fpts_per_game": 1.20, "injury": None,
            "ev_line": 1, "pp_unit": 1,
        }]
        result = format_free_agents(data)
        assert "L1/PP1" in result

    def test_no_line_data_empty_column(self) -> None:
        """Free agent without line data shows no line tag."""
        data = [{
            "player_name": "No Lines", "team": "CHI",
            "position": "D", "games_played": 30,
            "goals": 2, "assists": 5, "hits": 20, "blocks": 30,
            "fpts_per_game": 0.50, "injury": None,
        }]
        result = format_free_agents(data)
        assert "Line" in result  # header is there
        # But no L#/PP# tag on the data row
        lines = result.strip().split("\n")
        assert "L1" not in lines[-1]


# ---------------------------------------------------------------------------
# Line info in format_trade_targets
# ---------------------------------------------------------------------------


class TestFormatTradeTargetsLines:
    """Tests for Line column in trade targets."""

    def test_line_column_shown(self) -> None:
        data = [{
            "player_name": "Buy Low", "owner_team_name": "Rival",
            "position": "C", "games_played": 50,
            "season_fpg": 1.00, "recent_7_fpg": 1.50,
            "trend_pct": 50.0, "owner_rank": 10,
            "toi_per_game": 1100, "pp_toi": 200,
            "signal": "trending_up",
            "line_info": {"ev_line": 2, "pp_unit": 1},
        }]
        result = format_trade_targets(data)
        assert "L2/PP1" in result


# ---------------------------------------------------------------------------
# Line info in format_drop_candidates
# ---------------------------------------------------------------------------


class TestFormatDropCandidatesLines:
    """Tests for Line column in drop candidates."""

    def test_line_column_shown(self) -> None:
        data = [{
            "player_name": "Drop Me", "position": "LW",
            "games_played": 40, "season_fpg": 0.80,
            "recent_14_fpg": 0.40, "trend": "cold", "injury": None,
            "line_info": {"ev_line": 4, "pp_unit": None},
        }]
        result = format_drop_candidates(data)
        assert "L4" in result

    def test_no_line_info(self) -> None:
        data = [{
            "player_name": "No Lines", "position": "D",
            "games_played": 40, "season_fpg": 0.80,
            "recent_14_fpg": 0.40, "trend": "cold", "injury": None,
        }]
        result = format_drop_candidates(data)
        assert "Line" in result  # header
        lines = result.strip().split("\n")
        # data row shouldn't have L#
        assert "L1" not in lines[-1]


# ---------------------------------------------------------------------------
# Fix 7: trade targets trend_pct display
# ---------------------------------------------------------------------------


class TestFormatTradeTargetsTrendPct:
    """Tests for trend_pct display in format_trade_targets."""

    def test_high_toi_shows_toi_badge_and_pct(self) -> None:
        """High-TOI underperformer shows [TOI] badge with actual trend_pct."""
        data = [{
            "player_name": "Buy Low Player", "owner_team_name": "Other Team",
            "position": "LW", "games_played": 50,
            "season_fpg": 0.80, "recent_7_fpg": 0.60,
            "trend_pct": -25.0, "owner_rank": 10,
            "toi_per_game": 1200, "pp_toi": 300,
            "signal": "high_toi_underperformer",
        }]
        result = format_trade_targets(data)
        assert "[TOI]" in result
        assert "-25%" in result

    def test_positive_trending_player_uses_plus_sign(self) -> None:
        """Trending-up player shows +pct% with format spec."""
        data = [{
            "player_name": "Hot Player", "owner_team_name": "Other Team",
            "position": "C", "games_played": 50,
            "season_fpg": 1.20, "recent_7_fpg": 1.60,
            "trend_pct": 33.3, "owner_rank": 5,
            "toi_per_game": 1100, "pp_toi": 250,
            "signal": "trending_up",
        }]
        result = format_trade_targets(data)
        assert "+33%" in result

    def test_negative_trend_pct_shows_minus(self) -> None:
        """High-TOI underperformer with negative trend shows minus sign."""
        data = [{
            "player_name": "Cold But Talented", "owner_team_name": "Rebuilders",
            "position": "C", "games_played": 60,
            "season_fpg": 1.00, "recent_7_fpg": 0.70,
            "trend_pct": -30.0, "owner_rank": 15,
            "toi_per_game": 1100, "pp_toi": 200,
            "signal": "high_toi_underperformer",
        }]
        result = format_trade_targets(data)
        assert "-30%" in result
        assert "[TOI]" in result


# ---------------------------------------------------------------------------
# Fix 9: peripheral_fpg in format_free_agents
# ---------------------------------------------------------------------------


class TestFormatFreeAgentsPeripherals:
    """Tests for peripheral_fpg column in format_free_agents."""

    def test_peri_column_in_header(self) -> None:
        """Free agents table has Peri column header."""
        data = [{
            "player_name": "Physical Player", "team": "BOS",
            "position": "D", "games_played": 50,
            "goals": 3, "assists": 10, "hits": 150, "blocks": 100,
            "fpts_per_game": 0.76, "peripheral_fpg": 0.50,
            "injury": None,
        }]
        result = format_free_agents(data)
        assert "Peri" in result

    def test_skater_peripheral_value_shown(self) -> None:
        """Skater shows peripheral_fpg value."""
        data = [{
            "player_name": "Physical Player", "team": "BOS",
            "position": "D", "games_played": 50,
            "goals": 3, "assists": 10, "hits": 150, "blocks": 100,
            "fpts_per_game": 0.76, "peripheral_fpg": 0.50,
            "injury": None,
        }]
        result = format_free_agents(data)
        assert "0.50" in result

    def test_goalie_shows_dash_for_peripherals(self) -> None:
        """Goalie shows dash instead of peripheral value."""
        data = [{
            "player_name": "Test Goalie", "team": "NYR",
            "position": "G", "games_played": 30,
            "wins": 18, "shutouts": 2, "gaa": 2.45,
            "fpts_per_game": 1.20, "peripheral_fpg": 0.0,
            "injury": None,
        }]
        result = format_free_agents(data)
        # Goalie row should have dash in Peri column
        lines = result.strip().split("\n")
        goalie_line = [l for l in lines if "Test Goalie" in l][0]
        assert "-" in goalie_line
