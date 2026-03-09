"""Tests for assistant/formatters.py — compact JSON formatters."""

import json

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
    format_team_roster,
    format_web_search_results,
)


def _parse(result: str):
    """Parse JSON result; returns list or dict."""
    return json.loads(result)


# ---------------------------------------------------------------------------
# format_roster
# ---------------------------------------------------------------------------


class TestFormatRoster:
    def test_empty_list(self) -> None:
        assert format_roster([]) == "No players on roster."

    def test_single_skater(self) -> None:
        data = [{
            "player_name": "Connor McDavid", "position": "C",
            "games_played": 50, "goals": 30, "assists": 40,
            "hits": 20, "blocks": 10,
            "fantasy_points": 73.0, "fpts_per_game": 1.46, "injury": None,
        }]
        result = _parse(format_roster(data))
        assert len(result) == 1
        p = result[0]
        assert p["name"] == "Connor McDavid"
        assert p["g"] == 30
        assert p["a"] == 40
        assert p["fp"] == 73.0

    def test_goalie_row(self) -> None:
        data = [{
            "player_name": "Igor Shesterkin", "position": "G",
            "games_played": 40, "wins": 25, "losses": 10, "shutouts": 3,
            "fantasy_points": 56.0, "fpts_per_game": 1.40, "injury": None,
        }]
        result = _parse(format_roster(data))
        p = result[0]
        assert p["w"] == 25
        assert p["l"] == 10
        assert p["so"] == 3

    def test_injured_player_tag(self) -> None:
        data = [{
            "player_name": "Test Player", "position": "C",
            "games_played": 10, "goals": 5, "assists": 5,
            "hits": 2, "blocks": 1,
            "fantasy_points": 10.3, "fpts_per_game": 1.03,
            "injury": {"status": "IR", "injury_type": "Knee"},
        }]
        result = _parse(format_roster(data))
        assert result[0]["inj"] == "IR"

    def test_none_injury_no_tag(self) -> None:
        data = [{
            "player_name": "Healthy", "position": "D",
            "games_played": 10, "goals": 1, "assists": 2,
            "hits": 5, "blocks": 8,
            "fantasy_points": 4.3, "fpts_per_game": 0.43, "injury": None,
        }]
        result = _parse(format_roster(data))
        assert "inj" not in result[0]

    def test_none_player_name(self) -> None:
        data = [{
            "player_name": None, "position": "C",
            "games_played": 0, "goals": 0, "assists": 0,
            "hits": 0, "blocks": 0,
            "fantasy_points": 0.0, "fpts_per_game": 0.0, "injury": None,
        }]
        result = _parse(format_roster(data))
        assert result[0]["name"] == ""

    def test_zero_stats(self) -> None:
        data = [{
            "player_name": "Zero", "position": "LW",
            "games_played": 0, "goals": 0, "assists": 0,
            "hits": 0, "blocks": 0,
            "fantasy_points": 0.0, "fpts_per_game": 0.0, "injury": None,
        }]
        result = _parse(format_roster(data))
        p = result[0]
        assert p["g"] == 0
        assert p["a"] == 0
        assert p["h"] == 0
        assert p["b"] == 0

    def test_salary_displayed(self) -> None:
        data = [{
            "player_name": "Connor McDavid", "position": "C",
            "games_played": 50, "goals": 30, "assists": 40,
            "hits": 20, "blocks": 10,
            "fantasy_points": 73.0, "fpts_per_game": 1.46,
            "salary": 12_500_000, "injury": None,
        }]
        result = _parse(format_roster(data))
        assert result[0]["sal"] == 12.5

    def test_zero_salary_omitted(self) -> None:
        data = [{
            "player_name": "No Salary", "position": "C",
            "games_played": 10, "goals": 1, "assists": 2,
            "hits": 5, "blocks": 3,
            "fantasy_points": 3.8, "fpts_per_game": 0.38,
            "salary": 0, "injury": None,
        }]
        result = _parse(format_roster(data))
        assert "sal" not in result[0]

    def test_none_salary_omitted(self) -> None:
        data = [{
            "player_name": "None Salary", "position": "D",
            "games_played": 10, "goals": 1, "assists": 2,
            "hits": 5, "blocks": 8,
            "fantasy_points": 4.3, "fpts_per_game": 0.43,
            "salary": None, "injury": None,
        }]
        result = _parse(format_roster(data))
        assert "sal" not in result[0]

    def test_multiple_players(self) -> None:
        data = [
            {"player_name": "A", "position": "C", "games_played": 1, "goals": 1, "assists": 0,
             "hits": 0, "blocks": 0, "fantasy_points": 1.0, "fpts_per_game": 1.0, "injury": None},
            {"player_name": "B", "position": "D", "games_played": 2, "goals": 0, "assists": 1,
             "hits": 1, "blocks": 1, "fantasy_points": 1.2, "fpts_per_game": 0.6, "injury": None},
        ]
        result = _parse(format_roster(data))
        assert len(result) == 2
        assert result[0]["name"] == "A"
        assert result[1]["name"] == "B"


# ---------------------------------------------------------------------------
# format_free_agents
# ---------------------------------------------------------------------------


class TestFormatFreeAgents:
    def test_empty_list(self) -> None:
        assert format_free_agents([]) == "No free agents found."

    def test_single_skater(self) -> None:
        data = [{
            "player_name": "Leon Draisaitl", "team": "EDM",
            "position": "C", "games_played": 50,
            "goals": 25, "assists": 35, "hits": 40, "blocks": 15,
            "fpts_per_game": 1.31, "injury": None,
        }]
        result = _parse(format_free_agents(data))
        p = result[0]
        assert p["name"] == "Leon Draisaitl"
        assert p["team"] == "EDM"

    def test_goalie_free_agent(self) -> None:
        data = [{
            "player_name": "Test Goalie", "team": "NYR",
            "position": "G", "games_played": 30,
            "wins": 18, "shutouts": 2, "gaa": 2.45,
            "fpts_per_game": 1.20, "injury": None,
        }]
        result = _parse(format_free_agents(data))
        p = result[0]
        assert p["w"] == 18
        assert p["so"] == 2
        assert p["gaa"] == 2.45

    def test_injury_tag(self) -> None:
        data = [{
            "player_name": "Injured FA", "team": "CHI",
            "position": "C", "games_played": 20,
            "goals": 5, "assists": 5, "hits": 10, "blocks": 5,
            "fpts_per_game": 0.75,
            "injury": {"status": "IR", "injury_type": "Ankle"},
        }]
        result = _parse(format_free_agents(data))
        assert result[0]["inj"] == "IR"

    def test_peripheral_fpg(self) -> None:
        data = [{
            "player_name": "Physical", "team": "BOS",
            "position": "D", "games_played": 50,
            "goals": 3, "assists": 10, "hits": 150, "blocks": 100,
            "fpts_per_game": 0.76, "peripheral_fpg": 0.50,
            "injury": None,
        }]
        result = _parse(format_free_agents(data))
        assert result[0]["peri_fpg"] == 0.50

    def test_goalie_no_peripheral(self) -> None:
        data = [{
            "player_name": "Goalie", "team": "NYR",
            "position": "G", "games_played": 30,
            "wins": 18, "shutouts": 2, "gaa": 2.45,
            "fpts_per_game": 1.20, "peripheral_fpg": 0.0,
            "injury": None,
        }]
        result = _parse(format_free_agents(data))
        assert "peri_fpg" not in result[0]

    def test_line_tag_shown(self) -> None:
        data = [{
            "player_name": "Hot FA", "team": "TOR",
            "position": "C", "games_played": 40,
            "goals": 15, "assists": 25, "hits": 30, "blocks": 10,
            "fpts_per_game": 1.20, "injury": None,
            "ev_line": 1, "pp_unit": 1,
        }]
        result = _parse(format_free_agents(data))
        assert result[0]["line"] == "L1/PP1"

    def test_none_team(self) -> None:
        data = [{
            "player_name": "No Team", "team": None,
            "position": "D", "games_played": 10,
            "goals": 1, "assists": 2, "hits": 3, "blocks": 4,
            "fpts_per_game": 0.37, "injury": None,
        }]
        result = _parse(format_free_agents(data))
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# format_player_card
# ---------------------------------------------------------------------------


class TestFormatPlayerCard:
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
            "game_log": [], "injury": None, "news": [],
        }
        result = _parse(format_player_card(data))
        assert result["name"] == "Connor McDavid"
        assert result["team"] == "EDM"
        assert result["g"] == 30
        assert result["a"] == 40

    def test_goalie_card(self) -> None:
        data = {
            "player": {"full_name": "Juuse Saros", "team_abbrev": "NSH", "position": "G"},
            "is_goalie": True,
            "season_stats": {
                "wins": 20, "losses": 10, "ot_losses": 5, "shutouts": 3,
                "gaa": 2.45, "sv_pct": 0.915,
                "games_played": 35, "fantasy_points": 48.0, "fpts_per_game": 1.37,
            },
            "game_log": [], "injury": None, "news": [],
        }
        result = _parse(format_player_card(data))
        assert result["w"] == 20
        assert result["so"] == 3
        assert result["gaa"] == 2.45

    def test_with_injury(self) -> None:
        data = {
            "player": {"full_name": "Test", "team_abbrev": "TOR", "position": "C"},
            "is_goalie": False,
            "season_stats": {"games_played": 10, "fantasy_points": 10.0, "fpts_per_game": 1.0},
            "game_log": [],
            "injury": {"injury_type": "Upper Body", "status": "Day-to-Day"},
            "news": [],
        }
        result = _parse(format_player_card(data))
        assert "Upper Body" in result["inj"]

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
            "injury": None, "news": [],
        }
        result = _parse(format_player_card(data))
        assert len(result["log"]) == 1
        assert result["log"][0]["date"] == "2026-02-18"

    def test_with_news(self) -> None:
        data = {
            "player": {"full_name": "Test", "team_abbrev": "TOR", "position": "C"},
            "is_goalie": False,
            "season_stats": {"games_played": 1, "fantasy_points": 1.0, "fpts_per_game": 1.0},
            "game_log": [], "injury": None,
            "news": [{"published_at": "2026-02-18", "headline": "Big Trade"}],
        }
        result = _parse(format_player_card(data))
        assert result["news"][0]["hl"] == "Big Trade"

    def test_line_context(self) -> None:
        data = {
            "player": {"full_name": "Connor McDavid", "team_abbrev": "EDM", "position": "C"},
            "is_goalie": False,
            "season_stats": {
                "games_played": 50, "fantasy_points": 73.0, "fpts_per_game": 1.46,
                "goals": 30, "assists": 40, "points": 70,
                "hits": 20, "blocks": 10, "shots": 200, "plus_minus": 15,
            },
            "game_log": [], "injury": None, "news": [],
            "line_context": {
                "ev_line": 1, "pp_unit": 1,
                "ev_linemates": ["Draisaitl", "Hyman"],
                "pp_linemates": ["Draisaitl", "Nugent-Hopkins", "Bouchard"],
            },
        }
        result = _parse(format_player_card(data))
        assert result["line"] == "L1/PP1"
        assert "Draisaitl" in result["ev_mates"]
        assert "Bouchard" in result["pp_mates"]

    def test_no_line_context(self) -> None:
        data = {
            "player": {"full_name": "Test", "team_abbrev": "TOR", "position": "C"},
            "is_goalie": False,
            "season_stats": {"games_played": 10, "fantasy_points": 10.0, "fpts_per_game": 1.0},
            "game_log": [], "injury": None, "news": [],
        }
        result = _parse(format_player_card(data))
        assert "line" not in result

    def test_toi_and_pp_stats(self) -> None:
        data = {
            "player": {"full_name": "Test", "position": "C", "team_abbrev": "TST"},
            "is_goalie": False,
            "season_stats": {
                "goals": 10, "assists": 20, "points": 30,
                "hits": 50, "blocks": 25, "shots": 100,
                "plus_minus": 5, "toi_per_game": 120, "pp_toi": 1200,
                "powerplay_goals": 4, "powerplay_points": 12,
                "shorthanded_goals": 1, "shorthanded_points": 2,
                "games_played": 40, "fantasy_points": 37.5, "fpts_per_game": 0.94,
            },
            "game_log": [], "injury": None, "news": [],
        }
        result = _parse(format_player_card(data))
        assert result["toi"] == "2:00"
        assert result["pp_toi"] == "20:00"
        assert result["ppg"] == 4
        assert result["ppp"] == 12
        assert result["shg"] == 1
        assert result["shp"] == 2

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
            "injury": None, "news": [],
        }
        result = _parse(format_player_card(data))
        assert result["log"][0]["sv"] == 35

    def test_pp_only_context(self) -> None:
        data = {
            "player": {"full_name": "PP Specialist", "team_abbrev": "NYR", "position": "C"},
            "is_goalie": False,
            "season_stats": {"games_played": 10, "fantasy_points": 8.0, "fpts_per_game": 0.8},
            "game_log": [], "injury": None, "news": [],
            "line_context": {
                "ev_line": None, "pp_unit": 2,
                "ev_linemates": [], "pp_linemates": ["A", "B", "C"],
            },
        }
        result = _parse(format_player_card(data))
        assert result["line"] == "PP2"


# ---------------------------------------------------------------------------
# format_comparison
# ---------------------------------------------------------------------------


class TestFormatComparison:
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
        result = _parse(format_comparison(data))
        assert len(result) == 2
        assert result[0]["name"] == "Player A"
        assert result[1]["name"] == "Player B"
        assert result[0]["g"] == 30
        assert result[1]["g"] == 20

    def test_mixed_goalie_skater(self) -> None:
        data = [
            {"player": {"full_name": "Skater"}, "is_goalie": False,
             "games_played": 50, "fantasy_points": 50.0, "fpts_per_game": 1.0},
            {"player": {"full_name": "Goalie"}, "is_goalie": True,
             "games_played": 40, "fantasy_points": 48.0, "fpts_per_game": 1.2,
             "wins": 25, "losses": 10, "shutouts": 3, "gaa": 2.5},
        ]
        result = _parse(format_comparison(data))
        assert result[1]["w"] == 25
        assert result[1]["gaa"] == 2.5

    def test_missing_stat_key(self) -> None:
        data = [
            {"player": {"full_name": "Incomplete"}, "is_goalie": False,
             "games_played": 10, "fantasy_points": 5.0, "fpts_per_game": 0.5},
        ]
        result = _parse(format_comparison(data))
        assert result[0]["g"] == "-"

    def test_line_context_shown(self) -> None:
        data = [
            {"player": {"full_name": "A", "position": "C"}, "is_goalie": False,
             "games_played": 50, "fantasy_points": 73.0, "fpts_per_game": 1.46,
             "goals": 30, "assists": 40, "points": 70, "hits": 20, "blocks": 10,
             "line_context": {"ev_line": 1, "pp_unit": 1}},
        ]
        result = _parse(format_comparison(data))
        assert result[0]["line"] == "L1/PP1"

    def test_toi_formatted(self) -> None:
        data = [
            {"player": {"full_name": "A"}, "is_goalie": False,
             "games_played": 40, "fantasy_points": 50.0, "fpts_per_game": 1.25,
             "goals": 10, "assists": 20, "points": 30, "hits": 50, "blocks": 25,
             "toi_per_game": 1200},
        ]
        result = _parse(format_comparison(data))
        assert result[0]["toi"] == "20:00"


# ---------------------------------------------------------------------------
# format_trends
# ---------------------------------------------------------------------------


class TestFormatTrends:
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
        result = _parse(format_trends(data))
        assert result["trend"] == "hot"

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
        result = _parse(format_trends(data))
        assert result["trend"] == "cold"

    def test_neutral_trend(self) -> None:
        data = {
            "player": {"full_name": "Stable"},
            "windows": {
                "last_7": {"fpts_per_game": 1.5, "games": 7},
                "season": {"fpts_per_game": 1.5, "games": 50},
            },
            "trend": "neutral",
        }
        result = _parse(format_trends(data))
        assert result["trend"] == "neutral"

    def test_empty_windows(self) -> None:
        data = {"player": {"full_name": "New"}, "windows": {}, "trend": "neutral"}
        result = _parse(format_trends(data))
        assert result["last_7"]["fpg"] == 0.0

    def test_last_30_shown(self) -> None:
        data = {
            "player": {"full_name": "Test"},
            "windows": {
                "last_7": {"fpts_per_game": 1.5, "games": 7},
                "last_14": {"fpts_per_game": 1.4, "games": 14},
                "last_30": {"fpts_per_game": 1.3, "games": 25},
                "season": {"fpts_per_game": 1.2, "games": 50},
            },
            "trend": "hot",
        }
        result = _parse(format_trends(data))
        assert result["last_30"]["gp"] == 25


# ---------------------------------------------------------------------------
# format_standings
# ---------------------------------------------------------------------------


class TestFormatStandings:
    def test_empty_list(self) -> None:
        assert format_standings([]) == "No standings data."

    def test_single_team(self) -> None:
        data = [{
            "rank": 1, "team_name": "My Team",
            "games_played": 70, "points_for": 5000.5,
            "fantasy_points_per_game": 71.4, "streak": "W3",
        }]
        result = _parse(format_standings(data))
        assert result[0]["team"] == "My Team"
        assert result[0]["pf"] == 5000.5
        assert result[0]["streak"] == "W3"

    def test_none_team_name_fallback(self) -> None:
        data = [{
            "rank": 1, "team_name": None, "short_name": "MT",
            "games_played": 0, "points_for": 0.0,
            "fantasy_points_per_game": 0.0, "streak": "",
        }]
        result = _parse(format_standings(data))
        assert result[0]["team"] == "MT"

    def test_gp_remaining_shown(self) -> None:
        data = [{
            "rank": 1, "team_name": "My Team",
            "games_played": 70, "points_for": 5000.0,
            "fantasy_points_per_game": 71.4, "streak": "W3",
            "claims_remaining": 5,
            "gp_remaining": {
                "F": {"remaining": 270},
                "D": {"remaining": 123},
                "G": {"remaining": 17},
            },
        }]
        result = _parse(format_standings(data))
        assert result[0]["claims"] == 5
        assert result[0]["gp_rem"]["F"] == 270


# ---------------------------------------------------------------------------
# format_schedule
# ---------------------------------------------------------------------------


class TestFormatSchedule:
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
        result = _parse(format_schedule(data))
        assert result["team"] == "EDM"
        assert result["count"] == 2
        assert result["games"][0]["vs"] == "CGY"

    def test_back_to_back_marked(self) -> None:
        data = {
            "team": "EDM", "game_count": 2,
            "games": [
                {"game_date": "2026-02-20", "opponent": "CGY", "home_away": "home"},
                {"game_date": "2026-02-21", "opponent": "VAN", "home_away": "away"},
            ],
            "back_to_backs": [("2026-02-20", "2026-02-21")],
        }
        result = _parse(format_schedule(data))
        assert result["games"][0].get("b2b") is True
        assert result["b2b_count"] == 1

    def test_no_games(self) -> None:
        data = {"team": "EDM", "game_count": 0, "games": [], "back_to_backs": []}
        result = _parse(format_schedule(data))
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# format_news
# ---------------------------------------------------------------------------


class TestFormatNews:
    def test_empty_list(self) -> None:
        assert format_news([]) == "No recent news."

    def test_single_item(self) -> None:
        data = [{
            "player_name": "Connor McDavid",
            "headline": "Hat Trick Night",
            "published_at": "2026-02-18",
        }]
        result = _parse(format_news(data))
        assert result[0]["player"] == "Connor McDavid"
        assert result[0]["hl"] == "Hat Trick Night"
        assert result[0]["date"] == "2026-02-18"

    def test_none_player_name(self) -> None:
        data = [{"player_name": None, "headline": "Mystery", "published_at": "2026-02-18"}]
        result = _parse(format_news(data))
        assert result[0]["player"] == "Unknown"

    def test_prefix_stripped(self) -> None:
        data = [{
            "player_name": "Sidney Crosby",
            "headline": "Sidney Crosby: Back at practice",
            "published_at": "2026-02-18",
        }]
        result = _parse(format_news(data))
        assert result[0]["hl"] == "Back at practice"


# ---------------------------------------------------------------------------
# format_injuries
# ---------------------------------------------------------------------------


class TestFormatInjuries:
    def test_empty_list(self) -> None:
        assert format_injuries([]) == "No injuries to report."

    def test_single_injury(self) -> None:
        data = [{
            "full_name": "Sidney Crosby", "team_abbrev": "PIT",
            "position": "C", "injury_type": "Upper Body",
            "status": "Day-to-Day", "updated_at": "2026-02-18",
        }]
        result = _parse(format_injuries(data))
        assert result[0]["name"] == "Sidney Crosby"
        assert result[0]["team"] == "PIT"
        assert result[0]["injury"] == "Upper Body"

    def test_none_fields(self) -> None:
        data = [{
            "full_name": None, "team_abbrev": "",
            "position": None, "injury_type": None,
            "status": None, "updated_at": None,
        }]
        result = _parse(format_injuries(data))
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# format_web_search_results
# ---------------------------------------------------------------------------


class TestFormatWebSearchResults:
    def test_no_results(self) -> None:
        data = {"web": {"results": []}}
        result = format_web_search_results(data, "NHL trades")
        assert "no web results" in result.lower()

    def test_missing_web_key(self) -> None:
        result = format_web_search_results({}, "NHL trades")
        assert "no web results" in result.lower()

    def test_single_result(self) -> None:
        data = {
            "web": {"results": [{
                "title": "Test Title", "url": "https://example.com",
                "description": "Test snippet.", "age": "1 hour ago",
            }]}
        }
        result = _parse(format_web_search_results(data, "test query"))
        assert result["query"] == "test query"
        assert result["results"][0]["title"] == "Test Title"
        assert result["results"][0]["age"] == "1 hour ago"

    def test_html_entity_cleanup(self) -> None:
        data = {
            "web": {"results": [{
                "title": "Title", "url": "https://example.com",
                "description": "Rock &amp; Roll &#x27;s best",
            }]}
        }
        result = _parse(format_web_search_results(data, "test"))
        assert "Rock & Roll" in result["results"][0]["snippet"]


# ---------------------------------------------------------------------------
# _line_tag helper
# ---------------------------------------------------------------------------


class TestLineTag:
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

    def test_defense_prefix(self) -> None:
        assert _line_tag({"ev_line": 1}, "D") == "D1"

    def test_none_values_ignored(self) -> None:
        assert _line_tag({"ev_line": None, "pp_unit": None}) == ""

    def test_zero_values_ignored(self) -> None:
        assert _line_tag({"ev_line": 0, "pp_unit": 0}) == ""


# ---------------------------------------------------------------------------
# format_team_roster
# ---------------------------------------------------------------------------


class TestFormatTeamRoster:
    def test_none_data(self) -> None:
        assert format_team_roster(None) == "Team not found."

    def test_empty_dict(self) -> None:
        assert format_team_roster({}) == "Team not found."

    def test_full_team_roster(self) -> None:
        data = {
            "team_info": {
                "team_name": "Rival Team", "short_name": "RT",
                "rank": 5, "points_for": 4500.0, "fpg": 64.3,
            },
            "roster": [{
                "player_name": "Player One", "position": "C",
                "games_played": 50, "goals": 20, "assists": 30,
                "hits": 40, "blocks": 10,
                "fantasy_points": 53.0, "fpts_per_game": 1.06,
                "injury": None,
            }],
        }
        result = _parse(format_team_roster(data))
        assert result["team"] == "Rival Team"
        assert result["short"] == "RT"
        assert result["rank"] == 5
        assert result["roster"][0]["name"] == "Player One"


class TestFormatFreeAgentsDropEnrichment:
    """Tests for drop candidate / verdict fields in format_free_agents."""

    def test_drops_and_verdict_present(self) -> None:
        data = [{
            "player_name": "Pickup FA", "team": "EDM",
            "position": "C", "games_played": 50,
            "goals": 20, "assists": 30, "hits": 40, "blocks": 15,
            "fpts_per_game": 1.5, "injury": None,
            "drop_candidates": [
                {"player_name": "Drop A", "position": "C",
                 "fpts_per_game": 0.5, "recent_14_fpg": 0.4, "net_fpg": 1.0,
                 "verdict": "strong"},
                {"player_name": "Drop B", "position": "L",
                 "fpts_per_game": 0.6, "recent_14_fpg": 0.5, "net_fpg": 0.9,
                 "verdict": "strong"},
            ],
        }]
        result = _parse(format_free_agents(data))
        p = result[0]
        assert "drops" in p
        assert len(p["drops"]) == 2
        assert p["drops"][0]["name"] == "Drop A"
        assert p["drops"][0]["net"] == 1.0
        assert p["drops"][0]["verdict"] == "strong"
        assert p["drops"][1]["name"] == "Drop B"
        assert p["drops"][1]["verdict"] == "strong"

    def test_no_drops_when_empty(self) -> None:
        data = [{
            "player_name": "FA", "team": "NYR",
            "position": "C", "games_played": 30,
            "goals": 10, "assists": 10, "hits": 20, "blocks": 10,
            "fpts_per_game": 1.0, "injury": None,
            "drop_candidates": [],
            "verdict": "no room",
        }]
        result = _parse(format_free_agents(data))
        p = result[0]
        assert "drops" not in p
        assert p["verdict"] == "no room"

    def test_no_enrichment_keys_when_not_present(self) -> None:
        """Backward compat: no drop_candidates key in data → no drops in output."""
        data = [{
            "player_name": "Plain FA", "team": "BOS",
            "position": "D", "games_played": 40,
            "goals": 5, "assists": 15, "hits": 60, "blocks": 80,
            "fpts_per_game": 0.8, "injury": None,
        }]
        result = _parse(format_free_agents(data))
        p = result[0]
        assert "drops" not in p
        assert "verdict" not in p

    def test_drop_candidate_news_included(self) -> None:
        """Drop candidates with news have it passed through to output."""
        data = [{
            "player_name": "FA", "team": "EDM",
            "position": "C", "games_played": 50,
            "goals": 20, "assists": 30, "hits": 40, "blocks": 15,
            "fpts_per_game": 1.5, "injury": None,
            "drop_candidates": [
                {"player_name": "Drop A", "position": "C",
                 "fpts_per_game": 0.5, "recent_14_fpg": 0.4, "net_fpg": 1.0,
                 "verdict": "strong",
                 "news": [{"date": "2026-03-01", "hl": "Demoted to 4th line"}]},
                {"player_name": "Drop B", "position": "L",
                 "fpts_per_game": 0.6, "recent_14_fpg": 0.5, "net_fpg": 0.9,
                 "verdict": "strong"},
            ],
        }]
        result = _parse(format_free_agents(data))
        drops = result[0]["drops"]
        assert "news" in drops[0]
        assert drops[0]["news"][0]["hl"] == "Demoted to 4th line"
        assert "news" not in drops[1]  # no news for Drop B
