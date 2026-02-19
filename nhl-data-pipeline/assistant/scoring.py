"""Fantasy scoring calculations for the Hockeyclub league."""

# Fantasy scoring constants
SKATER_SCORING = {"goals": 1.0, "assists": 1.0, "blocks": 0.1, "hits": 0.1}
GOALIE_SCORING = {
    "goals": 1.0,
    "assists": 1.0,
    "wins": 2.0,
    "shutouts": 1.0,
    "ot_losses": 1.0,
    "losses": 0.0,
}


def calc_skater_fpts(goals: float, assists: float, blocks: float, hits: float) -> float:
    """Calculate fantasy points for a skater from raw stat values."""
    return (
        goals * SKATER_SCORING["goals"]
        + assists * SKATER_SCORING["assists"]
        + blocks * SKATER_SCORING["blocks"]
        + hits * SKATER_SCORING["hits"]
    )


def calc_goalie_fpts(
    goals: float,
    assists: float,
    wins: float,
    shutouts: float,
    ot_losses: float,
    losses: float = 0,
) -> float:
    """Calculate fantasy points for a goalie from raw stat values."""
    return (
        goals * GOALIE_SCORING["goals"]
        + assists * GOALIE_SCORING["assists"]
        + wins * GOALIE_SCORING["wins"]
        + shutouts * GOALIE_SCORING["shutouts"]
        + ot_losses * GOALIE_SCORING["ot_losses"]
        + losses * GOALIE_SCORING["losses"]
    )


def _safe_get(row: dict, key: str) -> float:
    """Get a numeric value from a row dict, treating None and missing keys as 0."""
    val = row.get(key)
    return val if val is not None else 0


def calc_skater_fpts_from_row(row: dict) -> float:
    """Calculate fantasy points for a skater from a database row dict.

    Extracts goals, assists, blocks, hits from the row.
    Missing keys or None values are treated as 0.
    """
    return calc_skater_fpts(
        goals=_safe_get(row, "goals"),
        assists=_safe_get(row, "assists"),
        blocks=_safe_get(row, "blocks"),
        hits=_safe_get(row, "hits"),
    )


def calc_goalie_fpts_from_row(row: dict) -> float:
    """Calculate fantasy points for a goalie from a database row dict.

    Extracts goals, assists, wins, shutouts, ot_losses, losses from the row.
    Missing keys or None values are treated as 0.
    Note: goalie_stats table does not have goals/assists columns —
    they will safely default to 0.
    """
    return calc_goalie_fpts(
        goals=_safe_get(row, "goals"),
        assists=_safe_get(row, "assists"),
        wins=_safe_get(row, "wins"),
        shutouts=_safe_get(row, "shutouts"),
        ot_losses=_safe_get(row, "ot_losses"),
        losses=_safe_get(row, "losses"),
    )
