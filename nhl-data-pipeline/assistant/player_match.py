"""Player name resolution — fuzzy matching between Fantrax and NHL data."""

from __future__ import annotations

import sqlite3

from fetchers.rotowire import _strip_accents


def _row_to_dict(row: sqlite3.Row) -> dict:
    """Convert a sqlite3.Row to a player result dict."""
    return {
        "id": row["id"],
        "full_name": row["full_name"],
        "team_abbrev": row["team_abbrev"],
        "position": row["position"],
    }


def resolve_player(
    conn: sqlite3.Connection, name_query: str
) -> dict | None:
    """Fuzzy-match a player name against the players table.

    4-level fallback:
      1. Exact full_name match
      2. Case-insensitive match
      3. Accent-normalized match
      4. Partial / last-name LIKE match

    Args:
        conn: Database connection with row_factory=sqlite3.Row.
        name_query: Player name to search for.

    Returns:
        Dict with id, full_name, team_abbrev, position — or None.
    """
    if not name_query or not name_query.strip():
        return None

    name_query = name_query.strip()

    # 1. Exact match
    row = conn.execute(
        "SELECT id, full_name, team_abbrev, position "
        "FROM players WHERE full_name = ?",
        (name_query,),
    ).fetchone()
    if row:
        return _row_to_dict(row)

    # 2. Case-insensitive match
    row = conn.execute(
        "SELECT id, full_name, team_abbrev, position "
        "FROM players WHERE LOWER(full_name) = LOWER(?)",
        (name_query,),
    ).fetchone()
    if row:
        return _row_to_dict(row)

    # 3. Accent-normalized match
    normalized = _strip_accents(name_query).lower()
    all_players = conn.execute(
        "SELECT id, full_name, team_abbrev, position FROM players"
    ).fetchall()
    for p in all_players:
        if p["full_name"] and _strip_accents(p["full_name"]).lower() == normalized:
            return _row_to_dict(p)

    # 4. Partial / last-name LIKE match
    like_pattern = f"%{name_query}%"
    row = conn.execute(
        "SELECT id, full_name, team_abbrev, position "
        "FROM players WHERE full_name LIKE ? OR last_name LIKE ?",
        (like_pattern, like_pattern),
    ).fetchone()
    if row:
        return _row_to_dict(row)

    # 5. Last-name match for nickname variants (e.g. Benjamin→Ben)
    parts = name_query.split()
    if len(parts) >= 2:
        last_name = parts[-1]
        # Try exact last name first
        rows = conn.execute(
            "SELECT id, full_name, team_abbrev, position "
            "FROM players WHERE last_name = ? COLLATE NOCASE",
            (last_name,),
        ).fetchall()
        if len(rows) == 1:
            return _row_to_dict(rows[0])

        # Try with first initial to disambiguate common last names
        first_initial = parts[0][0].upper()
        initial_matches = [
            r for r in rows
            if r["full_name"] and r["full_name"][0].upper() == first_initial
        ]
        if len(initial_matches) == 1:
            return _row_to_dict(initial_matches[0])

    # 6. Normalize hyphens/spaces in last name (e.g. "Sandin Pellikka" → "Sandin-Pellikka")
    if len(parts) >= 3:
        # Try joining last two parts with hyphen
        hyphenated = parts[-2] + "-" + parts[-1]
        row = conn.execute(
            "SELECT id, full_name, team_abbrev, position "
            "FROM players WHERE last_name = ? COLLATE NOCASE",
            (hyphenated,),
        ).fetchone()
        if row:
            return _row_to_dict(row)

    return None


def resolve_fantrax_to_nhl(
    conn: sqlite3.Connection, fantrax_player_name: str
) -> int | None:
    """Match a Fantrax roster player name to an NHL player ID.

    Args:
        conn: Database connection.
        fantrax_player_name: Player name as it appears on a Fantrax roster.

    Returns:
        NHL player ID or None.
    """
    result = resolve_player(conn, fantrax_player_name)
    return result["id"] if result else None


def get_rostered_nhl_ids(conn: sqlite3.Connection) -> set[int]:
    """Get all NHL player IDs that are on any fantasy roster.

    Resolves each player_name in fantasy_roster_slots to an NHL ID.

    Args:
        conn: Database connection.

    Returns:
        Set of NHL player IDs currently rostered.
    """
    cursor = conn.execute(
        "SELECT DISTINCT player_name FROM fantasy_roster_slots "
        "WHERE player_name IS NOT NULL AND player_name != ''"
    )
    ids: set[int] = set()
    for row in cursor.fetchall():
        nhl_id = resolve_fantrax_to_nhl(conn, row["player_name"])
        if nhl_id is not None:
            ids.add(nhl_id)
    return ids
