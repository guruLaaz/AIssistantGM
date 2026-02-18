"""Time-on-ice conversion utilities."""


def toi_to_seconds(toi: str | None) -> int:
    """Convert TOI string "MM:SS" to integer seconds.

    Args:
        toi: Time string in "MM:SS" format, or None/empty.

    Returns:
        Integer seconds. Returns 0 for None or empty string.

    Raises:
        ValueError: If format is invalid, minutes negative, or seconds >= 60.
    """
    if toi is None or toi == "":
        return 0

    if ":" not in toi:
        raise ValueError(f"Invalid TOI format: {toi}")

    parts = toi.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid TOI format: {toi}")

    try:
        minutes = int(parts[0])
        seconds = int(parts[1])
    except ValueError:
        raise ValueError(f"Invalid TOI format: {toi}")

    if minutes < 0:
        raise ValueError(f"Negative minutes not allowed: {toi}")
    if seconds < 0 or seconds >= 60:
        raise ValueError(f"Seconds must be 0-59: {toi}")

    return minutes * 60 + seconds


def seconds_to_toi(seconds: int | None) -> str:
    """Convert integer seconds to TOI string "MM:SS".

    Args:
        seconds: Integer seconds, or None.

    Returns:
        Time string in "M:SS" format. Returns "0:00" for None.

    Raises:
        ValueError: If seconds is negative.
    """
    if seconds is None:
        return "0:00"

    if seconds < 0:
        raise ValueError(f"Negative seconds not allowed: {seconds}")

    minutes = seconds // 60
    secs = seconds % 60
    return f"{minutes}:{secs:02d}"
