"""Tests for utils/time.py — TOI conversion functions."""

import pytest
from utils.time import toi_to_seconds, seconds_to_toi


class TestToiToSeconds:
    """Tests for toi_to_seconds conversion."""

    def test_toi_to_seconds_typical(self) -> None:
        """Standard TOI string converts correctly."""
        assert toi_to_seconds("18:30") == 1110

    def test_toi_to_seconds_zero(self) -> None:
        """Zero TOI converts to 0."""
        assert toi_to_seconds("0:00") == 0

    def test_toi_to_seconds_none(self) -> None:
        """None input returns 0."""
        assert toi_to_seconds(None) == 0

    def test_toi_to_seconds_empty(self) -> None:
        """Empty string returns 0."""
        assert toi_to_seconds("") == 0

    def test_toi_to_seconds_max_realistic(self) -> None:
        """Large TOI (overtime) converts correctly."""
        assert toi_to_seconds("99:59") == 5999

    def test_toi_to_seconds_single_digit_minutes(self) -> None:
        """Single digit minutes with leading zero seconds."""
        assert toi_to_seconds("5:09") == 309

    def test_toi_to_seconds_invalid_format(self) -> None:
        """Invalid format raises ValueError."""
        with pytest.raises(ValueError):
            toi_to_seconds("invalid")

    def test_toi_to_seconds_negative(self) -> None:
        """Negative minutes raises ValueError."""
        with pytest.raises(ValueError):
            toi_to_seconds("-1:00")

    def test_toi_to_seconds_seconds_out_of_range(self) -> None:
        """Seconds >= 60 raises ValueError."""
        with pytest.raises(ValueError):
            toi_to_seconds("10:60")


class TestSecondsToToi:
    """Tests for seconds_to_toi conversion."""

    def test_seconds_to_toi_typical(self) -> None:
        """Standard seconds convert correctly."""
        assert seconds_to_toi(1110) == "18:30"

    def test_seconds_to_toi_zero(self) -> None:
        """Zero seconds returns 0:00."""
        assert seconds_to_toi(0) == "0:00"

    def test_seconds_to_toi_none(self) -> None:
        """None input returns 0:00."""
        assert seconds_to_toi(None) == "0:00"

    def test_seconds_to_toi_large(self) -> None:
        """Large value converts correctly."""
        assert seconds_to_toi(5999) == "99:59"

    def test_seconds_to_toi_negative(self) -> None:
        """Negative seconds raises ValueError."""
        with pytest.raises(ValueError):
            seconds_to_toi(-1)
