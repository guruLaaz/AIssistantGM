"""Shared types and enums for the fantrax package."""

from enum import Enum


class OutputFormat(str, Enum):
    """Output format options for CLI commands."""
    table = "table"
    json = "json"
    simple = "simple"
