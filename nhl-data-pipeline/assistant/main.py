"""CLI entry point for the fantasy hockey assistant."""

from __future__ import annotations

import argparse
import io
import os
import subprocess
import sys
from pathlib import Path

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, io.UnsupportedOperation):
        pass  # pytest capture wrappers may not support reconfigure

# Add project root to path so imports work when running directly
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*a, **kw):  # type: ignore[misc]
        pass

from db.schema import get_db
from pipeline import current_season
from assistant.tools import SessionContext
from assistant.client import AssistantClient


DB_PATH = _PROJECT_ROOT / "db" / "nhl_data.db"
_ENV_PATH = _PROJECT_ROOT.parent / ".env"


def select_team(conn) -> tuple[str, str]:
    """Display fantasy teams and prompt the user to select one.

    Returns:
        Tuple of (team_id, team_name).
    """
    rows = conn.execute(
        "SELECT id, name FROM fantasy_teams ORDER BY name"
    ).fetchall()

    if not rows:
        print("No fantasy teams found in the database.")
        print("Run the pipeline first: python pipeline.py")
        sys.exit(1)

    print("\n=== Fantasy Teams ===\n")
    for i, row in enumerate(rows, 1):
        print(f"  {i:>2}. {row['name']}")
    print()

    while True:
        try:
            choice = input(f"Select your team (1-{len(rows)}): ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(rows):
                return rows[idx]["id"], rows[idx]["name"]
            print(f"Please enter a number between 1 and {len(rows)}.")
        except ValueError:
            print("Please enter a valid number.")
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            sys.exit(0)


def main() -> None:
    """Run the interactive assistant chat loop."""
    parser = argparse.ArgumentParser(description="Fantasy Hockey Assistant")
    parser.add_argument(
        "--run-pipeline-first",
        action="store_true",
        help="Run the data pipeline to refresh data before starting the chat.",
    )
    args = parser.parse_args()

    load_dotenv(_ENV_PATH)

    if args.run_pipeline_first:
        print("Refreshing data...")
        pipeline_script = _PROJECT_ROOT / "pipeline.py"
        result = subprocess.run(
            [sys.executable, str(pipeline_script)],
            cwd=str(_PROJECT_ROOT),
        )
        if result.returncode != 0:
            print("Warning: Pipeline exited with errors. Continuing with existing data.")
        else:
            print("Data refresh complete.\n")

    conn = get_db(DB_PATH)

    try:
        team_id, team_name = select_team(conn)
    except Exception as e:
        print(f"Error loading teams: {e}")
        sys.exit(1)

    season = current_season()
    context = SessionContext(conn=conn, team_id=team_id, season=season)

    try:
        client = AssistantClient(context=context, team_name=team_name)
    except RuntimeError as e:
        print(f"\nError: {e}")
        sys.exit(1)

    print(f"\n{'=' * 60}")
    print(f"  Fantasy Hockey Assistant - {team_name}")
    print(f"  Season: {season[:4]}-{season[4:]}")
    print(f"{'=' * 60}")
    print()
    print("  Try asking:")
    print("    - Show me my roster")
    print("    - Who are the best free agent forwards?")
    print("    - Compare Connor McDavid and Auston Matthews")
    print("    - Who should I pick up and drop?")
    print("    - Find me some buy-low trade targets")
    print("    - What's the league standings?")
    print()
    print("  Type 'quit' or 'exit' to leave.\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit"):
            print("Goodbye!")
            break

        try:
            response = client.chat(user_input)
            print(f"\nAssistant: {response}\n")
        except KeyboardInterrupt:
            print("\n(interrupted)")
            continue
        except Exception as e:
            print(f"\nError: {e}\n")

    conn.close()


if __name__ == "__main__":
    main()
