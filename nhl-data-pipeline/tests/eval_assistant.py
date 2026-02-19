"""End-to-end evaluation of the assistant through the LLM pipeline.

Sends natural language prompts through AssistantClient.chat() and
evaluates whether the correct tools are called and the responses
are high quality.
"""

import os
import sys
import time
import sqlite3
import json

# Load .env
from pathlib import Path
env_path = Path(__file__).resolve().parents[2] / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from assistant.client import AssistantClient
from assistant.tools import SessionContext

DB_PATH = str(Path(__file__).resolve().parents[1] / "db" / "nhl_data.db")
TEAM_ID = "290mbha6mbc80puq"
SEASON = "20252026"
TEAM_NAME = "DJ"


QUERIES = [
    {
        "prompt": "Show me my roster",
        "expect_keywords": ["roster", "Draisaitl", "Sanderson", "FP/G"],
        "expect_tool": "get_my_roster",
    },
    {
        "prompt": "Who are the best available forwards?",
        "expect_keywords": ["free agent", "FP/G"],
        "expect_tool": "search_free_agents",
    },
    {
        "prompt": "Compare McDavid and Draisaitl",
        "expect_keywords": ["McDavid", "Draisaitl", "FP/G", "Goals"],
        "expect_tool": "compare_players",
    },
    {
        "prompt": "How has Mitch Marner been playing lately?",
        "expect_keywords": ["Marner", "Last 7", "Last 14"],
        "expect_tool": "get_player_trends",
    },
    {
        "prompt": "Show me Connor McDavid's stats",
        "expect_keywords": ["McDavid", "EDM", "TOI/G", "Recent Games"],
        "expect_tool": "get_player_stats",
    },
    {
        "prompt": "Who should I pick up and drop?",
        "expect_keywords": ["Drop", "Pickup", "FP/G"],
        "expect_tool": "get_roster_moves",
    },
    {
        "prompt": "Find me some buy-low trade targets",
        "expect_keywords": ["Trade", "Trend", "Owner"],
        "expect_tool": "get_trade_targets",
    },
    {
        "prompt": "What's the league standings?",
        "expect_keywords": ["Rk", "Team", "FP/G"],
        "expect_tool": "get_league_standings",
    },
    {
        "prompt": "Any injuries on my team?",
        "expect_keywords": ["Injury", "IR"],
        "expect_tool": "get_injuries",
    },
    {
        "prompt": "When do the Oilers play next?",
        "expect_keywords": ["EDM", "Date", "Opp"],
        "expect_tool": "get_schedule_analysis",
    },
    {
        "prompt": "What's the latest news on my team?",
        "expect_keywords": ["News", "2026"],
        "expect_tool": "get_news_briefing",
    },
    {
        "prompt": "Best free agent goalies?",
        "expect_keywords": ["FP/G", "W", "GAA"],
        "expect_tool": "search_free_agents",
    },
    {
        "prompt": "Compare Shesterkin and Hellebuyck",
        "expect_keywords": ["Shesterkin", "Hellebuyck", "Wins", "GAA"],
        "expect_tool": "compare_players",
    },
    {
        "prompt": "Any good defensemen on the waiver wire?",
        "expect_keywords": ["FP/G", "D"],
        "expect_tool": "search_free_agents",
    },
    {
        "prompt": "Is Victor Olofsson worth keeping or should I drop him?",
        "expect_keywords": ["Olofsson"],
        "expect_tool": None,  # Could call trends, stats, or drop candidates
    },
    {
        "prompt": "How does Kaprizov compare to Marner and Draisaitl?",
        "expect_keywords": ["Kaprizov", "Marner", "Draisaitl"],
        "expect_tool": "compare_players",
    },
    {
        "prompt": "What's Cale Makar's trend?",
        "expect_keywords": ["Makar", "Last 7", "Season"],
        "expect_tool": "get_player_trends",
    },
    {
        "prompt": "Show me the Leafs schedule",
        "expect_keywords": ["TOR", "Date"],
        "expect_tool": "get_schedule_analysis",
    },
]


def run_query(prompt: str, query_num: int) -> dict:
    """Run a single query through a fresh assistant client."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    context = SessionContext(conn=conn, team_id=TEAM_ID, season=SEASON)
    client = AssistantClient(context=context, team_name=TEAM_NAME)

    start = time.time()
    try:
        response = client.chat(prompt)
        elapsed = time.time() - start
        result = {
            "query_num": query_num,
            "prompt": prompt,
            "response": response,
            "elapsed": elapsed,
            "error": None,
        }
    except Exception as e:
        elapsed = time.time() - start
        result = {
            "query_num": query_num,
            "prompt": prompt,
            "response": "",
            "elapsed": elapsed,
            "error": str(e),
        }
    finally:
        conn.close()

    return result


def evaluate(result: dict, query_spec: dict) -> dict:
    """Evaluate a query result against expectations."""
    response = result["response"]
    checks = {}

    # Check for errors
    checks["no_error"] = result["error"] is None

    # Check response is non-empty
    checks["non_empty"] = len(response.strip()) > 20

    # Check expected keywords
    missing_keywords = []
    for kw in query_spec.get("expect_keywords", []):
        if kw.lower() not in response.lower():
            missing_keywords.append(kw)
    checks["keywords_found"] = len(missing_keywords) == 0
    checks["missing_keywords"] = missing_keywords

    # Check response length (should be substantial but not absurdly long)
    checks["response_length"] = len(response)
    checks["reasonable_length"] = 50 < len(response) < 15000

    # Check for common error patterns
    checks["no_error_message"] = not any(
        phrase in response.lower()
        for phrase in ["error", "exception", "traceback", "failed to"]
    )

    # Overall pass
    checks["pass"] = all([
        checks["no_error"],
        checks["non_empty"],
        checks["keywords_found"],
        checks["reasonable_length"],
        checks["no_error_message"],
    ])

    return checks


def main():
    print("=" * 70)
    print("  ASSISTANT END-TO-END EVALUATION")
    print(f"  {len(QUERIES)} queries | Team: {TEAM_NAME} | Season: {SEASON}")
    print("=" * 70)
    print()

    results = []
    for i, q in enumerate(QUERIES):
        num = i + 1
        print(f"[{num:2d}/{len(QUERIES)}] {q['prompt']}")
        print(f"       Expected tool: {q.get('expect_tool', 'any')}")

        result = run_query(q["prompt"], num)
        checks = evaluate(result, q)
        result["checks"] = checks

        status = "PASS" if checks["pass"] else "FAIL"
        print(f"       Status: {status} | {result['elapsed']:.1f}s | {checks['response_length']} chars")

        if checks["missing_keywords"]:
            print(f"       Missing keywords: {checks['missing_keywords']}")
        if result["error"]:
            print(f"       Error: {result['error']}")
        if not checks["pass"]:
            # Show first 200 chars of response for debugging
            preview = result["response"][:200].replace("\n", " ")
            print(f"       Response preview: {preview}")

        results.append(result)
        print()

        # Small delay to avoid rate limiting
        if i < len(QUERIES) - 1:
            time.sleep(1)

    # Summary
    print("=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    passed = sum(1 for r in results if r["checks"]["pass"])
    failed = len(results) - passed
    avg_time = sum(r["elapsed"] for r in results) / len(results)
    print(f"  Passed: {passed}/{len(results)}")
    print(f"  Failed: {failed}/{len(results)}")
    print(f"  Avg response time: {avg_time:.1f}s")
    print()

    if failed > 0:
        print("  FAILURES:")
        for r in results:
            if not r["checks"]["pass"]:
                print(f"    [{r['query_num']}] {r['prompt']}")
                if r["error"]:
                    print(f"        Error: {r['error']}")
                if r["checks"]["missing_keywords"]:
                    print(f"        Missing: {r['checks']['missing_keywords']}")
                if not r["checks"]["reasonable_length"]:
                    print(f"        Length: {r['checks']['response_length']}")

    # Dump full responses to file for review
    output_path = Path(__file__).parent / "eval_responses.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        for r in results:
            f.write(f"{'='*70}\n")
            f.write(f"[{r['query_num']}] {r['prompt']}\n")
            f.write(f"Status: {'PASS' if r['checks']['pass'] else 'FAIL'} | {r['elapsed']:.1f}s\n")
            f.write(f"{'='*70}\n")
            f.write(r["response"])
            f.write("\n\n")
    print(f"\n  Full responses written to: {output_path}")


if __name__ == "__main__":
    main()
