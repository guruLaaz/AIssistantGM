#!/usr/bin/env python3
"""
Rotowire Endpoint Probe - Phase 2
===================================
Now that we know the endpoints exist, probe them with proper
parameters to discover the actual data shapes.

Usage:
    python3 probe_rotowire_endpoints.py
"""

import json
import urllib.request
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.rotowire.com/hockey/news.php",
}


def fetch_json(url, headers=None):
    hdrs = headers or HEADERS
    req = urllib.request.Request(url, headers=hdrs)
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="replace")
        return resp.status, json.loads(body)
    except urllib.error.HTTPError as e:
        return e.code, None
    except json.JSONDecodeError:
        return 200, "NOT_JSON"
    except Exception as e:
        return 0, str(e)


def fetch_raw(url, headers=None):
    hdrs = headers or HEADERS
    req = urllib.request.Request(url, headers=hdrs)
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="replace")
        return resp.status, body
    except urllib.error.HTTPError as e:
        return e.code, ""
    except Exception as e:
        return 0, str(e)


def probe_rss():
    """Examine the RSS feed structure."""
    print("=" * 60)
    print("1. RSS FEED: /rss/news.php?sport=nhl")
    print("=" * 60)

    url = "https://www.rotowire.com/rss/news.php?sport=nhl"
    status, body = fetch_raw(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/xml, text/xml, */*",
    })

    if status != 200:
        print(f"   Failed: status {status}")
        return

    try:
        root = ET.fromstring(body)
        channel = root.find("channel")
        if channel is not None:
            title = channel.findtext("title", "N/A")
            print(f"   Channel title: {title}")

        items = root.findall(".//item")
        print(f"   Total items: {len(items)}")

        for i, item in enumerate(items[:3]):
            print(f"\n   --- Item {i+1} ---")
            for child in item:
                tag = child.tag
                text = (child.text or "")[:150]
                print(f"   <{tag}>: {text}")
    except ET.ParseError as e:
        print(f"   XML parse error: {e}")
        print(f"   Raw (first 500): {body[:500]}")


def probe_injury_report():
    """Try different parameter combos for the injury report endpoint."""
    print("\n" + "=" * 60)
    print("2. INJURY REPORT: /hockey/tables/injury-report.php")
    print("=" * 60)

    # Try various POS values
    pos_values = ["ALL", "all", "C", "F", "D", "G", "LW", "RW", ""]

    # First: find the right POS value
    print("\n   Testing POS parameter values:")
    working_pos = None
    for pos in pos_values:
        url = f"https://www.rotowire.com/hockey/tables/injury-report.php?team=ALL&pos={pos}"
        status, data = fetch_json(url)
        if isinstance(data, dict) and "error" not in data:
            print(f"   pos={pos!r} -> status={status}, keys={list(data.keys())[:5]}")
            working_pos = pos
            break
        elif isinstance(data, list):
            print(f"   pos={pos!r} -> status={status}, list of {len(data)} items")
            working_pos = pos
            break
        elif isinstance(data, dict):
            err = data.get("error", "unknown")
            print(f"   pos={pos!r} -> error: {err}")
        else:
            print(f"   pos={pos!r} -> status={status}")

    if working_pos is None:
        # Try without team param
        print("\n   Trying without team param:")
        for pos in pos_values:
            url = f"https://www.rotowire.com/hockey/tables/injury-report.php?pos={pos}"
            status, data = fetch_json(url)
            if isinstance(data, (dict, list)) and not (isinstance(data, dict) and "error" in data):
                print(f"   pos={pos!r} (no team) -> works!")
                working_pos = pos
                break
            elif isinstance(data, dict):
                print(f"   pos={pos!r} -> {data.get('error', 'unknown')}")

    # If we found a working combo, show the data shape
    if working_pos is not None:
        url = f"https://www.rotowire.com/hockey/tables/injury-report.php?team=ALL&pos={working_pos}"
        status, data = fetch_json(url)
        print(f"\n   FULL RESPONSE SHAPE:")
        print(f"   Type: {type(data).__name__}")
        if isinstance(data, dict):
            print(f"   Top-level keys: {list(data.keys())}")
            for k, v in data.items():
                if isinstance(v, list) and len(v) > 0:
                    print(f"   '{k}' -> list of {len(v)} items")
                    print(f"   First item keys: {list(v[0].keys()) if isinstance(v[0], dict) else type(v[0]).__name__}")
                    print(f"   First item: {json.dumps(v[0], indent=2)[:500]}")
                elif isinstance(v, str):
                    print(f"   '{k}' -> string ({len(v)} chars): {v[:200]}")
                else:
                    print(f"   '{k}' -> {type(v).__name__}: {str(v)[:200]}")
        elif isinstance(data, list):
            print(f"   List length: {len(data)}")
            if len(data) > 0 and isinstance(data[0], dict):
                print(f"   First item keys: {list(data[0].keys())}")
                print(f"   First item: {json.dumps(data[0], indent=2)[:500]}")
                if len(data) > 1:
                    print(f"   Second item: {json.dumps(data[1], indent=2)[:500]}")


def probe_news_updates():
    """Try the get-more-updates endpoint with 'hockey' replacing ${J}."""
    print("\n" + "=" * 60)
    print("3. NEWS UPDATES: /hockey/ajax/get-more-updates.php")
    print("=" * 60)

    # The ${J} variable likely resolves to sport name
    sports = ["hockey", "football", "baseball", "basketball"]
    for sport in sports:
        url = f"https://www.rotowire.com/{sport}/ajax/get-more-updates.php"
        status, data = fetch_json(url)
        if status == 200 and data is not None and data != "NOT_JSON":
            print(f"   sport={sport} -> works!")
            if isinstance(data, dict):
                print(f"   Keys: {list(data.keys())[:10]}")
                # Check for HTML content
                for k, v in data.items():
                    if isinstance(v, str) and len(v) > 50:
                        print(f"   '{k}': {v[:200]}...")
                    elif isinstance(v, list):
                        print(f"   '{k}': list of {len(v)}")
                        if v and isinstance(v[0], dict):
                            print(f"   First item keys: {list(v[0].keys())}")
                            print(f"   First item: {json.dumps(v[0], indent=2)[:400]}")
                    else:
                        print(f"   '{k}': {v}")
            break
        else:
            print(f"   sport={sport} -> status={status}")

    # Also try with page/offset params
    print("\n   Testing with pagination params:")
    param_combos = [
        "?page=2", "?offset=10", "?page=1&count=10",
        "?start=0&limit=10", "?p=2",
    ]
    base = "https://www.rotowire.com/hockey/ajax/get-more-updates.php"
    for params in param_combos:
        url = base + params
        status, data = fetch_json(url)
        if status == 200 and data and data != "NOT_JSON":
            result_type = type(data).__name__
            has_error = isinstance(data, dict) and "error" in data
            if not has_error:
                print(f"   {params} -> {result_type}")
                if isinstance(data, dict):
                    for k in list(data.keys())[:3]:
                        v = data[k]
                        print(f"      '{k}': {str(v)[:150]}")
                break
            else:
                print(f"   {params} -> error: {data.get('error')}")
        else:
            print(f"   {params} -> status={status}")


def probe_search_players():
    """Test player search endpoint."""
    print("\n" + "=" * 60)
    print("4. PLAYER SEARCH: /frontend/ajax/search-players.php")
    print("=" * 60)

    queries = ["McDavid", "Suzuki", "Price"]
    for q in queries:
        url = f"https://www.rotowire.com/frontend/ajax/search-players.php?searchTerm={urllib.parse.quote(q)}"
        status, data = fetch_json(url)
        if status == 200 and data and data != "NOT_JSON":
            print(f"\n   Query: '{q}'")
            if isinstance(data, list):
                print(f"   Results: {len(data)}")
                for item in data[:2]:
                    print(f"   -> {json.dumps(item, indent=2)[:300]}")
            elif isinstance(data, dict):
                if "error" in data:
                    print(f"   Error: {data['error']}")
                else:
                    print(f"   Keys: {list(data.keys())}")
                    preview = json.dumps(data, indent=2)[:500]
                    print(f"   {preview}")


def probe_get_articles():
    """Test the articles endpoint with hockey filter."""
    print("\n" + "=" * 60)
    print("5. ARTICLES: /frontend/ajax/get-articles.php")
    print("=" * 60)

    param_combos = [
        "?sport=hockey",
        "?sport=nhl",
        "?sport=hockey&page=1",
        "?category=hockey",
        "?sport=hockey&type=news",
    ]

    for params in param_combos:
        url = f"https://www.rotowire.com/frontend/ajax/get-articles.php{params}"
        status, data = fetch_json(url)
        if status == 200 and isinstance(data, dict):
            has_error = "error" in data
            if not has_error:
                print(f"\n   {params}")
                print(f"   Keys: {list(data.keys())}")
                for k, v in data.items():
                    if isinstance(v, str) and "class=" in v:
                        # HTML content - show first bit
                        print(f"   '{k}': HTML ({len(v)} chars)")
                        print(f"   First 300: {v[:300]}")
                    elif isinstance(v, (bool, int, float)):
                        print(f"   '{k}': {v}")
                    elif isinstance(v, list):
                        print(f"   '{k}': list of {len(v)}")
                break
            else:
                print(f"   {params} -> {data.get('error')}")
        else:
            print(f"   {params} -> status={status}")


def main():
    print("ROTOWIRE ENDPOINT PROBE - PHASE 2")
    print("=" * 60)
    print("Probing discovered endpoints with proper parameters...\n")

    probe_rss()
    probe_injury_report()
    probe_news_updates()
    probe_search_players()
    probe_get_articles()

    print("\n" + "=" * 60)
    print("PROBE COMPLETE")
    print("=" * 60)
    print("\nPaste this output back to finalize Task 3 design!")


if __name__ == "__main__":
    main()
