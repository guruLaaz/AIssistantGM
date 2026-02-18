#!/usr/bin/env python3
"""
Rotowire Endpoint Discovery Script
====================================
Fetches Rotowire hockey news & injury pages, then analyzes the HTML/JS
to find AJAX endpoints, API URLs, and RSS feeds.

Usage:
    python3 discover_rotowire_endpoints.py

Also tests discovered endpoints to see if they return JSON.
"""

import re
import json
import urllib.request
import urllib.error
from html.parser import HTMLParser

PAGES = {
    "news": "https://www.rotowire.com/hockey/news.php",
    "injury": "https://www.rotowire.com/hockey/injury-report.php",
}

# Also check for RSS feeds directly
RSS_CANDIDATES = [
    "https://www.rotowire.com/rss/news.php?sport=nhl",
    "https://www.rotowire.com/hockey/news-rss.php",
    "https://www.rotowire.com/rss/hockey.php",
    "https://www.rotowire.com/hockey/rss.php",
    "https://feeds.feedburner.com/rotowire-hockey",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

AJAX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}


def fetch(url, headers=None, timeout=15):
    """Fetch a URL and return (status_code, content_type, body_text)."""
    hdrs = headers or HEADERS
    req = urllib.request.Request(url, headers=hdrs)
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        body = resp.read().decode("utf-8", errors="replace")
        ct = resp.headers.get("Content-Type", "")
        return resp.status, ct, body
    except urllib.error.HTTPError as e:
        return e.code, "", ""
    except Exception as e:
        return 0, "", str(e)


def extract_urls_from_html(html, base_url):
    """Find potential API/AJAX URLs in page source."""
    patterns = [
        # fetch() calls
        r"""fetch\s*\(\s*['"`]([^'"`]+)['"`]""",
        # $.ajax / $.get / $.post / $.getJSON
        r"""\$\.(?:ajax|get|post|getJSON)\s*\(\s*['"`]([^'"`]+)['"`]""",
        # url: "..." in JS objects
        r"""url\s*:\s*['"`]([^'"`]+)['"`]""",
        # XMLHttpRequest.open
        r"""\.open\s*\(\s*['"`](?:GET|POST)['"`]\s*,\s*['"`]([^'"`]+)['"`]""",
        # href/src with api/ajax/json/rss in path
        r"""(?:href|src|action)\s*=\s*['"`]([^'"`]*(?:api|ajax|json|rss|feed)[^'"`]*)['"`]""",
        # Any URL-like string with api/ajax/json keywords
        r"""['"`]((?:https?://|/)[^'"`]*(?:ajax|api|json|feed|rss|endpoint)[^'"`]*)['"`]""",
        # data-url or data-src attributes
        r"""data-(?:url|src|endpoint|ajax)\s*=\s*['"`]([^'"`]+)['"`]""",
    ]

    found = set()
    for pattern in patterns:
        for match in re.finditer(pattern, html, re.IGNORECASE):
            url = match.group(1)
            # Resolve relative URLs
            if url.startswith("/"):
                url = "https://www.rotowire.com" + url
            elif not url.startswith("http"):
                continue  # skip non-URL matches
            found.add(url)

    return found


def extract_inline_scripts(html):
    """Pull out all <script> tag contents."""
    scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL | re.IGNORECASE)
    return "\n".join(scripts)


def check_rss_feeds():
    """Test known RSS feed URL patterns."""
    print("\n" + "=" * 60)
    print("RSS FEED DISCOVERY")
    print("=" * 60)

    working = []
    for url in RSS_CANDIDATES:
        status, ct, body = fetch(url)
        is_xml = "<rss" in body[:500].lower() or "<feed" in body[:500].lower() or "<?xml" in body[:200].lower()
        indicator = "[OK]" if status == 200 and is_xml else "[X]"
        print(f"\n{indicator} {url}")
        print(f"   Status: {status} | Content-Type: {ct[:60]}")
        if status == 200:
            if is_xml:
                # Count items
                items = len(re.findall(r"<item>", body, re.IGNORECASE))
                print(f"   RSS items found: {items}")
                # Show first item title
                title_match = re.search(r"<item>.*?<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", body, re.DOTALL)
                if title_match:
                    print(f"   First item: {title_match.group(1)[:80]}")
                working.append(url)
            else:
                print(f"   Response is not XML/RSS (first 200 chars): {body[:200]}")

    return working


def analyze_page(name, url):
    """Fetch a page and analyze it for AJAX endpoints."""
    print(f"\n{'=' * 60}")
    print(f"ANALYZING: {name.upper()} - {url}")
    print("=" * 60)

    status, ct, html = fetch(url)
    print(f"Status: {status} | Content-Type: {ct[:60]} | Size: {len(html)} bytes")

    if status != 200:
        print(f"[!]  Failed to fetch page (status {status})")
        return []

    # Extract all potential URLs
    all_urls = extract_urls_from_html(html, url)

    # Also search inline scripts specifically
    scripts = extract_inline_scripts(html)
    script_urls = extract_urls_from_html(scripts, url)
    all_urls.update(script_urls)

    # Filter to rotowire.com URLs that look like endpoints
    endpoint_urls = set()
    for u in all_urls:
        if "rotowire.com" in u or u.startswith("/"):
            endpoint_urls.add(u)

    # Also look for any external script files that might contain endpoints
    ext_scripts = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, re.IGNORECASE)
    rw_scripts = [s for s in ext_scripts if "rotowire" in s.lower()]

    print(f"\nPotential endpoint URLs found in page source: {len(endpoint_urls)}")
    for u in sorted(endpoint_urls):
        print(f"   -> {u}")

    print(f"\nRotowire JS files referenced: {len(rw_scripts)}")
    for s in rw_scripts:
        print(f"   -> {s}")

    # Fetch external JS files and search them too
    js_endpoints = set()
    for script_url in rw_scripts[:10]:  # limit to 10
        if script_url.startswith("/"):
            script_url = "https://www.rotowire.com" + script_url
        elif not script_url.startswith("http"):
            continue
        print(f"\n   Fetching JS: {script_url[:80]}...")
        s_status, _, s_body = fetch(script_url)
        if s_status == 200:
            found = extract_urls_from_html(s_body, script_url)
            for u in found:
                if "rotowire.com" in u or u.startswith("/"):
                    js_endpoints.add(u)

    if js_endpoints:
        print(f"\nEndpoints found in external JS files: {len(js_endpoints)}")
        for u in sorted(js_endpoints):
            print(f"   -> {u}")

    all_endpoints = endpoint_urls | js_endpoints
    return sorted(all_endpoints)


def test_endpoints(endpoints):
    """Try hitting discovered endpoints with AJAX headers to find JSON APIs."""
    print(f"\n{'=' * 60}")
    print("TESTING ENDPOINTS FOR JSON RESPONSES")
    print("=" * 60)

    json_endpoints = []
    for url in endpoints:
        # Skip obvious non-API URLs
        if any(ext in url.lower() for ext in [".css", ".png", ".jpg", ".gif", ".svg", ".woff", ".ico"]):
            continue
        if len(url) > 200:
            continue

        status, ct, body = fetch(url, headers=AJAX_HEADERS)
        is_json = False
        if body:
            try:
                json.loads(body)
                is_json = True
            except (json.JSONDecodeError, ValueError):
                pass

        indicator = "[JSON]" if is_json else "[--]"
        print(f"\n{indicator} {url[:100]}")
        print(f"   Status: {status} | Content-Type: {ct[:60]} | JSON: {is_json}")
        if is_json and body:
            preview = body[:300].replace("\n", " ")
            print(f"   Preview: {preview}")
            json_endpoints.append(url)

    return json_endpoints


def main():
    print("[NHL] ROTOWIRE NHL ENDPOINT DISCOVERY")
    print("=" * 60)

    # 1. Check RSS feeds
    working_rss = check_rss_feeds()

    # 2. Analyze pages
    all_endpoints = set()
    for name, url in PAGES.items():
        endpoints = analyze_page(name, url)
        all_endpoints.update(endpoints)

    # 3. Test discovered endpoints
    json_endpoints = []
    if all_endpoints:
        json_endpoints = test_endpoints(all_endpoints)

    # 4. Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print("=" * 60)
    print(f"\n[OK] Working RSS feeds: {len(working_rss)}")
    for url in working_rss:
        print(f"   {url}")
    print(f"\n[JSON] JSON API endpoints: {len(json_endpoints)}")
    for url in json_endpoints:
        print(f"   {url}")
    print(f"\n[*] Total potential endpoints found: {len(all_endpoints)}")

    if not working_rss and not json_endpoints:
        print("\n[!]  No RSS feeds or JSON endpoints found.")
        print("   The pages may be fully server-rendered.")
        print("   Fallback: HTML scraping with BeautifulSoup.")


if __name__ == "__main__":
    main()
