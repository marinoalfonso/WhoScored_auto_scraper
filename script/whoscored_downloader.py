"""
whoscored_downloader.py
-----------------------
Playwright-based scraper for WhoScored Serie A match pages.

For each week's matches in the calendar:
  1. Checks which matchIds are already in the Parquet (skips them).
  2. Opens each new match page in a visible Chromium browser.
  3. Saves the raw HTML to partite/_inbox/.
  4. After all downloads, calls script_eventi.process_and_save() to parse
     and append new events to the Parquet, then clears the inbox.

Usage:
    python script/whoscored_downloader.py

Requirements:
    pip install playwright && playwright install chromium
"""

import re
import json
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, Page

# Local import — script_eventi must be on the Python path or in the same package
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from script_eventi import load_processed_ids, OUTPUT_PARQUET, process_and_save

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent.parent
PARTITE_FOLDER = BASE_DIR / "partite"
INBOX_FOLDER   = PARTITE_FOLDER / "_inbox"
INBOX_FOLDER.mkdir(parents=True, exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
CALENDARIO_URL = "https://it.whoscored.com/regions/108/tournaments/5/italia-serie-a"

# Maps WhoScored team names to canonical folder names.
# Update when a team is promoted/relegated.
SQUADRE_MAPPING: dict[str, str] = {
    "Inter":               "Inter",
    "AC Milan":            "AC Milan",
    "Juventus":            "Juventus",
    "Napoli":              "Napoli",
    "Roma":                "Roma",
    "Lazio":               "Lazio",
    "Atalanta":            "Atalanta",
    "Fiorentina":          "Fiorentina",
    "Bologna":             "Bologna",
    "Torino":              "Torino",
    "Genoa":               "Genoa",
    "Cagliari":            "Cagliari",
    "Lecce":               "Lecce",
    "Udinese":             "Udinese",
    "Verona":              "Verona",
    "Como":                "Como",
    "Parma Calcio 1913":   "Parma Calcio 1913",
    "Pisa":                "Pisa",
    "Sassuolo":            "Sassuolo",
    "Cremonese":           "Cremonese",
}

# Regex for the embedded JSON blob
_ARGS_REGEX = r'(?<=require\.config\.params\["args"\].=.)[\s\S]*?;'
_JSON_KEYS  = ["matchId", "matchCentreData", "matchCentreEventTypeJson", "formationIdNameMappings"]

# Seconds to wait between match page requests (be polite to the server)
REQUEST_DELAY = 1.5


# ── Utility functions ─────────────────────────────────────────────────────────

def safe_filename(name: str, max_len: int = 180) -> str:
    """Sanitise a string for use as a filename."""
    name = re.sub(r'[\\/:"*?<>|]+', " - ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:max_len]


def try_accept_cookies(page: Page) -> None:
    """
    Try common cookie-consent button selectors.
    Fails silently if none are found — not all sessions show a banner.
    """
    selectors = [
        "button:has-text('Accetta tutto')",
        "button:has-text('Accetta')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "button[id*='accept']",
        "button[class*='accept']",
        "button[id*='cookie']",
        "button[class*='cookie']",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() and loc.is_visible():
                loc.click(timeout=1500)
                page.wait_for_timeout(800)
                return
        except Exception:
            pass


# ── Fixtures JSON helpers ─────────────────────────────────────────────────────

def get_fixtures_json(page: Page) -> dict | None:
    """
    Extract the tournament fixtures JSON blob injected by WhoScored
    into a <script data-hypernova-key='tournamentfixtures'> tag.
    Returns None if the element is absent or the JSON is malformed.
    """
    el = page.query_selector(
        "script[data-hypernova-key='tournamentfixtures'][type='application/json']"
    )
    if not el:
        return None

    txt = (el.text_content() or "").strip()
    # WhoScored sometimes wraps the JSON in HTML comments
    if txt.startswith("<!--"):
        txt = txt[4:]
    if txt.endswith("-->"):
        txt = txt[:-3]
    txt = txt.strip()

    try:
        return json.loads(txt)
    except Exception:
        return None


def fixtures_all_future(fixtures: dict) -> bool:
    """
    Return True if every match in the visible week has no score yet
    (i.e. all matches are in the future).
    """
    try:
        matches = fixtures["tournaments"][0]["matches"]
    except Exception:
        return False
    if not matches:
        return False
    return all(
        m.get("homeScore") is None and m.get("awayScore") is None
        for m in matches
    )


# ── Calendar link extraction ──────────────────────────────────────────────────

def extract_match_links(page: Page) -> list[str]:
    """
    Return unique match URLs from the currently visible calendar week.
    Looks for anchor elements with id starting 'scoresBtn-' and href
    containing '/matches/'.
    """
    anchors = page.query_selector_all("a[id^='scoresBtn-'][href*='/matches/']")
    urls: list[str] = []
    for a in anchors:
        href = a.get_attribute("href") or ""
        if not href:
            continue
        url = f"https://it.whoscored.com{href}" if href.startswith("/") else href
        if url not in urls:
            urls.append(url)
    return urls


# ── Match page parsing ────────────────────────────────────────────────────────

def parse_args_from_html(html: str) -> dict | None:
    """
    Parse the embedded WhoScored args JSON from a match page HTML string.
    Returns None if the pattern is not found or JSON is invalid.
    """
    found = re.findall(_ARGS_REGEX, html)
    if not found:
        return None

    data_txt = found[0]
    for key in _JSON_KEYS:
        data_txt = data_txt.replace(key, f'"{key}"')
    data_txt = data_txt.replace("};", "}")

    try:
        return json.loads(data_txt)
    except Exception:
        return None


def extract_match_info(html: str) -> tuple[str, str, str] | None:
    """
    Extract (home_team, away_team, match_id) from a match page HTML string.
    Returns None if extraction fails.
    """
    data = parse_args_from_html(html)
    if not data:
        return None
    try:
        home     = data["matchCentreData"]["home"]["name"]
        away     = data["matchCentreData"]["away"]["name"]
        match_id = str(data["matchId"])
        return home, away, match_id
    except Exception:
        return None


# ── File saving ───────────────────────────────────────────────────────────────

def save_html_to_inbox(html: str, match_id: str, page_title: str) -> int:
    """
    Save a match HTML page to INBOX_FOLDER.

    Returns 1 if the file was written, 0 if it already existed.
    """
    filename = f"{match_id} - {safe_filename(page_title)}.html"
    dest     = INBOX_FOLDER / filename

    if dest.exists():
        print(f"   ⏭️  Già presente: _inbox/{filename}")
        return 0

    dest.write_text(html, encoding="utf-8")
    print(f"   ✅ Salvato: _inbox/{filename}")
    return 1


# ── Main ──────────────────────────────────────────────────────────────────────

def run() -> None:
    processed_ids = load_processed_ids(OUTPUT_PARQUET)
    print(f"📋 Match già nel Parquet: {len(processed_ids)}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        # ── Open calendar ─────────────────────────────────────────────────────
        print(f"\n🌐 Apertura calendario: {CALENDARIO_URL}")
        page.goto(CALENDARIO_URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(2500)
        try_accept_cookies(page)
        page.wait_for_timeout(1200)

        # Wait for fixtures JSON to be injected
        page.wait_for_selector(
            "script[data-hypernova-key='tournamentfixtures'][type='application/json']",
            state="attached",
            timeout=60_000,
        )
        fixtures = get_fixtures_json(page)

        # If this week is all-future matches, step back one week
        if fixtures and fixtures_all_future(fixtures):
            print("↩️  Settimana corrente: solo partite future → torno alla settimana precedente")
            page.locator("#dayChangeBtn-prev").click(timeout=5000)
            page.wait_for_timeout(2500)
            fixtures = get_fixtures_json(page)

        match_links = extract_match_links(page)
        print(f"🔗 Partite trovate nel calendario: {len(match_links)}")

        saved_count = 0
        opened      = 0

        for url in match_links:
            try:
                print(f"\n📥 Apertura: {url}")
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                opened += 1

                # Poll until matchCentreData appears in the page source
                for _ in range(20):
                    if "matchCentreData" in page.content():
                        break
                    time.sleep(0.8)
                else:
                    print("   ⚠️  Timeout: matchCentreData non trovato, salto.")
                    continue

                html = page.content()
                info = extract_match_info(html)
                if not info:
                    print("   ⚠️  Impossibile estrarre info partita, salto.")
                    continue

                home, away, match_id = info

                if int(match_id) in processed_ids:
                    print(f"   ⏭️  matchId {match_id} già nel Parquet, salto.")
                    continue

                print(f"   → {home} vs {away} | ID: {match_id}")
                saved_count += save_html_to_inbox(html, match_id, page.title())
                time.sleep(REQUEST_DELAY)

            except Exception as exc:
                print(f"   ❌ Errore: {exc}")

        browser.close()

    print(f"\n✅ Pagine aperte: {opened}")
    print(f"✅ HTML salvati (nuovi): {saved_count}")

    if saved_count > 0:
        print("\n📊 Aggiornamento Parquet...")
        process_and_save(INBOX_FOLDER)

        # Clear inbox after successful processing
        deleted = 0
        for f in INBOX_FOLDER.glob("*.html"):
            f.unlink()
            deleted += 1
        print(f"🧹 Inbox svuotata ({deleted} file rimossi).")
    else:
        print("\nℹ️  Nessun nuovo HTML salvato — Parquet non aggiornato.")


if __name__ == "__main__":
    run()
