# WhoScored Serie A — Event Data Pipeline

Automated pipeline for scraping match event data from [WhoScored](https://www.whoscored.com) and collecting it into an incrementally updated **Parquet** file.

Every week, the downloader opens the Serie A calendar, identifies played matches, saves the raw HTML of each match page, and the parser extracts the embedded JSON event data — producing a structured dataset of all on-ball events with coordinates, qualifiers, player and team information.

---

## How it works

```
WhoScored calendar
       │
       ▼
whoscored_downloader.py        ← Playwright browser automation
  • Opens the weekly calendar
  • Finds links to played matches
  • Skips matchIds already in the Parquet
  • Saves raw HTML → partite/_inbox/
       │
       ▼
script_eventi.py               ← HTML → Parquet parser
  • Reads HTML files from _inbox/
  • Extracts embedded JSON (matchCentreData)
  • Parses events + qualifiers into a flat DataFrame
  • Appends new rows to eventi_serie_a.parquet
  • Clears _inbox/ after processing
```

The two scripts share the same **matchId deduplication logic**: if a match is already in the Parquet it is skipped at every stage, making re-runs safe.

---

## Repository Structure

```
whoscored_auto_scraper/
│
├── script/
│   ├── whoscored_downloader.py    # Playwright scraper — downloads HTML from WhoScored
│   └── script_eventi.py           # Parser — converts HTML → Parquet
│
├── partite/
│   └── _inbox/                    # Temporary staging area for raw HTML files
│
├── eventi_serie_a.parquet         # Output dataset (git-ignored — regenerate locally)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Dataset Schema

Each row in `eventi_serie_a.parquet` represents a single on-ball event.

| Column | Type | Description |
|---|---|---|
| `matchId` | int | Unique WhoScored match identifier |
| `match_date` | date | Match date |
| `Player ID` | float | WhoScored player identifier |
| `player_name` | str | Player name (resolved from match dictionary) |
| `Event Type` | str | Event category (Pass, Shot, Tackle, …) |
| `Event Value` | int | Numeric event type code |
| `Outcome` | str | Successful / Unsuccessful |
| `Minuto` | int | Minute of the event |
| `Secondo` | int | Second within the minute |
| `Team ID` | int | WhoScored team identifier |
| `team_name` | str | Team name |
| `Start X / Y` | float | Event start coordinates (0–100 scale) |
| `End X / Y` | float | Event end coordinates (where applicable) |
| `[qualifier columns]` | str | Dynamic columns — one per WhoScored qualifier type (e.g. `Zone`, `PassEndType`, `BodyPart`, …) |

> **Qualifier columns** are dynamic: their number and names vary across event types. Boolean qualifiers (no value in the source JSON) are stored as `"Yes"`.

---

## Installation

```bash
git clone https://github.com/marinoalfonso/Whoscored_auto_scraper.git
cd whoscored-serie-a

python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

pip install -r requirements.txt
playwright install chromium
```

---

## Usage

### Full automated run (download + parse)

```bash
python script/whoscored_downloader.py
```

This opens a visible Chromium window, navigates the WhoScored calendar, downloads any new match HTML, parses events, updates the Parquet, and clears the inbox.

### Parse only (if you already have HTML files)

Place HTML files in `partite/` (or any subfolder) and run:

```bash
python script/script_eventi.py
```

### Read the dataset

```python
import pandas as pd

df = pd.read_parquet("eventi_serie_a.parquet")
print(df.shape)
print(df["Event Type"].value_counts())
```

---

## Design Decisions

**Incremental updates.** The Parquet is never rewritten from scratch. Each run reads the existing `matchId` values and skips already-processed matches. This makes weekly runs fast even as the dataset grows.

**Staging inbox.** Raw HTML files land in `partite/_inbox/` before processing. After a successful parse they are deleted. This separation means you can re-run the parser independently of the downloader, and a crash mid-parse does not corrupt the Parquet.

**Headless = False.** The Chromium browser runs in visible mode to reduce the likelihood of bot detection by WhoScored. Do not switch to headless without testing — the site uses behavioural signals to block automated requests.

**Dynamic qualifier columns.** WhoScored attaches a variable list of qualifiers to each event. Rather than pre-defining a fixed schema, the parser creates one column per qualifier type encountered, using `"Yes"` for boolean flags and the qualifier value otherwise. Duplicate qualifier names within a single event are suffixed `_1`, `_2`, etc.

---

## Notes & Limitations

- **Rate limiting / bot detection.** WhoScored actively blocks scrapers. The downloader includes `time.sleep()` pauses and a realistic user-agent string, but prolonged use may trigger blocks. Use responsibly.
- **Terms of service.** Scraping WhoScored may violate their ToS. This project is intended for personal, non-commercial research only.
- **Season scope.** The calendar URL is currently set to Serie A (`regions/108/tournaments/5`). To scrape a different competition, update `CALENDARIO_URL` and `SQUADRE_MAPPING` in `whoscored_downloader.py`.
- **One week at a time.** The downloader scrapes only the currently visible week of the calendar. Run it weekly to keep the dataset current.

---

## Requirements

| Package | Purpose |
|---|---|
| `playwright` | Browser automation for WhoScored |
| `pandas` | DataFrame construction and Parquet I/O |
| `pyarrow` | Parquet backend |
| `numpy` | Numeric operations |
| `duckdb` | Optional: SQL queries on the Parquet file |

---

## License

MIT License — see [LICENSE](LICENSE).  
Data scraped from WhoScored belongs to WhoScored / Opta. Do not redistribute raw data.
