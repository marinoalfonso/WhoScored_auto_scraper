# WhoScored Serie A — Event Data Pipeline

Automated pipeline for scraping match event data from [WhoScored](https://www.whoscored.com) and storing it in a **PostgreSQL** database, fully containerised with Docker.

Every week, the downloader opens the Serie A calendar, identifies played matches, saves the raw HTML of each match page, and the parser extracts the embedded JSON event data — producing a structured dataset of all on-ball events with coordinates, qualifiers, player and team information.

Qualifier columns (~130 dynamic fields) are normalised into a **JSONB** column, eliminating sparse nulls and enabling native PostgreSQL JSON queries.

---

## Architecture

```
WhoScored calendar
       │
       ▼
whoscored_downloader.py        ← Playwright browser automation
  • Opens the weekly calendar
  • Finds links to played matches
  • Skips matchIds already in PostgreSQL
  • Saves raw HTML → partite/_inbox/
       │
       ▼
script_eventi_pg.py            ← HTML → PostgreSQL parser
  • Reads HTML files from _inbox/
  • Extracts embedded JSON (matchCentreData)
  • Parses events — fixed fields as columns, qualifiers as JSONB
  • Inserts new rows into PostgreSQL
  • Clears _inbox/ after processing
```

The two scripts share the same **matchId deduplication logic**: if a match is already in the database it is skipped at every stage, making re-runs safe.

---

## Repository Structure

```
whoscored_auto_scraper/
│
├── script/
│   ├── whoscored_downloader.py    # Playwright scraper — downloads HTML from WhoScored
│   ├── script_eventi.py           # Core parsing functions (shared)
│   └── script_eventi_pg.py        # PostgreSQL writer — fixed fields + JSONB qualifiers
│
├── sql/
│   └── 01_setup.sql               # PostgreSQL schema — auto-executed on first Docker run
│
├── partite/
│   └── _inbox/                    # Temporary staging area for raw HTML files
│
├── Dockerfile                     # Python 3.12 image with Playwright
├── docker-compose.yml             # PostgreSQL + pgAdmin + scraper services
├── .env.example                   # Environment variables template
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Database Schema

Each row in the `eventi` table represents a single on-ball event.

### Fixed columns

| Column | Type | Description |
|---|---|---|
| `id` | serial | Auto-incremented primary key |
| `match_id` | int | Unique WhoScored match identifier |
| `match_date` | date | Match date |
| `player_id` | float | WhoScored player identifier |
| `player_name` | varchar | Player name |
| `event_type` | varchar | Event category (Pass, Shot, Tackle, …) |
| `event_value` | int | Numeric event type code |
| `outcome` | varchar | Successful / Unsuccessful |
| `minuto` | int | Minute of the event |
| `secondo` | float | Second within the minute |
| `team_id` | int | WhoScored team identifier |
| `team_name` | varchar | Team name |
| `start_x / start_y` | float | Event start coordinates (0–100 scale) |
| `end_x / end_y` | float | Event end coordinates (where applicable) |
| `qualifiers` | jsonb | All dynamic qualifier fields as a JSON object |

### JSONB qualifiers

Instead of ~130 sparse nullable columns, all WhoScored qualifiers are stored in a single JSONB column. Each event contains only the qualifiers relevant to its type:

```json
// Pass event
{"PassLength": "Short", "PassType": "Simple", "Accurate": true, "Zone": "LeftChannel"}

// Shot event
{"ShotOnTarget": true, "BodyPart": "Head", "Zone": "Central"}
```

Query qualifiers natively in SQL:

```sql
-- All accurate short passes by Juventus
SELECT player_name, minuto
FROM eventi
WHERE event_type = 'Pass'
  AND team_name = 'Juventus'
  AND qualifiers->>'Accurate' = 'true'
  AND qualifiers->>'PassLength' = 'Short';
```

---

## Setup

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

No other installation required — PostgreSQL, pgAdmin and all Python dependencies run inside Docker containers.

### 1. Clone the repository

```bash
git clone https://github.com/marinoalfonso/WhoScored_auto_scraper.git
cd WhoScored_auto_scraper
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```
DB_USER=postgres
DB_PASSWORD=your_password_here
DB_NAME=whoscored
PGADMIN_EMAIL=admin@admin.com
PGADMIN_PASSWORD=admin
```

### 3. Start the containers

```bash
docker-compose up
```

This will:
- Start a PostgreSQL instance and create the `eventi` table automatically
- Start pgAdmin at [http://localhost:5050](http://localhost:5050)
- Build the Python scraper image

### 4. Connect pgAdmin to the database

1. Open [http://localhost:5050](http://localhost:5050)
2. Login with `PGADMIN_EMAIL` and `PGADMIN_PASSWORD`
3. Add a new server: host = `db`, port = `5432`, user and password from `.env`

---

## Usage

### Full automated run (download + parse)

```bash
docker-compose run scraper python script/whoscored_downloader.py
```

Or if running locally outside Docker:

```bash
python script/whoscored_downloader.py
```

### Parse only (if you already have HTML files)

Place HTML files in `partite/` and run:

```bash
python script/script_eventi_pg.py
```

### Query the dataset

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:password@localhost:5432/whoscored")

df = pd.read_sql("SELECT * FROM eventi WHERE event_type = 'Pass' LIMIT 1000", engine)
print(df.shape)
```

---

## Design Decisions

**PostgreSQL + JSONB.** WhoScored events carry ~130 dynamic qualifier fields, most of which are null for any given event type. Storing qualifiers as JSONB eliminates sparse columns, reduces storage and enables native PostgreSQL JSON queries with GIN indexing.

**Incremental updates.** The database is never rewritten from scratch. Each run queries the existing `match_id` values and skips already-processed matches, making weekly runs fast even as the dataset grows.

**Staging inbox.** Raw HTML files land in `partite/_inbox/` before processing. After a successful parse they are deleted. This separation means you can re-run the parser independently of the downloader, and a crash mid-parse does not corrupt the database.

**Headless = False.** The Chromium browser runs in visible mode to reduce the likelihood of bot detection by WhoScored. Do not switch to headless without testing.

**Dynamic qualifier columns.** Rather than pre-defining a fixed schema for qualifiers, the parser creates a JSON object per event with only the qualifiers present. Boolean qualifiers (no value in the source JSON) are stored as `"Yes"`. Duplicate qualifier names within a single event are suffixed `_1`, `_2`, etc.

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
| `pandas` | DataFrame construction and data manipulation |
| `pyarrow` | Parquet backend |
| `psycopg2-binary` | PostgreSQL connector |
| `sqlalchemy` | Python–PostgreSQL connection |
| `python-dotenv` | Environment variable management |

---

## License

MIT License — see [LICENSE](LICENSE).  
Data scraped from WhoScored belongs to WhoScored / Opta. Do not redistribute raw data.
