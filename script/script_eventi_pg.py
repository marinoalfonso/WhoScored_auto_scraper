"""
script_eventi_pg.py
-------------------
PostgreSQL version of script_eventi.py.

Parses WhoScored match HTML files and inserts event data into PostgreSQL,
with qualifier columns normalised into a JSONB column.

Pipeline:
    HTML file → extract embedded JSON → flatten events
             → separate fixed fields from qualifiers
             → build JSONB column → insert into PostgreSQL
"""

import json
import os
import re
import glob
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Import parsing functions from original script
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from script_eventi import (
    extract_json_from_html,
    extract_data_from_dict,
    extract_event_data,
)

# ── Environment ───────────────────────────────────────────────────────────────
load_dotenv()

DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_NAME     = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent.parent
PARTITE_FOLDER = BASE_DIR / "partite"

# ── Fixed columns — everything else goes into qualifiers JSONB ────────────────
FIXED_FIELDS = [
    "matchId", "match_date", "Player ID", "player_name",
    "Event Type", "Event Value", "Outcome", "Minuto", "Secondo",
    "Team ID", "team_name", "Start X", "Start Y", "End X", "End Y"
]

# Mapping from DataFrame column names to PostgreSQL column names (snake_case)
COLUMN_MAPPING = {
    "matchId":     "match_id",
    "match_date":  "match_date",
    "Player ID":   "player_id",
    "player_name": "player_name",
    "Event Type":  "event_type",
    "Event Value": "event_value",
    "Outcome":     "outcome",
    "Minuto":      "minuto",
    "Secondo":     "secondo",
    "Team ID":     "team_id",
    "team_name":   "team_name",
    "Start X":     "start_x",
    "Start Y":     "start_y",
    "End X":       "end_x",
    "End Y":       "end_y",
}


# ── Database helpers ──────────────────────────────────────────────────────────

def load_processed_ids() -> set:
    """
    Return the set of match_ids already present in PostgreSQL.
    Returns an empty set if the table is empty or does not exist.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT match_id FROM eventi"))
            return {row[0] for row in result}
    except Exception:
        return set()


# ── DataFrame building ────────────────────────────────────────────────────────

def create_events_dataframe(path: str | Path) -> pd.DataFrame:
    """
    Parse a WhoScored HTML file into a tidy events DataFrame
    ready for PostgreSQL insertion.

    Fixed fields are mapped to snake_case column names.
    All qualifier columns are collapsed into a single JSONB column.
    """
    json_str = extract_json_from_html(path)
    data     = json.loads(json_str)
    events_list, match_id, match_date, players_ids, teams_dict = extract_data_from_dict(data)

    rows = [extract_event_data(event) for event in events_list]
    df   = pd.DataFrame(rows)

    # Add match-level columns
    df.insert(0, "matchId",    match_id)
    df.insert(1, "match_date", pd.to_datetime(match_date))

    # Resolve player names
    pid_pos = df.columns.get_loc("Player ID")
    df.insert(pid_pos + 1, "player_name", df["Player ID"].map(
        lambda pid: players_ids.get(str(int(pid))) if pd.notna(pid) else None
    ))

    # Resolve team names
    tid_pos = df.columns.get_loc("Team ID")
    df.insert(tid_pos + 1, "team_name", df["Team ID"].map(teams_dict))

    # ── Separate fixed fields from qualifiers ─────────────────────────────────
    qualifier_fields = [col for col in df.columns if col not in FIXED_FIELDS]

    # Build JSONB column — only non-null qualifiers per event
    df["qualifiers"] = df[qualifier_fields].apply(
        lambda row: json.dumps(
            {k: v for k, v in row.items() if pd.notna(v)}
        ),
        axis=1
    )

    # Keep only fixed fields + qualifiers, rename to snake_case
    df_clean = df[FIXED_FIELDS + ["qualifiers"]].copy()
    df_clean = df_clean.rename(columns=COLUMN_MAPPING)

    return df_clean


# ── Main processing function ──────────────────────────────────────────────────

def process_and_save(folder_path: str | Path) -> None:
    """
    Scan folder_path for HTML files, parse new matches,
    insert into PostgreSQL.

    Skips matches already present in the database.
    """
    folder_path = Path(folder_path)
    html_files  = glob.glob(str(folder_path / "**" / "*.html"), recursive=True)

    # Deduplicate by filename
    seen_names:   set  = set()
    unique_files: list = []
    for fp in html_files:
        basename = os.path.basename(fp)
        if basename not in seen_names:
            seen_names.add(basename)
            unique_files.append(fp)

    if not unique_files:
        print(f"Nessun file HTML trovato in {folder_path}")
        return

    processed_ids = load_processed_ids()
    print(f"Match già nel database: {len(processed_ids)}")

    new_dataframes: list = []
    skipped = 0
    errors  = 0

    for file_path in unique_files:
        try:
            json_str = extract_json_from_html(file_path)
            data     = json.loads(json_str)
            match_id = data["matchId"]

            if match_id in processed_ids:
                skipped += 1
                continue

            print(f"Elaborazione: {os.path.basename(file_path)}")
            df = create_events_dataframe(file_path)
            new_dataframes.append(df)
            print(f"  ✓ {len(df):,} eventi trovati (matchId={match_id})")

        except Exception as exc:
            errors += 1
            print(f"  ✗ Errore in {os.path.basename(file_path)}: {exc}")

    print(f"\n⏭️  File già presenti, saltati : {skipped}")
    if errors:
        print(f"⚠️  File con errori           : {errors}")

    if not new_dataframes:
        print("Nessun nuovo match da aggiungere.")
        return

    new_df = pd.concat(new_dataframes, ignore_index=True)

    # Insert into PostgreSQL
    new_df.to_sql(
        "eventi",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000
    )

    print(f"\n✅ Database aggiornato: {len(new_df):,} nuovi eventi inseriti")
    print(f"\n=== Riepilogo ===")
    print(f"Nuovi match aggiunti : {len(new_dataframes)}")
    print(f"Nuovi eventi         : {len(new_df):,}")


if __name__ == "__main__":
    process_and_save(PARTITE_FOLDER)