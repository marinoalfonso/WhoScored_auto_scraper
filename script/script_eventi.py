"""
script_eventi.py
----------------
Parses WhoScored match HTML files and appends event data to a Parquet file.

Usage (standalone):
    python script/script_eventi.py

Called internally by whoscored_downloader.py after saving HTML to _inbox/.

Pipeline:
    HTML file → extract embedded JSON → flatten events + qualifiers → DataFrame
             → deduplicate by matchId → append to eventi_serie_a.parquet
"""

import json
import re
import glob
import os
from pathlib import Path

import numpy as np
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent.parent
PARTITE_FOLDER = BASE_DIR / "partite"
OUTPUT_PARQUET = BASE_DIR / "eventi_serie_a.parquet"

# ── Regex for embedded WhoScored JSON ─────────────────────────────────────────
_ARGS_REGEX = r'(?<=require\.config\.params\["args"\].=.)[\s\S]*?;'

# Keys that need quoting to make the embedded JS object valid JSON
_JSON_KEYS = [
    "matchId",
    "matchCentreData",
    "matchCentreEventTypeJson",
    "formationIdNameMappings",
]


# ── HTML / JSON extraction ─────────────────────────────────────────────────────

def extract_json_from_html(html_path: str | Path) -> str:
    """
    Extract the raw args JSON string embedded in a WhoScored HTML page.

    WhoScored inlines match data as an unquoted JS object assigned to
    require.config.params["args"]. This function isolates that block and
    makes it valid JSON by quoting the known top-level keys.

    Parameters
    ----------
    html_path : path to the saved HTML file.

    Returns
    -------
    str : JSON-parseable string.

    Raises
    ------
    ValueError  if the expected pattern is not found in the HTML.
    """
    with open(html_path, "r", encoding="utf-8") as fh:
        html = fh.read()

    matches = re.findall(_ARGS_REGEX, html)
    if not matches:
        raise ValueError(f"matchCentreData pattern not found in {html_path}")

    data_txt = matches[0]
    for key in _JSON_KEYS:
        data_txt = data_txt.replace(key, f'"{key}"')
    data_txt = data_txt.replace("};", "}")

    return data_txt


def extract_data_from_dict(data: dict) -> tuple:
    """
    Unpack the top-level WhoScored data dictionary.

    Returns
    -------
    tuple : (events_list, match_id, match_date, players_ids, teams_dict)
    """
    match_id   = data["matchId"]
    match_date = data["matchCentreData"]["startDate"][:10]

    events_list = data["matchCentreData"]["events"]
    teams_dict  = {
        data["matchCentreData"]["home"]["teamId"]: data["matchCentreData"]["home"]["name"],
        data["matchCentreData"]["away"]["teamId"]: data["matchCentreData"]["away"]["name"],
    }
    players_ids = data["matchCentreData"]["playerIdNameDictionary"]

    return events_list, match_id, match_date, players_ids, teams_dict


# ── Event parsing ──────────────────────────────────────────────────────────────

def extract_event_data(event: dict) -> dict:
    """
    Flatten a single WhoScored event dict into a plain dictionary.

    Qualifier handling:
    - If a qualifier has no value → stored as "Yes" (boolean flag).
    - If the same qualifier name appears more than once → suffixed _1, _2, …
    """
    event_data = {
        "Player ID":   event.get("playerId"),
        "Event Type":  event["type"]["displayName"],
        "Event Value": event["type"]["value"],
        "Outcome":     event["outcomeType"]["displayName"],
        "Minuto":      event["minute"],
        "Secondo":     event.get("second"),
        "Team ID":     event["teamId"],
        "Start X":     event["x"],
        "Start Y":     event["y"],
        "End X":       event.get("endX"),
        "End Y":       event.get("endY"),
    }

    qualifier_columns: dict = {}
    for qualifier in event.get("qualifiers", []):
        name  = qualifier["type"]["displayName"]
        value = qualifier.get("value")

        if value is None:
            value = "Yes"

        if name in qualifier_columns:
            i = 1
            while f"{name}_{i}" in qualifier_columns:
                i += 1
            name = f"{name}_{i}"

        qualifier_columns[name] = value

    event_data.update(qualifier_columns)
    return event_data


def create_events_dataframe(path: str | Path) -> pd.DataFrame:
    """
    Parse a single WhoScored HTML file into a tidy events DataFrame.

    Columns added beyond raw event data:
    - matchId, match_date  (inserted at position 0–1)
    - player_name          (inserted after Player ID)
    - team_name            (inserted after Team ID)
    - Any qualifier columns present in this match that were absent from
      the event-level parsing are added as null columns for schema consistency.

    Parameters
    ----------
    path : path to the saved HTML file.

    Returns
    -------
    pd.DataFrame with one row per event.
    """
    json_str    = extract_json_from_html(path)
    data        = json.loads(json_str)
    events_list, match_id, match_date, players_ids, teams_dict = extract_data_from_dict(data)

    rows = [extract_event_data(event) for event in events_list]
    df   = pd.DataFrame(rows)

    # ── Prepend match-level columns ───────────────────────────────────────────
    df.insert(0, "matchId",    match_id)
    df.insert(1, "match_date", pd.to_datetime(match_date))

    # ── Resolve player names ──────────────────────────────────────────────────
    def _resolve_player(pid):
        if pd.isna(pid):
            return None
        return players_ids.get(str(int(pid)))

    pid_pos = df.columns.get_loc("Player ID")
    df.insert(pid_pos + 1, "player_name", df["Player ID"].map(_resolve_player))

    # ── Resolve team names ────────────────────────────────────────────────────
    tid_pos = df.columns.get_loc("Team ID")
    df.insert(tid_pos + 1, "team_name", df["Team ID"].map(teams_dict))

    # ── Ensure all qualifier columns from this match are present ─────────────
    all_qualifiers: set = set()
    for event in events_list:
        for q in event.get("qualifiers", []):
            all_qualifiers.add(q["type"]["displayName"])

    for qualifier in all_qualifiers:
        if qualifier not in df.columns:
            df[qualifier] = None

    return df


# ── Parquet helpers ────────────────────────────────────────────────────────────

def load_processed_ids(parquet_path: str | Path) -> set:
    """
    Return the set of matchIds already present in the Parquet file.
    Returns an empty set if the file does not exist yet.
    """
    path = Path(parquet_path)
    if not path.exists():
        return set()
    df = pd.read_parquet(path, columns=["matchId"])
    return set(df["matchId"].unique())


# ── Main processing function ───────────────────────────────────────────────────

def process_and_save(folder_path: str | Path) -> None:
    """
    Scan folder_path for HTML files, parse new matches, append to Parquet.

    - Skips files whose matchId is already present in OUTPUT_PARQUET.
    - Deduplicates by filename to avoid processing the same file twice if it
      appears in multiple subfolders.
    - Prints a summary at the end.

    Parameters
    ----------
    folder_path : directory to scan (recursively) for *.html files.
    """
    folder_path = Path(folder_path)
    html_files  = glob.glob(str(folder_path / "**" / "*.html"), recursive=True)

    # Deduplicate by basename
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

    processed_ids = load_processed_ids(OUTPUT_PARQUET)
    print(f"Match già nel Parquet: {len(processed_ids)}")

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

    if Path(OUTPUT_PARQUET).exists():
        existing_df = pd.read_parquet(OUTPUT_PARQUET)
        final_df    = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
        final_df = new_df

    final_df.to_parquet(OUTPUT_PARQUET, index=False)

    print(f"\n✅ Parquet aggiornato: {len(final_df):,} eventi totali ({len(new_df):,} nuovi)")
    print(f"\n=== Riepilogo ===")
    print(f"Nuovi match aggiunti : {len(new_dataframes)}")
    print(f"Nuovi eventi         : {len(new_df):,}")
    print(f"Totale eventi        : {len(final_df):,}")


if __name__ == "__main__":
    process_and_save(PARTITE_FOLDER)
