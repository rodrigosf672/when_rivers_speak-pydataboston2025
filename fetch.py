"""
USGS SITE INDEX FETCHER
-----------------------

This script downloads the **complete list of USGS water-monitoring sites** for
every U.S. state and combines them into a single, deduplicated Parquet file
(`usgs_all_sites.parquet`).

WHAT IT DOES
============

1. For each U.S. state, the script calls:
       https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=XX

   This returns a tab-separated RDB (USGS "relational data base") text file
   listing *all monitoring sites* in that state.

2. It removes USGS comment/header lines beginning with '#', then loads the
   cleaned file into a **string-typed Pandas DataFrame** to avoid dtype errors.

3. It removes the USGS "format specification" row (e.g., agency_cd="5s"),
   which is not data but describes field widths.

4. A `source_state` column is added so the origin of each site is preserved.

5. After all states are fetched:
   - DataFrames are concatenated
   - Duplicate `site_no` values (sites belonging to multiple agencies) are
     removed cleanly (all values are strings → no numeric coercion issues)

6. The final unified dataset of all USGS site metadata is written to:
       usgs_all_sites.parquet

   All columns are converted to string dtype so the Parquet file loads cleanly
   in DuckDB, Polars, Pandas, Spark, and Arrow-native tools.

WHY THIS SCRIPT EXISTS
======================
This produces the master site index used by downstream ingestion scripts to:
   • Resolve site numbers to states  
   • Map metadata (lat/lon, site type, HUC region, etc.)  
   • Join with IV (instantaneous values) time-series data  
   • Build dashboards and filters (e.g., list only river/stream sites)

RUNTIME NOTES
=============
- Each state fetch typically completes within 0.2–1.5 seconds.
- The entire pipeline usually finishes in under a minute.
- Network issues or USGS throttling may cause slowdowns or empty files,
  which are handled gracefully.

OUTPUT
======
File: usgs_all_sites.parquet
Shape: (unique_site_count, all_metadata_columns)

This file becomes your fast lookup table for all USGS monitoring sites
nationwide, used by DuckDB and your Python Shiny dashboard.
"""

import time
import requests
import pandas as pd
from io import StringIO

US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL",
    "GA","HI","ID","IL","IN","IA","KS","KY","LA","ME",
    "MD","MA","MI","MN","MS","MO","MT","NE","NV","NH",
    "NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI",
    "SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

BASE = "https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd={state}"


def fetch_state(state):
    url = BASE.format(state=state)
    r = requests.get(url, timeout=30)

    if r.status_code != 200:
        print(f"[WARN] {state} → HTTP {r.status_code}")
        return pd.DataFrame()

    # Remove comment lines starting with '#'
    lines = [line for line in r.text.splitlines() if not line.startswith("#")]

    if len(lines) < 2:
        print(f"[WARN] {state} returned empty/invalid file")
        return pd.DataFrame()

    # Load as all strings (critical!)
    df = pd.read_csv(StringIO("\n".join(lines)), sep="\t", dtype=str)

    # Remove the format row: agency_cd = '5s', site_no = '15s', station_nm = '50s'
    fmt = df["agency_cd"].str.contains(r"s$", na=False)
    df = df[~fmt]

    # Add state column
    df["source_state"] = state

    return df


def main():
    t0 = time.time()

    dfs = []
    for s in US_STATES:
        print(f"Fetching {s} ...")
        df = fetch_state(s)
        if not df.empty:
            dfs.append(df)

    print("Concatenating...")
    big = pd.concat(dfs, ignore_index=True)

    print("Dropping duplicates on site_no...")
    # All site_no are strings → dedupe works correctly
    big = big.drop_duplicates(subset=["site_no"])

    print("Final shape:", big.shape)
    print(big.head())

    print("Writing parquet...")

    # Ensure ALL columns are string dtype (Arrow-safe)
    for col in big.columns:
        big[col] = big[col].astype("string")

    big.to_parquet("usgs_all_sites.parquet", index=False)

    t1 = time.time()
    print(f"\nTotal time: {t1 - t0:.2f} seconds")


if __name__ == "__main__":
    main()
