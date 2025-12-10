"""
ASYNC USGS IV DOWNLOADER (3-YEAR WINDOW)
----------------------------------------

This script downloads **3 years** of USGS Instantaneous Values (IV) time-series
data for all 50 U.S. states + DC **asynchronously** using aiohttp.

It produces one Parquet file per state:
    state_parquet_3yrs/states_iv_{STATE}_3yrs.parquet

The script is optimized for:
    • High-concurrency HTTP fetching (asyncio + aiohttp)
    • Efficient resume behavior (skip states already written)
    • Clean, long-format time-series suitable for DuckDB/Polars/Shiny
    • Date-filtered retrieval using START_DATE → FINAL_DATE

DATA SOURCES
============
All requests hit:

    https://waterservices.usgs.gov/nwis/iv/?format=json&sites={site}&startDT=...&endDT=...

Parameters downloaded:
    00010 – Temperature (°C)
    00095 – Specific Conductance
    00300 – Dissolved Oxygen (mg/L)
    00400 – pH

The site list is provided by:
    usgs_all_sites.parquet
which contains metadata from NWIS (fetched separately).

WORKFLOW OVERVIEW
=================
1. Load nationwide site index (≈100k sites)
2. Filter to:
       state == {STATE}
       site_tp_cd == "ST"     # stream gages only
3. For each site:
       asynchronously fetch JSON IV data for 2022-11-07 → 2025-11-07
4. Flatten nested JSON into tidy rows with:
       site_no, state, datetime, date, param_code, param_name, unit, value
5. Remove sentinel missing values ("-999999.00") and empty strings
6. Save one Parquet per state using Snappy compression

ASYNC CONCURRENCY
=================
We control HTTP load with a semaphore:

    MAX_CONCURRENCY = 30

This yields **very large speedups** compared to requests/ThreadPoolExecutor,
especially for states with thousands of monitored sites (CA, PA, NY, TX).

DATE WINDOW
===========
Only records within the 3-year range are kept:

    START_DATE = 2022-11-07
    FINAL_DATE = 2025-11-07

This prevents unnecessary downloads and dramatically shrinks output size.

OUTPUT
======
Directory: `state_parquet_3yrs/`

Each file:
    states_iv_{STATE}_3yrs.parquet

Contains all time-series observations for all stream gages in the state for the
chosen parameter set.

WHY THIS MATTERS
================
This script is the ingestion backbone for the *When Rivers Speak* dashboard,
enabling:
    • Real-time and historical analysis
    • Seasonal/monthly trends
    • Polars/DuckDB query workflows
    • Python Shiny visualizations
    • Large-scale water quality analytics

It is optimized for **robustness**, **speed**, and **clean downstream data**.
"""

import os
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timezone
from tqdm.asyncio import tqdm_asyncio


OUTPUT_DIR = "state_parquet_3yrs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

PARAMS = "00010,00095,00300,00400"
URL_TMPL = (
    "https://waterservices.usgs.gov/nwis/iv/"
    "?format=json&sites={site}&startDT={start}&endDT={end}&parameterCd=" + PARAMS
)

START_DATE = datetime(2022, 11, 7, tzinfo=timezone.utc)
FINAL_DATE = datetime(2025, 11, 7, tzinfo=timezone.utc)

MAX_CONCURRENCY = 30
semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL",
    "GA","HI","ID","IL","IN","IA","KS","KY","LA","ME",
    "MD","MA","MI","MN","MS","MO","MT","NE","NV","NH",
    "NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI",
    "SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]


# =======================================================================
# JSON FLATTENER
# =======================================================================

def flatten_ts(site_no, ts_list, state):
    rows = []
    for ts in ts_list:
        var = ts.get("variable", {})
        param_code = var.get("variableCode", [{}])[0].get("value")
        param_name = var.get("variableName")
        unit = var.get("unit", {}).get("unitCode")

        values = ts.get("values", [{}])[0].get("value", [])

        for v in values:
            dt = v.get("dateTime")
            val = v.get("value")

            if not dt:
                continue

            dt_obj = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            if not (START_DATE <= dt_obj <= FINAL_DATE):
                continue

            # missing values
            if val in ("", None, "-999999.00"):
                val_clean = ""   # store as missing
            else:
                val_clean = val

            rows.append({
                "site_no": site_no,
                "state": state,
                "datetime": dt,
                "date": dt.split("T")[0],
                "param_code": param_code,
                "param_name": param_name,
                "unit": unit,
                "value": val_clean,
            })
    return rows


# =======================================================================
# FETCH (returns (site_no, ts_list))
# =======================================================================

async def fetch_with_site(session, site_no, start_str, end_str):
    url = URL_TMPL.format(site=site_no, start=start_str, end=end_str)
    for attempt in range(3):
        try:
            async with semaphore:
                async with session.get(url, timeout=20) as resp:
                    if resp.status == 200:
                        js = await resp.json()
                        ts = js.get("value", {}).get("timeSeries", [])
                        return site_no, ts
        except Exception:
            await asyncio.sleep(0.5 * (attempt + 1))

    return site_no, None


# =======================================================================
# STATE PROCESSOR
# =======================================================================

async def process_state(state, df):
    out_path = os.path.join(OUTPUT_DIR, f"states_iv_{state}_3yrs.parquet")

    if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
        print(f"{state}: already completed → skipping")
        return

    df_state = df[(df["source_state"] == state) & (df["site_tp_cd"] == "ST")]
    sites = df_state["site_no"].tolist()

    print(f"\n=== Processing {state} ===")
    print(f"{state}: {len(sites)} ST sites")

    start_str = START_DATE.isoformat().replace("+00:00","Z")
    end_str   = FINAL_DATE.isoformat().replace("+00:00","Z")

    async with aiohttp.ClientSession() as session:

        tasks = [
            fetch_with_site(session, site_no, start_str, end_str)
            for site_no in sites
        ]

        rows = []

        # FIX: iterate sync, await inside
        for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc=state):
            site_no, ts_list = await coro
            if ts_list:
                rows.extend(flatten_ts(site_no, ts_list, state))

    df_out = pd.DataFrame(rows)
    df_out.to_parquet(out_path, index=False, compression="snappy")
    print(f"{state}: wrote {len(df_out)} rows → {out_path}")

    out_path = os.path.join(OUTPUT_DIR, f"states_iv_{state}_3yrs.parquet")

    if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
        print(f"{state}: already completed → skipping")
        return

    df_state = df[(df["source_state"] == state) & (df["site_tp_cd"] == "ST")]
    sites = df_state["site_no"].tolist()

    print(f"\n=== Processing {state} ===")
    print(f"{state}: {len(sites)} ST sites")

    start_str = START_DATE.isoformat().replace("+00:00", "Z")
    end_str = FINAL_DATE.isoformat().replace("+00:00", "Z")

    async with aiohttp.ClientSession() as session:

        tasks = [
            fetch_with_site(session, site_no, start_str, end_str)
            for site_no in sites
        ]

        rows = []

        async for site_no, ts_list in tqdm_asyncio.as_completed(
            tasks, total=len(tasks), desc=state
        ):
            if ts_list:
                rows.extend(flatten_ts(site_no, ts_list, state))

    df_out = pd.DataFrame(rows)
    df_out.to_parquet(out_path, index=False, compression="snappy")

    print(f"{state}: wrote {len(df_out)} rows → {out_path}")


# =======================================================================
# MAIN
# =======================================================================

async def main():
    df = pd.read_parquet("usgs_all_sites.parquet")
    print(f"Loaded site list with {len(df)} rows")

    for state in US_STATES:
        await process_state(state, df)

    print("\nAll states completed!")


if __name__ == "__main__":
    asyncio.run(main())
