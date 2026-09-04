
import re
import csv
from io import StringIO


import pandas as pd

from config_handler.config import *


# --------------------------------------------------------------------------- #
# Step 1 - download the FDA page with curl_cffi (browser TLS impersonation,
#          no real browser needed)
# --------------------------------------------------------------------------- #
def fetch_fda_html(url, output_file):

    from curl_cffi import requests as creq
    for profile in ("chrome124", "chrome120", "chrome"):
        try:
            resp = creq.get(url, impersonate=profile, timeout=60)
            if resp.status_code == 200 and len(resp.text) > 1000:
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(resp.text)
                print(f"[scrape] fetched FDA table via curl_cffi "
                      f"({profile}, {len(resp.text)} bytes)")
                return True
            print(f"[scrape] curl_cffi {profile} -> HTTP {resp.status_code}")
        except Exception as error:
            print(f"[scrape] curl_cffi {profile} failed: {error}")
    return False


# --------------------------------------------------------------------------- #
# Step 2 - read the biggest HTML table on the saved page and normalize its
#          column names.
# --------------------------------------------------------------------------- #
def fetch_pgx_table():

    if not fetch_fda_html(FDA_TABLE_URL, FDA_DRUG_TABLE_HTML):
        raise SystemExit("Page could not be scraped; check the browser setup and retry.")

    # The raw HTML is uploaded to S3 later, by runner.py.

    print(f"[html] reading saved page: {FDA_DRUG_TABLE_HTML}")
    with open(FDA_DRUG_TABLE_HTML,
              encoding="utf-8", errors="ignore") as f:
        html = f.read()

    tables = pd.read_html(StringIO(html))
    if not tables:
        raise RuntimeError("No HTML tables found on the FDA page.")
    df = max(tables, key=len)  # the biomarker table is the largest

    df.columns = [re.sub(r"\s+", " ", str(c)).strip().rstrip("*").strip()
                  for c in df.columns]
    rename = {}
    for col in df.columns:
        lower = col.lower()
        if "drug" in lower:
            rename[col] = "Drug"
        elif "therapeutic" in lower:
            rename[col] = "Therapeutic Area"
        elif "biomarker" in lower:
            rename[col] = "Biomarker"
        elif "labeling" in lower or "labelling" in lower:
            rename[col] = "Labeling Sections"
    df = df.rename(columns=rename)

    missing = {"Drug", "Therapeutic Area"} - set(df.columns)
    if missing:
        raise RuntimeError(f"Expected columns not found: {missing}. Got: {list(df.columns)}")
    return df


# --------------------------------------------------------------------------- #
# Step 3 - clean drug names and write the oncology CSV
# --------------------------------------------------------------------------- #
def clean_drug_name(raw):
    """Strip footnote markers ('Abemaciclib (1)' -> 'Abemaciclib')."""
    name = str(raw).strip()
    name = re.sub(r"\s*\((?:\s*\d+\s*)(?:,\s*\d+\s*)*\)", "", name)
    name = re.sub(r"(?<=[A-Za-z)])\s+\d+\s*$", "", name)
    return re.sub(r"\s+", " ", name).strip()


def generic_query_name(clean_name):
    return clean_name.lower()


def build_oncology_csv(df):
    """Write oncology_drug_biomarkers.csv and return {query_name -> original label(s)}."""
    oncology = df[df["Therapeutic Area"].astype(str)
    .str.contains(TARGET_AREA, case=False, na=False)].copy()

    oncology["Drug (original)"] = oncology["Drug"].astype(str)
    oncology["Drug (clean)"] = oncology["Drug"].map(clean_drug_name)
    oncology["generic_name_query"] = oncology["Drug (clean)"].map(generic_query_name)

    oncology.to_csv(BIOMARKERS_CSV, index=False, columns=OUT_COLS_BIOMARKERS,
                    quoting=csv.QUOTE_MINIMAL)
    print(f"[csv]  wrote {len(oncology)} oncology rows -> {BIOMARKERS_CSV}")

    name_map = {}
    for original, query in zip(oncology["Drug (original)"], oncology["generic_name_query"]):
        if not query:
            continue
        name_map.setdefault(query, [])
        if original not in name_map[query]:
            name_map[query].append(original)
    name_map = {query: "; ".join(originals) for query, originals in sorted(name_map.items())}
    print(f"[csv]  {len(name_map)} unique drug names for the API")
    return name_map

def main():
    df = fetch_pgx_table()
    name_map=build_oncology_csv(df)
    print(f"[scrape] biomarkers CSV -> {BIOMARKERS_CSV}")
    return name_map


if __name__ == "__main__":
    main()
