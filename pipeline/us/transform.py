"""
Build the oncology drug table.

Reads the biomarker list (oncology_drug_biomarkers.csv) and the openFDA results
(oncology_openfda_results.json), figures out the originator application and its
current label for each drug, and writes oncology_drug_table.csv.

Label history is kept one level deep: when a rebuild finds a different label
than the one already stored, the old row is kept with is_label_latest=0 and
the new label is added as is_label_latest=1.
"""

import re
import json
import hashlib
from datetime import datetime

import pandas as pd

from config_handler.config import *
from pipeline.us.versioning import read_previous_table, reconcile_with_previous


def url_hash(url):
    """SHA-256 of a label URL. Blank/null URLs stay null so they never collide."""
    if not url or url == NULL:
        return NULL
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def normalize_name(text):
    """Upper-case, tidy hyphens and spaces. Used to compare drug names."""
    text = re.sub(r"\s*-\s*", "-", str(text).upper())
    return re.sub(r"\s+", " ", text).strip()


def normalize_ingredient(text):
    """Like normalize_name but also drop a trailing salt word (e.g. 'SULFATE')
    and the 4-letter salt suffixes, so 'IMATINIB MESYLATE' matches 'IMATINIB'."""
    text = re.sub(r"\s*-\s*", "-", str(text).upper())
    text = re.sub(r"-[A-Z]{4}\b", "", text)
    words = [w for w in text.split() if w and w not in SALT_WORDS]
    return " ".join(words).strip()


def join_unique(values):
    """Join non-empty values with '; ', keeping first-seen order and no repeats."""
    seen = []
    for v in values or []:
        if v and v not in seen:
            seen.append(v)
    return "; ".join(seen)


def to_yyyymmdd(series, missing=""):
    """Parse a YYYYMMDD text column into yyyy-mm-dd text via pandas datetime.
    Blank or unparseable values become `missing`."""
    parsed = pd.to_datetime(series, format="%Y%m%d", errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), missing)


# --------------------------------------------------------------------------- #
# Attribute openFDA applications to each drug query
# --------------------------------------------------------------------------- #
def ingredient_set(app):
    """All normalized active-ingredient names in an application record."""
    names = set()
    for product in app.get("products", []) or []:
        for ingredient in product.get("active_ingredients", []) or []:
            name = ingredient.get("name")
            if name:
                names.add(normalize_ingredient(name))
    return {n for n in names if n}


def group_by_generic_name(results, query_names):
    """First pass: attach each application to a query when the query name appears
    in the application's openFDA generic_name. Combination queries ('A AND B')
    must have every part present."""
    groups = {q: {} for q in query_names}
    for app in results:
        generic_names = [normalize_name(g)
                         for g in (app.get("openfda", {}).get("generic_name") or [])]
        app_no = app.get("application_number") or f"_id{id(app)}"
        for query in query_names:
            nq = normalize_name(query)
            is_combo = " AND " in nq
            for gname in generic_names:
                if is_combo:
                    matched = all(part in gname for part in nq.split(" AND "))
                else:
                    matched = (" AND " not in gname) and (nq in gname)
                if matched:
                    groups[query][app_no] = app
                    break
    return groups


def fill_gaps_by_ingredient(groups, results):
    """Second pass: for queries still empty, match on the exact set of active
    ingredients instead of the generic name."""
    unmatched = [q for q, apps in groups.items() if not apps]
    for app in results:
        app_ingredients = ingredient_set(app)
        if not app_ingredients:
            continue
        app_no = app.get("application_number") or f"_id{id(app)}"
        for query in unmatched:
            query_ingredients = {normalize_ingredient(p)
                                 for p in normalize_name(query).split(" AND ")}
            if query_ingredients == app_ingredients:
                groups[query][app_no] = app
    return groups


def load_drug_groups(json_path, query_names):
    """Return {query -> [application records]} for either JSON shape we support."""
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    # Shape 1: already grouped by drug.
    if isinstance(data, dict) and "drugs" in data:
        return {d["query"]: d.get("applications", []) for d in data["drugs"]}

    # Shape 2: a flat list of results we have to attribute ourselves.
    results = data.get("results", []) if isinstance(data, dict) else []
    groups = group_by_generic_name(results, query_names)
    groups = fill_gaps_by_ingredient(groups, results)
    return {q: list(apps.values()) for q, apps in groups.items()}


# --------------------------------------------------------------------------- #
# Turn one application into the fields we need
# --------------------------------------------------------------------------- #
def earliest_orig_date(app):
    """Date of the earliest ORIG submission (used to pick the originator).
    Missing dates sort last."""
    dates = [s.get("submission_status_date")
             for s in app.get("submissions", []) or []
             if s.get("submission_type") == "ORIG" and s.get("submission_status_date")]
    return min(dates) if dates else "99999999"


# The submission-level fields we carry for the submission that owns the latest label.
SUBMISSION_FIELDS = ["submission_type", "submission_number", "submission_status",
                     "submission_status_date", "review_priority",
                     "submission_class_code", "submission_class_code_description"]


def _label_filename(url):
    """Last path segment of a label URL, lowercased — used to match a pinned label
    URL against the openFDA docs regardless of http/https or host differences."""
    return url.rsplit("/", 1)[-1].strip().lower() if url else ""


def latest_label(app, preferred_url=None):
    """Choose the Label PDF for this application and return
    (url, doc_date, submission_fields), where submission_fields is the metadata of
    the SUBMISSION that owns that label.

    Normally the newest-dated Label doc is used. If `preferred_url` is given (a
    hand-pinned label for this drug, from MANUAL_LABEL_URLS), that URL is used
    verbatim as the label_url, and the doc_date + submission fields are taken from
    the openFDA doc that matches it by file name (falling back to the newest doc,
    with a warning, if no doc matches). Returns (None, None, {blanks}) when the
    application has no label PDF and nothing was pinned."""
    candidates = []
    for submission in app.get("submissions", []) or []:
        for doc in (submission.get("application_docs") or []):
            if doc.get("type") == "Label" and doc.get("url"):
                candidates.append({
                    "sort_date": doc.get("date") or "00000000",
                    "url": doc.get("url"),
                    "doc_date": doc.get("date"),
                    "submission": {f: (submission.get(f) or "") for f in SUBMISSION_FIELDS},
                })

    blanks = {f: "" for f in SUBMISSION_FIELDS}

    if preferred_url:
        wanted = _label_filename(preferred_url)
        pinned = next((c for c in candidates
                       if _label_filename(c["url"]) == wanted), None)
        if pinned is None and candidates:
            print(f"[label] pinned label '{wanted}' not found in application "
                  f"{app.get('application_number', '')}; using newest-label "
                  f"metadata with the pinned URL")
            pinned = max(candidates, key=lambda c: c["sort_date"])
        doc_date = pinned["doc_date"] if pinned else None
        submission = pinned["submission"] if pinned else blanks
        # Use the pinned URL verbatim; metadata comes from the matching doc.
        return preferred_url, doc_date, submission

    if not candidates:
        return None, None, blanks

    # Newest label by its document date. key= avoids comparing None across tuples.
    best = max(candidates, key=lambda c: c["sort_date"])
    return best["url"], best["doc_date"], best["submission"]


def application_fields(query, app):
    """Flatten one application into a flat dict for selection and merging."""
    openfda = app.get("openfda", {}) or {}
    product_brands = [p.get("brand_name") for p in app.get("products", []) or []]
    product_status = [p.get("marketing_status") for p in app.get("products", []) or []]
    product_ingredients = [ing.get("name")
                           for p in app.get("products", []) or []
                           for ing in p.get("active_ingredients", []) or []]
    url, date, submission = latest_label(app, MANUAL_LABEL_URLS.get(str(query).lower()))
    row = {
        "query": query,
        "application_number": app.get("application_number", ""),
        "orig_approval_date": earliest_orig_date(app),
        "brand_name": join_unique(openfda.get("brand_name") or product_brands),
        "sponsor_name": app.get("sponsor_name", ""),
        "manufacturer_name": join_unique(openfda.get("manufacturer_name")),
        "substance_name": join_unique(openfda.get("substance_name") or product_ingredients),
        "marketing_status": join_unique(product_status),
        "_url": url,
        "_date": date,
    }
    row.update(submission)          # the 7 submission fields for the latest label
    return row


def pick_originators(groups):
    """One row per drug: its originator application (earliest ORIG approval, then
    lowest application_number) with that application's current label resolved."""
    rows = []
    for query, apps in groups.items():
        for app in apps:
            app_no = (app.get("application_number") or "").upper()
            if app_no.startswith(ALLOWED_APP_TYPES):
                rows.append(application_fields(query, app))

    columns = ["query", "application_number", "orig_approval_date", "brand_name",
               "sponsor_name", "manufacturer_name", "substance_name",
               "marketing_status", "_url", "_date", "submission_type","submission_number","submission_status","submission_status_date"
               ,"review_priority","submission_class_code","submission_class_code_description"]
    apps_df = pd.DataFrame(rows, columns=columns)
    if apps_df.empty:
        return apps_df.assign(label_url=NULL, label_version_date=NULL)

    # Originator = earliest ORIG date, ties broken by application_number.
    apps_df = (apps_df
               .sort_values(["orig_approval_date", "application_number"], kind="stable")
               .drop_duplicates("query", keep="first")
               .reset_index(drop=True))

    # Fill the two label columns; null them out when there is no label / bad date.
    has_url = apps_df["_url"].notna() & apps_df["_url"].astype(bool)
    apps_df["label_url"] = apps_df["_url"].where(has_url, NULL)
    apps_df["label_version_date"] = to_yyyymmdd(apps_df["_date"], missing=NULL).where(has_url, NULL)
    apps_df["submission_status_date"] = to_yyyymmdd(apps_df["submission_status_date"], missing="")
    return apps_df.drop(columns=["_url", "_date", "orig_approval_date"])


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    biomarkers = pd.read_csv(BIOMARKERS_CSV, dtype=str, keep_default_na=False,
                             encoding="utf-8-sig")
    if KEY_COL not in biomarkers.columns:
        raise SystemExit(f"'{KEY_COL}' column not found in {BIOMARKERS_CSV}; "
                         f"columns are: {list(biomarkers.columns)}")
    drug_name_col = next((c for c in DRUG_NAME_SOURCES if c in biomarkers.columns),
                         biomarkers.columns[0])

    query_names = sorted({q for q in biomarkers[KEY_COL] if q})
    groups = load_drug_groups(RESULTS_JSON, query_names)
    per_drug = pick_originators(groups)

    # Attach the per-drug fields to every biomarker row.
    merge_cols = ["query"] + DRUG_FIELDS
    if per_drug.empty:
        per_drug = pd.DataFrame(columns=merge_cols)
    table = biomarkers.merge(per_drug[merge_cols],
                             left_on=KEY_COL, right_on="query", how="left")

    # Drugs with no matching application get blanks / null label.
    for col in (["brand_name", "application_number", "sponsor_name",
                 "manufacturer_name", "substance_name", "marketing_status"]
                + SUBMISSION_FIELDS):
        if col in table.columns:
            table[col] = table[col].fillna("")
    table["label_url"] = table["label_url"].fillna(NULL)
    table["label_version_date"] = table["label_version_date"].fillna(NULL)

    table["data_source"] = "api"
    query_lower = table[KEY_COL].astype(str).str.lower()
    manual_mask = query_lower.isin(set(MANUAL_OVERRIDES))
    table.loc[manual_mask, "data_source"] = "manual"
    no_app = (table["application_number"] == "") & ~manual_mask
    table.loc[no_app, "data_source"] = "none"

    table["drug_name"] = table[drug_name_col]
    table["therapeutic_area"] = table["Therapeutic Area"]
    table["biomarker"] = table["Biomarker"]
    table["labeling_sections"] = table["Labeling Sections"]
    table["source_reference"] = SOURCE_REFERENCE
    table["document_hash"] = table["label_url"].map(url_hash)

    now_iso = datetime.now().astimezone().isoformat(timespec="seconds")

    # Incremental reconcile against the previously saved table.
    previous = read_previous_table(DRUG_TABLE_OUT_CSV)
    table, changes, pdf_hashes = reconcile_with_previous(table, previous, now_iso)

    # Sort by drug name, A -> Z (case-insensitive).
    table = table.sort_values("drug_name", key=lambda c: c.str.lower(),
                              kind="stable").reset_index(drop=True)
    table[OUT_COLS_DRUG_TABLE].to_csv(DRUG_TABLE_OUT_CSV, index=False, encoding="utf-8")

    # Summary line.
    current = table[table["is_label_latest"] == 1]
    previous_count = int((table["is_label_latest"] == 0).sum())
    changed_n = sum(1 for c in changes if c["type"] == "changed")
    new_n = sum(1 for c in changes if c["type"] == "new")
    print(f"[table] {len(table)} rows -> {DRUG_TABLE_OUT_CSV} "
          f"({len(current)} current, {previous_count} retained previous versions)")
    print(f"[table] changes: {changed_n} updated, {new_n} new; "
          f"{len(pdf_hashes)} labels need PDF extraction")

    # No mail is sent here; the caller sends one summary covering the table,
    # the openFDA calls and the PDFs together, once every step has run.
    return {"changed": bool(changes), "pdf_hashes": pdf_hashes,
            "changed_count": changed_n, "new_count": new_n,
            "changes": changes, "retrieved_at": now_iso}


if __name__ == "__main__":
    main()
