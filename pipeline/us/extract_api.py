"""
Fetch the oncology drugs from the FDA pharmacogenomic-biomarker table and pull
their Drugs@FDA records from openFDA.

Queries openFDA for those drugs in four passes and saves the grouped records
to oncology_openfda_results.json. The first three passes match by
generic_name / active_ingredients; the fourth fetches the MANUAL_OVERRIDES
drugs directly by their pinned application_number.
"""

import re
import csv
import json
import time
import collections
from urllib.parse import quote

from pipeline.common import http_fetch
from config_handler.config import *



# openFDA batches that could not be fetched this run. Reported in the summary
# mail and queried again next run.
FAILED_BATCHES = []


def api_result():
    """What happened to the openFDA calls, for the run summary mail."""
    return {"batches": TOTAL_BATCHES[0], "failures": list(FAILED_BATCHES)}


# Counted across every call_openfda() pass in the run.
TOTAL_BATCHES = [0]


# --------------------------------------------------------------------------- #
# call openFDA (auto-batched)
# --------------------------------------------------------------------------- #
def build_search(drug_names, field="openfda.generic_name"):
    """Build a search clause: field:("a" "b" ...). Terms are space-separated so
    openFDA treats them as OR (a literal '+' would become an AND operator)."""
    quoted = " ".join(f'"{name}"' for name in drug_names)
    return f"{field}:({quoted})"


def build_url(drug_names, field="openfda.generic_name"):
    url = (f"{OPENFDA_URL}?search={quote(build_search(drug_names, field), safe='')}"
           f"&limit={LIMIT}")
    if API_KEY:
        url += f"&api_key={API_KEY}"
    print(url)
    return url


def get_json(url, retries=None):
    """GET one openFDA URL and return its JSON.

    Retrying, pacing between calls and Retry-After handling are all done by
    http_fetch. openFDA answers 404 with a JSON body meaning "no matches",
    which is a normal result, so that status is fetched directly instead of
    going through the retrying path.
    """
    response = http_fetch.session().get(url, headers=HEADERS,
                                        timeout=HTTP_TIMEOUT_SECONDS)
    if response.status_code == 404:
        return response.json()                          # {"error": {"code": "NOT_FOUND"}}

    if response.status_code in http_fetch.RETRY_STATUS_CODES:
        response = http_fetch.get(url, headers=HEADERS)  # paced + retried
    else:
        response.raise_for_status()

    return response.json()


def call_openfda(drug_names, field="openfda.generic_name"):
    """Query openFDA for all names, splitting into batches of MAX_NAMES_PER_CALL
    when needed and de-duplicating applications by application_number."""
    batches = [drug_names[i:i + MAX_NAMES_PER_CALL]
               for i in range(0, len(drug_names), MAX_NAMES_PER_CALL)] or [[]]
    merged = []
    seen = set()
    totals = []
    TOTAL_BATCHES[0] += len(batches)

    for i, batch in enumerate(batches, 1):
        url = build_url(batch, field)
        print(f"[api]  batch {i}/{len(batches)} ({len(batch)} drug_names, URL {len(url)} chars)")

        # A batch that fails after every retry is recorded and skipped, not
        # raised, so the rest of the run keeps going. Failures go into the run
        # summary mail and are queried again next run.
        try:
            data = get_json(url)
        except Exception as error:
            print(f"[api]  batch {i} FAILED after retries: {error}")
            FAILED_BATCHES.append({"batch": i, "drug_names": list(batch),
                                   "error": str(error)})
            continue

        error = data.get("error")
        if error and error.get("code") == "NOT_FOUND":
            print(f"[api]  batch {i}: no matches")
            continue
        if error:
            print(f"[api]  batch {i} returned an error: {error}")
            FAILED_BATCHES.append({"batch": i, "drug_names": list(batch),
                                   "error": str(error)})
            continue

        totals.append(data.get("meta", {}).get("results", {}).get("total"))
        for record in data.get("results", []):
            key = record.get("application_number") or id(record)
            if key not in seen:
                seen.add(key)
                merged.append(record)
        time.sleep(0.2)

    return {
        "query_names": drug_names,
        "num_batches": len(batches),
        "reported_totals_per_batch": totals,
        "unique_applications_returned": len(merged),
        "results": merged,
    }


# --------------------------------------------------------------------------- #
# Direct fetch by application_number (used for the MANUAL_OVERRIDES drugs)
# --------------------------------------------------------------------------- #
def _override_appno(value):
    """MANUAL_OVERRIDES value -> application_number. Accepts either a plain
    string ("NDA020298") or a dict form ({'application_number': ...})."""
    if isinstance(value, dict):
        return (value.get("application_number") or "").strip()
    return (value or "").strip()


def build_appno_url(application_number):
    """openFDA URL that fetches one application by its exact application_number,
    e.g. search=application_number:"NDA020298"&limit=1."""
    search = f'application_number:"{application_number}"'
    url = f"{OPENFDA_URL}?search={quote(search, safe='')}&limit={LIMIT}"
    if API_KEY:
        url += f"&api_key={API_KEY}"
    print(url)
    return url


def fetch_by_application_number(application_number):
    """Return the single Drugs@FDA record for an application_number, or None."""
    data = get_json(build_appno_url(application_number))
    error = data.get("error")
    if error and error.get("code") == "NOT_FOUND":
        print(f"[appno] no Drugs@FDA record for {application_number}")
        return None
    if error:
        raise RuntimeError(f"openFDA error for {application_number}: {error}")
    results = data.get("results", [])
    return results[0] if results else None


def normalize_name(text):
    """Uppercase and tidy hyphen spacing ('isatuximab- irfc' -> 'ISATUXIMAB-IRFC')."""
    text = re.sub(r"\s*-\s*", "-", str(text).upper())
    return re.sub(r"\s+", " ", text).strip()


# Salt / ester words dropped when matching on active_ingredients.name.
SALT_WORDS = {
    "HYDROCHLORIDE", "HYDROCHLOIDE", "DIHYDROCHLORIDE", "SULFATE", "SULPHATE",
    "CITRATE", "MESYLATE", "MALEATE", "TARTRATE", "SUCCINATE", "FUMARATE",
    "PHOSPHATE", "ACETATE", "SODIUM", "POTASSIUM", "CALCIUM", "BROMIDE",
    "CHLORIDE", "ESYLATE", "BESYLATE", "TOSYLATE", "LACTATE", "NITRATE",
    "PAMOATE", "DECANOATE", "HYCLATE", "MONOHYDRATE", "DIHYDRATE", "HYDRATE",
    "XINAFOATE", "ISETHIONATE", "DIMALEATE", "MEPESUCCINATE", "PROPIONATE",
    "PIVALATE", "ENANTATE", "ENANTHATE", "HEMIFUMARATE", "DIASPARTATE",
}


def normalize_ingredient(text):
    """Uppercase, tidy hyphens, drop a 4-letter biologic suffix and salt words."""
    text = re.sub(r"\s*-\s*", "-", str(text).upper())
    text = re.sub(r"-[A-Z]{4}\b", "", text)             # e.g. -GXLY, -HZIY
    words = [w for w in text.split() if w and w not in SALT_WORDS]
    return " ".join(words).strip()


def ingredient_set(record):
    """Normalized set of active ingredients for one application."""
    names = set()
    for product in record.get("products", []) or []:
        for ingredient in product.get("active_ingredients", []) or []:
            if ingredient.get("name"):
                names.add(normalize_ingredient(ingredient["name"]))
    return {n for n in names if n}


def attribute_records(query_names, records, exact):
    """Attach each application to the drug(s) it belongs to by generic_name.
    A single-ingredient drug skips combination records; a combo drug matches only
    records that contain all its parts. Returns (groups, unattributed)."""
    drug_info = {q: (normalize_name(q), " AND " in normalize_name(q),
                     normalize_name(q).split(" AND "))
                 for q in query_names}
    groups = collections.OrderedDict((q, {}) for q in query_names)
    unattributed = {}

    for record in records:
        generic_names = [normalize_name(g)
                         for g in (record.get("openfda", {}).get("generic_name") or [])]
        app_no = record.get("application_number") or f"_noappno_{id(record)}"
        matched_any = False
        for query in query_names:
            norm_query, query_is_combo, query_parts = drug_info[query]
            hit = False
            for gname in generic_names:
                gname_is_combo = " AND " in gname
                if exact:
                    hit = (gname == norm_query)
                elif query_is_combo:
                    hit = all(part in gname for part in query_parts)
                else:
                    hit = (not gname_is_combo) and (norm_query in gname)
                if hit:
                    break
            if hit:
                groups[query][app_no] = record
                matched_any = True
        if not matched_any:
            unattributed[app_no] = record
    return groups, unattributed


def attribute_by_ingredients(query_names, records):
    """Fallback: match a drug to a record when their active-ingredient SETS are
    equal (so a single drug never picks up a combination product)."""
    groups = collections.OrderedDict((q, {}) for q in query_names)
    for record in records:
        record_ingredients = ingredient_set(record)
        if not record_ingredients:
            continue
        app_no = record.get("application_number") or f"_noappno_{id(record)}"
        for query in query_names:
            query_ingredients = {normalize_ingredient(p)
                                 for p in normalize_name(query).split(" AND ")}
            if query_ingredients == record_ingredients:
                groups[query][app_no] = record
    return groups


def split_components(name):
    """Split a combo drug name on ' and ' into its component names."""
    parts = [p.strip() for p in re.split(r"\s+and\s+", name, flags=re.IGNORECASE)]
    return [p for p in parts if p]


def attribute_split(query_names, records):
    """For combo drugs whose full name matched nothing, attach a record when its
    ingredient set equals the component set (the true combo product). If none
    of those exist, fall back to a non-empty subset or a component generic_name
    (a co-packaged component NDA)."""
    components = {q: {normalize_ingredient(p) for p in split_components(q)}
                 for q in query_names}
    groups = collections.OrderedDict((q, {}) for q in query_names)
    for query in query_names:
        component_set = components[query]
        if not component_set:
            continue
        full_matches, partial_matches = {}, {}
        for record in records:
            record_ingredients = ingredient_set(record)
            record_generics = {normalize_ingredient(g)
                               for g in (record.get("openfda", {}).get("generic_name") or [])}
            app_no = record.get("application_number") or f"_noappno_{id(record)}"
            if record_ingredients and record_ingredients == component_set:
                full_matches[app_no] = record
            elif (record_ingredients and record_ingredients <= component_set) \
                    or (record_generics & component_set):
                partial_matches[app_no] = record
        groups[query] = full_matches or partial_matches     # prefer the true combo record
    return groups


def build_grouped_json_via_api_call(path, name_map):
    """Four-pass, drug-grouped output:
      Pass 1 - openfda.generic_name (also catches salt forms).
      Pass 2 - for drugs still empty, products.active_ingredients.name.
      Pass 3 - for drugs still empty, split the name on ' and ' and query each
               component by generic_name, then stitch them back together.
      Pass 4 - drugs listed in MANUAL_OVERRIDES: fetched directly by their pinned
               application_number and NOT run through passes 1-3 at all.
    """
    all_names = list(name_map)

    # Split off the MANUAL_OVERRIDES drugs: they are fetched by application_number
    # (Pass 4) and excluded from the three generic-name/ingredient passes.
    overrides = {q: _override_appno(MANUAL_OVERRIDES[q.lower()])
                 for q in all_names if q.lower() in MANUAL_OVERRIDES}
    drug_names = [q for q in all_names if q not in overrides]
    if overrides:
        print(f"[appno] {len(overrides)} drugs pinned by application_number "
              f"(excluded from passes 1-3): {overrides}")

    # Pass 1
    pass1 = call_openfda(drug_names)
    groups1, _ = attribute_records(drug_names, pass1["results"], exact=False)

    # Pass 2 - only the drugs Pass 1 could not match
    unmatched1 = [q for q in drug_names if not groups1[q]]
    groups2 = {}
    if unmatched1:
        print(f"[fallback] {len(unmatched1)} drugs unmatched by generic_name; ", unmatched1, "\n"
              "retrying via products.active_ingredients.name")
        pass2 = call_openfda(unmatched1, field="products.active_ingredients.name")
        groups2 = attribute_by_ingredients(unmatched1, pass2["results"])

    # Pass 3 - drugs still empty: split the name and query each component
    unmatched2 = [q for q in unmatched1 if not groups2.get(q)]
    groups3 = {}
    if unmatched2:
        components = sorted({c for q in unmatched2 for c in split_components(q)})
        print(f"[split] {len(unmatched2)} drugs unmatched by generic_name AND "
              f"active_ingredients; trying SPLIT generic_name on components:")
        for query in unmatched2:
            print(f"[split]   '{query}' -> {split_components(query)}")
        print(f"[split] querying {len(components)} split components by "
              f"openfda.generic_name: {components}")
        pass3 = call_openfda(components)
        groups3 = attribute_split(unmatched2, pass3["results"])

    # Pass 4 - MANUAL_OVERRIDES drugs: one direct application_number lookup each.
    groups4 = {}
    for query, app_no in overrides.items():
        if not app_no:
            print(f"[appno] '{query}' has no application_number in MANUAL_OVERRIDES; skipping")
            groups4[query] = {}
            continue
        record = fetch_by_application_number(app_no)
        groups4[query] = {app_no: record} if record else {}

    # Stitch the passes together: Pass 1 -> 2 -> 3 for normal drugs; Pass 4 for
    # the application_number-pinned drugs (which only ever appear in groups4).
    drugs = []
    for query in all_names:
        records = groups1.get(query) or {}
        matched_via = "openfda.generic_name" if records else None
        if not records and groups2.get(query):
            records = groups2[query]
            matched_via = "products.active_ingredients.name"
        if not records and groups3.get(query):
            records = groups3[query]
            matched_via = "openfda.generic_name (split)"
        if not records and groups4.get(query):
            records = groups4[query]
            matched_via = "application_number"
        drugs.append({
            "query": query,
            "drug_original": name_map.get(query, query),
            "matched_via": matched_via,
            "match_count": len(records),
            "applications": list(records.values()),
        })

    still_unmatched = [d["query"] for d in drugs if not d["match_count"]]
    output = {
        "meta": {
            "source": "openFDA Drugs@FDA (api.fda.gov/drug/drugsfda.json)",
            "queried_drugs": len(all_names),
            "matched_via_generic_name": sum(
                1 for d in drugs if d["matched_via"] == "openfda.generic_name"),
            "matched_via_active_ingredients": sum(
                1 for d in drugs if d["matched_via"] == "products.active_ingredients.name"),
            "matched_via_split_generic_name": sum(
                1 for d in drugs if d["matched_via"] == "openfda.generic_name (split)"),
            "matched_via_application_number": sum(
                1 for d in drugs if d["matched_via"] == "application_number"),
            "drugs_with_matches": sum(1 for d in drugs if d["match_count"]),
            "still_unmatched_count": len(still_unmatched),
            "still_unmatched_drugs": still_unmatched,
        },
        "drugs": drugs,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    return output


# --------------------------------------------------------------------------- #
# Step 4 - optional exact-match summary (one row per drug)
# --------------------------------------------------------------------------- #
def original_approval_date(record):
    """Earliest ORIGINAL approval date (YYYYMMDD). Prefers approved ORIG
    submissions, then falls back to any ORIG that has a date."""
    submissions = record.get("submissions", []) or []
    approved = [s.get("submission_status_date") for s in submissions
                if s.get("submission_type") == "ORIG"
                and s.get("submission_status") == "AP"
                and s.get("submission_status_date")]
    if approved:
        return min(approved)
    any_orig = [s.get("submission_status_date") for s in submissions
                if s.get("submission_type") == "ORIG"
                and s.get("submission_status_date")]
    return min(any_orig) if any_orig else None


def format_date(date):
    return f"{date[0:4]}-{date[4:6]}-{date[6:8]}" if date and len(date) == 8 else ""


def summarize_by_drug(drug_names, records, name_map=None):
    """One row per drug: the earliest original approval across all applications
    whose exact generic_name matches, plus that winning application's details."""
    records_by_name = {}
    for record in records:
        for generic in (record.get("openfda", {}).get("generic_name") or []):
            records_by_name.setdefault(generic.upper(), []).append(record)

    name_map = name_map or {}
    rows = []
    for name in drug_names:
        matches = records_by_name.get(name.upper(), [])
        best = None
        for record in matches:
            date = original_approval_date(record)
            if date and (best is None or date < best[0]):
                best = (date, record)
        row = {
            "drug_original": name_map.get(name, name),
            "drug": name,
            "matched": bool(best),
            "n_applications": len(matches),
            "first_original_approval": "",
            "application_number": "",
            "brand_name": "",
            "sponsor_name": "",
        }
        if best:
            date, record = best
            openfda = record.get("openfda", {})
            row.update({
                "first_original_approval": format_date(date),
                "application_number": record.get("application_number", ""),
                "brand_name": "; ".join(openfda.get("brand_name", []) or []),
                "sponsor_name": record.get("sponsor_name", ""),
            })
        rows.append(row)
    return rows


def run_exact_summary(name_map):
    """Query with .exact matching and save the one-row-per-drug approval summary."""
    drug_names = list(name_map)
    exact_names = sorted({n.upper() for n in drug_names})
    print(f"[exact] querying {len(exact_names)} names with openfda.generic_name.exact")
    payload = call_openfda(exact_names, field="openfda.generic_name.exact")
    rows = summarize_by_drug(drug_names, payload["results"], name_map)

    fields = ["drug_original", "drug", "matched", "n_applications",
              "first_original_approval", "application_number", "brand_name", "sponsor_name"]
    with open(SUMMARY_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    with open(SUMMARY_JSON, "w") as f:
        json.dump(rows, f, indent=2)

    matched = sum(1 for r in rows if r["matched"])
    print(f"[exact] {matched}/{len(rows)} drugs matched exactly -> {SUMMARY_CSV}, {SUMMARY_JSON}")
    misses = [r["drug"] for r in rows if not r["matched"]]
    if misses:
        print(f"[exact] no exact Drugs@FDA match (often biologics/BLAs or name "
              f"differences): {', '.join(misses)}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main(JSON_OUT, name_map):

    output = build_grouped_json_via_api_call(JSON_OUT, name_map)
    meta = output["meta"]
    print(f"[json] {meta['drugs_with_matches']}/{meta['queried_drugs']} drugs matched "
          f"({meta['matched_via_generic_name']} via generic_name, "
          f"{meta['matched_via_active_ingredients']} via active_ingredients, "
          f"{meta['matched_via_split_generic_name']} via split_ingredients, "
          f"{meta['matched_via_application_number']} via application_number) -> {JSON_OUT}")
    if meta["still_unmatched_drugs"]:
        print(f"[json] still not found in Drugs@FDA ({meta['still_unmatched_count']}): "
              f"{', '.join(meta['still_unmatched_drugs'])}")

    if WRITE_EXACT_SUMMARY:
        run_exact_summary(name_map)


if __name__ == "__main__":
    main()
    raise SystemExit("This module is called by the Glue job as "
                     "main(JSON_OUT, name_map); it is not meant to run standalone.")
