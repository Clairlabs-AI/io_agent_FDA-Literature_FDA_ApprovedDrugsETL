"""Label version history for the drug table.

The rules, applied by comparing this run's rows against the table saved last run:

  * unchanged row  -> keep it, and keep its PREVIOUS retrieved_at (not bumped)
  * changed row    -> update the changed fields, bump retrieved_at, record the diff
  * new row        -> add it, retrieved_at = now, record as new
  * a changed/new LABEL (document_hash) is added to pdf_hashes, so only those
    PDFs are read again
  * label history: when the label changes, the old current row is kept as
    is_label_latest=0 (one previous); otherwise any stored previous is carried.
"""

import os

import pandas as pd

from config_handler.config import *


# --------------------------------------------------------------------------- #
# Label version history (compare against the table we saved last time)
# --------------------------------------------------------------------------- #
# Columns that identify the same logical row across runs. Only the label fields
# are expected to change over time.
VERSION_KEY = ["drug_name", "therapeutic_area", "biomarker", "labeling_sections"]


def read_previous_table(path):
    """Load the drug table from the previous run, or None if there isn't one."""
    if not path or not os.path.exists(path):
        return None
    table = pd.read_csv(path, dtype=str, keep_default_na=False)
    if "is_label_latest" not in table.columns:
        table["is_label_latest"] = "1"
    return table


def index_by_key(rows):
    """Map (version key + duplicate counter) -> row, for quick lookup."""
    rows = rows.copy()
    rows["_occ"] = rows.groupby(VERSION_KEY).cumcount()
    lookup = {}
    for _, row in rows.iterrows():
        key = tuple(row[c] for c in VERSION_KEY) + (row["_occ"],)
        lookup[key] = row
    return lookup


def is_real_label(document_hash):
    return bool(document_hash) and document_hash != NULL


# Fields compared to decide whether a row actually changed. Everything in the
# output schema except the timestamp, the derived flag, and the constant.
COMPARE_COLS = [c for c in OUT_COLS_DRUG_TABLE
                if c not in ("retrieved_at", "is_label_latest", "source_reference")]


def reconcile_with_previous(new_rows, previous, now_iso):
    """Incrementally reconcile this run's rows against the previously saved table.

    Returns (result_df, changes, pdf_hashes):
      * result_df   - the table to write (is_label_latest set; label history kept)
      * changes     - list of {drug, biomarker, type: 'new'|'changed', fields:{col:(old,new)}}
      * pdf_hashes  - document_hashes whose LABEL is new/changed (need PDF re-extract)

    Rules:
      * unchanged row  -> keep it, and keep its PREVIOUS retrieved_at (not bumped)
      * changed row    -> update the changed fields, bump retrieved_at, record the diff
      * new row        -> add it, retrieved_at = now, record as new
      * a changed/new LABEL (document_hash) is added to pdf_hashes
      * label history: when the label changes, the old current row is kept as
        is_label_latest=0 (one previous); otherwise any stored previous is carried.
    """
    new_rows = new_rows.copy()
    new_rows["is_label_latest"] = 1
    new_rows["_occ"] = new_rows.groupby(VERSION_KEY).cumcount()

    changes = []
    pdf_hashes = set()
    output = []

    def _is_new_or_changed_label(new_row, prev):
        new_hash = new_row.get("document_hash")
        if not is_real_label(new_hash):
            return False
        if prev is None:
            return True                                   # new labeled row
        return prev.get("document_hash") != new_hash      # label changed

    if previous is None or previous.empty:
        for _, nr in new_rows.iterrows():
            row = dict(nr.drop(labels=["_occ"]))
            row["is_label_latest"] = 1
            row["retrieved_at"] = now_iso
            output.append(row)
            changes.append({"drug": row.get("drug_name", ""),
                            "biomarker": row.get("biomarker", ""),
                            "type": "new", "fields": {}})
            if is_real_label(row.get("document_hash")):
                pdf_hashes.add(row["document_hash"])
        result = pd.DataFrame(output)
        result["is_label_latest"] = result["is_label_latest"].astype(int)
        return result, changes, pdf_hashes

    previous = previous.copy()
    previous["is_label_latest"] = previous["is_label_latest"].astype(str)
    for col in VERSION_KEY + ["document_hash", "retrieved_at"] + COMPARE_COLS:
        if col not in previous.columns:
            previous[col] = ""
    stored_current = index_by_key(previous[previous["is_label_latest"] == "1"])
    stored_previous = index_by_key(previous[previous["is_label_latest"] == "0"])

    for _, nr in new_rows.iterrows():
        key = tuple(nr[c] for c in VERSION_KEY) + (nr["_occ"],)
        new_row = dict(nr.drop(labels=["_occ"]))
        new_row["is_label_latest"] = 1
        prev = stored_current.get(key)

        if prev is None:
            # NEW row
            new_row["retrieved_at"] = now_iso
            changes.append({"drug": new_row.get("drug_name", ""),
                            "biomarker": new_row.get("biomarker", ""),
                            "type": "new", "fields": {}})
            if _is_new_or_changed_label(new_row, None):
                pdf_hashes.add(new_row["document_hash"])
            output.append(new_row)
            continue

        # Diff the comparable fields.
        diff = {}
        for col in COMPARE_COLS:
            old_val = str(prev.get(col, ""))
            new_val = str(new_row.get(col, ""))
            if old_val != new_val:
                diff[col] = (old_val, new_val)

        label_changed = _is_new_or_changed_label(new_row, prev)

        if not diff:
            # UNCHANGED -> keep the previous retrieved_at (do NOT bump it).
            new_row["retrieved_at"] = prev.get("retrieved_at", "") or now_iso
            output.append(new_row)
            carried = stored_previous.get(key)          # keep any stored previous
            if carried is not None:
                kept = dict(carried.drop(labels=["_occ"]))
                kept["is_label_latest"] = 0
                output.append(kept)
        else:
            # CHANGED -> update changed fields (already in new_row), bump timestamp.
            new_row["retrieved_at"] = now_iso
            changes.append({"drug": new_row.get("drug_name", ""),
                            "biomarker": new_row.get("biomarker", ""),
                            "type": "changed", "fields": diff})
            if label_changed:
                pdf_hashes.add(new_row["document_hash"])
            output.append(new_row)
            if label_changed and is_real_label(prev.get("document_hash")):
                demoted = dict(prev.drop(labels=["_occ"]))   # keep old label as previous
                demoted["is_label_latest"] = 0
                output.append(demoted)
            else:
                carried = stored_previous.get(key)
                if carried is not None:
                    kept = dict(carried.drop(labels=["_occ"]))
                    kept["is_label_latest"] = 0
                    output.append(kept)

    result = pd.DataFrame(output)
    result["is_label_latest"] = result["is_label_latest"].astype(int)
    return result, changes, pdf_hashes
