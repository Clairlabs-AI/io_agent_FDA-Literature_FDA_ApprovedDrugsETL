"""Row-level versioning for a CSV that gets rebuilt from scratch on every run.

This module is project-independent: it knows nothing about EMA, medicines or
labels. Give it the newly built rows and the rows already in the output file,
and it reports what is unchanged, changed or new.

The four rules it applies:

  unchanged row  -> keep it, and keep its previous timestamp (not bumped)
  changed row    -> update the changed fields, bump the timestamp, record the diff
  new row        -> add it, timestamp = now, record it as new
  changed/new label -> its hash is collected in report["labels_to_extract"],
                       so a later PDF-extraction step knows what to re-read

Label history: when a label changes, the old current row is kept with
is_label_latest = 0 (one previous version kept per row key). If the label did
not change, any previous row already stored is carried through untouched.

Everything is compared as plain text, exactly as it appears in the CSV, so a
value never counts as changed just because pandas read it as a float in one
run and a string in the next.
"""

import io
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Column this module adds to the output file.
LABEL_LATEST_COLUMN = "is_label_latest"

# Defaults you can override when calling apply_versioning().
DEFAULT_TIMESTAMP_COLUMN = "retrieved_at"
DEFAULT_LABEL_COLUMNS = ["label_url", "document_hash"]
DEFAULT_HASH_COLUMN = "document_hash"


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def as_text(value):
    """Turn any cell value into a plain string for comparison. Blanks -> ''."""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in ("nan", "none", "nat", "<na>"):
        return ""
    return text


def dataframe_to_rows(df):
    """Convert a DataFrame to a list of dicts of strings.

    The DataFrame is written to CSV text and read straight back, so the values
    look exactly the way they will look on disk. Without this step a date could
    be a Timestamp in memory but a string in the file, and every row would
    look changed on the next run.
    """
    csv_text = df.to_csv(index=False)
    read_back = pd.read_csv(io.StringIO(csv_text), dtype=str, keep_default_na=False)
    return read_back.to_dict("records")


def read_csv_rows(csv_path):
    """Read a CSV into a list of dicts of strings. Missing file -> empty list.

    csv_path may be a Path or a plain string.
    """
    csv_path = Path(csv_path)

    if not csv_path.exists():
        logger.info("No previous file at %s, so every row counts as new.", csv_path.name)
        return []

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    return df.to_dict("records")


def write_rows_to_csv(rows, csv_path, column_order=None):
    """Write a list of dicts back out to CSV. csv_path may be a Path or string."""
    csv_path = Path(csv_path)
    df = pd.DataFrame(rows)
    if column_order:
        # Keep the caller's column order, then anything extra at the end.
        ordered = [c for c in column_order if c in df.columns]
        extra = [c for c in df.columns if c not in ordered]
        df = df[ordered + extra]
    df.to_csv(csv_path, index=False)


def build_key(row, key_columns, seen_counts):
    """Build the identity of a row: its key column values, as text.

    If the same key values appear more than once in one file (the source data
    does contain a few genuine duplicates), an occurrence number is appended so
    the second copy is matched against the second copy, not against the first.
    """
    values = tuple(as_text(row.get(column)) for column in key_columns)
    seen_counts[values] = seen_counts.get(values, 0) + 1
    return values + (str(seen_counts[values]),)


def key_as_label(key):
    """Turn a key tuple into a short readable string for logs and messages."""
    return " | ".join(part for part in key[:-1] if part) or "(blank key)"


def find_changed_fields(old_row, new_row, compare_columns):
    """Return {column: {"old": ..., "new": ...}} for every column that differs."""
    changes = {}
    for column in compare_columns:
        old_value = as_text(old_row.get(column))
        new_value = as_text(new_row.get(column))
        if old_value != new_value:
            changes[column] = {"old": old_value, "new": new_value}
    return changes


def pick_compare_columns(new_rows, previous_rows, timestamp_column):
    """Columns to compare: every column both files share, minus the bookkeeping ones."""
    if not new_rows:
        return []

    new_columns = list(new_rows[0].keys())
    if not previous_rows:
        shared = new_columns
    else:
        previous_columns = set(previous_rows[0].keys())
        shared = [c for c in new_columns if c in previous_columns]

        only_in_new = [c for c in new_columns if c not in previous_columns]
        if only_in_new:
            logger.warning(
                "These columns are new since the last run and cannot be compared: %s",
                ", ".join(only_in_new),
            )

    skip = {timestamp_column, LABEL_LATEST_COLUMN}
    return [c for c in shared if c not in skip]


# ---------------------------------------------------------------------------
# The main function
# ---------------------------------------------------------------------------

def apply_versioning(
    new_rows,
    previous_rows,
    key_columns,
    now,
    compare_columns=None,
    label_columns=None,
    hash_column=DEFAULT_HASH_COLUMN,
    timestamp_column=DEFAULT_TIMESTAMP_COLUMN,
):
    """Compare freshly built rows against what is already in the output file.

    Arguments:
        new_rows        list of dicts - the rows this run produced
        previous_rows   list of dicts - the rows already in the output file
        key_columns     list of column names that identify a row across runs
        now             timestamp string to stamp on changed and new rows
        compare_columns columns to compare; None means "all shared columns"
        label_columns   columns that mean "the label changed"
        hash_column     column holding the label hash to collect for PDF re-extract
        timestamp_column  column holding the retrieval timestamp

    Returns (final_rows, report).
    """
    if label_columns is None:
        label_columns = list(DEFAULT_LABEL_COLUMNS)

    if compare_columns is None:
        compare_columns = pick_compare_columns(new_rows, previous_rows, timestamp_column)

    # --- Split the previous file into "current" rows and "kept previous" rows.
    previous_current = {}
    previous_kept = {}
    current_counts = {}
    kept_counts = {}

    for row in previous_rows:
        flag = as_text(row.get(LABEL_LATEST_COLUMN))
        if flag == "0":
            previous_kept[build_key(row, key_columns, kept_counts)] = row
        else:
            # A missing flag means an older file without versioning; treat it as current.
            previous_current[build_key(row, key_columns, current_counts)] = row

    # --- Walk through the new rows and decide what happened to each one.
    final_rows = []
    report = new_report()
    new_counts = {}
    seen_keys = set()

    for new_row in new_rows:
        key = build_key(new_row, key_columns, new_counts)
        seen_keys.add(key)
        old_row = previous_current.get(key)

        row_to_keep = dict(new_row)
        row_to_keep[LABEL_LATEST_COLUMN] = "1"

        # ---- Rule 3: brand new row.
        if old_row is None:
            row_to_keep[timestamp_column] = now
            final_rows.append(row_to_keep)

            report["new"].append({"key": key_as_label(key)})
            add_label_to_extract(report, row_to_keep, key, hash_column, "new row")
            continue

        changes = find_changed_fields(old_row, new_row, compare_columns)

        # ---- Rule 1: nothing changed, so keep the older timestamp.
        if not changes:
            row_to_keep[timestamp_column] = as_text(old_row.get(timestamp_column))
            final_rows.append(row_to_keep)
            carry_kept_previous(final_rows, previous_kept, key)

            report["unchanged"].append({"key": key_as_label(key)})
            continue

        # ---- Rule 2: something changed, so bump the timestamp and record the diff.
        row_to_keep[timestamp_column] = now
        final_rows.append(row_to_keep)

        report["changed"].append({"key": key_as_label(key), "changes": changes})

        # ---- Rule 4: did the label itself change?
        label_changed = any(column in changes for column in label_columns)
        if label_changed:
            # Keep the old current row as the one previous version.
            old_version = dict(old_row)
            old_version[LABEL_LATEST_COLUMN] = "0"
            final_rows.append(old_version)

            report["label_changed"].append({
                "key": key_as_label(key),
                "changes": {c: changes[c] for c in label_columns if c in changes},
            })
            add_label_to_extract(report, row_to_keep, key, hash_column, "label changed")
        else:
            # Label is the same, so whatever previous version was stored stays.
            carry_kept_previous(final_rows, previous_kept, key)

    # --- Rows that were in the file but are not in the new data any more.
    for key in previous_current:
        if key not in seen_keys:
            report["removed"].append({"key": key_as_label(key)})

    report["counts"] = {
        "unchanged": len(report["unchanged"]),
        "changed": len(report["changed"]),
        "new": len(report["new"]),
        "removed": len(report["removed"]),
        "label_changed": len(report["label_changed"]),
        "labels_to_extract": len(report["labels_to_extract"]),
        "rows_written": len(final_rows),
    }
    return final_rows, report


def new_report():
    """An empty report, so every caller sees the same shape."""
    return {
        "unchanged": [],
        "changed": [],
        "new": [],
        "removed": [],
        "label_changed": [],
        "labels_to_extract": [],
        "counts": {},
    }


def carry_kept_previous(final_rows, previous_kept, key):
    """Keep the previous-version row that is already stored for this key, if any."""
    old_version = previous_kept.get(key)
    if old_version is not None:
        carried = dict(old_version)
        carried[LABEL_LATEST_COLUMN] = "0"
        final_rows.append(carried)


def add_label_to_extract(report, row, key, hash_column, reason):
    """Collect a label hash that the PDF-extraction step still has to read."""
    document_hash = as_text(row.get(hash_column))
    if not document_hash:
        return

    report["labels_to_extract"].append({
        "key": key_as_label(key),
        hash_column: document_hash,
        "label_url": as_text(row.get("label_url")),
        "reason": reason,
    })


def format_report_as_text(report, title, run_timestamp, max_rows_per_section=20):
    """Turn a report into plain text, ready to be logged or sent as one message.

    Only the first `max_rows_per_section` rows of each section are listed, so a
    run with hundreds of changes still produces a readable message. The counts
    at the top are always complete.
    """
    lines = [title, "Run at: " + str(run_timestamp), ""]

    lines.append("Counts")
    for name, value in report["counts"].items():
        lines.append("  {:<18} {}".format(name.replace("_", " ") + ":", value))
    lines.append("")

    lines += section_lines("New rows", report["new"], max_rows_per_section)
    lines += section_lines("Changed rows", report["changed"], max_rows_per_section)
    lines += section_lines("Rows no longer in the source", report["removed"], max_rows_per_section)
    lines += section_lines("Labels needing PDF extraction", report["labels_to_extract"], max_rows_per_section)

    unchanged_count = report["counts"].get("unchanged", 0)
    lines.append("Unchanged rows: {} (kept with their previous timestamp)".format(unchanged_count))

    return "\n".join(lines)


def section_lines(heading, entries, max_rows):
    """Format one section of the report."""
    if not entries:
        return ["{}: none".format(heading), ""]

    lines = ["{} ({})".format(heading, len(entries))]

    for entry in entries[:max_rows]:
        lines.append("  - " + entry["key"])

        # Changed rows carry the field-by-field diff.
        for column, change in entry.get("changes", {}).items():
            lines.append("      {}: '{}' -> '{}'".format(column, change["old"], change["new"]))

        # Label rows carry the hash that has to be re-extracted.
        if "document_hash" in entry:
            lines.append("      hash: {}  ({})".format(entry["document_hash"], entry["reason"]))

    if len(entries) > max_rows:
        lines.append("  ... and {} more".format(len(entries) - max_rows))

    lines.append("")
    return lines


def write_labels_to_extract(report, csv_path):
    """Write the hashes needing PDF re-extraction to their own small CSV.

    The PDF-extraction step reads this file to find out which labels to fetch,
    instead of re-reading every PDF every run.

    csv_path may be a Path or a plain string.
    """
    csv_path = Path(csv_path)
    rows = report["labels_to_extract"]
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    logger.info("Wrote %s label(s) needing PDF extraction to %s", len(rows), csv_path.name)
