"""Step 2 of the pipeline: turn the raw EMA downloads into two CSV files.

  ema_excel_file.xlsx  ->  ema_transformed_csv_file.csv    (clean medicine table)
  + ema_json_file.json ->  ema_transformed_label_file.csv  (cancer medicines + labels)

The label file is versioned rather than overwritten: the file from the previous
run is read back, compared row by row, and rewritten with unchanged rows keeping
their old timestamp. See versioning.py for the rules.

As in the extract step, problems raise rather than being printed and ignored,
so a failure here can never look like a successful run.
"""

import json
import logging
import re

import pandas as pd

from config_handler.config import (
    BASE_EMA_URL,
    CANCER_ALIASES,
    EMA_JSON_FILE,
    EMA_EXCEL_FILE,
    EMA_TRANSFORMED_CSV_FILE,
    EMA_TRANSFORMED_LABEL_FILE,
    EXCEL_HEADER_MARKER_COLUMN,
    LABEL_COLUMNS,
    LABEL_HASH_COLUMN,
    TIMESTAMP_COLUMN,
    VERSION_KEY_COLUMNS,
    run_timestamp,
    url_hash,
    MANUAL_LABEL_URLS,
)

from pipeline.eu import versioning  # noqa: E402

logger = logging.getLogger(__name__)

# How many rows to look through when hunting for the header row.
MAX_HEADER_SEARCH_ROWS = 20


def find_header_row(excel_path, marker_column=EXCEL_HEADER_MARKER_COLUMN):
    """Return the 0-based row number of the real header row in the EMA workbook.

    EMA puts a few lines of notes above the table, so the header row is found
    by searching for the row that contains the marker column.
    """
    preview = pd.read_excel(excel_path, header=None, nrows=MAX_HEADER_SEARCH_ROWS)

    for row_number in range(len(preview)):
        row_values = preview.iloc[row_number].astype(str).str.strip()
        if row_values.eq(marker_column).any():
            logger.info("Found the header row at row %s of the workbook", row_number + 1)
            return row_number

    raise ValueError(
        f"Could not find a header row containing '{marker_column}' in the first "
        f"{MAX_HEADER_SEARCH_ROWS} rows of {excel_path.name}. "
        "The EMA file layout may have changed."
    )


def transform_excel_data_to_csv():
    """Read the EMA Excel report, tidy it up and save it as a CSV."""
    header_row = find_header_row(EMA_EXCEL_FILE)

    df = pd.read_excel(EMA_EXCEL_FILE, skiprows=header_row)
    df = df.sort_values(by=["Name of medicine"])

    # Trim stray spaces from every text column.
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    df.to_csv(EMA_TRANSFORMED_CSV_FILE, index=False)
    logger.info("Wrote %s rows to %s", len(df), EMA_TRANSFORMED_CSV_FILE.name)


def apply_manual_label_urls(df_merged_label):
    """Fill in the label URLs listed in config.MANUAL_LABEL_URLS.

    Matching is done on the medicine name, after the merge, case-insensitive
    and ignoring surrounding spaces, so the name in config does not have to be
    typed exactly as EMA writes it.
    """
    urls_by_name = {name.strip().lower(): url for name, url in MANUAL_LABEL_URLS.items()}
    names = df_merged_label["Name of medicine"].fillna("").str.strip().str.lower()

    is_manual = names.isin(urls_by_name)
    df_merged_label.loc[is_manual, "label_url"] = names[is_manual].map(urls_by_name)
    df_merged_label["label_source"] = is_manual.map({True: "manual", False: "automated"})

    logger.info("Filled in %s label URL(s) from MANUAL_LABEL_URLS", int(is_manual.sum()))
    return df_merged_label


def build_label_table(retrieved_at):
    """Keep the cancer medicines and attach their product-information labels.

    Returns the freshly built table. Nothing is written to disk here, because
    the label file is versioned against its own previous contents afterwards.
    """
    df_csv = pd.read_csv(EMA_TRANSFORMED_CSV_FILE)
    logger.info("The medicine data had %s rows", len(df_csv))
    # Keep only Human medicines.
    df_csv = df_csv[df_csv["Category"].str.lower() == "human"]
    # Keep only Authorised medicines.
    df_csv = df_csv[df_csv["Medicine status"].str.lower() == "authorised"]
    # Keep only rows whose text mentions one of the cancer terms (case-insensitive).
    pattern = "|".join(re.escape(alias) for alias in CANCER_ALIASES)

    df_csv = df_csv[df_csv["Therapeutic indication"].str.contains(pattern, case=False, na=False)]
    logger.info("%s rows left after filtering on Category, Medicine status and Therapeutic indication", len(df_csv))

    df_csv = df_csv[df_csv["Therapeutic area (MeSH)"].str.contains(pattern, case=False, na=False)]
    logger.info("%s rows left after filtering on Therapeutic area (MeSH)", len(df_csv))

    with open(EMA_JSON_FILE, encoding="utf-8") as f:
        json_data = json.load(f)

    df_json = pd.DataFrame(json_data.get("data", []))
    # Boolean indexing drops the unwanted rows outright, unlike .where which
    # would keep them as NaN.
    df_json = df_json[df_json["type"] == "product-information"]

    df_merged_label = pd.merge(
        df_csv,
        df_json,
        how="left",
        left_on=["Name of medicine", "EMA product number"],
        right_on=["medicine_name", "ema_product_number"],
    )
    df_merged_label.rename(columns={"document_url": "label_url"},inplace=True)
    df_merged_label = apply_manual_label_urls(df_merged_label)
    df_merged_label["document_hash"] = df_merged_label["label_url"].map(url_hash)
    df_merged_label[TIMESTAMP_COLUMN] = retrieved_at
    df_merged_label["source_reference"] = BASE_EMA_URL
    df_merged_label["data_source"] = "automated"
    logger.info("Built %s label rows from this run", len(df_merged_label))
    return df_merged_label


def version_label_file(df_new_labels, retrieved_at):
    """Compare the new label rows against the file from the previous run.

    The same file is both the previous version and the new output: it is read,
    compared, then rewritten with the versioning rules applied.
    """
    # Compare the values exactly as they appear in the CSV on both sides.
    new_rows = versioning.dataframe_to_rows(df_new_labels)
    previous_rows = versioning.read_csv_rows(EMA_TRANSFORMED_LABEL_FILE)
    logger.info("Read %s row(s) from the previous version of the label file", len(previous_rows))

    final_rows, report = versioning.apply_versioning(
        new_rows=new_rows,
        previous_rows=previous_rows,
        key_columns=VERSION_KEY_COLUMNS,
        now=retrieved_at,
        label_columns=LABEL_COLUMNS,
        hash_column=LABEL_HASH_COLUMN,
        timestamp_column=TIMESTAMP_COLUMN,
    )

    # Keep the column order of the new data, with is_label_latest at the end.
    column_order = list(df_new_labels.columns) + [versioning.LABEL_LATEST_COLUMN]
    versioning.write_rows_to_csv(final_rows, EMA_TRANSFORMED_LABEL_FILE, column_order)
    logger.info("Wrote %s rows to %s", len(final_rows), EMA_TRANSFORMED_LABEL_FILE.name)

    return report


def populate_drug_table():
    """Build the label table, version it against the previous run, report back.

    Returns a small result dict:

        changed        True when any row changed or was added, which is the
                       signal to run the PDF extraction step at all
        pdf_hashes     document_hashes whose label is new or changed, so only
                       those PDFs get re-read
        changed_count  how many rows changed
        new_count      how many rows are new
        report         the full versioning report, used to build the mail
        retrieved_at   this run's timestamp
    """
    retrieved_at = run_timestamp()  # one timestamp for every row in this run

    df_new_labels = build_label_table(retrieved_at)
    report = version_label_file(df_new_labels, retrieved_at)

    # The hashes needing a PDF re-read are collected straight from the report.
    pdf_hashes = {
        entry[LABEL_HASH_COLUMN]
        for entry in report["labels_to_extract"]
        if entry.get(LABEL_HASH_COLUMN)
    }

    counts = report["counts"]
    logger.info(
        "%s changed, %s new, %s label(s) need PDF extraction",
        counts["changed"], counts["new"], len(pdf_hashes),
    )

    return {
        "changed": bool(counts["changed"] or counts["new"]),
        "pdf_hashes": pdf_hashes,
        "changed_count": counts["changed"],
        "new_count": counts["new"],
        "report": report,
        "retrieved_at": retrieved_at,
    }


def run():
    """Run the whole transform step and return its result dict."""
    transform_excel_data_to_csv()
    return populate_drug_table()


# Only run when this file is executed directly, e.g. `python transform_ema_data.py`.
# Running this one step by hand still sends a summary mail, with the PDF part
# marked as skipped. A full run goes through main.py or the Glue job, which send
# one mail covering the PDF step too.
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    import notifications

    result = run()
    notifications.send_run_summary(result["report"], None, result["retrieved_at"])
