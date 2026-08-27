"""Run the US pipeline end to end.

The Glue job calls run() and nothing else. Everything region-specific comes
from config. The FDA data needs two extract steps: a page scrape and an
openFDA API sweep.
"""

import logging

from config_handler import config

from pipeline.common import s3_io
from pipeline.us import extract_api, extract_table, load, notifications, transform

logger = logging.getLogger(__name__)


def upload(files):
    """Send each (layer, path) pair to its place in S3."""
    if not config.DATA_BUCKET:
        logger.info("No DATA_BUCKET set, so nothing was uploaded.")
        return

    from config_handler import paths
    for layer, path in files:
        s3_io.upload(path, config.DATA_BUCKET,
                     paths.data_key(layer, path.name, config.REGION))


def fetch_previous_state():
    """Bring last run's silver files down, so versioning has something to compare.

    Missing objects are not an error - the first run has none.

    A file that S3 no longer has is deleted locally, so a stale local copy is
    never reused by mistake.
    """
    if not config.DATA_BUCKET:
        return

    from config_handler import paths
    for path in (config.DRUG_TABLE_FILE, config.SECTIONS_FILE):
        key = paths.data_key(paths.SILVER, path.name, config.REGION)
        downloaded = s3_io.download_if_exists(config.DATA_BUCKET, key, path)
        if not downloaded and path.exists():
            path.unlink()
            logger.info("Deleted the leftover %s: S3 does not have it any more.",
                        path.name)


def run():
    """Scrape, query openFDA, transform, read the labels, and send one summary mail."""
    fetch_previous_state()

    # Step 1 - bronze. The approvals page gives us the drug names.
    name_map = extract_table.main()
    logger.info("Scraped %s drugs from the FDA approvals page.", len(name_map))

    # Step 2 - bronze. openFDA fills in application, sponsor and label details.
    # A failed batch is collected instead of raised, reported in the mail, and
    # retried next run.
    extract_api.main(config.OPENFDA_FILE, name_map)
    api_result = extract_api.api_result()
    logger.info("openFDA done: %s batches, %s failed",
                api_result["batches"], len(api_result["failures"]))

    # Step 3 - silver.
    result = transform.main()
    logger.info("Drug table built: %s updated, %s new",
                result["changed_count"], result["new_count"])
    upload(config.UPLOADS_AFTER_TRANSFORM)

    # Step 4 - silver + gold.
    pending = load.pending_hashes()
    if pending:
        logger.info("%s label(s) have no sections yet.", len(pending))

    pdf_result = None
    if result["changed"] or pending:
        pdf_result = load.main(only_hashes=result["pdf_hashes"])
        upload(config.UPLOADS_AFTER_PDF)
        logger.info("Sections and merged dataset updated: %s extracted, %s failed, %s still missing",
                    pdf_result["extracted"], len(pdf_result["warnings"]),
                    pdf_result["still_missing"])
    else:
        logger.info("Nothing changed and nothing pending; skipped the PDF step.")

    notifications.send_run_summary(
        result["changes"], pdf_result, api_result, result["retrieved_at"],
        blank_labels=load.labels_with_no_sections())

    return {"result": result, "pdf_result": pdf_result, "api_result": api_result}
