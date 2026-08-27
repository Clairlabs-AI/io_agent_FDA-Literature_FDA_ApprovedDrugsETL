"""Run the EU pipeline end to end.

The Glue job calls run() and nothing else. Everything region-specific - which
files, which URLs, which columns - comes from config, so this file reads the
same as the US runner beside it.
"""

import logging

from config_handler import config

from pipeline.common import s3_io
from pipeline.eu import extract, load, notifications, transform

logger = logging.getLogger(__name__)


def upload(files):
    """Send each (layer, path) pair to its place in S3.

    Silently does nothing when no bucket is configured, which is what running on
    a laptop wants.
    """
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

    A file that S3 no longer has is deleted locally too, since Glue can reuse
    /tmp between runs and an old file left behind would be read as if it were
    still current.
    """
    if not config.DATA_BUCKET:
        return

    from config_handler import paths
    for path in (config.EMA_TRANSFORMED_LABEL_FILE, config.SECTIONS_FILE):
        key = paths.data_key(paths.SILVER, path.name, config.REGION)
        downloaded = s3_io.download_if_exists(config.DATA_BUCKET, key, path)
        if not downloaded and path.exists():
            path.unlink()
            logger.info("Deleted the leftover %s: S3 does not have it any more.",
                        path.name)


def run():
    """Download, transform, read the labels, and send one summary mail."""
    fetch_previous_state()

    # Step 1 - bronze. A download failure sends its own alert and stops the run,
    # because none of the later steps can do anything without the source files.
    extract.run()
    logger.info("Downloads done.")

    # Step 2 - silver. Returns which label hashes need PDF extraction, in memory.
    result = transform.run()
    logger.info("Label index built: %s updated, %s new, %s label(s) need PDFs",
                result["changed_count"], result["new_count"], len(result["pdf_hashes"]))
    upload(config.UPLOADS_AFTER_TRANSFORM)

    # Step 3 - silver + gold. Runs when something changed OR when anything is
    # still pending, so a PDF that failed on an earlier run is picked up even in
    # a run where nothing changed.
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

    # One mail for the whole run, including every label that came back empty.
    notifications.send_run_summary(
        result["report"], pdf_result, result["retrieved_at"],
        blank_labels=load.labels_with_no_sections())

    return {"result": result, "pdf_result": pdf_result}
