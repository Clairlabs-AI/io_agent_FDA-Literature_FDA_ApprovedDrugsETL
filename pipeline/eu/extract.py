"""Step 1 of the pipeline: download the raw Excel and JSON files from EMA.

If a download fails, an alert is sent and the error is raised so the pipeline
stops instead of continuing with stale data.

This step sends its own alert rather than waiting for the end-of-run summary.
"""

import logging
import traceback

from pipeline.common import http_fetch
from pipeline.eu import notifications
from config_handler.config import (
    EMA_EXCEL_FILE,
    EMA_EXCEL_URL,
    EMA_JSON_FILE,
    EMA_JSON_URL,
    SNS_DOWNLOAD_ERROR_SUBJECT,
)

logger = logging.getLogger(__name__)


def notify_download_failure(url, destination, error):
    """Send one alert describing a failed download.

    Must be called from inside an `except` block, so traceback.format_exc() can
    capture the error. Any problem sending the alert is only logged, never
    raised, so it cannot hide the original error.
    """
    message = "\n".join([
        "A download in the EMA pipeline failed.",
        "",
        "File to download : {}".format(url),
        "Saving to        : {}".format(destination),
        "Error type       : {}".format(type(error).__name__),
        "Error            : {}".format(error),
        "",
        "The pipeline stopped at this step, so the output files still hold the",
        "data from the previous successful run.",
        "",
        "Full details:",
        traceback.format_exc(),
    ])

    try:
        notifications.send_alert(SNS_DOWNLOAD_ERROR_SUBJECT, message)
    except Exception:
        logger.exception("Could not send the download failure notification.")


def download_file(url, destination):
    """Download `url` and save it to `destination`.

    The file is written to a temporary '.part' file first and renamed only
    after the download succeeds, so a half-written file is never left behind.

    The request goes through http_fetch, so it carries the browser-like
    headers and is retried automatically.

    Once every attempt is used up, an alert is sent and the error is raised.
    """
    logger.info("Downloading %s", url)

    temporary_file = destination.with_suffix(destination.suffix + ".part")

    try:
        response = http_fetch.get(url)

        temporary_file.write_bytes(response.content)
        temporary_file.replace(destination)

    except Exception as error:
        logger.error("Download failed for %s : %s", url, error)

        # Do not leave a half-written .part file behind.
        if temporary_file.exists():
            temporary_file.unlink()

        notify_download_failure(url, destination, error)
        raise

    size_in_mb = destination.stat().st_size / (1024 * 1024)
    logger.info("Saved %s (%.1f MB)", destination.name, size_in_mb)


def extract_excel_data():
    """Download the EMA medicines report (Excel)."""
    download_file(EMA_EXCEL_URL, EMA_EXCEL_FILE)


def extract_json_data():
    """Download the EMA documents report (JSON)."""
    download_file(EMA_JSON_URL, EMA_JSON_FILE)


def run():
    """Run the whole extract step."""
    extract_excel_data()
    extract_json_data()


# Only run when this file is executed directly, e.g. `python extract_ema_data.py`.
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    run()
