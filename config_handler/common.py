"""Settings both regions share.

A region config starts with `from config_handler.common import *` and then adds
only what is its own - source URLs, section titles, versioning keys.
"""

import hashlib

import pandas as pd

from config_handler.settings import (          # noqa: F401  (re-exported)
    DEPLOY_ENV_FILE,
    is_true,
    number_setting,
    setting,
)

# --- AWS --------------------------------------------------------------------
DATA_BUCKET = setting("DATA_BUCKET")
AWS_REGION = setting("AWS_REGION")
GLUE_JOB = setting("GLUE_JOB")

# --- Notifications ----------------------------------------------------------
# Leave SNS_ENABLED false to have every message written to the log instead of
# published, which is what a laptop without AWS wants.
SNS_ENABLED = is_true(setting("SNS_ENABLED"))
SNS_TOPIC_ARN = setting("SNS_TOPIC_ARN")

# How many rows to list per section in a mail. Counts are always complete.
SNS_MAX_ROWS_PER_SECTION = int(number_setting("SNS_MAX_ROWS_PER_SECTION", 20))

# --- PDF extraction ---------------------------------------------------------
# When true, read every label PDF again instead of trusting the cache.
# Turn this on for one run after changing how sections are parsed, then turn
# it back off. New or removed sections are picked up automatically either way.
FORCE_PDF_REEXTRACT = is_true(setting("FORCE_PDF_REEXTRACT"))

# --- Downloading ------------------------------------------------------------
# Controls how fast and how hard the pipeline hits the source sites. Every
# value can be overridden from deploy.env or a job parameter.

# Seconds to wait for one response before giving up on it.
HTTP_TIMEOUT_SECONDS = number_setting("HTTP_TIMEOUT_SECONDS", 90)

# How many times to try one URL before treating it as failed.
HTTP_MAX_ATTEMPTS = int(number_setting("HTTP_MAX_ATTEMPTS", 5))

# First wait after a failure; it doubles each attempt (3s, 6s, 12s, 24s...).
HTTP_BACKOFF_SECONDS = number_setting("HTTP_BACKOFF_SECONDS", 3)

# Never wait longer than this between attempts, even if the server asks.
HTTP_MAX_RETRY_WAIT_SECONDS = number_setting("HTTP_MAX_RETRY_WAIT_SECONDS", 120)

# Minimum gap between the START of one request and the next, across ALL threads.
# This, not the worker count, is what caps the request rate: 0.5 means at most
# two requests a second however many workers are running.
HTTP_MIN_INTERVAL_SECONDS = number_setting("HTTP_MIN_INTERVAL_SECONDS", 0.5)

# Sent with every download so the source answers us like a browser.
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/125.0.0.0 Safari/537.36"),
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}

# --- Small shared helpers ---------------------------------------------------
NULL = "null"


def run_timestamp():
    """Current time as an ISO string like '2026-08-26T18:30:00+05:30'.

    Call it once per run and reuse the result, so every row carries the same
    retrieved_at.
    """
    from datetime import datetime
    return datetime.now().astimezone().isoformat(timespec="seconds")


def url_hash(url):
    """SHA-256 of a label URL. A blank or null URL stays null so it never collides."""
    if url is None or pd.isna(url) or str(url).strip() == "":
        return None
    return hashlib.sha256(str(url).strip().encode("utf-8")).hexdigest()
