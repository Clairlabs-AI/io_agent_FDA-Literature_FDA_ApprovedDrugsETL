"""Every HTTP GET the pipeline makes goes through here.

Both the source-file downloads and the label PDFs use this module, so they
share the same browser-like headers, one connection pool, a minimum gap
between requests, and retries that respect the server's Retry-After header.

The minimum gap is global across all threads, so it is what actually caps the
request rate, not the number of workers.
"""

import logging
import random
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from config_handler.config import (
    HEADERS,
    HTTP_BACKOFF_SECONDS,
    HTTP_MAX_ATTEMPTS,
    HTTP_MAX_RETRY_WAIT_SECONDS,
    HTTP_MIN_INTERVAL_SECONDS,
    HTTP_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)

# Status codes worth trying again: rate limiting and the server-side ones that
# are usually temporary. Anything else (404, 403) will not change on a retry.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

_pace_lock = threading.Lock()
_next_allowed_at = 0.0

_session_lock = threading.Lock()
_session = None


def session():
    """One shared requests.Session, created on first use.

    Reusing it keeps connections alive between files, which is both faster and
    gentler on the server than a fresh connection per request.
    """
    global _session
    with _session_lock:
        if _session is None:
            _session = requests.Session()
        return _session


def wait_for_turn(min_interval):
    """Hold the caller until at least `min_interval` has passed since the last
    request started. Shared by every thread, so parallel workers queue up
    instead of firing at once."""
    global _next_allowed_at

    if min_interval <= 0:
        return

    with _pace_lock:
        now = time.monotonic()
        wait = max(0.0, _next_allowed_at - now)
        _next_allowed_at = max(now, _next_allowed_at) + min_interval

    if wait > 0:
        time.sleep(wait)


def retry_after_seconds(response):
    """How long the server asked us to wait, or None if it did not say.

    Retry-After can be a number of seconds or an HTTP date; both are handled.
    """
    header = (response.headers.get("Retry-After") or "").strip()
    if not header:
        return None

    try:
        return max(0.0, float(header))
    except ValueError:
        pass

    try:
        when = parsedate_to_datetime(header)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return None


def backoff_seconds(attempt, backoff, maximum):
    """Wait longer after each failure, with a little randomness.

    The randomness matters with several workers: without it they all fail at the
    same moment, sleep the same time and hit the server together again.
    """
    delay = backoff * (2 ** (attempt - 1))
    return min(maximum, delay + random.uniform(0, backoff))


def get(url,
        headers=None,
        timeout=HTTP_TIMEOUT_SECONDS,
        attempts=HTTP_MAX_ATTEMPTS,
        min_interval=HTTP_MIN_INTERVAL_SECONDS,
        backoff=HTTP_BACKOFF_SECONDS,
        max_wait=HTTP_MAX_RETRY_WAIT_SECONDS):
    """GET a URL, pacing and retrying. Returns the response, or raises.

    Retries a 429 or a 5xx, and any connection error. Does not retry a 404 or a
    403, which would only fail the same way again. When every attempt is used
    up, the last error is raised for the caller to handle.
    """
    last_error = None

    for attempt in range(1, attempts + 1):
        wait_for_turn(min_interval)

        try:
            response = session().get(url, headers=headers or HEADERS, timeout=timeout)

            if response.status_code not in RETRY_STATUS_CODES:
                response.raise_for_status()          # raises for 404, 403, ...
                return response

            last_error = requests.HTTPError(
                "{} {} for url: {}".format(response.status_code, response.reason, url))

            # Prefer the server's own instruction over our guess.
            delay = retry_after_seconds(response)
            if delay is None:
                delay = backoff_seconds(attempt, backoff, max_wait)
            else:
                delay = min(delay, max_wait)

        except requests.HTTPError:
            raise                                     # not worth retrying
        except Exception as error:
            last_error = error
            delay = backoff_seconds(attempt, backoff, max_wait)

        if attempt < attempts:
            logger.warning("%s (attempt %s/%s), waiting %.1fs: %s",
                           url, attempt, attempts, delay, last_error)
            time.sleep(delay)

    raise last_error
