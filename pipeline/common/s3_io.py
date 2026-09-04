"""Small S3 download/upload helpers used by the Glue job.

Same shape as the US project's s3_io.py so both pipelines read alike. boto3 is
imported lazily, so importing this module on a laptop without AWS installed
does not fail.
"""

import logging
import os

logger = logging.getLogger(__name__)

_client = None


def client():
    """Return a shared S3 client, created on first use."""
    global _client
    if _client is None:
        import boto3
        _client = boto3.client("s3")
    return _client


def download(bucket, key, local_path):
    """Download s3://bucket/key to local_path (creating parent folders)."""
    local_path = str(local_path)
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    client().download_file(bucket, key, local_path)
    logger.info("Downloaded s3://%s/%s", bucket, key)
    return local_path


def download_if_exists(bucket, key, local_path):
    """Like download(), but returns None instead of raising when the key is not
    there. Used for the previous run's outputs, which do not exist on the very
    first run."""
    try:
        return download(bucket, key, local_path)
    except Exception as error:
        logger.info("Nothing to download at s3://%s/%s (%s)", bucket, key, error)
        return None


def upload(local_path, bucket, key):
    """Upload local_path to s3://bucket/key. Missing files are skipped."""
    local_path = str(local_path)

    if not os.path.exists(local_path):
        logger.warning("Not uploading %s - the file was not produced.", local_path)
        return None

    client().upload_file(local_path, bucket, key)
    logger.info("Uploaded %s -> s3://%s/%s", os.path.basename(local_path), bucket, key)
    return "s3://{}/{}".format(bucket, key)
