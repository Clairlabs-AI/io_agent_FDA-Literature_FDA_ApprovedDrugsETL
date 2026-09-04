"""Where every deployment value comes from, and in what order.

Precedence, highest first:

    1. a Glue job parameter          --DATA_BUCKET my-bucket
    2. an environment variable       DATA_BUCKET=my-bucket
    3. config_handler/deploy.env     DATA_BUCKET=my-bucket
    4. the default written in code

The Glue job promotes its parameters into the environment before anything here
is read, so a setting can be changed for one run without rebuilding pipeline.zip.

This module holds no pipeline values of its own - only the machinery for
reading them.
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent
PROJECT_DIR = CONFIG_DIR.parent

# Not committed to git: bucket, SNS topic, AWS region.
DEPLOY_ENV_FILE = CONFIG_DIR / "deploy.env"


def read_env_file(env_path):
    """Read a simple KEY=value file into a dict.

    Blank lines and lines starting with # are ignored, surrounding quotes are
    removed, and everything after the first = is the value - so ARNs and URLs
    survive intact.
    """
    values = {}
    env_path = Path(env_path)

    if not env_path.exists():
        logger.warning("No settings file at %s, using environment variables only.", env_path)
        return values

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")

    return values


_DEPLOY_SETTINGS = read_env_file(DEPLOY_ENV_FILE)


def setting(name, default=""):
    """Environment variable if set, else deploy.env, else the default.

    "Set" means present, not non-empty. Exporting DATA_BUCKET= with nothing
    after it means "no bucket" rather than falling through to deploy.env.
    """
    if name in os.environ:
        return os.environ[name]
    return _DEPLOY_SETTINGS.get(name, default)


def is_true(value):
    """Treat true/yes/1/on, in any capitalisation, as true."""
    return str(value).strip().lower() in ("true", "yes", "1", "on")


def number_setting(name, default):
    """A numeric setting, falling back to `default` when unset or not a number."""
    value = str(setting(name)).strip()
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning("%s is not a number (%r), using %s", name, value, default)
        return default
