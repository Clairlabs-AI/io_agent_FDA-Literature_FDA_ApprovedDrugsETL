"""The settings for this run.

Every pipeline module starts with `from config_handler import config`.

Which region you get depends on one environment variable:

    REGION=eu   ->  config_handler/eu.py
    REGION=us   ->  config_handler/us.py

The Glue job sets it from its --REGION job parameter; scripts/run_local.py sets
it from the command line. Nothing else in the project decides the region.

That is the whole mechanism. There is no registry, no loader and no magic: this
is an ordinary module you can open, and your IDE can follow every name in it
straight to the region file it came from.
"""

import os

REGION = os.environ.get("REGION", "").strip().lower()

if REGION == "eu":
    from config_handler.eu import *          # noqa: F401,F403
elif REGION == "us":
    from config_handler.us import *          # noqa: F401,F403
else:
    raise RuntimeError(
        "REGION is %r. Set it to 'eu' or 'us' before importing config.\n"
        "  Glue:  pass the --REGION job parameter\n"
        "  local: python scripts/run_local.py eu" % (REGION or "not set",))
