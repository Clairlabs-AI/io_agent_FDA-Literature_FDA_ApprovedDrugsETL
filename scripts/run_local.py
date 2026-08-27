"""Run one region's pipeline on this machine.

    python scripts/run_local.py eu             everything local, no S3
    python scripts/run_local.py us --s3        also read and write S3

Same steps and the same order as the Glue job, so what you test here is what
runs in AWS. Two differences: the code is imported from this folder rather than
from pipeline.zip, and S3 is off unless you ask for it.

S3 is off by default. With it off, every file is written under
./data/<layer>/<region>/ and nothing is fetched or sent. Pass --s3 to use
the real bucket; you need working AWS credentials for that.

The run stops at the first failure, with the full traceback in the log and a
non-zero exit code, so a scheduler can tell that something went wrong.
"""

import importlib
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config_handler import REGIONS                           # noqa: E402

logger = logging.getLogger("drug_label_etl")


def main(region, use_s3=False):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    # config.py reads REGION when it is imported, which happens as soon as
    # the runner below imports its first pipeline module.
    os.environ["REGION"] = region

    if not use_s3:
        # An empty DATA_BUCKET overrides deploy.env; the runners check for
        # it before downloading or uploading anything.
        os.environ["DATA_BUCKET"] = ""
        logger.info("S3 is off. Files stay under ./data/ - pass --s3 to change that.")
    runner = importlib.import_module("pipeline.%s.runner" % region)

    try:
        runner.run()
    except Exception:
        # logger.exception() logs the full traceback.
        logger.exception("The %s pipeline failed and stopped.", region.upper())
        return 1

    logger.info("The %s pipeline finished successfully.", region.upper())
    return 0


if __name__ == "__main__":
    arguments = [a for a in sys.argv[1:] if not a.startswith("-")]
    if len(arguments) != 1 or arguments[0].lower() not in REGIONS:
        print(__doc__)
        raise SystemExit(2)
    raise SystemExit(main(arguments[0].lower(), use_s3="--s3" in sys.argv))
