"""Where every file lives, locally and in S3.

One medallion layout, region-partitioned:

    bronze   exactly what the source gave us - the EMA workbook, the FDA page,
             the openFDA responses. Never edited.
    silver   parsed and versioned: the label table with is_label_latest, and the
             per-PDF section text.
    gold     the joined, business-ready dataset that downstream consumes.

    s3://<bucket>/output_data/<layer>/region=<eu|us>/<filename>
    s3://<bucket>/code/pipeline.zip
    s3://<bucket>/code/common_packages/wheels/<wheel>

Data lives under output_data/, code lives under code/, so the two never mix.
Each layer/region folder holds the current file, overwritten in place; row
history is tracked inside the silver label table, not in the file path.
"""

from pathlib import Path

from config_handler.settings import setting

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
LAYERS = (BRONZE, SILVER, GOLD)

# Everything the job writes lives under this one prefix.
DATA_PREFIX = setting("DATA_PREFIX", "output_data")

# Everything the job is run BY lives under this one.
CODE_PREFIX = setting("CODE_PREFIX", "code")
WHEELS_PREFIX = setting("WHEELS_PREFIX", CODE_PREFIX + "/common_packages/wheels")

# Where files are read and written while the job runs. Glue can only write to
# /tmp, so the job sets WORKDIR; locally it defaults to ./data next to the repo.
WORK_DIR = Path(setting("WORKDIR") or (Path(__file__).resolve().parent.parent / "data"))


def local(layer, filename, region):
    """The path a file has on disk while the job is running.

    The layer/region folders are created on demand, so a fresh /tmp in Glue
    needs no setup step.
    """
    if layer not in LAYERS:
        raise ValueError("unknown layer %r, expected one of %s" % (layer, ", ".join(LAYERS)))

    folder = WORK_DIR / layer / region
    folder.mkdir(parents=True, exist_ok=True)
    return folder / filename


def data_key(layer, filename, region):
    """The S3 key for one data file.

    e.g. 'output_data/gold/region=eu/EU_FDA_oncology_drug_label_dataset.csv'
    """
    if layer not in LAYERS:
        raise ValueError("unknown layer %r, expected one of %s" % (layer, ", ".join(LAYERS)))
    return "{}/{}/region={}/{}".format(DATA_PREFIX, layer, region, filename)


def code_key(filename):
    """The S3 key for one code file, e.g. 'code/pipeline.zip'."""
    return "{}/{}".format(CODE_PREFIX, filename)


def wheel_key(filename):
    """The S3 key for one prebuilt wheel."""
    return "{}/{}".format(WHEELS_PREFIX, filename)
