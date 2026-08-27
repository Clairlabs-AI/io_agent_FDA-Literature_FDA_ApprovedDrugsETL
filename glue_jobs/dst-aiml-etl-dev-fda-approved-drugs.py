"""The one Glue job. Which pipeline it runs is a job parameter.

    --REGION eu        run the EMA pipeline
    --REGION us        run the FDA pipeline
    --DATA_BUCKET ...  required: where pipeline.zip and the data live

Schedule it twice - one trigger per region, same job, different --REGION - and
both pipelines stay on one piece of deployed code.

Any other job parameter overrides the matching entry in
config_handler/deploy.env for that run only. For example, pass
--FORCE_PDF_REEXTRACT true to turn that setting on for a single run.

Glue Python Shell job. It downloads its own code from
s3://<bucket>/code/pipeline.zip, and installs the packages Glue does not
ship, falling back to prebuilt wheels in
s3://<bucket>/code/common_packages/wheels/ when PyPI is not reachable.
"""

import logging
import os
import subprocess
import sys
import zipfile

# Bump this by hand whenever this file changes. It shows in the log
# which version of the job script is running.
JOB_SCRIPT_VERSION = "2026-08-26c"

CODE_KEY = "code/pipeline.zip"
WHEELS_PREFIX = "code/common_packages/wheels/"
CODE_DIR = "/tmp/pipeline_code"

# Packages each region needs installed before its PDF step runs.
# Module name -> what to pip install. Versions are left unpinned here
# because pymupdf 1.27 and later need Python 3.10, which this Glue Python
# Shell job does not have. The installed version is printed at startup.
PACKAGES = {
    "eu": {"openpyxl": "openpyxl", "pymupdf": "pymupdf"},
    "us": {"curl_cffi": "curl_cffi", "lxml": "lxml", "pymupdf": "pymupdf"},
}


# --- bootstrap ---------------------------------------------------------------
# Runs before config_handler or pipeline are imported: it downloads the
# code and adds it to sys.path.
def job_arguments_to_environment():
    """Copy every --KEY value job parameter into an environment variable.

    Glue passes job parameters on the command line, not as environment
    variables, but settings.setting() reads os.environ. This makes job
    parameters visible to that code.
    """
    promoted = {}
    index = 1                                    # skip the script name
    while index < len(sys.argv):
        argument = sys.argv[index]
        has_value = (index + 1 < len(sys.argv)
                     and not sys.argv[index + 1].startswith("--"))
        if argument.startswith("--") and has_value:
            promoted[argument[2:]] = sys.argv[index + 1]
            index += 2
        else:
            index += 1                           # a flag with no value

    os.environ.update(promoted)
    if promoted:
        print("[config] job parameters applied: %s" % sorted(promoted))
    return promoted


def bootstrap_code():
    """Download pipeline.zip from S3 and put it on sys.path."""
    job_arguments_to_environment()

    bucket = os.environ.get("DATA_BUCKET")
    if not bucket:
        raise RuntimeError("Set the --DATA_BUCKET job parameter "
                           "(needed to download pipeline.zip).")

    # Glue can only write to /tmp, so that is where the data files go too.
    os.environ.setdefault("WORKDIR", "/tmp/work")

    if CODE_DIR not in sys.path:
        import boto3
        os.makedirs(CODE_DIR, exist_ok=True)
        boto3.client("s3").download_file(bucket, CODE_KEY, "/tmp/pipeline.zip")
        with zipfile.ZipFile("/tmp/pipeline.zip") as archive:
            archive.extractall(CODE_DIR)
        sys.path.insert(0, CODE_DIR)


bootstrap_code()


def print_build_stamp():
    """Print which build of pipeline.zip this run loaded.

    build_info.py is written into the zip by scripts/build_pipeline_zip.py.
    Compare the hash printed here with the hash printed when the zip was
    built to confirm the job ran the latest upload.
    """
    try:
        import build_info
    except ImportError:
        print("[build] pipeline.zip has no build stamp - it was built before "
              "the stamp existed, so this is an OLD zip.")
        return
    print("[build] pipeline.zip built %s  hash %s  (%d files)"
          % (build_info.BUILT_AT, build_info.CONTENT_HASH, build_info.FILE_COUNT))


print_build_stamp()


# --- dependencies ------------------------------------------------------------
def pip_install(*arguments):
    subprocess.check_call([sys.executable, "-m", "pip", "install",
                           "--disable-pip-version-check", *arguments])


def installed_version(module):
    """The version of `module` as installed, or "" when it is not importable."""
    try:
        loaded = __import__(module)
    except Exception:
        return ""
    version = getattr(loaded, "__version__", "")
    if not version:
        version = getattr(loaded, "VersionBind", "")     # older pymupdf
    if not version:
        raw = getattr(loaded, "version", "")
        if isinstance(raw, (tuple, list)) and raw:
            version = raw[0]
    return str(version).strip() or "unknown"


def wanted_version(requirement):
    """'pymupdf==1.28.2' -> '1.28.2'. An unpinned requirement gives ""."""
    if "==" in requirement:
        return requirement.split("==", 1)[1].strip()
    return ""


def install_from_s3_wheel(requirement, bucket):
    """Install from a prebuilt wheel in code/common_packages/wheels/.

    When the requirement is pinned, only a wheel matching that exact
    version is accepted.
    """
    import boto3
    client = boto3.client("s3")

    name = requirement.split("==", 1)[0].strip()
    version = wanted_version(requirement)

    local_dir = os.path.join(os.environ.get("WORKDIR", "/tmp/work"), "wheels")
    os.makedirs(local_dir, exist_ok=True)

    picked = []
    prefix = name.lower().replace("-", "_")
    listing = client.list_objects_v2(Bucket=bucket, Prefix=WHEELS_PREFIX)
    for entry in listing.get("Contents", []):
        base = os.path.basename(entry["Key"]).lower()
        if not base.endswith(".whl"):
            continue
        if not (base.startswith(prefix) or base.startswith(name.lower())):
            continue
        if version and version not in base:
            print("[deps] skipping %s, it is not version %s" % (base, version))
            continue
        destination = os.path.join(local_dir, os.path.basename(entry["Key"]))
        client.download_file(bucket, entry["Key"], destination)
        picked.append(destination)

    if not picked:
        raise RuntimeError("no wheel for %r in s3://%s/%s"
                           % (requirement, bucket, WHEELS_PREFIX))
    pip_install("--no-index", "--no-deps", *picked)


def ensure(module, requirement, bucket):
    """Make `module` importable at the pinned version: try it, then PyPI, then S3.

    Checking the installed version matters because Glue may already carry
    an older copy of the package.
    """
    version = wanted_version(requirement)
    present = installed_version(module)

    if present and (not version or present == version):
        print("[deps] %s %s already present" % (module, present))
        return
    if present:
        print("[deps] %s %s present but %s wanted; replacing it"
              % (module, present, version))

    try:
        pip_install(requirement)
    except Exception as error:
        print("[deps] PyPI install of %s failed (%s); trying the S3 wheel"
              % (requirement, error))
        install_from_s3_wheel(requirement, bucket)

    print("[deps] %s -> %s" % (module, installed_version(module) or "STILL MISSING"))


# --- the job -----------------------------------------------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    print("[build] job script %s  python %s"
          % (JOB_SCRIPT_VERSION, sys.version.split()[0]))

    region = os.environ.get("REGION", "").strip().lower()
    if not region:
        raise RuntimeError("Set the --REGION job parameter to eu or us.")

    # config.py reads REGION from the environment. job_arguments_to_environment()
    # has already set it from --REGION, so importing config here selects the region.
    os.environ["REGION"] = region
    from config_handler import config as settings

    for module, requirement in sorted(PACKAGES.get(region, {}).items()):
        ensure(module, requirement, settings.DATA_BUCKET)

    print("[pipeline] region=%s bucket=%s workdir=%s"
          % (region, settings.DATA_BUCKET, os.environ.get("WORKDIR")))
    print("[config] deploy.env found=%s sns_enabled=%s topic=%s force_pdf_reextract=%s"
          % (settings.DEPLOY_ENV_FILE.exists(),
             settings.SNS_ENABLED,
             "set" if settings.SNS_TOPIC_ARN else "EMPTY",
             settings.FORCE_PDF_REEXTRACT))

    import importlib
    runner = importlib.import_module("pipeline.%s.runner" % region)
    runner.run()

    print("[pipeline] %s finished; outputs under "
          "s3://%s/output_data/{bronze,silver,gold}/region=%s/"
          % (region.upper(), settings.DATA_BUCKET, region))


if __name__ == "__main__":
    main()
