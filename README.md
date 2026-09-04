# drug_label_etl

One codebase, one Glue job, two regions. `--REGION eu` runs the EMA pipeline;
`--REGION us` runs the FDA one. Nothing else about the deployment changes.

```
drug_label_etl/
  config_handler/          every setting, one file per region
    config.py              picks the region from the REGION variable
    settings.py            where values come from and in what order
    paths.py               the bronze / silver / gold layout, local and in S3
    common.py              settings both regions share
    eu.py  us.py           settings that are genuinely one region's own
    deploy.env             bucket, SNS topic, AWS region  (not committed)

  pipeline/
    common/                plumbing that knows nothing about EMA or the FDA
      http_fetch.py  s3_io.py
    eu/                    extract -> transform -> load, plus runner.py
    us/                    extract -> transform -> load, plus runner.py

  glue_jobs/dst-aiml-etl-dev-fda-approved-drugs.py       the one job
  common_packages/wheels/         prebuilt wheels for Glue  (not committed)
  scripts/
    run_local.py                  run one region on this machine
    build_pipeline_zip.py         build and upload pipeline.zip
    blank_sections_report.py      which labels came out empty, from the gold CSV
```

## How the region gets chosen

One environment variable, `REGION`, and one ordinary file,
`config_handler/config.py`:

```python
REGION = os.environ.get("REGION", "").strip().lower()

if REGION == "eu":
    from config_handler.eu import *
elif REGION == "us":
    from config_handler.us import *
else:
    raise RuntimeError("REGION is not set ...")
```

The Glue job sets `REGION` from its `--REGION` job parameter;
`scripts/run_local.py` sets it from the command line. Every pipeline module then
does `from config_handler import config` and never learns which region it is
running for.

Unset `REGION` raises immediately, so a mistake is a loud failure at startup,
never a run that quietly used the wrong region.

## What gets uploaded to AWS

Three things, and only two of them are code.

| what | where | how |
|---|---|---|
| `glue_jobs/dst-aiml-etl-dev-fda-approved-drugs.py` | the Glue job's **Script path** | Upload it as the Python Shell job's script. It is deliberately NOT in the zip - it is what downloads the zip. |
| `config_handler/`, `pipeline/` | `s3://<bucket>/code/pipeline.zip` | `python scripts/build_pipeline_zip.py --upload` |
| the wheels | `s3://<bucket>/code/common_packages/wheels/` | `aws s3 sync common_packages/wheels s3://<bucket>/code/common_packages/wheels/` |

**Yes, config goes to AWS** - everything in `config_handler/`, `deploy.env`
included, is inside `pipeline.zip`. There is nothing to upload
separately. The job extracts the zip to `/tmp/pipeline_code` and puts it on
`sys.path`, so `from config_handler import config` finds it exactly as it does on your
machine.

`data/`, `dist/` and `scripts/` never leave your machine.

## Adding a region

Three steps, no change to the Glue job:

1. `config_handler/<region>.py` — its URLs, file names, section titles and
   versioning keys. Start from `eu.py`.
2. `pipeline/<region>/` — `extract.py`, `transform.py`, `load.py`, `runner.py`.
   `runner.run()` is the only thing the job calls.
3. `config_handler/config.py` — one more `elif`, and add the name to `REGIONS`
   in `config_handler/__init__.py`.

Then a new schedule with `--REGION <region>`.

## Where the data goes

```
s3://<bucket>/
  output_data/
    bronze/region=eu/   ema_medicines_report.xlsx   ema_documents_report.json
    bronze/region=us/   Table_DrugLabeling_FDA.html oncology_openfda_results.json
    silver/region=eu/   ema_medicines_table.csv  ema_label_index.csv  ema_label_sections.csv
    silver/region=us/   oncology_drug_biomarkers.csv  oncology_drug_table.csv  fda_label_sections.csv
    gold/region=eu/     eu_drug_labels.csv  eu_drug_labels.json
    gold/region=us/     us_drug_labels.csv  us_drug_labels.json
  code/
    pipeline.zip
    common_packages/wheels/*.whl
```

Two prefixes, and the split is the point: everything the job **writes** is under
`output_data/`, everything it is **run by** is under `code/`. One lifecycle rule
or bucket policy can cover all the data without touching the deployment.

- **bronze** — exactly what the source served, never edited.
- **silver** — parsed, filtered, and versioned. The label index carries
  `is_label_latest`, so history lives in the rows.
- **gold** — the joined dataset downstream reads.

Inside `output_data/` the layer comes first and the region second, so a
lifecycle rule can age bronze into cheap storage while gold stays hot, and
`region=` is an Athena partition for free.

**No date partition**, by design: each layer holds the current file and is
overwritten in place. History is the versioned rows in the silver label index,
not a folder per day.

## Settings precedence

```
Glue job parameter   -->   environment variable   -->   deploy.env   -->   default in code
```

The job promotes every `--KEY value` into the environment before any settings
are read, so `--FORCE_PDF_REEXTRACT true` turns a flag on for one run without
rebuilding the zip.

## Running it

Locally:

```bash
python scripts/run_local.py eu            # everything local, no S3
python scripts/run_local.py us --s3       # also read and write S3
```

**S3 is off by default.** `deploy.env` names a real bucket, so without that
default the first upload would kill a local run several minutes in, with no AWS
credentials to hand. With S3 off, every file is written under
`./data/<layer>/<region>/` and nothing is fetched or sent.

Pass `--s3` when you do want the real thing - you need working credentials and
`boto3` installed. The Glue job is unaffected either way: it always has a
bucket, from its `--DATA_BUCKET` job parameter.

Deploying:

```bash
python scripts/build_pipeline_zip.py --upload      # -> s3://<bucket>/code/pipeline.zip
aws s3 sync common_packages/wheels s3://<bucket>/code/common_packages/wheels/
```

Then one Glue Python Shell job whose script is `glue_jobs/dst-aiml-etl-dev-fda-approved-drugs.py`, with
default parameters `--DATA_BUCKET <bucket>`, and two schedules pointing at it:

| trigger | parameters |
|---|---|
| `drug-labels-eu-daily` | `--REGION eu` |
| `drug-labels-us-daily` | `--REGION us` |

The job installs what Glue does not ship (`pymupdf`, plus `openpyxl` for EU and
`curl_cffi`/`lxml` for US), trying PyPI first and falling back to the wheels in
`code/common_packages/wheels/`.

## Checking a run

```bash
python scripts/blank_sections_report.py eu
python scripts/blank_sections_report.py us
```

Writes `<region>_blank_sections_report.xlsx` beside the gold CSV: one sheet per
label with an empty section, one counting each section, and one listing the
cells too long for Excel to show.

It reads the **gold CSV**, never a spreadsheet export. Excel truncates any cell
over 32,767 characters without saying so - one US export had 77 cells cut to
32,765 - and a report built from such a file measures the spreadsheet rather
than the pipeline.

## What is not shared yet

`versioning.py` and `notifications.py` still live per region. That is deliberate
for this first cut, not an oversight:

- The two **versioning** modules are different implementations of the same
  rules — EU works on lists of CSV-string rows (15 functions), US on pandas
  DataFrames (4). The EU one already takes its keys as arguments and hardcodes
  nothing about EMA, so it is the one to keep; moving US onto it is a behaviour
  change that deserves its own validation run rather than being folded into a
  restructure.
- The two **notification** modules have the same shape and differ only in which
  sections the mail carries. Unifying them means a `SUMMARY_SECTIONS` list in
  each region config — small, and worth doing straight after the versioning
  merge.

Both now sit side by side under `pipeline/`, which is what makes those two
merges easy. Until then, a fix to either still has to be applied twice.

## Ideas worth considering

- **Keep the label PDFs in bronze.** Every PDF is currently downloaded, parsed
  and thrown away. Storing them under `bronze/region=<r>/labels/<hash>.pdf`
  makes `FORCE_PDF_REEXTRACT` cost nothing instead of re-downloading hundreds of
  files, and makes any extraction bug reproducible against the exact file.
- **Write gold as Parquet.** The EU gold CSV is 44 MB for 277 rows, with 365
  cells over Excel's 32,767-character limit — it cannot be opened in a
  spreadsheet at all. Parquet fixes size, types and Athena queries at once; keep
  a CSV export for anyone who wants one.
- **Flag scanned pages.** One FDA label has image-only pages exactly where its
  Clinical Studies section sits. In the output that is indistinguishable from a
  parsing bug. A "no text layer on N pages" warning in the run summary would
  separate the two.

## The Glue run still returns old data

Every build now stamps `build_info.py` into `pipeline.zip`, and the job prints
it at startup, so the log says which build actually ran:

    [build] job script 2026-08-26
    [build] pipeline.zip built 2026-08-26 12:52:31 UTC  hash 6831b571eca9  (27 files)
    [config] job parameters applied: ['DATA_BUCKET', 'REGION']
    [pipeline] region=us bucket=... workdir=/tmp/work

Compare that hash with the one `scripts/build_pipeline_zip.py` printed when you
built the zip. If they differ, the job is reading an older upload. If there is
no `[build]` line at all, the console is still pointing at an old job script -
`glue_jobs/dst-aiml-etl-dev-fda-approved-drugs.py` is uploaded separately from `pipeline.zip` and is
NOT inside it.

Also worth checking, in this order:

1. Which job ran. The old job `dst-aiml-etl-dev-fda-approved-drugs` reads the
   old project's `US/code/pipeline.zip` and writes to `US/data/...`. The new
   job writes to `output_data/gold/region=us/`.
2. `aws s3 ls s3://<bucket>/code/pipeline.zip` - the upload timestamp.
3. Where the output you looked at came from: `output_data/gold/region=us/` is
   this project, `US/data/` is the old one.
