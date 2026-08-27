"""Every setting, one file per region.

    settings.py   where a value comes from: job parameter, environment,
                  deploy.env, default - in that order
    paths.py      the bronze / silver / gold layout, local and in S3
    common.py     settings both regions share
    eu.py         EMA only
    us.py         FDA only
    deploy.env    bucket, SNS topic, AWS region  (not committed)

Nothing imports these directly. The pipeline says `import config`, and
../config.py picks the region file based on the REGION environment variable.
"""

REGIONS = ("eu", "us")
