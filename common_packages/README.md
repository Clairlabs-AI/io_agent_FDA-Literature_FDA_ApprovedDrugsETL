# common_packages

Prebuilt wheels for the packages Glue Python Shell does not ship and cannot
always install from PyPI.

    common_packages/wheels/*.whl   ->   s3://<bucket>/code/common_packages/wheels/

The Glue job tries three things in order for each package it needs: import it,
`pip install` it from PyPI, then fall back to a wheel from here. The fallback
exists because a Glue job in a private subnet often has no route to PyPI.

Which packages need a wheel today:

| package    | needed by | why it is here                                  |
|------------|-----------|-------------------------------------------------|
| `pymupdf`  | both      | binary wheel, no source build available in Glue |
| `curl_cffi`| US        | binary wheel, and the FDA page needs its TLS    |
| `lxml`     | US        | binary wheel                                    |

Download the ones matching Glue's Python version (3.9, `manylinux`, `x86_64`):

    pip download pymupdf curl_cffi lxml \
        --only-binary=:all: --python-version 3.9 --platform manylinux2014_x86_64 \
        -d common_packages/wheels

then sync the folder to the bucket:

    aws s3 sync common_packages/wheels s3://<bucket>/code/common_packages/wheels/

Wheels sit under `code/` because they are part of the deployment, not output.
They are not committed to git.
