"""The pipelines, one package per region plus the code they share.

    pipeline/
        common/   plumbing that knows nothing about EMA or the FDA
        eu/       extract -> transform -> load, for EMA
        us/       extract -> transform -> load, for the FDA

Every region package exposes the same entry point:

    pipeline.<region>.runner.run()

This is the only thing the Glue job calls. Adding a third region means adding a
folder here and a file in config_handler.
"""
