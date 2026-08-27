"""The US pipeline: FDA approved drugs.

    extract_table.py  scrape the FDA oncology approvals page               -> bronze
    extract_api.py    query openFDA for each drug                          -> bronze
    transform.py      build and version the drug table                     -> silver
    load.py           read the label PDFs, build the merged dataset        -> silver + gold
    runner.py         runs the four steps in order
"""
