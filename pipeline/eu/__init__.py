"""The EU pipeline: EMA approved drugs.

    extract.py    download the EMA medicines workbook and documents JSON   -> bronze
    transform.py  filter to oncology, join, version the label index        -> silver
    load.py       read the label PDFs, build the merged dataset            -> silver + gold
    runner.py     the three steps in order, which is what the Glue job calls
"""
