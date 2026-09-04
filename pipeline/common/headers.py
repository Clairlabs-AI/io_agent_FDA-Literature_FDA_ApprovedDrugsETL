"""Normalise the column headers of a silver-layer file to one common set of names.

Every region names the same thing differently. This file holds one dictionary of
canonical names and the names each region uses for them, plus the code that
applies it, so a new region needs nothing but new entries in that dictionary.

    python pipeline/common/headers.py <silver file>            show what would change
    python pipeline/common/headers.py <silver file> --apply    rename the header row

Only the file the PDF step reads is accepted, because every later file is built
from it. EXTRACTION_INPUT_FILES lists those files; pass --force to rename the
headers of any other file.

Only the header row is touched. The data rows are copied through unchanged, so
nothing has to be extracted again, and the file as it was is kept as <name>.bak.

Two records are written beside the file, both one column name per line:

    <name>_headers_before.txt    the header row as it was
    <name>_headers_after.txt     the header row as it now is
"""

import csv
import os
import re
import shutil
import sys

csv.field_size_limit(1000000000)


# The file each region's PDF step reads. The label sections and the merged
# dataset are both built from it, so renaming its headers is enough.
EXTRACTION_INPUT_FILES = {
    "us": "oncology_drug_table.csv",
    "eu": "ema_label_index.csv",
}


# Canonical name -> every name that means the same thing. Matching ignores case,
# punctuation and spacing, so "Name of medicine", "name_of_medicine" and
# "NAME OF MEDICINE" all count as the same entry.
HEADER_ALIASES = {
    "drug_name": [
        "drug_name", "drug", "Name of medicine", "medicine_name",
        "Drug (clean)", "medicinal product",
    ],
    "substance_name": [
        "substance_name", "Active substance",
        "International non-proprietary name (INN) / common name",
    ],
    "product_number": [
        "application_number", "EMA product number", "ema_product_number",
        "reference_number",
    ],
    "sponsor_name": [
        "sponsor_name",
        "Marketing authorisation developer / applicant / holder",
        "MARKETING AUTHORISATION HOLDER",
    ],
    "manufacturer_name": ["manufacturer_name"],
    "brand_name": ["brand_name"],
    "marketing_status": ["marketing_status", "Medicine status", "status"],
    "therapeutic_area": ["therapeutic_area", "Therapeutic area (MeSH)"],
    "therapeutic_indication": ["therapeutic_indication", "Therapeutic indication"],
    "biomarker": ["biomarker", "Biomarker"],
    "labeling_sections": ["labeling_sections", "Labeling Sections"],
    "label_url": ["label_url", "document_url"],
    "label_version_date": ["label_version_date"],
    "first_published_date": ["first_published_date", "First published date"],
    "authorisation_date": ["authorisation_date", "Marketing authorisation date"],
    "document_hash": ["document_hash"],
    "retrieved_at": ["retrieved_at"],
    "is_label_latest": ["is_label_latest"],
    "label_source": ["label_source"],
    "data_source": ["data_source"],
    "source_reference": ["source_reference"],
}


def match_key(name):
    """The form a header is compared in: lowercase, no punctuation, single spaces."""
    name = str(name or "").lower()
    name = re.sub(r"[\s_\-/]+", " ", name)
    name = re.sub(r"[^a-z0-9() ]+", "", name)
    return name.strip()


def build_lookup(aliases=HEADER_ALIASES):
    """Turn the alias dictionary into one match key -> canonical name map."""
    lookup = {}
    for canonical, names in aliases.items():
        for name in [canonical] + list(names):
            key = match_key(name)
            if key and key not in lookup:
                lookup[key] = canonical
    return lookup


LOOKUP = build_lookup()


def canonical_for(name, lookup=LOOKUP):
    """The canonical name for one header, or the header unchanged when unknown."""
    return lookup.get(match_key(name), name)


def normalise_columns(columns, lookup=LOOKUP):
    """Return the new column names, the ones that changed, and any clashes.

    A file can hold two headers meaning the same thing. A column that already
    carries the canonical name keeps it, a column that is renamed onto a name
    another column has taken is left as it was, and the clash is reported. No
    column is lost and no name is used twice.
    """
    # Reserve the names already in the file first, so renaming never lands on one.
    taken = set(columns)

    new_columns = []
    changes = []
    clashes = []

    for column in columns:
        canonical = canonical_for(column, lookup)
        if canonical != column and canonical in taken:
            clashes.append((column, canonical))
            canonical = column
        elif canonical != column:
            taken.add(canonical)
        new_columns.append(canonical)
        if canonical != column:
            changes.append((column, canonical))

    return new_columns, changes, clashes


def read_header(path):
    """The header row of a CSV file."""
    with open(path, encoding="utf-8", newline="") as handle:
        return next(csv.reader(handle))


def write_record(path, suffix, columns):
    """Save a header row beside the file, one column name per line."""
    stem = os.path.splitext(path)[0]
    record = "%s_headers_%s.txt" % (stem, suffix)
    with open(record, "w", encoding="utf-8") as handle:
        handle.write("\n".join(columns) + "\n")
    return record


def rewrite_header(path, new_columns):
    """Replace the header row and copy every data row through unchanged."""
    shutil.copyfile(path, path + ".bak")

    temporary = path + ".tmp"
    with open(path, encoding="utf-8", newline="") as source, \
            open(temporary, "w", encoding="utf-8", newline="") as target:
        reader = csv.reader(source)
        writer = csv.writer(target)
        next(reader)
        writer.writerow(new_columns)
        for row in reader:
            writer.writerow(row)

    os.replace(temporary, path)


def normalise_file(path, apply=False):
    """Normalise one silver file's headers and return the new column names.

    With apply=False nothing is written except the two header records, so the
    change can be read before it is made.
    """
    columns = read_header(path)
    new_columns, changes, clashes = normalise_columns(columns)

    write_record(path, "before", columns)
    write_record(path, "after", new_columns)

    print("[headers] %s: %s column(s), %s renamed"
          % (os.path.basename(path), len(columns), len(changes)))
    for old, new in changes:
        print("    %-58s -> %s" % (old[:58], new))
    for old, new in clashes:
        print("    [kept] %-51s would also be %s" % (old[:51], new))
    if not changes:
        print("    nothing to rename")

    if apply and changes:
        rewrite_header(path, new_columns)
        print("[headers] header row rewritten, original saved as %s.bak"
              % os.path.basename(path))
    elif apply:
        print("[headers] nothing to write")
    else:
        print("[headers] nothing written, pass --apply to rename")

    return new_columns


def is_extraction_input(path):
    """True when this file is the one a region's PDF step reads."""
    return os.path.basename(path) in EXTRACTION_INPUT_FILES.values()


def main():
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python pipeline/common/headers.py <silver file> [--apply] [--force]")

    path = sys.argv[1]
    if not os.path.exists(path):
        raise SystemExit("No such file: %s" % path)

    if not is_extraction_input(path) and "--force" not in sys.argv:
        raise SystemExit(
            "%s is not a file the PDF step reads, so its headers are left alone.\n"
            "Those files are: %s\nPass --force to rename this one anyway."
            % (os.path.basename(path), ", ".join(sorted(EXTRACTION_INPUT_FILES.values()))))

    normalise_file(path, apply="--apply" in sys.argv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
