"""Check a heading-matcher change without downloading a single PDF.

    python scripts/heading_check.py us
    python scripts/heading_check.py eu

Every section already stored in the gold CSV was, when it was extracted, one
unbroken run of text. If the current matcher finds a top-level heading inside
one of those stored sections, it would cut the section there on the next run,
and everything after the cut would be lost.

Run this after any change to the heading rules, for both regions. It reads
the gold CSV that is already on disk, so it takes seconds.

A FAIL means at least one stored section would be cut by the current rules.
Fix the heading rules before running the pipeline again.
"""

import csv
import os
import sys

csv.field_size_limit(1000000000)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_handler import REGIONS                     # noqa: E402

REGION = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
if REGION not in REGIONS:
    raise SystemExit("Usage: python scripts/heading_check.py %s" % "|".join(REGIONS))

# config.py reads REGION from the environment, so REGION must be set
# before it is imported.
os.environ["REGION"] = REGION
os.environ.setdefault("DATA_BUCKET", "")

from config_handler import config                        # noqa: E402
import importlib                                         # noqa: E402

load = importlib.import_module("pipeline.%s.load" % REGION)

# How many offending lines to print in full.
MAX_EXAMPLES = 25


def stored_sections(path):
    """Yield (drug, url, section name, section text) from the gold CSV."""
    with open(path, encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            name = row.get(load.MEDICINE_NAME_COLUMN) or ""
            url = row.get("label_url") or ""
            for section in load.LABEL_SECTIONS:
                text = row.get(section) or ""
                if text:
                    yield name, url, section, text


def cuts_inside(text, own_section):
    """Boundaries the matcher would find inside a section's own text.

    The section's own heading sits on the first line, so a boundary there is
    correct and is skipped. Anything after it is a cut.
    """
    # WARNINGS AND PRECAUTIONS is assembled from the two pre-PLR sections for
    # older labels, so those two headings sit inside it on purpose.
    expected_inside = set(getattr(load, "SECTION_FALLBACKS", {}).get(own_section, []))

    found = []
    first_line_end = text.find("\n")
    for position, title in load.find_boundaries(text, load.BOUNDARY_TITLES):
        if first_line_end != -1 and position <= first_line_end:
            continue
        if title == own_section and position == 0:
            continue
        if title in expected_inside:
            continue
        line_end = text.find("\n", position)
        line = text[position:line_end if line_end != -1 else len(text)]
        found.append((title, line.strip(), len(text) - position))
    return found


def main():
    path = config.MERGED_CSV_FILE
    if not os.path.exists(path):
        raise SystemExit("No gold CSV at %s - run the pipeline first." % path)

    print("[check] region=%s reading %s" % (REGION, os.path.basename(str(path))))

    sections = 0
    affected = 0
    characters_at_risk = 0
    examples = []
    by_line = {}

    for name, url, section, text in stored_sections(path):
        sections += 1
        cuts = cuts_inside(text, section)
        if not cuts:
            continue
        affected += 1
        characters_at_risk += max(size for _, _, size in cuts)
        for title, line, size in cuts:
            by_line[line] = by_line.get(line, 0) + 1
            if len(examples) < MAX_EXAMPLES:
                examples.append((name, section, title, line, size))

    print("[check] %s section(s) checked, %s would be cut" % (sections, affected))
    print("[check] %s character(s) would be lost" % format(characters_at_risk, ","))

    if not affected:
        print("[check] PASS - no stored section would be cut by the current rules")
        return 0

    print("\nThe lines doing the cutting, most common first:")
    for line, count in sorted(by_line.items(), key=lambda item: -item[1])[:MAX_EXAMPLES]:
        print("  %5d x  %r" % (count, line[:100]))

    print("\nExamples:")
    for name, section, title, line, size in examples:
        print("  %s / %s" % (name[:34], section))
        print("      cut as %s by %r, losing %s characters"
              % (title, line[:80], format(size, ",")))

    print("\n[check] FAIL - fix the rules before running the pipeline")
    return 1


if __name__ == "__main__":
    sys.exit(main())
