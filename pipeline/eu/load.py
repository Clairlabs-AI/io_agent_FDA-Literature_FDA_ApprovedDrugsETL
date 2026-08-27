"""
Extract label sections from the drug table's PDF labels.

For every distinct label PDF in oncology_drug_table.csv this downloads the file,
turns it into reading-order text (data tables become Markdown), splits out the
sections we care about, and writes:

  * oncology_label_sections.csv         - one row per PDF, one column per section
  * US_FDA_oncology_drug_label_dataset.csv     - the drug table joined to those sections
  * US_FDA_oncology_drug_label_dataset.json    - the same data keyed by medicine_name

"""

import os
import re
import threading
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


import pandas as pd

from pipeline.common import http_fetch
from config_handler.config import (EMA_TRANSFORMED_LABEL_FILE,
    SECTIONS_FILE,
    MERGED_CSV_FILE,
    MERGED_JSON_FILE,
    HEADERS,
    FORCE_PDF_REEXTRACT,)


MAX_WORKERS =16          # how many PDFs to download/parse at once
RENDER_TABLES = True      # keep genuine data tables as Markdown; set False for plain text

# How many times to retry a PDF download that comes back as an error page or a
# truncated body instead of the PDF itself.
DOWNLOAD_ATTEMPTS = 4
RETRY_WAIT_SECONDS = 3    # waits 3s, 6s, 9s... between attempts

# Temporary key used to carry a failure reason out of a worker thread. It is
# removed before anything is written to CSV, so it never reaches the outputs.
WARNING_KEY = "_extract_error"

# Sections we pull out of every label.
LABEL_SECTIONS = [
    "NAME OF THE MEDICINAL PRODUCT","QUALITATIVE AND QUANTITATIVE COMPOSITION","PHARMACEUTICAL FORM",
    "CLINICAL PARTICULARS","Therapeutic indications", "Posology and method of administration","Contraindications", "Special warnings and precautions for use",
                     "Interaction with other medicinal products and other forms of interaction","Fertility, pregnancy and lactation","Effects on ability to drive and use machines", "Undesirable effects",
                     "Overdose",
    "PHARMACOLOGICAL PROPERTIES", "Pharmacodynamic properties","Pharmacokinetic properties", "Preclinical safety data",
    "PHARMACEUTICAL PARTICULARS", "List of excipients", "Incompatibilities", "Shelf life", "Special precautions for storage",
                     "Nature and contents of container", "Special precautions for disposal and other handling",
    "MARKETING AUTHORISATION HOLDER",
    "MARKETING AUTHORISATION NUMBER(S)","DATE OF FIRST AUTHORISATION/RENEWAL OF THE AUTHORISATION","DATE OF REVISION OF THE TEXT",
]

# These live as numbered subsections (e.g. "4.1 Therapeutic indications").
SUBSECTION_TITLES = { "Therapeutic indications","Posology and method of administration","Contraindications", "Special warnings and precautions for use",
                     "Interaction with other medicinal products and other forms of interaction","Fertility, pregnancy and lactation","Effects on ability to drive and use machines", "Undesirable effects",
                     "Overdose","Pharmacodynamic properties","Pharmacokinetic properties", "Preclinical safety data", "List of excipients", "Incompatibilities", "Shelf life", "Special precautions for storage",
                     "Nature and contents of container", "Special precautions for disposal and other handling"}

# The number EMA prints in front of each heading. It is fixed by the QRD template
# every SmPC follows, so it is written down here rather than read off the page:
# some labels print the number in a margin column that reads AFTER the title,
# so a fixed table gives every label the same numbers regardless of layout.
SECTION_NUMBERS = {
    "NAME OF THE MEDICINAL PRODUCT": "1.",
    "QUALITATIVE AND QUANTITATIVE COMPOSITION": "2.",
    "PHARMACEUTICAL FORM": "3.",
    "CLINICAL PARTICULARS": "4.",
    "Therapeutic indications": "4.1",
    "Posology and method of administration": "4.2",
    "Contraindications": "4.3",
    "Special warnings and precautions for use": "4.4",
    "Interaction with other medicinal products and other forms of interaction": "4.5",
    "Fertility, pregnancy and lactation": "4.6",
    "Effects on ability to drive and use machines": "4.7",
    "Undesirable effects": "4.8",
    "Overdose": "4.9",
    "PHARMACOLOGICAL PROPERTIES": "5.",
    "Pharmacodynamic properties": "5.1",
    "Pharmacokinetic properties": "5.2",
    "Preclinical safety data": "5.3",
    "PHARMACEUTICAL PARTICULARS": "6.",
    "List of excipients": "6.1",
    "Incompatibilities": "6.2",
    "Shelf life": "6.3",
    "Special precautions for storage": "6.4",
    "Nature and contents of container": "6.5",
    "Special precautions for disposal and other handling": "6.6",
    "MARKETING AUTHORISATION HOLDER": "7.",
    "MARKETING AUTHORISATION NUMBER(S)": "8.",
    "DATE OF FIRST AUTHORISATION/RENEWAL OF THE AUTHORISATION": "9.",
    "DATE OF REVISION OF THE TEXT": "10.",
}


# Every top-level heading, used only to mark where one section ends and the next
# begins.
BOUNDARY_TITLES = [
    "NAME OF THE MEDICINAL PRODUCT","QUALITATIVE AND QUANTITATIVE COMPOSITION","PHARMACEUTICAL FORM",
    "CLINICAL PARTICULARS", "PHARMACOLOGICAL PROPERTIES", "PHARMACEUTICAL PARTICULARS", "MARKETING AUTHORISATION HOLDER",
    "MARKETING AUTHORISATION NUMBER(S)","DATE OF FIRST AUTHORISATION/RENEWAL OF THE AUTHORISATION","DATE OF REVISION OF THE TEXT",
]


# --------------------------------------------------------------------------- #
# PDF -> reading-order text
# --------------------------------------------------------------------------- #
def table_to_markdown(rows):
    """Render extracted table rows as a Markdown table (blank rows dropped)."""
    cells = [[(c or "").strip().replace("\n", " ") for c in row] for row in (rows or [])]
    cells = [row for row in cells if any(row)]
    if not cells:
        return ""
    width = max(len(row) for row in cells)
    cells = [row + [""] * (width - len(row)) for row in cells]
    body = ["| " + " | ".join(row) + " |" for row in cells]
    separator = "| " + " | ".join(["---"] * width) + " |"
    return "\n".join([body[0], separator] + body[1:])


# A cell holding only "." or a stray character is not real content.
MIN_CELL_CHARACTERS = 2

# Sanity ceiling so a giant bordered block of prose is still not called a table.
# Set generously, since some genuine data tables hold long instructions in a
# single cell.
MAX_AVERAGE_CELL_CHARACTERS = 800
MAX_SINGLE_CELL_CHARACTERS = 4000


def filled_cells(row):
    """How many cells in this row hold real content."""
    return sum(1 for cell in row if len(cell.strip()) >= MIN_CELL_CHARACTERS)


def looks_like_data_table(rows):
    """True only for a real data grid, not a bordered text box that table
    detection also flags.

    The test that does the work is "at least one row has two or more filled
    cells". That is what separates a grid from a bordered paragraph: a text box
    has content in one column only, however many rows it is split into.

    The cell-size limits are only a sanity ceiling to rule out a block of prose.
    """
    grid = [[(c or "").strip() for c in row] for row in (rows or [])]
    grid = [row for row in grid if any(row)]
    if len(grid) < 2:
        return False

    if max(len(row) for row in grid) < 2:
        return False

    # A grid, not a bordered text box.
    if max(filled_cells(row) for row in grid) < 2:
        return False

    cell_lengths = [len(c) for row in grid for c in row if c]
    if not cell_lengths:
        return False

    average = sum(cell_lengths) / len(cell_lengths)
    if average > MAX_AVERAGE_CELL_CHARACTERS:
        return False
    if max(cell_lengths) > MAX_SINGLE_CELL_CHARACTERS:
        return False

    return True


def is_heading_line(text):
    """True if a line is a real heading: a top-level title from BOUNDARY_TITLES,
    or a known subsection title from SUBSECTION_TITLES.

    Heading lines are kept even when they overlap a detected table, so section
    boundaries survive. Everything else inside a table band is dropped, because
    the table is already being written out as Markdown.

    Matching against known titles (rather than any numbered-looking line) means
    a table cell like "2.4" or "11.7" is never mistaken for a heading.
    """
    return (section_for_line(text, BOUNDARY_TITLES, require_upper=True) is not None
            or section_for_line(text, SUBSECTION_TITLES) is not None)


def page_text_lines(page):
    """(y_top, text) for each text LINE on the page. We work line-by-line (not
    block-by-block) because PyMuPDF often merges prose, a heading, and a table
    into one block; lines have their own y-coordinates, so only the actual
    table-row lines fall inside a table band."""
    lines = []
    for block in page.get_text("dict").get("blocks", []):
        if block.get("type") != 0:                    # 0 = text (skip images)
            continue
        for line in block.get("lines", []):
            text = "".join(span.get("text", "") for span in line.get("spans", []))
            if text.strip():
                lines.append((line["bbox"][1], text))
    return lines


# --------------------------------------------------------------------------- #
# Borderless tables, found by their caption
# --------------------------------------------------------------------------- #
# Some tables are drawn with ruling lines and PyMuPDF finds them directly.
# Others are laid out with nothing but whitespace, and find_tables() returns
# nothing for those regardless of cell size.
#
# Those labels caption a table on its own line ("Table 5. ..."), so the caption
# marks where a table starts, and the columns are read from the whitespace
# between the values rather than from any ruling.
#
# Caption detection is an EXTRA way in, never a replacement for ruled-table
# detection: some labels have ruled tables and no captions at all.

# A caption is a line that STARTS with "Table <n>". A cross-reference in prose
# ("PFS results are summarised in Table 5 and Figure 3.") is mid-sentence and so
# does not match.
TABLE_CAPTION_RE = re.compile(r"(?i)^\s*table\s+\d+\s*[.:)]?\s+\S")

# A gap wider than this between two words starts a new cell.
CELL_GAP = 6.0

# A corridor of whitespace this wide, running down the rows, separates columns.
MIN_CORRIDOR = 3

# A line holding one very wide cell is a title or a paragraph, not a data row,
# so it is left out when working out where the columns are.
WIDE_CELL_SHARE = 0.55

# Below these a region is prose, not a table.
MIN_TABLE_ROWS = 3
MIN_TABLE_COLUMNS = 2


def page_line_cells(page, gap=CELL_GAP):
    """Every text line on the page as (y, [(x0, x1, text), ...]).

    Words are joined into a cell while they are close together, and a wider gap
    starts the next cell. This is what makes a whitespace table readable at all:
    the cells are the runs of text, and the gaps between them are the columns.
    """
    words = [w for w in page.get_text("words") if w[4].strip()]
    if not words:
        return []

    by_line = {}
    for x0, y0, x1, y1, text in [(w[0], w[1], w[2], w[3], w[4]) for w in words]:
        by_line.setdefault(round(y0 / 3), []).append((x0, x1, text))

    lines = []
    for key in sorted(by_line):
        ordered = sorted(by_line[key])
        cells = []
        cell_x0, cell_x1, buffer = ordered[0][0], ordered[0][1], [ordered[0][2]]
        for x0, x1, text in ordered[1:]:
            if x0 - cell_x1 > gap:
                cells.append((cell_x0, cell_x1, " ".join(buffer)))
                cell_x0, buffer = x0, [text]
            else:
                buffer.append(text)
            cell_x1 = max(cell_x1, x1)
        cells.append((cell_x0, cell_x1, " ".join(buffer)))
        lines.append((key * 3, cells))
    return lines


def whitespace_corridors(rows, low, high, width, min_width=MIN_CORRIDOR):
    """The x ranges between `low` and `high` that no row writes into."""
    covered = [0] * (int(width) + 2)
    for cells in rows:
        for x0, x1, _ in cells:
            for x in range(max(int(x0), low), min(int(x1) + 1, high, len(covered))):
                covered[x] += 1

    corridors, start = [], None
    for x in range(low, high):
        if covered[x] == 0:
            if start is None:
                start = x
        else:
            if start is not None and x - start >= min_width:
                corridors.append((start, x))
            start = None
    return corridors


def column_edges(rows, width):
    """Where the columns of a whitespace table begin and end.

    Worked out in two passes. The first splits the region on the corridors that
    run its whole height. The second looks inside each of those columns for a
    corridor of its own, which is what separates a pair of values sitting under
    one shared header.
    """
    data_rows = [cells for cells in rows if len(cells) >= 2]
    if not data_rows:
        return []

    left = min(int(cells[0][0]) for cells in data_rows)
    right = max(int(cells[-1][1]) for cells in data_rows)
    span = right - left
    if span <= 0:
        return []

    # Titles and paragraphs stretch across everything and would hide every
    # corridor, so the columns are read from the data rows only.
    narrow = [cells for cells in data_rows
              if max(x1 - x0 for x0, x1, _ in cells) < WIDE_CELL_SHARE * span]
    if len(narrow) < MIN_TABLE_ROWS:
        return []

    edges = [left] + [(a + b) // 2 for a, b in
                      whitespace_corridors(narrow, left, right + 1, width)] + [right + 1]

    refined = [edges[0]]
    for i in range(len(edges) - 1):
        low, high = edges[i], edges[i + 1]
        inside = [[c for c in cells if low <= (c[0] + c[1]) / 2 < high] for cells in narrow]
        inside = [cells for cells in inside if len(cells) >= 2]
        if len(inside) >= MIN_TABLE_ROWS:
            refined.extend((a + b) // 2 for a, b in
                           whitespace_corridors(inside, low, high, width))
        refined.append(high)

    return sorted(set(refined))


def rows_to_grid(rows, edges):
    """Drop every cell into the column its middle falls in."""
    grid = []
    for _, cells in rows:
        row = [""] * (len(edges) - 1)
        for x0, x1, text in cells:
            middle = (x0 + x1) / 2
            for i in range(len(edges) - 1):
                if edges[i] <= middle < edges[i + 1]:
                    row[i] = (row[i] + " " + text).strip()
                    break
        grid.append(row)

    # A corridor midpoint can leave a column with nothing in it.
    keep = [i for i in range(len(edges) - 1) if any(row[i] for row in grid)]
    return [[row[i] for i in keep] for row in grid if any(row)]


# A table can hold a line or two with only one cell - a wrapped label such as
# "(95% CI)" - but a longer run of them is the prose that follows the table.
MAX_SINGLE_CELL_RUN = 4


def caption_table_regions(lines):
    """The lines that belong to each "Table N" caption.

    Where a table ENDS matters as much as where it starts, so a region does not
    run into the text that follows the table. A region stops at the first of

      * the next caption,
      * a real heading, or
      * a run of single-cell lines long enough to be a paragraph.
    """
    is_caption = [bool(cells) and bool(TABLE_CAPTION_RE.match(" ".join(c[2] for c in cells)))
                  for _, cells in lines]

    regions = []
    for start, caption in enumerate(is_caption):
        if not caption:
            continue

        end = len(lines)
        run = 0
        for i in range(start + 1, len(lines)):
            _, cells = lines[i]
            text = " ".join(c[2] for c in cells)

            if is_caption[i] or is_heading_line(text):
                end = i
                break

            if len(cells) < 2:
                run += 1
                if run >= MAX_SINGLE_CELL_RUN:
                    end = i - run + 1        # stop before the prose began
                    break
            else:
                run = 0

        regions.append(lines[start:end])
    return regions


def is_already_rendered(grid, already_rendered, share=0.8):
    """True when this rebuilt table's values are already in a rendered table."""
    values = [cell for row in grid for cell in row if cell]
    if not values:
        return False
    for _, rendered in already_rendered:
        found = sum(1 for value in values if value in rendered)
        if found >= share * len(values):
            return True
    return False


def caption_tables(page, already_rendered):
    """Rebuild the whitespace tables on this page, one per caption.

    A region is skipped when a ruled table has already been rendered over it, so
    a table PyMuPDF found is never rebuilt a second time.
    """
    results = []
    lines = page_line_cells(page)

    for region in caption_table_regions(lines):
        if len(region) < MIN_TABLE_ROWS + 1:
            continue

        top = region[0][0]
        bottom = region[-1][0]
        if any(box[1] - 2 <= top <= box[3] + 2 for box, _ in already_rendered):
            continue

        body = region[1:]                       # the caption itself is not a row
        edges = column_edges([cells for _, cells in body], page.rect.width)
        if len(edges) - 1 < MIN_TABLE_COLUMNS:
            continue

        grid = rows_to_grid(body, edges)
        if len(grid) < MIN_TABLE_ROWS or not grid or len(grid[0]) < MIN_TABLE_COLUMNS:
            continue
        if sum(1 for row in grid if sum(1 for c in row if c) >= 2) < MIN_TABLE_ROWS:
            continue

        markdown = table_to_markdown(grid)
        if not markdown:
            continue

        left = min(c[0] for _, cells in body for c in cells)
        right = max(c[1] for _, cells in body for c in cells)
        bbox = (left, body[0][0], right, bottom)
        cells_text = squeeze(" ".join(c for row in grid for c in row))

        # A caption sits ABOVE its table, so a region whose caption falls
        # outside a ruled table's box can still pass the position check above
        # even when it IS that table. Comparing the values themselves catches
        # it: if nearly every value here is already inside a rendered table,
        # this would print the table twice.
        if is_already_rendered(grid, already_rendered):
            continue

        results.append((bbox, markdown, cells_text))

    return results


def squeeze(text):
    """One-line form of a piece of text, for comparing content."""
    return re.sub(r"\s+", " ", str(text or "")).strip()


def is_duplicate_of_rendered_table(y_top, text, rendered_tables):
    """True when this line sits in a table we rendered AND its text is already
    in that table's cells, so writing it again would duplicate it."""
    line = squeeze(text)
    if not line:
        return False

    for bbox, cells in rendered_tables:
        if bbox[1] - 2 <= y_top <= bbox[3] + 2 and line in cells:
            return True
    return False


# Counts of failures for each table-detection pass, so a failing pass shows up
# in the log instead of being silently ignored.
TABLE_FAILURES = {}


def note_table_failure(kind, page, error):
    """Count a table pass that blew up, and show the first few."""
    TABLE_FAILURES[kind] = TABLE_FAILURES.get(kind, 0) + 1
    if TABLE_FAILURES[kind] <= 3:
        print("[tables] the %s pass failed on page %s: %s: %s"
              % (kind, page.number + 1, type(error).__name__, error))


def table_failure_summary():
    """One line per failing pass, for the end of the run."""
    for kind, count in sorted(TABLE_FAILURES.items()):
        print("[tables] the %s pass failed on %s page(s) in this run" % (kind, count))


def pdf_to_text(doc):
    """All pages as reading-order text. Genuine data tables are inserted as
    Markdown; a line that is a heading is kept even when it overlaps a table, so
    section boundaries survive."""
    pages = []
    for page in doc:
        if not RENDER_TABLES:
            pages.append(page.get_text("text") or "")
            continue

        rendered_tables = []   # (bbox, text of every cell) for tables we wrote out
        items = []             # (y_top, text) fragments to sort into reading order
        try:
            for table in page.find_tables().tables:
                try:
                    rows = table.extract()
                except Exception:
                    rows = None
                if rows and looks_like_data_table(rows):
                    markdown = table_to_markdown(rows)
                    if markdown:
                        cells = " ".join(str(c or "") for row in rows for c in row)
                        rendered_tables.append((table.bbox, squeeze(cells)))
                        items.append((table.bbox[1], markdown))
        except Exception as error:
            note_table_failure("ruled-table", page, error)

        # Tables drawn with whitespace instead of ruling lines: find them by
        # their caption and rebuild them from the gaps between the values.
        try:
            for bbox, markdown, cells in caption_tables(page, rendered_tables):
                rendered_tables.append((bbox, cells))
                items.append((bbox[1], markdown))
        except Exception as error:
            note_table_failure("caption-table", page, error)

        for y_top, text in page_text_lines(page):
            # A line is dropped only when the table already written out actually
            # CONTAINS its text, not just when it sits inside the table's box.
            # This keeps things like a footnote printed under the last row, and
            # still stops a cell value like "1.0" appearing a second time as a
            # loose line after the table.
            if is_duplicate_of_rendered_table(y_top, text, rendered_tables) \
                    and not is_heading_line(text):
                continue
            items.append((y_top, text.rstrip()))

        items.sort(key=lambda item: item[0])
        pages.append("\n".join(text for _, text in items))
    return "\n".join(pages)


def describe_response(response):
    """Describe an HTTP response for an error message: status, content type,
    content encoding, size and the first few bytes of the body."""
    content = response.content or b""
    start = content[:40].decode("utf-8", errors="replace").replace("\n", " ")
    return ("HTTP {}, content-type {!r}, content-encoding {!r}, {} bytes, "
            "starts with {!r}"
            .format(response.status_code,
                    response.headers.get("Content-Type", ""),
                    response.headers.get("Content-Encoding", ""),
                    len(content),
                    start))


# The PDF spec allows junk before the "%PDF" marker as long as it is near the
# start, and readers scan the first 1024 bytes for it, so we do the same instead
# of insisting the body begins with it.
PDF_MAGIC_SEARCH_BYTES = 1024


def check_looks_like_pdf(response):
    """Raise a clear error when the downloaded bytes are not a PDF, such as a
    throttling page or a body that could not be decompressed."""
    content = response.content or b""
    if b"%PDF" in content[:PDF_MAGIC_SEARCH_BYTES]:
        return

    # A body that is still compressed looks like a corrupt PDF otherwise: the
    # content type is right but the bytes are not. Name the encoding and the fix.
    encoding = (response.headers.get("Content-Encoding") or "").strip().lower()
    if encoding and encoding != "identity":
        raise ValueError(
            "the body is still {0}-compressed, so it could not be read. requests "
            "cannot decode {0} unless the matching package is installed - remove "
            "{0} from Accept-Encoding in config.HEADERS, or add the decoder to "
            "requirements.txt. {1}".format(encoding, describe_response(response)))

    raise ValueError("the server did not return a PDF: " + describe_response(response))


# PyMuPDF is not safe to drive from several threads at once, so parsing is
# taken one PDF at a time while downloads stay parallel.
PDF_PARSE_LOCK = threading.Lock()


def fetch_pdf_text(url, headers=None, retries=DOWNLOAD_ATTEMPTS):
    """Download a PDF and return its text.

    The download itself is handled by http_fetch, which paces requests across
    all workers and retries a 429 for as long as the server asks. This function
    only retries the cases http_fetch cannot see: a body that arrived but is not
    a readable PDF. If every attempt fails the last error is raised, and
    process_pdf() turns it into a warning rather than stopping the run.
    """
    import time
    import pymupdf

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = http_fetch.get(url, headers=headers or HEADERS)
            check_looks_like_pdf(response)

            with PDF_PARSE_LOCK:
                doc = pymupdf.open(stream=response.content, filetype="pdf")
                try:
                    return pdf_to_text(doc)
                finally:
                    doc.close()

        except Exception as error:
            last_error = error
            if attempt < retries:
                time.sleep(RETRY_WAIT_SECONDS * attempt)

    raise last_error


# --------------------------------------------------------------------------- #
# Split the text into sections
# --------------------------------------------------------------------------- #
# A section number alone on a line ("12" / "12.1") followed by a title line. Some
# labels put the number in a left-margin column, so it lands on its own line; we
# rejoin it with the title so the heading matchers see "12.1 Title".
_SPLIT_NUMBER_RE = re.compile(r"(?m)^[ \t]*(\d{1,2}(?:\.\d{1,2})?)[ \t]*\n(?=[ \t]*[A-Z])")


def rejoin_split_headings(text):
    return _SPLIT_NUMBER_RE.sub(lambda m: m.group(1) + " ", text)


# Where the SmPC starts. An EMA label opens with "ANNEX I" and then the line
# "SUMMARY OF PRODUCT CHARACTERISTICS"; the numbered sections follow it.
#
# A leading "| " is tolerated because table detection sometimes wraps the line
# into a table row, and a leading number because rejoin_split_headings() pulls a
# page number onto the following line.
SMPC_START_RE = re.compile(
    r"(?m)^[ \t|]*(?:\d{1,3}[ \t]+)?SUMMARY OF PRODUCT CHARACTERISTICS[ \t]*$")


# --------------------------------------------------------------------------- #
# Recognising a heading
# --------------------------------------------------------------------------- #
# Labels do not write their headings the same way. The same section can turn up
# with different wording, plurals, hyphens, or extra articles, for example:
#
#     1.   "NAME OF THE MEDICINAL PRODUCT"     "NAME OF MEDICINAL PRODUCT"
#     4.5  "Interaction with ..."              "Interactions with ... interactions"
#     4.6  "Fertility, pregnancy and lactation"  "... and breast-feeding"
#     4.7  "Effects on ability to drive ..."   "Effects on THE ability to drive ..."
#     5.3  "Preclinical safety data"           "Pre-clinical safety data"
#     6.3  "Shelf life"                        "Shelf-life"
#     6.5  "Nature and contents of container"  "... of THE container"
#     6.6  "Special precautions for disposal and other handling"  "... for disposal"
#     8.   "MARKETING AUTHORISATION NUMBER(S)" "MARKETING AUTHORISATION NUMBER"
#
# A heading is recognised two ways, and either one is enough:
#
#   by its number   EU numbering is fixed by the QRD template, so a line that
#                   starts with "4.6" IS section 4.6 however the label worded
#                   it. Only numbers that appear in SECTION_NUMBERS count, which
#                   is what keeps "2.4-fold", "11.7 weeks" and "2.6 l/h/m2" from
#                   ever looking like headings again - those numbers are not
#                   sections, so they are never even considered.
#
#   by its words    compared after plurals, hyphens, "the" and "(s)" are taken
#                   out, so the variants above collapse onto one another. This
#                   is what carries the top-level headings, which have no number
#                   left once it has been split off.

# Words a label adds, drops or swaps for punctuation without changing which
# section it means. "and" is in here because labels write both
# "Fertility, pregnancy and lactation" and "Fertility, pregnancy, lactation".
# Dropping these creates no collision: no two titles in LABEL_SECTIONS or
# BOUNDARY_TITLES reduce to the same words, in either project.
IGNORED_HEADING_WORDS = {"the", "a", "an", "and", "or"}

# How much of a heading's wording has to survive when the number already says
# which section it is. "Fertility, pregnancy and breast-feeding" keeps two words
# of three against "Fertility, pregnancy and lactation"; a measurement that opens
# with the same figures ("4.2 mg/kg was given to ...") keeps none.
MIN_HEADING_OVERLAP = 0.5

# A heading is a short line. Past this it is prose that happens to start with a
# number, so the number alone is not allowed to claim it. The longest real title
# is 72 characters ("Interaction with other medicinal products and other forms
# of interaction"), so this leaves room without letting a sentence through.
MAX_HEADING_CHARACTERS = 90

# How many words a heading may carry beyond the title it matches. A heading is
# roughly the title; a sentence that merely mentions the title's words is not.
MAX_EXTRA_HEADING_WORDS = 2

# The number a section is printed under, e.g. "4.5" -> the interactions section.
SECTION_BY_NUMBER = {number.rstrip("."): title
                     for title, number in SECTION_NUMBERS.items()}

# The separator after the number is optional, so a heading like "12.1Mechanism
# of Action" with no space still matches. This is safe because the words after
# the number still have to match the section it names - "12mg" and "5 years"
# match nothing.
LEADING_NUMBER_RE = re.compile(r"^[ \t]*(\d{1,3}(?:\.\d{1,2})?)[ \t.):\-]*(.*)$")

# A page number left sitting at the end of a heading line.
TRAILING_PAGE_NUMBER_RE = re.compile(r"[ \t]+\d{1,3}[ \t]*$")

# A "N.N" number that is not one of ours. Labels number their own sub-parts
# inside a top-level section, and one of those sub-part titles can read like the
# top-level title itself, so a line carrying an unknown subsection number is
# never treated as a heading we want.
UNKNOWN_SUBSECTION_RE = re.compile(r"^[ \t]*\d{1,2}\.\d{1,2}[ \t.):\-]")


# A rendered table row or a bullet line is never a heading, whatever words it
# holds. Tables are written out as Markdown, so a row like "| Adverse Reactions |"
# can reduce to exactly the words of a section title once punctuation is
# dropped, so table rows and bullets are excluded outright.
NOT_A_HEADING_RE = re.compile(r"[|\u2022]")


def looks_upper(text):
    """True when a line is printed in capitals, ignoring digits and punctuation."""
    letters = [character for character in text if character.isalpha()]
    return bool(letters) and all(character.isupper() for character in letters)


def heading_words(line, glue_hyphens=False):
    """A heading reduced to the words worth comparing.

    Plurals, hyphens, "(s)" and articles all vary between labels without
    changing the section, so they are taken out of both sides before comparing.
    Hyphens are tried twice because labels disagree on which way to go:
    "Shelf-life" means "Shelf life" (hyphen becomes a space) while
    "Pre-clinical" means "Preclinical" (hyphen disappears).
    """
    line = re.sub(r"\(s\)", "s", line.lower())
    line = line.replace("-", "" if glue_hyphens else " ")

    words = []
    for word in re.findall(r"[a-z0-9]+", line):
        if word in IGNORED_HEADING_WORDS:
            continue
        if len(word) > 3 and word.endswith("s"):
            word = word[:-1]
        words.append(word)
    return words


def words_match(line, title):
    """True when a printed heading means this title.

    A heading that is a shortened version of the title counts, but only in that
    direction, and only from three words on. A line that carries on PAST the
    title is not accepted, since that would also match ordinary prose that
    happens to open with the same words.
    """
    for glue_hyphens in (False, True):
        found = heading_words(line, glue_hyphens)
        wanted = heading_words(title, glue_hyphens)
        if not found or not wanted:
            continue
        if found == wanted:
            return True
        if len(found) >= 3 and wanted[:len(found)] == found:
            return True
    return False


def words_overlap(line, title):
    """How much of the shorter heading the two have in common, 0.0 to 1.0."""
    found = set(heading_words(line))
    wanted = set(heading_words(title))
    if not found or not wanted:
        return 0.0
    return len(found & wanted) / min(len(found), len(wanted))


def split_heading_number(line):
    """Split a heading line into (section number, the rest of the line).

    The number is "" when the line does not start with one we know. A page
    number can be sitting in front of the real heading, since
    rejoin_split_headings pulls a lone number onto the following line, so a
    leading number that is not a section number is dropped and the next one
    tried.
    """
    rest = line
    for _ in range(2):
        match = LEADING_NUMBER_RE.match(rest)
        if not match:
            break
        number, tail = match.group(1), match.group(2)
        if number in SECTION_BY_NUMBER:
            return number, tail
        rest = tail
    return "", rest


def heading_text(line):
    """The heading line with its numbers and padding taken off."""
    _, rest = split_heading_number(line)
    rest = TRAILING_PAGE_NUMBER_RE.sub("", rest)
    return rest.strip().rstrip(".:;")


def numbers_agree(printed, known):
    """True when a heading's printed number matches the section's own number.

    A parent number counts as agreement, so "12 Mechanism of Action" is still
    section 12.1 when the label drops the decimal.
    """
    printed = printed.rstrip(".")
    known = known.rstrip(".")
    if printed == known:
        return True
    return known.startswith(printed + ".") or printed.startswith(known + ".")


def section_for_line(line, titles, require_upper=False):
    """Which of `titles` this line is the heading for, or None.

    require_upper is for the top-level headings, which are printed in capitals.
    Without it, a subheading written in title case inside a section could be
    read as the start of a later top-level section. Subsections are matched
    without it, since labels print those in sentence case (e.g.
    "12.1 Mechanism of Action").

    A line carrying a section number is exempt from require_upper: the number
    is evidence enough on its own.
    """
    if NOT_A_HEADING_RE.search(line):
        return None

    number, _ = split_heading_number(line)
    rest = heading_text(line)
    expected = SECTION_BY_NUMBER.get(number)

    # Only a number we RECOGNISE excuses a heading from being in capitals.
    # Accepting any leading digits let a page number carry a title-case table
    # header past this check, and that ended a section tens of pages early.
    if require_upper and not number and not looks_upper(rest):
        return None

    if expected in titles:
        # The number has already said which section this is. The words only have
        # to be close enough to show the line is a heading at all, and not a
        # measurement that happens to open with the same figures.
        #
        # A number with NOTHING after it is never a heading. Labels print the
        # top-level numbers in a margin column, so a bare number sits on lines of
        # its own all through the document, including inside tables, where it is
        # just a list marker. The title on the next line is matched on its own
        # words instead, which is what the section is actually found by.
        if not rest:
            return None
        if words_match(rest, expected):
            return expected

        # Below here the words only OVERLAP the title, which is the rule for
        # subsection headings - labels wrap and reword those. A top-level line
        # in ordinary case must not qualify on overlap alone, or a page footer
        # that happens to share a word with a section title could be read as
        # that section's heading.
        if require_upper and not looks_upper(rest):
            return None

        if (len(rest) <= MAX_HEADING_CHARACTERS
                and len(heading_words(rest)) <= len(heading_words(expected)) + MAX_EXTRA_HEADING_WORDS
                and words_overlap(rest, expected) >= MIN_HEADING_OVERLAP):
            return expected
        return None

    if not rest or UNKNOWN_SUBSECTION_RE.match(line):
        return None

    matches = [title for title in titles if words_match(rest, title)]
    # A heading that could belong to two sections ("Special precautions for") is
    # not used at all, because guessing would cut in the wrong place.
    if len(matches) != 1:
        return None

    # And a line whose own number contradicts the section it matched is not that
    # section's heading, whatever the words say.
    matched = matches[0]
    printed = split_heading_number(line)[0]
    known = SECTION_NUMBERS.get(matched)
    if printed and known and not numbers_agree(printed, known):
        return None
    return matched


def known_subsection_title(line, titles=SUBSECTION_TITLES):
    """The known subsection this line is a heading for, or None."""
    return section_for_line(line, titles)


def find_boundaries(text, titles):
    """List of (position, title) for every top-level heading found."""
    boundaries = []
    offset = 0
    for line in text.split("\n"):
        title = section_for_line(line, titles, require_upper=True)
        if title:
            boundaries.append((offset, title))
        offset += len(line) + 1
    return boundaries


# A line holding only the next section's number, e.g. "2." or "10.". The heading
# text sits on the following line, so the number is left dangling at the end of
# the section before it.
TRAILING_NUMBER_RE = re.compile(r"\n[ \t]*\d{1,2}(?:\.\d{1,2})?\.?[ \t]*$")


def tidy(text):
    """Collapse form-feeds, runs of spaces, and blank lines."""
    text = text.replace("\x0c", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = TRAILING_NUMBER_RE.sub("", text.strip())
    return text.strip()


def extract_top_level_section(text, target, boundaries):
    """Text between a top-level heading and the next boundary. If the heading
    appears more than once, keep the longest span (the real body, not a ToC line)."""
    best = ""
    for i, (position, title) in enumerate(boundaries):
        if title != target:
            continue
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(text)
        newline = text.find("\n", position)
        start = newline + 1 if newline != -1 else position
        span = text[start:end].strip()
        if len(span) > len(best):
            best = span
    return best


def parent_block(text, target, boundaries):
    """The text of the section `target` belongs to, or the whole label.

    Returning the whole label when the parent could not be found is deliberate.
    Losing a section entirely is worse than searching too widely, and the
    heading marks below still stop a subsection running past the next top-level
    heading.
    """
    span = parent_section_range(target, boundaries, len(text))
    return text[span[0]:span[1]] if span else text


def heading_marks(block, titles=SUBSECTION_TITLES):
    """Every heading line in `block`, as (start of the line, title, line length).

    Top-level headings are marked as well as subsections. Inside a parent block
    there are none, so they change nothing there; when the parent could not be
    found and the whole label is being scanned, they are what stops a subsection
    running on into the next section.
    """
    marks, offset = [], 0
    for line in block.split("\n"):
        title = (section_for_line(line, titles)
                 or section_for_line(line, BOUNDARY_TITLES, require_upper=True))
        if title:
            marks.append((offset, title, len(line)))
        offset += len(line) + 1
    return marks


def extract_subsection(text, target, boundaries):
    """The text under a numbered subsection heading, cut out of its own parent.

    A subsection belongs to one section and nothing else: 4.1-4.9 live inside
    CLINICAL PARTICULARS, 6.1-6.6 inside PHARMACEUTICAL PARTICULARS. So the
    parent is cut out first and the split happens inside it. Every boundary is
    then local - a subsection ends at the next heading in its own parent, or at
    the end of the parent - and nothing further down the label can reach it.

    Where the heading appears more than once (a leftover contents entry as well
    as the real one) the longest span wins.
    """
    block = parent_block(text, target, boundaries)
    marks = heading_marks(block)

    best = ""
    for index, (position, title, length) in enumerate(marks):
        if title != target:
            continue
        start = position + length + 1
        end = marks[index + 1][0] if index + 1 < len(marks) else len(block)
        span = block[start:end].strip()
        if len(span) > len(best):
            best = span
    return best


# An EMA product-information PDF is several documents in one file:
#
#   ANNEX I    Summary of Product Characteristics (the SmPC)  <- what we want
#   ANNEX II   manufacturing and supply conditions
#   ANNEX III  labelling (carton text) and the package leaflet
#
# ANNEX III repeats the SmPC's numbered headings once per pack size, so a
# heading like "1. NAME OF THE MEDICINAL PRODUCT" can appear several times in
# one label. Cutting the text down to ANNEX I first is what keeps every section
# coming from the SmPC and not from a later repeat.
# The optional leading number matters: rejoin_split_headings() pulls a lone page
# number onto the following line, so the annex heading reads "30 ANNEX II".
SMPC_END_RE = re.compile(r"(?m)^[ \t|]*(?:\d{1,3}[ \t]+)?ANNEX[ \t]+I{2,3}\b")


def smpc_text(text):
    """Narrow a whole EMA product-information text down to just the SmPC.

    Start: the line after "SUMMARY OF PRODUCT CHARACTERISTICS".
    End:   the "ANNEX II" / "ANNEX III" heading that follows it.

    Either marker missing is not an error - the corresponding end of the text is
    used instead, so a label with an unexpected layout still yields everything it
    has rather than nothing.
    """
    text = rejoin_split_headings(text)

    start_match = SMPC_START_RE.search(text)
    start = start_match.end() if start_match else 0

    end_match = SMPC_END_RE.search(text, start)
    end = end_match.start() if end_match else len(text)

    return text[start:end]


# --------------------------------------------------------------------------- #
# Section numbers
# --------------------------------------------------------------------------- #
# Every section comes out with its numbered heading on the first line and its
# text under it, always in the same shape:
#
#     4.8 Undesirable effects
#     Summary of the safety profile
#     ...
#
# The number comes from SECTION_NUMBERS and the title from LABEL_SECTIONS, so it
# is written the same way for every label. What the PDF itself printed is only
# used to find the section, never to build the heading.


def drop_leading_number(number, body):
    """Remove the section's own number from the front of its text.

    Most labels print the number and then the title, so the number never reaches
    the body. Some print it in a margin column that reads AFTER the title,
    leaving the number at the front of the body instead, where it would
    otherwise show up twice. Only this section's own number is removed, and
    only from the very front, so nothing else in the text is touched.

    The (?![0-9.]) stops "4.1" from matching the start of "4.12 mg".
    """
    if not number:
        return body
    pattern = r"^[ \t]*" + re.escape(number) + r"(?![0-9.])[ \t]*\n?"
    return re.sub(pattern, "", body, count=1).strip()


def parent_title(target):
    """The top-level section a numbered subsection belongs to, or "".

    "Mechanism of action" is 12.1, so its parent is whichever section is
    numbered 12 - CLINICAL PHARMACOLOGY. Worked out from SECTION_NUMBERS rather
    than written down a second time, so adding a section to that table is still
    a one-line change.
    """
    number = SECTION_NUMBERS.get(target, "").rstrip(".")
    if "." not in number:
        return ""                                  # already a top-level section
    parent = number.split(".")[0]
    for title, value in SECTION_NUMBERS.items():
        if title not in SUBSECTION_TITLES and value.rstrip(".") == parent:
            return title
    return ""


def parent_section_range(target, boundaries, text_length):
    """Where a subsection's parent section starts and ends, or None.

    A subsection can only be inside its own parent, and restricting the search
    to that range is what stops a stray match elsewhere in the label from
    winning, since extract_subsection keeps the LONGEST span it finds.

    The parent title can appear more than once - a contents entry as well as the
    real heading - so the widest occurrence is used, the same rule
    extract_top_level_section applies.
    """
    title = parent_title(target)
    if not title:
        return None

    best = None
    for i, (position, name) in enumerate(boundaries):
        if name != title:
            continue
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else text_length
        if best is None or (end - position) > (best[1] - best[0]):
            best = (position, end)
    return best


def with_heading(number, title, body):
    """A section's text with its numbered heading on the first line.

    The heading is always built the same way - the number from SECTION_NUMBERS
    followed by the title from LABEL_SECTIONS - so every label reads the same
    however its own PDF printed the heading.

    An empty section stays empty. It has to: an empty section is what tells the
    pipeline this label still needs reading, and a row holding nothing but a
    heading would look like one we had extracted successfully.
    """
    if not body:
        return ""
    body = drop_leading_number(number, body)
    heading = (number + " " + title).strip()
    return heading + "\n" + body


def extract_sections(text, targets=LABEL_SECTIONS, boundary_titles=BOUNDARY_TITLES):
    """Return {section title -> text} for the target sections."""
    text = smpc_text(text)
    boundaries = find_boundaries(text, boundary_titles)

    sections = {}
    for target in targets:
        if target in SUBSECTION_TITLES:
            span = extract_subsection(text, target, boundaries)
        else:
            span = extract_top_level_section(text, target, boundaries)
        sections[target] = with_heading(SECTION_NUMBERS.get(target, ""),
                                        target, tidy(span))
    return sections


def process_pdf(document_hash, url):
    """Download + parse one PDF into a record of its sections.

    A failure is not raised: the record comes back with blank sections and the
    reason stored under WARNING_KEY, so main() can collect every failure and
    hand them to the run summary mail.
    """
    record = {"document_hash": document_hash, "label_url": url}
    try:
        record.update(extract_sections(fetch_pdf_text(url, HEADERS)))
    except Exception as error:
        print(f"   [warn] {url}: {error}")
        record.update({s: "" for s in LABEL_SECTIONS})
        record[WARNING_KEY] = str(error)
    return record


def _lower_key(text):
    return (text or "").strip().lower()


def apply_manual_label_sections(merged):
    """Overlay hand-written section text from config.MANUAL_LABEL_SECTIONS (keyed
    by medicine_name, case-insensitive) and set label_source = 'manual'/'automated'.
    Only the sections listed are overwritten; the rest keep their parsed text."""
    overrides = globals().get("MANUAL_LABEL_SECTIONS", {}) or {}
    overrides_by_key = {_lower_key(k): v for k, v in overrides.items()}
    source_col = globals().get("LABEL_SOURCE_COL", "label_source")

    sources = []
    for idx in merged.index:
        key = _lower_key(merged.at[idx, "medicine_name"])
        override = overrides_by_key.get(key)
        # A row can already be marked manual by an earlier step - the EU
        # transform sets it for a label URL taken from MANUAL_LABEL_URLS - and
        # overwriting that here would throw the fact away.
        already_manual = (source_col in merged.columns
                          and _lower_key(str(merged.at[idx, source_col])) == "manual")

        if override:
            for section, text in override.items():
                if section in LABEL_SECTIONS:
                    merged.at[idx, section] = "" if text is None else str(text)
            sources.append("manual")
        else:
            sources.append("manual" if already_manual else "automated")
    merged[source_col] = sources
    return merged


def order_merged_columns(merged):
    """Move the provenance columns (config.MERGED_TAIL_COLS) to the end."""
    tail = [c for c in globals().get("MERGED_TAIL_COLS", []) if c in merged.columns]
    front = [c for c in merged.columns if c not in tail]
    return merged[front + tail]


SECTION_COLUMNS = ["document_hash", "label_url"] + LABEL_SECTIONS


def load_previous_sections():
    """Load the per-PDF sections cache, or None when it must not be trusted.

    None means "read every PDF again", and is returned when:

      * there is no cache yet (the first run),
      * FORCE_PDF_REEXTRACT is on,
      * the cache is missing a column we now extract - that means a section was
        added to LABEL_SECTIONS since it was written, so every cached PDF is
        missing that section and has to be read again,
      * the file cannot be read.
    """
    if FORCE_PDF_REEXTRACT:
        print("[cache] FORCE_PDF_REEXTRACT is on: reading every label PDF again")
        return None

    if not os.path.exists(SECTIONS_FILE):
        print("[cache] no cached sections yet: reading every label PDF")
        return None

    try:
        prev = pd.read_csv(SECTIONS_FILE, dtype=str, keep_default_na=False)
    except Exception as error:
        print(f"[warn] could not read previous sections ({error}); reading every PDF")
        return None

    new_columns = [c for c in SECTION_COLUMNS if c not in prev.columns]
    if new_columns:
        print(f"[cache] these sections are new since the cache was written, so "
              f"every label PDF is read again: {new_columns}")
        return None

    return prev[SECTION_COLUMNS]


def current_label_pdfs():
    """The distinct label PDFs the current label table points at."""
    drug = pd.read_csv(EMA_TRANSFORMED_LABEL_FILE, dtype=str, keep_default_na=False)
    return (drug[drug["label_url"] != ""]
            [["document_hash", "label_url"]]
            .drop_duplicates("document_hash")
            .reset_index(drop=True))


def pending_hashes():
    """Label PDFs that still have no sections stored, so they need reading.

    A PDF that failed is never written to the cache, so it turns up here again
    on the next run even when nothing in the label table changed. The caller
    uses this to decide whether to run the PDF step at all.

    Reads only local files, so it is cheap enough to call before deciding.
    """
    previous = load_previous_sections()
    cached = set(previous["document_hash"]) if previous is not None else set()
    return set(current_label_pdfs()["document_hash"]) - cached


# The column holding the medicine name in the merged dataset.
MEDICINE_NAME_COLUMN = "Name of medicine"


def labels_with_no_sections():
    """Every medicine whose label produced no section text at all.

    Read from the merged dataset rather than from this run's extraction, so the
    list is complete even on a run where the PDF step was skipped.

    An empty list is returned when there is no merged dataset yet - the very
    first run - rather than raising, since a missing file there is normal.
    """
    try:
        merged = pd.read_csv(str(MERGED_CSV_FILE), dtype=str, keep_default_na=False)
    except (OSError, ValueError):
        return []

    columns = [column for column in LABEL_SECTIONS if column in merged.columns]
    if not columns:
        return []

    blank = []
    for _, row in merged.iterrows():
        if any(str(row[column]).strip() for column in columns):
            continue
        blank.append({
            "medicine": str(row.get(MEDICINE_NAME_COLUMN, "")),
            "label_url": str(row.get("label_url", "")),
            "document_hash": str(row.get("document_hash", "")),
        })
    return blank


def write_outputs(sections):
    """Write the per-PDF sections CSV, the merged CSV, and the merged JSON, given a
    sections DataFrame that covers all current label PDFs."""
    for col in SECTION_COLUMNS:
        if col not in sections.columns:
            sections[col] = ""
    sections = (sections[SECTION_COLUMNS]
                .drop_duplicates("document_hash").reset_index(drop=True))
    sections.to_csv(SECTIONS_FILE, index=False, encoding="utf-8")

    drug = pd.read_csv(EMA_TRANSFORMED_LABEL_FILE, dtype=str, keep_default_na=False)
    merged = drug.merge(sections.drop(columns=["label_url"]),
                        on="document_hash", how="left")
    for section in LABEL_SECTIONS:
        merged[section] = merged[section].fillna("")

    merged = apply_manual_label_sections(merged)
    merged = order_merged_columns(merged)

    merged.to_csv(MERGED_CSV_FILE, index=False, encoding="utf-8")
    write_merged_json(merged)
    return sections, merged


def write_merged_json(merged):
    """Write the merged dataset as JSON keyed by medicine_name. medicine_name is unique,
    but if a duplicate ever appears the values are collected into a list so
    nothing is dropped."""
    result = {}
    for row in merged.to_dict(orient="records"):
        key = row.get("medicine_name", "") or ""
        record = dict(row)
        if key in result:
            existing = result[key]
            result[key] = existing + [record] if isinstance(existing, list) else [existing, record]
        else:
            result[key] = record
    with open(MERGED_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def main(only_hashes=None):
    """Extract label sections and (re)build the merged dataset.

    The list of label PDFs comes from the versioned label file itself, and which
    of them to re-read comes from `only_hashes`, passed in memory by the caller.

    only_hashes:
      * None            -> extract every current label PDF (full run).
      * set of hashes   -> extract only those document_hashes; reuse the cached
                           sections (ema_label_sections.csv) for the rest.
                           An EMPTY set means "re-extract nothing, just rebuild the
                           merged dataset from the cache + current label table".
    Any current label PDF that has no cached sections yet is always extracted, so
    nothing is silently left blank.

    Returns a result dict for the run summary mail:
        extracted  how many PDFs were read this run
        reused      how many came from the cache
        warnings    one entry per PDF that could not be read
        total       how many distinct label PDFs the table holds
    """
    current_pdfs = current_label_pdfs()
    current_hashes = set(current_pdfs["document_hash"])

    previous = load_previous_sections()
    cached_hashes = set(previous["document_hash"]) if previous is not None else set()

    if only_hashes is None:
        to_process = current_pdfs
        retry_count = 0
    else:
        want = set(only_hashes)
        # A label is read either because it changed (it is in `want`), or because
        # there are no cached sections for it. The second case covers labels that
        # failed last run: failures are never written to the cache, so they come
        # back here on the next run until they succeed.
        changed_mask = current_pdfs["document_hash"].isin(want)
        not_cached_mask = ~current_pdfs["document_hash"].isin(cached_hashes)
        to_process = current_pdfs[changed_mask | not_cached_mask].reset_index(drop=True)

        # How many are being read only because they are missing from the cache.
        retry_count = int((not_cached_mask & ~changed_mask).sum())

    total = len(to_process)
    print(f"[start] extracting {total} of {len(current_pdfs)} label PDFs "
          f"({MAX_WORKERS} workers)")
    if retry_count:
        print(f"[start] {retry_count} of those have no cached sections yet "
              f"(new, or failed on an earlier run)")

    records = []
    if total:
        done = 0
        print("Pdf processing start time:", datetime.now().strftime("%H:%M:%S"))
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(process_pdf, row["document_hash"], row["label_url"]):
                       row["label_url"] for _, row in to_process.iterrows()}
            for future in as_completed(futures):
                records.append(future.result())
                done += 1
                if done % 10 == 0 or done == total:
                    print(f"[progress] {done}/{total}")
        print("Pdf processing end time:", datetime.now().strftime("%H:%M:%S"))

    # Split the results into the ones that worked and the ones that did not.
    warnings = [
        {"document_hash": record.get("document_hash", ""),
         "label_url": record.get("label_url", ""),
         "error": record[WARNING_KEY]}
        for record in records
        if record.get(WARNING_KEY)
    ]

    # Only successful records go into the sections cache. A failed PDF is left
    # out on purpose: with no cached sections it is picked up again by the
    # not_cached_mask above on the next run, so it keeps being retried until it
    # is read, instead of being remembered as permanently blank.
    good_records = [record for record in records if not record.get(WARNING_KEY)]
    for record in good_records:
        record.pop(WARNING_KEY, None)

    if warnings:
        print(f"[warn] {len(warnings)} of {total} label PDFs could not be extracted; "
              f"they stay out of the cache and will be retried next run")

    new_df = pd.DataFrame(good_records, columns=SECTION_COLUMNS)
    reprocessed = set(new_df["document_hash"]) if not new_df.empty else set()

    # Carry cached sections for current PDFs we did not re-extract successfully.
    # A PDF that failed this run is not in `reprocessed`, so if it had good
    # sections from an earlier run they are carried over rather than lost to a
    # one-off download problem.
    if previous is not None and not previous.empty:
        carry = previous[previous["document_hash"].isin(current_hashes - reprocessed)]
    else:
        carry = pd.DataFrame(columns=SECTION_COLUMNS)

    sections = pd.concat([new_df, carry], ignore_index=True)
    sections, merged = write_outputs(sections)
    print(f"[done] {len(sections)} PDF sections "
          f"({len(reprocessed)} re-extracted, {len(sections) - len(reprocessed)} reused) "
          f"-> {SECTIONS_FILE}; {len(merged)} merged rows -> {MERGED_CSV_FILE}")

    # How many current labels still have no sections at all. These are the ones
    # the next run will try again.
    still_missing = len(current_hashes - set(sections["document_hash"]))
    if still_missing:
        print(f"[warn] {still_missing} label(s) still have no sections; "
              f"the next run will retry them")

    table_failure_summary()

    return {
        "extracted": len(reprocessed),
        "reused": len(sections) - len(reprocessed),
        "warnings": warnings,
        "total": len(current_pdfs),
        "retried_not_cached": retry_count,
        "still_missing": still_missing,
    }


# Running this file on its own extracts every label PDF and prints a summary.
# A full run goes through main.py or the Glue job, which pass only the changed
# hashes and put these warnings into the run summary mail.
if __name__ == "__main__":
    result = main()
    for warning in result["warnings"]:
        print(f"[warn] {warning['label_url']}: {warning['error']}")