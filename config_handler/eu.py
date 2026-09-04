"""Settings for the EU pipeline - EMA approved drugs.

Everything here is EMA's own: where the source files live, which sections an
SmPC has, and which columns identify a medicine across runs. Everything shared
with the US pipeline comes from config_handler.common.
"""

from config_handler import paths
from config_handler.common import *          # noqa: F401,F403

REGION = "eu"

# --- Source URLs ------------------------------------------------------------
BASE_EMA_URL = "https://www.ema.europa.eu/"
EMA_EXCEL_DOWNLOAD_PATH = "en/documents/report/medicines-output-medicines-report_en.xlsx"
EMA_JSON_DOWNLOAD_PATH = "en/documents/report/documents-output-json-report_en.json"

EMA_EXCEL_URL = BASE_EMA_URL + EMA_EXCEL_DOWNLOAD_PATH
EMA_JSON_URL = BASE_EMA_URL + EMA_JSON_DOWNLOAD_PATH

# --- Files, by layer --------------------------------------------------------
# bronze  exactly what EMA served us
# silver  parsed, filtered and versioned
# gold    the joined dataset downstream reads
EMA_EXCEL_FILE = paths.local(paths.BRONZE, "ema_medicines_report.xlsx", REGION)
EMA_JSON_FILE = paths.local(paths.BRONZE, "ema_documents_report.json", REGION)

EMA_TRANSFORMED_CSV_FILE = paths.local(paths.SILVER, "ema_medicines_table.csv", REGION)
EMA_TRANSFORMED_LABEL_FILE = paths.local(paths.SILVER, "ema_label_index.csv", REGION)
SECTIONS_FILE = paths.local(paths.SILVER, "ema_label_sections.csv", REGION)

MERGED_CSV_FILE = paths.local(paths.GOLD, "EU_FDA_oncology_drug_label_dataset.csv", REGION)
MERGED_JSON_FILE = paths.local(paths.GOLD, "EU_FDA_oncology_drug_label_dataset.json", REGION)

# What each step uploads, paired with the layer it belongs to. The Glue job
# walks these, so adding an output is a one-line change here.
UPLOADS_AFTER_TRANSFORM = [
    (paths.BRONZE, EMA_EXCEL_FILE),
    (paths.BRONZE, EMA_JSON_FILE),
    (paths.SILVER, EMA_TRANSFORMED_CSV_FILE),
    (paths.SILVER, EMA_TRANSFORMED_LABEL_FILE),
]
UPLOADS_AFTER_PDF = [
    (paths.SILVER, SECTIONS_FILE),
    (paths.GOLD, MERGED_CSV_FILE),
    (paths.GOLD, MERGED_JSON_FILE),
]

# Empty: EU compares every column except the timestamp when versioning rows.
OUT_COLS_DRUG_TABLE = []

# --- Filtering --------------------------------------------------------------
CANCER_ALIASES = [
    "cancer",
    "carcinoma",
    "malignant",
    "blastoma",
    "neoplasm",
    "tumor",
    "Hodgkin Disease",
    "Leukemia",
    "Lymphoma",
    "Multiple Myeloma",
    "Melanoma",
    "Sarcoma",
    "Gastrointestinal Stromal Tumors",
]

# The column that marks the real header row inside the EMA workbook.
EXCEL_HEADER_MARKER_COLUMN = "Name of medicine"

# --- Versioning -------------------------------------------------------------
# Which columns say "this is the same row as last run". Every OTHER column is
# then compared to decide whether the row changed.
VERSION_KEY_COLUMNS = ["Name of medicine", "EMA product number"]

# A change in any of these means the label itself changed, so the old row is
# kept as the one previous version and the new hash needs PDF extraction.
LABEL_COLUMNS = ["label_url", "document_hash"]

LABEL_HASH_COLUMN = "document_hash"
TIMESTAMP_COLUMN = "retrieved_at"

# --- Notifications ----------------------------------------------------------
SNS_REPORT_TITLE = "EMA approved drugs - label versioning summary"
EMAIL_SUBJECT_PREFIX = setting("EU_EMAIL_SUBJECT_PREFIX", "[EU-FDA Approved Drugs ETL]")
SNS_DOWNLOAD_ERROR_SUBJECT = setting(
    "EU_FAILED_EMAIL_SUBJECT_PREFIX",
    "[EU-FDA Approved Drugs ETL] pipeline FAILED - could not download source file")

# --- Manual overrides -------------------------------------------------------
MANUAL_LABEL_URLS = {
    "Blenrep": "https://www.ema.europa.eu/en/documents/product-information/blenrep-epar-product-information_en.pdf",
    "Lenalidomide Krka d.d. Novo mesto (previously Lenalidomide Krka)": "https://www.ema.europa.eu/en/documents/product-information/lenalidomide-krka-dd-epar-product-information_en.pdf",
    "Neofordex": "https://www.ema.europa.eu/en/documents/product-information/neofordex-epar-product-information_en.pdf",
    "Rybrevant": "https://www.ema.europa.eu/en/documents/product-information/rybrevant-epar-product-information_en.pdf",
    "Tibsovo": "https://www.ema.europa.eu/en/documents/product-information/tibsovo-epar-product-information_en.pdf",
    "Tuznue": "https://www.ema.europa.eu/en/documents/product-information/tuznue-epar-product-information_en.pdf",
    "Zynyz": "https://www.ema.europa.eu/en/documents/product-information/zynyz-epar-product-information_en.pdf"
}
