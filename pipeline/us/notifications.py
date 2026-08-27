"""Every mail the pipeline sends goes out from here.

Two kinds of message:

  send_run_summary()  One mail at the end of a successful run, covering the
                      drug-table changes, the openFDA calls, and the PDF
                      extraction, including any PDF that could not be read.
                      Sent once all steps are done.

  send_alert()        A standalone mail sent the moment something fatal
                      happens, independent of the run summary.
"""

import logging

from config_handler import config

logger = logging.getLogger(__name__)

# How many rows to list per section before summarising the rest as "... and N more".
MAX_ROWS_LISTED = config.SNS_MAX_ROWS_PER_SECTION

# SNS limits. Subjects are hard-limited by AWS; messages are capped well under
# the 256 KB limit so a long list of changes can never fail the publish call.
MAX_SUBJECT_CHARACTERS = 100
MAX_MESSAGE_CHARACTERS = 200000

# Added to the end of a message that had to be cut short.
CUT_NOTE = "\n... (message cut short)"


# --------------------------------------------------------------------------- #
# The end-of-run summary
# --------------------------------------------------------------------------- #
def build_subject(changes, pdf_result=None, api_result=None, blank_labels=None):
    """One short subject line saying what happened, e.g.
    '[US-FDA Approved Drugs ETL] 3 updated, 2 new, 1 PDF failed'."""
    changed_rows = [c for c in changes if c["type"] == "changed"]
    new_rows = [c for c in changes if c["type"] == "new"]

    parts = []
    if changed_rows:
        parts.append("{} updated".format(len(changed_rows)))
    if new_rows:
        parts.append("{} new".format(len(new_rows)))

    failed_pdfs = len(pdf_result.get("warnings", [])) if pdf_result else 0
    if failed_pdfs:
        parts.append("{} PDF failed".format(failed_pdfs))

    failed_api = len(api_result.get("failures", [])) if api_result else 0
    if failed_api:
        parts.append("{} API batch failed".format(failed_api))

    blank = len(blank_labels) if blank_labels else 0
    if blank:
        parts.append("{} label(s) blank".format(blank))

    if not parts:
        parts.append("no updates")

    return "{} {}".format(config.EMAIL_SUBJECT_PREFIX, ", ".join(parts))


def build_changes_section(changes):
    """What changed in the drug table this run."""
    changed_rows = [c for c in changes if c["type"] == "changed"]
    new_rows = [c for c in changes if c["type"] == "new"]

    lines = [
        "DRUG TABLE",
        "----------",
        "Rows changed : {}".format(len(changed_rows)),
        "Rows added   : {}".format(len(new_rows)),
        "",
    ]

    if not changes:
        lines.append("No field updates and no new rows.")
        lines.append("")
        return lines

    if changed_rows:
        lines.append("CHANGED ROWS ({})".format(len(changed_rows)))
        for change in changed_rows[:MAX_ROWS_LISTED]:
            lines.append("  - {}  (biomarker: {})".format(change["drug"], change["biomarker"]))
            for column, (old, new) in change["fields"].items():
                old_text = (old[:80] + "...") if len(old) > 80 else old
                new_text = (new[:80] + "...") if len(new) > 80 else new
                lines.append("      {}: '{}'  ->  '{}'".format(column, old_text, new_text))
        if len(changed_rows) > MAX_ROWS_LISTED:
            lines.append("  ... and {} more".format(len(changed_rows) - MAX_ROWS_LISTED))
        lines.append("")

    if new_rows:
        lines.append("NEW ROWS ({})".format(len(new_rows)))
        for change in new_rows[:MAX_ROWS_LISTED]:
            lines.append("  - {}  (biomarker: {})".format(change["drug"], change["biomarker"]))
        if len(new_rows) > MAX_ROWS_LISTED:
            lines.append("  ... and {} more".format(len(new_rows) - MAX_ROWS_LISTED))
        lines.append("")

    return lines


def build_api_section(api_result):
    """How the openFDA calls went."""
    if api_result is None:
        return []

    failures = api_result.get("failures", [])
    lines = [
        "OPENFDA",
        "-------",
        "Batches called : {}".format(api_result.get("batches", 0)),
        "Batches failed : {}".format(len(failures)),
        "",
    ]

    if not failures:
        lines.append("Every openFDA batch answered.")
        lines.append("")
        return lines

    lines.append("These batches could not be fetched, so the drugs in them have no")
    lines.append("openFDA data this run and will be queried again next run:")
    lines.append("")
    for failure in failures[:MAX_ROWS_LISTED]:
        lines.append("  - batch {}: {}".format(failure.get("batch", "?"), failure.get("error", "")))
        names = failure.get("drug_names") or []
        if names:
            shown = ", ".join(names[:8])
            more = " (+{} more)".format(len(names) - 8) if len(names) > 8 else ""
            lines.append("      drugs: {}{}".format(shown, more))
    if len(failures) > MAX_ROWS_LISTED:
        lines.append("  ... and {} more".format(len(failures) - MAX_ROWS_LISTED))
    lines.append("")
    return lines


def build_pdf_section(pdf_result):
    """The PDF-extraction part of the mail body."""
    if pdf_result is None:
        return [
            "PDF EXTRACTION",
            "--------------",
            "Skipped - nothing changed and nothing was pending, so no label needed reading.",
            "",
        ]

    lines = [
        "PDF EXTRACTION",
        "--------------",
        "Labels extracted this run : {}".format(pdf_result.get("extracted", 0)),
        "Reused from the cache     : {}".format(pdf_result.get("reused", 0)),
        "Read again after a miss   : {}".format(pdf_result.get("retried_not_cached", 0)),
        "Failed                    : {}".format(len(pdf_result.get("warnings", []))),
        "Still without sections    : {}".format(pdf_result.get("still_missing", 0)),
        "",
    ]

    warnings = pdf_result.get("warnings", [])
    if not warnings:
        lines.append("Every label PDF was read successfully.")
        lines.append("")
        return lines

    lines.append("These label PDFs could not be extracted, and every download was")
    lines.append("retried before giving up. They are NOT saved as blank, so the next")
    lines.append("run reads them again; their section columns stay empty until then:")
    lines.append("")

    for warning in warnings[:MAX_ROWS_LISTED]:
        lines.append("  - {}".format(warning.get("label_url", "(no url)")))
        lines.append("      hash  : {}".format(warning.get("document_hash", "")))
        lines.append("      reason: {}".format(warning.get("error", "")))

    if len(warnings) > MAX_ROWS_LISTED:
        lines.append("  ... and {} more".format(len(warnings) - MAX_ROWS_LISTED))

    lines.append("")
    return lines


def build_blank_labels_section(blank_labels, max_rows):
    """Medicines whose label came back with no section text at all.

    Always included, even when the PDF step did not run, so an empty label
    keeps being raised until it is fixed.
    """
    if blank_labels is None:
        return []

    lines = [
        "LABELS WITH NO SECTIONS",
        "-----------------------",
        "Medicines affected : {}".format(len(blank_labels)),
        "",
    ]

    if not blank_labels:
        lines.append("Every medicine has at least one section filled in.")
        lines.append("")
        return lines

    lines.append("Nothing at all was extracted for these labels. They are listed")
    lines.append("every run, changed or not, until they are fixed:")
    lines.append("")

    for item in blank_labels[:max_rows]:
        lines.append("  - {}".format(item.get("medicine") or "(no name)"))
        lines.append("      url : {}".format(item.get("label_url", "")))
        lines.append("      hash: {}".format(item.get("document_hash", "")))

    if len(blank_labels) > max_rows:
        lines.append("  ... and {} more".format(len(blank_labels) - max_rows))

    lines.append("")
    return lines


def build_body(changes, pdf_result=None, api_result=None, run_timestamp="",
               blank_labels=None):
    """The whole mail body: table changes, then openFDA, then PDF extraction."""
    lines = [config.SNS_REPORT_TITLE, "Run at: " + str(run_timestamp), ""]
    lines += build_changes_section(changes)
    lines += build_api_section(api_result)
    lines += build_pdf_section(pdf_result)
    lines += build_blank_labels_section(blank_labels, MAX_ROWS_LISTED)
    return "\n".join(lines)


def send_run_summary(changes, pdf_result=None, api_result=None, run_timestamp="",
                     blank_labels=None):
    """Build the combined summary and send it as one message.

    pdf_result is None when the PDF step was skipped.
    """
    subject = build_subject(changes, pdf_result, api_result, blank_labels)
    body = build_body(changes, pdf_result, api_result, run_timestamp, blank_labels)
    return send(subject, body)


# --------------------------------------------------------------------------- #
# Standalone alerts
# --------------------------------------------------------------------------- #
def send_alert(subject, body):
    """Send one message straight away, on its own.

    Used for failures that stop the run. The caller builds the subject and
    body, since only the caller knows what went wrong.
    """
    return send(subject, body)


# --------------------------------------------------------------------------- #
# Delivery
# --------------------------------------------------------------------------- #
def clean_subject(subject):
    """Make a subject SNS will accept: one line, at most 100 characters."""
    one_line = " ".join(str(subject).split())
    return one_line[:MAX_SUBJECT_CHARACTERS]


def shorten_message(text):
    """Cut a message down to the size limit and say so if anything was cut."""
    if len(text) <= MAX_MESSAGE_CHARACTERS:
        return text
    return text[: MAX_MESSAGE_CHARACTERS - len(CUT_NOTE)] + CUT_NOTE


def send(subject, body):
    """Publish one message to the SNS topic. Returns True if it was sent.

    Returns False when SNS is disabled, not configured, or boto3 is missing;
    the message is still written to the log in that case.
    """
    subject = clean_subject(subject)
    body = shorten_message(body)

    logger.info("Notification:\n%s\n%s", subject, body)
    print("[notify] {}\n{}".format(subject, body))

    if not config.SNS_ENABLED:
        print("[notify] SNS_ENABLED is not true, so nothing was sent.")
        return False

    if not config.SNS_TOPIC_ARN:
        print("[notify] SNS_ENABLED is true but SNS_TOPIC_ARN is empty, so nothing was sent.")
        return False

    try:
        import boto3
    except ImportError:
        print("[notify] boto3 is not installed, so nothing was sent.")
        return False

    try:
        client = (boto3.client("sns", region_name=config.AWS_REGION)
                  if config.AWS_REGION else boto3.client("sns"))
        response = client.publish(TopicArn=config.SNS_TOPIC_ARN,
                                  Subject=subject, Message=body)
        print("[notify] published to {}, message id {}".format(
            config.SNS_TOPIC_ARN, response.get("MessageId")))
        return True
    except Exception as error:
        print("[notify] publish failed ({}); the summary above was logged instead".format(error))
        return False
