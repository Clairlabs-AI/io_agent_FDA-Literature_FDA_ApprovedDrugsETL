"""Every mail the pipeline sends goes out from here.

Two kinds of message:

  send_run_summary()  one mail at the end of a run, covering the versioning
                      result and the PDF extraction, including any PDF that
                      could not be read.

  send_alert()        a standalone mail sent the moment something fatal
                      happens, such as a failed download.

Everything under "Delivery" is the plumbing: settings come from config, and
boto3 is imported only when a message is actually published, so the pipeline
still runs on a machine with no AWS installed.
"""

import logging

from config_handler import config
from pipeline.eu import versioning

logger = logging.getLogger(__name__)

# How many failed PDFs to list before summarising the rest as "... and N more".
MAX_WARNINGS_LISTED = 30

# SNS limits. Subjects are hard-limited by AWS; messages are capped well under
# the 256 KB limit so a long list of changes can never fail the publish call.
MAX_SUBJECT_CHARACTERS = 100
MAX_MESSAGE_CHARACTERS = 200000

# Added to the end of a message that had to be cut short.
CUT_NOTE = "\n... (message cut short)"


# --------------------------------------------------------------------------- #
# The end-of-run summary
# --------------------------------------------------------------------------- #
def build_subject(version_report, pdf_result, blank_labels=None):
    """One short subject line saying what happened, e.g.
    '[EU-FDA Approved Drugs ETL] 3 changed, 2 new, 1 PDF failed'."""
    counts = version_report.get("counts", {})
    parts = []

    if counts.get("changed"):
        parts.append("{} changed".format(counts["changed"]))
    if counts.get("new"):
        parts.append("{} new".format(counts["new"]))
    if counts.get("removed"):
        parts.append("{} removed".format(counts["removed"]))

    failed = len(pdf_result.get("warnings", [])) if pdf_result else 0
    if failed:
        parts.append("{} PDF failed".format(failed))

    blank = len(blank_labels) if blank_labels else 0
    if blank:
        parts.append("{} label(s) blank".format(blank))

    if not parts:
        parts.append("no updates")

    return "{} {}".format(config.EMAIL_SUBJECT_PREFIX, ", ".join(parts))


def build_pdf_section(pdf_result):
    """The PDF-extraction part of the mail body."""
    if pdf_result is None:
        return [
            "PDF EXTRACTION",
            "--------------",
            "Skipped - no row changed this run, so no label needed re-reading.",
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

    for warning in warnings[:MAX_WARNINGS_LISTED]:
        lines.append("  - {}".format(warning.get("label_url", "(no url)")))
        lines.append("      hash  : {}".format(warning.get("document_hash", "")))
        lines.append("      reason: {}".format(warning.get("error", "")))

    if len(warnings) > MAX_WARNINGS_LISTED:
        lines.append("  ... and {} more".format(len(warnings) - MAX_WARNINGS_LISTED))

    lines.append("")
    return lines


def build_blank_labels_section(blank_labels, max_rows):
    """Medicines whose label came back with no section text at all.

    Always included, even when the PDF step did not run, so a label that has
    been empty for several runs keeps being raised until it is dealt with.
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


def build_body(version_report, pdf_result, run_timestamp, blank_labels=None):
    """The whole mail body: versioning summary first, then PDF extraction."""
    versioning_text = versioning.format_report_as_text(
        version_report,
        title=config.SNS_REPORT_TITLE,
        run_timestamp=run_timestamp,
        max_rows_per_section=config.SNS_MAX_ROWS_PER_SECTION,
    )

    return "\n".join([versioning_text, ""]
                     + build_pdf_section(pdf_result)
                     + build_blank_labels_section(blank_labels,
                                                  config.SNS_MAX_ROWS_PER_SECTION))


def send_run_summary(version_report, pdf_result, run_timestamp, blank_labels=None):
    """Build the combined summary and send it as one message.

    pdf_result is None when the PDF step was skipped because nothing changed.
    """
    subject = build_subject(version_report, pdf_result, blank_labels)
    body = build_body(version_report, pdf_result, run_timestamp, blank_labels)
    return send(subject, body)


# --------------------------------------------------------------------------- #
# Standalone alerts
# --------------------------------------------------------------------------- #
def send_alert(subject, body):
    """Send one message straight away, on its own.

    For failures that stop the run, where waiting for the end-of-run summary
    would mean no mail at all. The caller builds the text, because only the
    caller knows what went wrong.
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

    A False return is not an error the caller has to handle: the message has
    already been written to the log, which is what happens when SNS is switched
    off, the topic is not configured, or boto3 is missing.
    """
    subject = clean_subject(subject)
    body = shorten_message(body)

    logger.info("Notification:\n%s\n%s", subject, body)

    if not config.SNS_ENABLED:
        logger.info("SNS_ENABLED is not true, so nothing was sent.")
        return False

    if not config.SNS_TOPIC_ARN:
        logger.warning("SNS_ENABLED is true but SNS_TOPIC_ARN is empty, "
                       "so nothing was sent.")
        return False

    try:
        import boto3
    except ImportError:
        logger.warning("boto3 is not installed (pip install boto3), "
                       "so nothing was sent.")
        return False

    client = (boto3.client("sns", region_name=config.AWS_REGION)
              if config.AWS_REGION else boto3.client("sns"))
    response = client.publish(TopicArn=config.SNS_TOPIC_ARN,
                              Subject=subject, Message=body)

    logger.info("Sent SNS notification, message id %s", response.get("MessageId"))
    return True
