"""
Robust CSV text parsing: preprocessing, delimiter detection, validation.
Used by the universal pipeline so rows are never collapsed into a single column.
"""

from __future__ import annotations

import csv
import io
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_MARKDOWN_MAILTO = re.compile(
    r"\[([^\]]*)\]\(\s*mailto:([^)]+)\)",
    re.IGNORECASE,
)


class CSVParsingError(ValueError):
    """CSV text could not be split into a valid multi-column table."""


def preprocess_csv_text(text: str) -> str:
    """
    Normalize newlines, strip outer whitespace, and replace markdown mailto links
    with the bare e-mail address (e.g. ``[a@b.com](mailto:a@b.com)`` → ``a@b.com``).
    """
    if not text:
        return text
    t = text.replace("\r\n", "\n").replace("\r", "\n")

    def _mailto_repl(m: re.Match[str]) -> str:
        addr = m.group(2).strip()
        return addr if addr else m.group(1).strip()

    t = _MARKDOWN_MAILTO.sub(_mailto_repl, t)
    return t.strip()


def _dict_rows_with_delimiter(text: str, delimiter: str) -> tuple[list[str], list[dict[str, Any]]] | None:
    """Return (columns, rows) or None if this delimiter does not yield ≥2 columns."""
    try:
        reader = csv.DictReader(
            io.StringIO(text),
            delimiter=delimiter,
            skipinitialspace=True,
        )
        raw_headers = reader.fieldnames
        if not raw_headers:
            return None
        columns = [str(f).strip() for f in raw_headers if f is not None and str(f).strip()]
        if len(columns) < 2:
            return None
        rows = list(reader)
        return columns, rows
    except Exception as ex:
        logger.debug("CSV try delimiter %r failed: %s", delimiter, ex)
        return None


def _sniff_and_parse(text: str) -> tuple[list[str], list[dict[str, Any]]] | None:
    sample = text[:8192] if len(text) > 8192 else text
    if not sample.strip():
        return None
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
    except csv.Error:
        return None
    try:
        reader = csv.DictReader(
            io.StringIO(text),
            dialect=dialect,
            skipinitialspace=True,
        )
        raw_headers = reader.fieldnames
        if not raw_headers:
            return None
        columns = [str(f).strip() for f in raw_headers if f is not None and str(f).strip()]
        if len(columns) < 2:
            return None
        return columns, list(reader)
    except Exception as ex:
        logger.debug("CSV sniff parse failed: %s", ex)
        return None


def parse_csv_text_to_rows(text: str) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Parse pre-decoded CSV/TSV text into header names and row dicts.

    Tries comma, semicolon, tab, pipe, then csv.Sniffer. Requires at least two columns.

    Raises:
        CSVParsingError: fewer than two columns for all strategies or empty input.
    """
    text = preprocess_csv_text(text)
    if not text.strip():
        raise CSVParsingError("CSV parsing failed: invalid structure")

    best: tuple[list[str], list[dict[str, Any]]] | None = None
    best_score = -1

    for delim in (",", ";", "\t", "|"):
        got = _dict_rows_with_delimiter(text, delim)
        if not got:
            continue
        cols, rows = got
        score = len(cols) * 1_000_000 + len(rows)
        if score > best_score:
            best_score = score
            best = got

    sniffed = _sniff_and_parse(text)
    if sniffed:
        cols, rows = sniffed
        score = len(cols) * 1_000_000 + len(rows)
        if score > best_score:
            best = sniffed

    if not best:
        raise CSVParsingError("CSV parsing failed: invalid structure")

    columns, all_rows = best
    if len(columns) < 2:
        raise CSVParsingError("CSV parsing failed: invalid structure")

    logger.info("CSV columns detected (%d): %s", len(columns), columns)
    if all_rows:
        first = {k: all_rows[0].get(k) for k in columns}
        logger.info("CSV first parsed row: %s", first)
    else:
        logger.info("CSV first parsed row: (no data rows)")

    return columns, all_rows
