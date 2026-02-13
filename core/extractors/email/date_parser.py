"""
Consolidated date parsing utilities for the email extraction pipeline.

Handles:
- Full dates:  YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD, YYYY年MM月DD日
- Partial Chinese dates: 11月1日 (month + day, year inferred from email)
- Email header Date fields (RFC-2822 + fallback patterns)
- Year inference for cross-year scenarios
"""

from __future__ import annotations

import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional, Tuple

from core.extractors.email.config import (
    CN_FULL_DATE_RE,
    COMPACT_DATE_RE,
    HEADER_DATE_PATTERNS,
    ISO_DATE_RE,
    MONTH_MAP,
    PARTIAL_CN_DATE_RE,
)


class DateParser:
    """Stateless helper that unifies every date-parsing strategy used by
    the email extractor."""

    # ------------------------------------------------------------------
    # Full-date parsing
    # ------------------------------------------------------------------

    @staticmethod
    def parse_any_date(text: str) -> Optional[datetime]:
        """Parse common full-date formats.

        Supports: YYYY年MM月DD日, YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD.
        Returns *None* if none of the patterns match.
        """
        if not text:
            return None

        text = text.strip()

        # Pattern 1: YYYY年MM月DD日
        match = CN_FULL_DATE_RE.search(text)
        if match:
            try:
                return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            except ValueError:
                pass

        # Pattern 2: YYYY-MM-DD or YYYY/MM/DD
        match = ISO_DATE_RE.search(text)
        if match:
            try:
                return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            except ValueError:
                pass

        # Pattern 3: YYYYMMDD
        match = COMPACT_DATE_RE.search(text)
        if match:
            try:
                return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            except ValueError:
                pass

        return None

    # ------------------------------------------------------------------
    # Partial (month + day only) Chinese dates
    # ------------------------------------------------------------------

    @staticmethod
    def parse_partial_cn_date(text: str) -> Optional[Tuple[int, int]]:
        """Parse partial Chinese date (e.g. ``11月1日``).

        Returns ``(month, day)`` or *None*.
        """
        if not text:
            return None
        match = PARTIAL_CN_DATE_RE.search(text)
        if match:
            month = int(match.group(1))
            day = int(match.group(2))
            if 1 <= month <= 12 and 1 <= day <= 31:
                return (month, day)
        return None

    # ------------------------------------------------------------------
    # Year inference
    # ------------------------------------------------------------------

    @staticmethod
    def infer_year_from_email_date(email_dt: datetime, month: int) -> int:
        """Infer the year for a partial date using the email send date.

        Rule: default year = email_dt.year.
        Cross-year: email in Dec → target in Jan → next year.
        """
        year = email_dt.year
        if email_dt.month == 12 and month == 1:
            year += 1
        return year

    @staticmethod
    def infer_year_extended(email_dt: Optional[datetime], month: int) -> int:
        """Extended variant used inside regex leave-record extraction.

        More lenient: Q4 email → Q1 date → next year.
        """
        if not email_dt:
            return datetime.now().year
        year = email_dt.year
        if email_dt.month >= 10 and month <= 2:
            year += 1
        return year

    # ------------------------------------------------------------------
    # ISO helper
    # ------------------------------------------------------------------

    @staticmethod
    def to_iso_date(year: int, month: int, day: int) -> str:
        """Return ``YYYY-MM-DD`` string."""
        return f"{year:04d}-{month:02d}-{day:02d}"

    # ------------------------------------------------------------------
    # Email header Date
    # ------------------------------------------------------------------

    @staticmethod
    def parse_email_header_date(date_str: str) -> Optional[datetime]:
        """Parse RFC-2822 *Date* header with fallback patterns."""
        if not date_str:
            return None
        try:
            return parsedate_to_datetime(date_str)
        except Exception:
            pass

        for pat in HEADER_DATE_PATTERNS:
            match = re.search(pat, date_str, re.IGNORECASE)
            if match:
                groups = match.groups()
                try:
                    if len(groups) == 3 and groups[1] in MONTH_MAP:
                        return datetime(int(groups[2]), MONTH_MAP[groups[1]], int(groups[0]))
                    elif len(groups) == 3:
                        return datetime(int(groups[0]), int(groups[1]), int(groups[2]))
                except ValueError:
                    pass
        return None
