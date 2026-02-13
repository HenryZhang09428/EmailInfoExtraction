"""
DataCleaner: value normalisation utilities for the Excel pipeline.

Responsibilities:
- Cell-level string conversion (``cell_to_str``)
- Empty-cell detection
- Semantic value normalisation (dates, serial numbers)
- Header-text normalisation (compact form for matching)
- Month-start date parsing
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Optional

import pandas as pd


class DataCleaner:
    """Stateless helper that normalises raw cell values and header text."""

    # ----- cell → string ---------------------------------------------------

    @staticmethod
    def is_empty(value: Any) -> bool:
        if value is None or pd.isna(value):
            return True
        return str(value).strip() == ""

    @staticmethod
    def cell_to_str(value: Any) -> str:
        """Convert an arbitrary cell value to a clean string."""
        if value is None or pd.isna(value):
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (datetime, date, pd.Timestamp)):
            try:
                if isinstance(value, datetime):
                    return value.isoformat(sep=" ", timespec="seconds")
                return value.isoformat()
            except Exception:
                return str(value).strip()
        # Rich-text objects from openpyxl may expose .plain or .text
        plain_attr = getattr(value, "plain", None)
        if isinstance(plain_attr, str):
            return plain_attr.strip()
        text_attr = getattr(value, "text", None)
        if isinstance(text_attr, str):
            return text_attr.strip()
        text = str(value).strip()
        if text.lower() in {"nan", "none", "nat"}:
            return ""
        return text

    # ----- semantic value normalisation ------------------------------------

    @staticmethod
    def normalize_value(value: Any) -> str:
        """
        Normalise a cell value for record output.

        Handles date objects, Excel serial dates, and common Chinese / ISO
        date strings, falling back to plain-string conversion.
        """
        if value is None or pd.isna(value):
            return ""
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime().date().isoformat()
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return ""
            if 1 <= value <= 60000:
                try:
                    dt = pd.to_datetime(value, unit="d", origin="1899-12-30", errors="coerce")
                    if pd.notna(dt):
                        return dt.to_pydatetime().date().isoformat()
                except Exception:
                    pass
            return DataCleaner.cell_to_str(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return ""
            # YYYY-MM  or  YYYY/MM
            m = re.match(r"^\s*(\d{4})[./-](\d{1,2})\s*$", text)
            if m:
                year, month = int(m.group(1)), int(m.group(2))
                if 1 <= month <= 12:
                    return date(year, month, 1).isoformat()
            # YYYY-MM-DD
            m = re.match(r"^\s*(\d{4})[./-](\d{1,2})[./-](\d{1,2})\s*$", text)
            if m:
                year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
                try:
                    return date(year, month, day).isoformat()
                except ValueError:
                    return DataCleaner.cell_to_str(text)
            # 2024年3月15日
            m = re.match(r"^\s*(\d{4})\s*年\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?\s*$", text)
            if m:
                year, month = int(m.group(1)), int(m.group(2))
                day = int(m.group(3) or 1)
                try:
                    return date(year, month, day).isoformat()
                except ValueError:
                    return DataCleaner.cell_to_str(text)
            # Pure-digit Excel serial
            if text.isdigit():
                num = int(text)
                if 1 <= num <= 60000:
                    try:
                        dt = pd.to_datetime(num, unit="d", origin="1899-12-30", errors="coerce")
                        if pd.notna(dt):
                            return dt.to_pydatetime().date().isoformat()
                    except Exception:
                        pass
            return DataCleaner.cell_to_str(text)
        return DataCleaner.cell_to_str(value)

    # ----- month-start parsing (used by social-security profile) -----------

    @staticmethod
    def parse_month_start(value: Any) -> Optional[datetime]:
        """Parse a value into ``datetime(year, month, 1)`` or ``None``."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return datetime(value.year, value.month, 1)
        if isinstance(value, date):
            return datetime(value.year, value.month, 1)
        if isinstance(value, pd.Timestamp):
            return datetime(value.year, value.month, 1)
        text = str(value).strip()
        if not text:
            return None
        m = re.match(r"^\s*(\d{4})[./-](\d{1,2})\s*$", text)
        if m:
            year, month = int(m.group(1)), int(m.group(2))
            if 1 <= month <= 12:
                return datetime(year, month, 1)
        m = re.match(r"^\s*(\d{4})[./-](\d{1,2})[./-]\d{1,2}\s*$", text)
        if m:
            year, month = int(m.group(1)), int(m.group(2))
            if 1 <= month <= 12:
                return datetime(year, month, 1)
        m = re.match(r"^\s*(\d{4})\s*年\s*(\d{1,2})\s*月?\s*$", text)
        if m:
            year, month = int(m.group(1)), int(m.group(2))
            if 1 <= month <= 12:
                return datetime(year, month, 1)
        return None

    # ----- header text normalisation ---------------------------------------

    @staticmethod
    def normalize_header_compact(text: Any) -> str:
        """
        Collapse a header string into a compact, lower-case form
        suitable for keyword matching (removes parentheticals,
        punctuation, whitespace).
        """
        if text is None:
            return ""
        raw = str(text)
        raw = re.sub(r"\([^)]*\)", "", raw)
        raw = re.sub(r"（[^）]*）", "", raw)
        raw = raw.replace("／", "/").replace("\u3000", "")
        raw = re.sub(r"[\s]+", "", raw)
        raw = re.sub(r"[，,。.:：;；/\\\-\_|]+", "", raw)
        return raw.strip().lower()

    @staticmethod
    def normalize_header_for_semantic_key(text: Any) -> str:
        """
        Like :meth:`normalize_header_compact` but with an additional
        domain-specific substitution (pension insurance).
        """
        normed = DataCleaner.normalize_header_compact(text)
        if "参加保险情况养老" in normed:
            normed = normed.replace("参加保险情况养老", "参加保险情况")
        return normed
