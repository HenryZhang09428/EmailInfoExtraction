"""
Centralised configuration for the Excel extraction pipeline.

All magic numbers, regex patterns, keyword lists, and tunable thresholds
live here so that the rest of the code can stay free of hard-coded values.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import FrozenSet, Tuple


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Pre-compiled regex patterns (shared across modules)
# ---------------------------------------------------------------------------

DATE_LIKE_RE = re.compile(
    r"^\s*\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\s*$"
)
PURE_NUMBER_RE = re.compile(r"^\s*-?\d+(?:\.\d+)?\s*$")
LONG_DIGITS_RE = re.compile(r"^\s*\d{6,}\s*$")
ALPHA_LIKE_RE = re.compile(r"[A-Za-z\u4e00-\u9fff]")


# ---------------------------------------------------------------------------
# Keyword / intent constants
# ---------------------------------------------------------------------------

REMOVE_INTENT_KEYWORDS: Tuple[str, ...] = (
    "减员", "离职", "退工", "退保", "终止", "停保",
)

TERMINATION_KEYS: FrozenSet[str] = frozenset({
    "termination_date", "terminationdate",
    "leave_date", "leavedate",
    "end_date", "enddate",
})


# ---------------------------------------------------------------------------
# ExtractorConfig — tunable thresholds
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExtractorConfig:
    """Immutable bag of tunable thresholds used throughout extraction."""

    # Row / record limits
    max_rows_to_process: int = 1000
    max_records_per_workbook: int = _env_int("EXCEL_MAX_RECORDS_PER_WORKBOOK", 5000)
    max_csv_chars: int = 100_000

    # Sampling strategy
    min_rows_for_sampling: int = 500
    fast_rows_threshold: int = 30
    sample_head_rows: int = 10
    sample_spread_rows: int = 10
    sample_max_per_column: int = 8

    # Schema inference
    schema_infer_coverage_threshold: float = 0.5

    # Header detection — scoring weights
    header_text_weight: float = 3.5
    header_long_digit_penalty: float = 2.5
    header_date_penalty: float = 2.0
    header_numeric_penalty: float = 1.5
    header_fill_bonus: float = 0.2

    # Header detection — _is_header_like thresholds
    header_min_text_ratio: float = 0.35
    header_max_long_digit_ratio: float = 0.02
    header_max_date_ratio: float = 0.02
    header_max_numeric_ratio: float = 0.15

    # Header scan depth
    header_scan_rows: int = 20


# Singleton default config
DEFAULT_CONFIG = ExtractorConfig()
