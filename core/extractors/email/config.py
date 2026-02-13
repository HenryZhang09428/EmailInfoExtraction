"""
Centralised configuration for the email extraction pipeline.

All regex patterns, keyword lists, separator rules, and magic-number
thresholds live here.
"""

from __future__ import annotations

import re
from typing import List, Tuple

# ---------------------------------------------------------------------------
# Date patterns
# ---------------------------------------------------------------------------

CN_FULL_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
ISO_DATE_RE = re.compile(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})")
COMPACT_DATE_RE = re.compile(r"(\d{4})(\d{2})(\d{2})")
PARTIAL_CN_DATE_RE = re.compile(r"(\d{1,2})月(\d{1,2})日")

# ---------------------------------------------------------------------------
# Email header date fallback patterns
# ---------------------------------------------------------------------------

HEADER_DATE_PATTERNS: List[str] = [
    r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})",
    r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})",
]

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# ---------------------------------------------------------------------------
# Image detection
# ---------------------------------------------------------------------------

IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".svg"})

# ---------------------------------------------------------------------------
# Content-type → extension mapping (not reliably covered by mimetypes)
# ---------------------------------------------------------------------------

CONTENT_TYPE_EXTENSION_MAP = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "text/csv": ".csv",
    "application/csv": ".csv",
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/msword": ".doc",
}

# ---------------------------------------------------------------------------
# Thread truncation — explicit separator patterns (order = priority)
# ---------------------------------------------------------------------------

EXPLICIT_SEPARATORS: List[Tuple[str, str]] = [
    (r"^\s*-{3,}\s*原始邮件\s*-{3,}\s*$", "原始邮件_separator"),
    (r"^\s*-{3,}\s*Original\s+Message\s*-{3,}\s*$", "original_message_separator"),
    (r"^\s*-{3,}\s*Forwarded\s+message\s*-{3,}\s*$", "forwarded_message_separator"),
    (r"^\s*Begin\s+forwarded\s+message\s*[:：]?\s*$", "begin_forwarded"),
    (r"^\s*-{5,}\s*$", "dashes_separator"),
    (r"^\s*On\s.+wrote\s*[:：]\s*$", "on_wrote_separator"),
]

HEADER_FIELD_PATTERNS: List[str] = [
    r"^\s*发件人\s*[:：]",
    r"^\s*From\s*[:：]",
    r"^\s*发送时间\s*[:：]",
    r"^\s*Sent\s*[:：]",
    r"^\s*Date\s*[:：]",
    r"^\s*收件人\s*[:：]",
    r"^\s*To\s*[:：]",
    r"^\s*主题\s*[:：]",
    r"^\s*Subject\s*[:：]",
    r"^\s*抄送\s*[:：]",
    r"^\s*Cc\s*[:：]",
]

THREAD_WINDOW_SIZE = 10
THREAD_MIN_FIELDS = 3
THREAD_MIN_CHAR_POS = 100

# ---------------------------------------------------------------------------
# Leave / resignation detection
# ---------------------------------------------------------------------------

LEAVE_KEYWORDS: List[str] = [
    "离职", "申请离职", "离职生效", "离岗", "解除", "退场", "减员", "停保", "社保减员",
]

NAME_ID_RE = re.compile(r"[\u4e00-\u9fa5]{2,4}[（(]\d{4,}[）)]")
NAME_ID_CAPTURE_RE = re.compile(r"([\u4e00-\u9fa5]{2,4})[（(](\d{4,})[）)]")

LEAVE_KEYWORD_RE = re.compile("|".join(re.escape(kw) for kw in LEAVE_KEYWORDS))

LEAVE_DATE_PATTERNS = [
    re.compile(r"\d{1,2}月\d{1,2}日"),
    re.compile(r"\d{4}年\d{1,2}月\d{1,2}日"),
    re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}"),
]

CHINESE_NAME_RE = re.compile(r"^[\u4e00-\u9fa5]{2,4}$")
DIGIT_ID_RE = re.compile(r"^\d{4,}$")
ISO_DATE_STRICT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Regex date extraction patterns used in resignation detector
LEAVE_EXTRACT_DATE_PATTERNS = [
    (re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日"), "full_cn"),
    (re.compile(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})"), "iso"),
    (re.compile(r"(\d{1,2})月(\d{1,2})日"), "partial_cn"),
]

# ---------------------------------------------------------------------------
# Body text limits
# ---------------------------------------------------------------------------

DEFAULT_MAX_BODY_CHARS = 15000
