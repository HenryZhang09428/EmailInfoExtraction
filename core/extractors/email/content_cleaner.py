"""
Content cleaning and normalisation for email bodies.

Responsibilities:
- Strip HTML to plain text (scripts, styles, entities).
- Truncate quoted email threads (separators + header-block heuristic).
- Combine raw text / HTML into a single normalised body string.
"""

from __future__ import annotations

import html as _html
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from core.extractors.email.config import (
    DEFAULT_MAX_BODY_CHARS,
    EXPLICIT_SEPARATORS,
    HEADER_FIELD_PATTERNS,
    THREAD_MIN_CHAR_POS,
    THREAD_MIN_FIELDS,
    THREAD_WINDOW_SIZE,
)


class ContentCleaner:
    """Stateless utilities for cleaning / normalising email body text."""

    # ------------------------------------------------------------------
    # HTML → text
    # ------------------------------------------------------------------

    @staticmethod
    def strip_html_to_text(html_text: str) -> str:
        """Convert HTML to plain text, stripping scripts, styles and tags."""
        if not html_text:
            return ""
        text = html_text
        # Remove scripts / styles
        text = re.sub(r"<\s*script[^>]*>[\s\S]*?<\s*/\s*script\s*>", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"<\s*style[^>]*>[\s\S]*?<\s*/\s*style\s*>", " ", text, flags=re.IGNORECASE)
        # Common line-break tags
        text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", text)
        text = re.sub(r"(?i)</\s*p\s*>", "\n", text)
        # Strip remaining tags
        text = re.sub(r"<[^>]+>", " ", text)
        text = _html.unescape(text)
        # Normalise whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ------------------------------------------------------------------
    # Thread truncation
    # ------------------------------------------------------------------

    @staticmethod
    def truncate_email_thread(text: str) -> Tuple[str, dict]:
        """Keep only the latest message, heuristically cutting quoted thread.

        Priority:
        1. Explicit separators (``--- 原始邮件 ---``, etc.)
        2. Quoted header block (3+ email header fields within 10 lines)

        Returns ``(truncated_text, truncation_meta)``.
        """
        if not text:
            return "", {"truncated": False, "rule": None, "marker": None}

        lines = text.split("\n")

        # --- Pass 1: explicit separators ---
        for line_idx, line in enumerate(lines):
            for pattern, marker_name in EXPLICIT_SEPARATORS:
                if re.match(pattern, line, flags=re.IGNORECASE):
                    char_pos = sum(len(l) + 1 for l in lines[:line_idx])
                    if char_pos > 0:
                        return text[:char_pos].strip(), {
                            "truncated": True,
                            "rule": "explicit_separator",
                            "marker": marker_name,
                            "pattern": pattern,
                            "line_index": line_idx,
                        }

        # --- Pass 2: quoted header block ---
        for start_idx in range(len(lines)):
            end_idx = min(start_idx + THREAD_WINDOW_SIZE, len(lines))
            window_lines = lines[start_idx:end_idx]

            matched_fields: set = set()
            for win_line in window_lines:
                for hp in HEADER_FIELD_PATTERNS:
                    if re.match(hp, win_line, flags=re.IGNORECASE):
                        field_name = hp.split(r"\s*[:：]")[0].replace(r"^\s*", "").strip("\\")
                        matched_fields.add(field_name.lower())
                        break

            if len(matched_fields) >= THREAD_MIN_FIELDS:
                char_pos = sum(len(l) + 1 for l in lines[:start_idx])
                if char_pos > THREAD_MIN_CHAR_POS:
                    return text[:char_pos].strip(), {
                        "truncated": True,
                        "rule": "quoted_header_block",
                        "marker": f"header_block_{len(matched_fields)}_fields",
                        "fields_found": list(matched_fields),
                        "line_index": start_idx,
                        "window_size": THREAD_WINDOW_SIZE,
                    }

        return text.strip(), {"truncated": False, "rule": None, "marker": None}

    # ------------------------------------------------------------------
    # Body normalisation
    # ------------------------------------------------------------------

    @classmethod
    def normalize_body_text(
        cls,
        eml_data: dict,
        max_chars: int = DEFAULT_MAX_BODY_CHARS,
    ) -> Tuple[str, dict]:
        """Produce a single normalised body string from an ``eml_data`` dict.

        Prefers ``text/plain``; falls back to converted ``text/html``.
        Truncates quoted thread and enforces *max_chars*.

        Returns ``(text, meta_dict)``.
        """
        raw_text = (eml_data.get("bodies") or {}).get("text")
        raw_html = (eml_data.get("bodies") or {}).get("html")
        used = "text/plain" if raw_text else ("text/html" if raw_html else "none")
        base = raw_text or cls.strip_html_to_text(raw_html or "")

        truncated_thread, truncation_meta = cls.truncate_email_thread(base)
        truncated_to_max = False
        text = truncated_thread
        if len(text) > max_chars:
            text = text[:max_chars]
            truncated_to_max = True

        meta: Dict[str, Any] = {
            "used_body": used,
            "thread_truncated": truncation_meta.get("truncated", False),
            "thread_truncation_rule": truncation_meta.get("rule"),
            "thread_marker": truncation_meta.get("marker"),
            "max_chars": max_chars,
            "truncated_to_max_chars": truncated_to_max,
            "char_count": len(text),
        }
        if truncation_meta.get("fields_found"):
            meta["thread_fields_found"] = truncation_meta["fields_found"]
        return text, meta

    # ------------------------------------------------------------------
    # Shape coercion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def coerce_body_extracted_shape(
        obj: Any,
        body_meta: dict,
        warnings: List[str],
    ) -> dict:
        """Ensure ``{"data": [...], "metadata": {...}, "warnings": [...]}``.

        Never raises; always returns a dict in the required shape.
        """
        if obj is None:
            warnings.append("body_extracted: llm returned None")
            return {"data": [], "metadata": dict(body_meta or {}), "warnings": list(warnings)}
        if isinstance(obj, list):
            warnings.append("body_extracted: llm returned array; wrapped into {data: ...}")
            return {"data": obj, "metadata": dict(body_meta or {}), "warnings": list(warnings)}
        if not isinstance(obj, dict):
            warnings.append(f"body_extracted: llm returned {type(obj).__name__}; wrapped into metadata.raw_output")
            return {
                "data": [],
                "metadata": {**dict(body_meta or {}), "raw_output": str(obj)[:2000]},
                "warnings": list(warnings),
            }
        data = obj.get("data", [])
        meta = obj.get("metadata", {})
        w = obj.get("warnings", [])
        if not isinstance(data, list):
            warnings.append("body_extracted: 'data' is not a list; replaced with []")
            data = []
        if not isinstance(meta, dict):
            warnings.append("body_extracted: 'metadata' is not an object; replaced with {}")
            meta = {}
        if not isinstance(w, list):
            warnings.append("body_extracted: 'warnings' is not a list; replaced with []")
            w = []
        merged_meta = dict(body_meta or {})
        merged_meta.update(meta)
        merged_warnings: List[str] = []
        for msg in list(warnings) + [str(x) for x in w if x is not None]:
            if msg and msg not in merged_warnings:
                merged_warnings.append(msg)
        return {"data": data, "metadata": merged_meta, "warnings": merged_warnings}

    @staticmethod
    def stringify_record_values(record: Optional[dict]) -> dict:
        """Convert every value in *record* to a string."""

        def _to_str(v: Any) -> str:
            if v is None:
                return ""
            if isinstance(v, str):
                return v
            if isinstance(v, (int, float, bool)):
                return str(v)
            try:
                return json.dumps(v, ensure_ascii=False)
            except Exception:
                return str(v)

        out: Dict[str, str] = {}
        for k, v in (record or {}).items():
            if not isinstance(k, str) or not k.strip():
                continue
            out[k] = _to_str(v)
        return out
