"""
HeaderDetector: identify which row(s) in an Excel sheet are headers.

Uses structural heuristics (text-vs-numeric ratios, date patterns,
long-digit patterns) rather than hard-coded field names, so it
generalises across different HR spreadsheet layouts.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.extractors.excel.config import (
    DATE_LIKE_RE,
    LONG_DIGITS_RE,
    PURE_NUMBER_RE,
    ALPHA_LIKE_RE,
    ExtractorConfig,
    DEFAULT_CONFIG,
)
from core.extractors.excel.data_cleaner import DataCleaner


class HeaderDetector:
    """
    Stateless detector that scores rows and identifies the best header row.

    All public methods are class-level or static; no per-instance state
    is required.  An :class:`ExtractorConfig` can be passed in to
    override default thresholds.
    """

    def __init__(self, cfg: ExtractorConfig = DEFAULT_CONFIG):
        self._cfg = cfg

    # -----------------------------------------------------------------
    # Row feature extraction
    # -----------------------------------------------------------------

    @staticmethod
    def extract_row_features(cells: List[str]) -> Dict[str, float]:
        """Compute structural feature ratios for a single row."""
        values = [str(v).strip() for v in (cells or [])]
        if not values:
            return {
                "non_empty_ratio": 0.0,
                "text_ratio": 0.0,
                "long_digit_ratio": 0.0,
                "date_ratio": 0.0,
                "numeric_ratio": 0.0,
            }
        total = len(values)
        non_empty = [v for v in values if v]
        text_like = long_digit = date_like = numeric_like = 0
        for v in non_empty:
            if DATE_LIKE_RE.match(v):
                date_like += 1
                numeric_like += 1
                continue
            if LONG_DIGITS_RE.match(v):
                long_digit += 1
                numeric_like += 1
                continue
            if PURE_NUMBER_RE.match(v):
                numeric_like += 1
                continue
            if ALPHA_LIKE_RE.search(v) and not re.search(r"\d", v):
                text_like += 1
        denom = max(1, len(non_empty))
        return {
            "non_empty_ratio": len(non_empty) / max(1, total),
            "text_ratio": text_like / denom,
            "long_digit_ratio": long_digit / denom,
            "date_ratio": date_like / denom,
            "numeric_ratio": numeric_like / denom,
        }

    # -----------------------------------------------------------------
    # Scoring
    # -----------------------------------------------------------------

    def header_score(self, features: Dict[str, float]) -> float:
        """
        Compute a scalar header-likelihood score.

        Positive = header-like, negative = data-like.
        """
        c = self._cfg
        return (
            c.header_text_weight * features.get("text_ratio", 0.0)
            - c.header_long_digit_penalty * features.get("long_digit_ratio", 0.0)
            - c.header_date_penalty * features.get("date_ratio", 0.0)
            - c.header_numeric_penalty * features.get("numeric_ratio", 0.0)
            + c.header_fill_bonus * features.get("non_empty_ratio", 0.0)
        )

    def is_header_like(self, features: Dict[str, float]) -> bool:
        """Return ``True`` if *features* pass the header thresholds."""
        c = self._cfg
        return (
            features.get("text_ratio", 0.0) >= c.header_min_text_ratio
            and features.get("long_digit_ratio", 0.0) <= c.header_max_long_digit_ratio
            and features.get("date_ratio", 0.0) <= c.header_max_date_ratio
            and features.get("numeric_ratio", 0.0) <= c.header_max_numeric_ratio
        )

    # -----------------------------------------------------------------
    # Legacy heuristic kept for choose_best_sheet fast-path
    # -----------------------------------------------------------------

    @staticmethod
    def looks_like_header_row(cells: List[str]) -> bool:
        """
        Quick boolean heuristic deciding if a row "looks like a header".

        Uses hard-coded ratio thresholds suitable for the fast sheet-
        selection scan.
        """
        if not cells:
            return False
        values = [str(v).strip() for v in cells]
        non_empty = [v for v in values if v]
        if not non_empty:
            return False
        n = len(non_empty)
        fill_ratio = n / max(1, len(values))
        date_like = sum(1 for v in non_empty if DATE_LIKE_RE.match(v))
        long_digits = sum(1 for v in non_empty if LONG_DIGITS_RE.match(v))
        pure_number = sum(1 for v in non_empty if PURE_NUMBER_RE.match(v))
        alpha_like = sum(1 for v in non_empty if ALPHA_LIKE_RE.search(v))
        date_ratio = date_like / n
        long_digits_ratio = long_digits / n
        pure_number_ratio = pure_number / n
        alpha_ratio = alpha_like / n
        if date_like >= 2:
            return False
        if fill_ratio >= 0.85 and (date_like > 0 or long_digits > 0) and n >= 6:
            return False
        if date_ratio >= 0.3:
            return False
        if long_digits_ratio >= 0.3:
            return False
        if pure_number_ratio >= 0.6 and alpha_ratio < 0.4:
            return False
        if alpha_ratio < 0.25 and (date_like > 0 or long_digits > 0):
            return False
        return True

    # -----------------------------------------------------------------
    # Header row selection
    # -----------------------------------------------------------------

    def select_header_row_index(self, df: pd.DataFrame) -> Tuple[int, Dict[str, Any]]:
        """
        Scan the first *N* rows and return ``(best_idx, debug_info)``.

        Returns ``-1`` when no plausible header row is found.
        """
        total = len(df)
        if total == 0:
            return -1, {
                "scanned_rows": [],
                "chosen_header_row_idx": -1,
                "chosen_reason": "no_header_row_detected",
            }
        scan = min(self._cfg.header_scan_rows, total)
        num_cols = df.shape[1] if df is not None and len(df.shape) > 1 else 0
        scanned: List[dict] = []
        candidates: List[Tuple[int, float]] = []

        for i in range(scan):
            row_str = [DataCleaner.cell_to_str(c) for c in df.iloc[i].tolist()]
            feats = self.extract_row_features(row_str)
            score = self.header_score(feats)
            # preview (up to 12 non-empty cells)
            raw_preview: List[Tuple[str, str]] = []
            str_preview: List[str] = []
            for col in range(num_cols):
                rv = df.iat[i, col]
                if rv is None or pd.isna(rv):
                    continue
                if isinstance(rv, str) and rv == "":
                    continue
                raw_preview.append((repr(rv), type(rv).__name__))
                str_preview.append(DataCleaner.cell_to_str(rv))
                if len(raw_preview) >= 12:
                    break
            scanned.append({
                "row_idx": i,
                "score": score,
                "text_ratio": feats.get("text_ratio", 0.0),
                "date_ratio": feats.get("date_ratio", 0.0),
                "long_digit_ratio": feats.get("long_digit_ratio", 0.0),
                "non_empty_ratio": feats.get("non_empty_ratio", 0.0),
                "raw_preview": raw_preview,
                "str_preview": str_preview,
            })
            if self.is_header_like(feats):
                candidates.append((i, score))

        if candidates:
            candidates.sort(key=lambda x: (-x[1], x[0]))
            best = candidates[0][0]
            return best, {
                "scanned_rows": scanned,
                "chosen_header_row_idx": best,
                "chosen_reason": "score_max_header_like",
            }
        return -1, {
            "scanned_rows": scanned,
            "chosen_header_row_idx": -1,
            "chosen_reason": "no_header_row_detected",
        }

    def first_non_empty_row_idx(self, df: pd.DataFrame, scan_rows: int = 20) -> int:
        """Return the index of the first row that has any content."""
        total = len(df)
        if total == 0:
            return 0
        limit = min(scan_rows, total)
        for idx in range(limit):
            row_str = [DataCleaner.cell_to_str(c) for c in df.iloc[idx].tolist()]
            feats = self.extract_row_features(row_str)
            if feats.get("non_empty_ratio", 0.0) > 0:
                return idx
        return 0

    # -----------------------------------------------------------------
    # Multi-line header resolution
    # -----------------------------------------------------------------

    def get_header_rows(
        self,
        df: pd.DataFrame,
        header_row_idx: int,
        warnings: Optional[List[str]] = None,
    ) -> Tuple[List[Any], List[Any], int]:
        """
        Determine single- vs two-line header starting at *header_row_idx*.

        Returns ``(header1, header2, data_start_idx)``.
        """
        total = len(df)
        if total == 0:
            return [], [], 0
        header1 = df.iloc[header_row_idx].tolist()
        header2: List[Any] = []
        data_start = header_row_idx + 1
        if header_row_idx + 1 < total:
            row2 = df.iloc[header_row_idx + 1].tolist()
            row2_str = [DataCleaner.cell_to_str(v) for v in row2]
            feats = self.extract_row_features(row2_str)
            if self.is_header_like(feats):
                header2 = row2
                data_start = header_row_idx + 2
            elif warnings is not None and "second_header_row_rejected_as_data" not in warnings:
                warnings.append("second_header_row_rejected_as_data")
        return header1, header2, data_start

    def pick_header_layout(
        self,
        df: pd.DataFrame,
        preferred_idx: int,
        build_column_summaries_fn: Any,
        warnings: Optional[List[str]] = None,
    ) -> Tuple[int, List[Any], List[Any], int]:
        """
        Try multiple candidate header positions and pick the one that
        yields the most data rows.

        *build_column_summaries_fn* is the
        ``(df, h1, h2, start) -> (summaries, paths, data_rows)`` callable
        used to count viable data rows for each candidate.
        """
        total = len(df)
        if total == 0:
            return 0, [], [], 0
        candidate_indices = [preferred_idx] + list(range(min(3, total)))
        seen: set = set()
        candidates: list = []
        for idx in candidate_indices:
            if idx in seen or idx < 0 or idx >= total:
                continue
            seen.add(idx)
            for use_second in (True, False):
                h1 = df.iloc[idx].tolist()
                h2: List[Any] = []
                ds = idx + 1
                if use_second and idx + 1 < total:
                    row2 = df.iloc[idx + 1].tolist()
                    row2_str = [DataCleaner.cell_to_str(v) for v in row2]
                    feats = self.extract_row_features(row2_str)
                    if self.is_header_like(feats):
                        h2 = row2
                        ds = idx + 2
                    else:
                        if warnings is not None and "second_header_row_rejected_as_data" not in warnings:
                            warnings.append("second_header_row_rejected_as_data")
                        continue
                _, _, data_rows = build_column_summaries_fn(df, h1, h2, ds)
                candidates.append((data_rows, idx, use_second, h1, h2, ds))
        if not candidates:
            return preferred_idx, [], [], 0
        candidates.sort(key=lambda x: (-x[0], x[1], not x[2]))
        _, h_idx, _, h1, h2, ds = candidates[0]
        return h_idx, h1, h2, ds

    @staticmethod
    def make_header_path(header1: str, header2: str, fallback: str) -> str:
        """Merge a two-line header pair into a single display path."""
        h1 = str(header1).strip() if header1 is not None else ""
        h2 = str(header2).strip() if header2 is not None else ""
        if h1 and h2:
            return f"{h1} / {h2}"
        return h1 or h2 or fallback
