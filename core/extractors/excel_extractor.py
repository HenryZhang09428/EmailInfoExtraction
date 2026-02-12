"""
Excel extractor module for extracting content from Excel files.
"""
import json
import re
import uuid
import zipfile
import os
from datetime import date, datetime
from pathlib import Path
from typing import List, Any, Tuple, Optional, Dict

import pandas as pd
from openpyxl import load_workbook

from core.extractors.base import BaseExtractor
from core.ir import SourceDoc, SourceBlock, BlockType
from core.llm import LLMClient
from core.logger import get_logger

logger = get_logger(__name__)

# Configuration constants
MAX_ROWS_TO_PROCESS = 1000  # Maximum rows to process (excluding header)
def _env_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)).strip())
        return value
    except Exception:
        return default

MAX_RECORDS_PER_WORKBOOK = _env_int("EXCEL_MAX_RECORDS_PER_WORKBOOK", 5000)
MAX_CSV_CHARS = 100000  # Maximum CSV text characters (~100K)
MIN_ROWS_FOR_SAMPLING = 500  # Use sampling strategy above this row count
FAST_ROWS_THRESHOLD = 30  # Enable FAST mode when total_rows exceeds this threshold
SAMPLE_HEAD_ROWS = 10  # Number of first data rows to sample for schema inference
SAMPLE_SPREAD_ROWS = 10  # Number of evenly spaced data rows to sample
SAMPLE_MAX_PER_COLUMN = 8  # Max unique samples per column
SCHEMA_INFER_COVERAGE_THRESHOLD = 0.5  # Minimum mapping coverage to accept


def _is_empty_cell(value: Any) -> bool:
    if value is None or pd.isna(value):
        return True
    return str(value).strip() == ""


def _cell_to_str(value: Any) -> str:
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
    plain_attr = getattr(value, "plain", None)
    if isinstance(plain_attr, str):
        return plain_attr.strip()
    text_attr = getattr(value, "text", None)
    if isinstance(text_attr, str):
        return text_attr.strip()
    value_str = str(value).strip()
    if value_str.lower() in {"nan", "none", "nat"}:
        return ""
    return value_str


_DATE_LIKE_RE = re.compile(
    r"^\s*\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\s*$"
)
_PURE_NUMBER_RE = re.compile(r"^\s*-?\d+(?:\.\d+)?\s*$")
_LONG_DIGITS_RE = re.compile(r"^\s*\d{6,}\s*$")
_ALPHA_LIKE_RE = re.compile(r"[A-Za-z\u4e00-\u9fff]")
_REMOVE_INTENT_KEYWORDS = ("减员", "离职", "退工", "退保", "终止", "停保")
_TERMINATION_KEYS = ("termination_date", "terminationdate", "leave_date", "leavedate", "end_date", "enddate")


def _looks_like_header_row(cells: List[str]) -> bool:
    """
    Heuristic: decide whether a row "looks like a header".

    Requirements:
    - Generalized (no hard-coded field names)
    - Uses structural cues:
      - header row: low ratio of date-like values, low ratio of long pure digits (>=6)
      - data row: higher ratio of dates / long IDs, values are more concrete
    """
    if not cells:
        return False

    values = [str(v).strip() for v in cells]
    non_empty = [v for v in values if v]
    if not non_empty:
        return False

    n = len(non_empty)
    fill_ratio = n / max(1, len(values))
    date_like = sum(1 for v in non_empty if _DATE_LIKE_RE.match(v))
    long_digits = sum(1 for v in non_empty if _LONG_DIGITS_RE.match(v))
    pure_number = sum(1 for v in non_empty if _PURE_NUMBER_RE.match(v))
    alpha_like = sum(1 for v in non_empty if _ALPHA_LIKE_RE.search(v))

    date_ratio = date_like / n
    long_digits_ratio = long_digits / n
    pure_number_ratio = pure_number / n
    alpha_ratio = alpha_like / n

    # Strong "data row" signals
    # Multiple date-like values in the same row is very uncommon for header rows,
    # but common in data rows (e.g., onboard date + contract date).
    if date_like >= 2:
        return False
    # A highly-filled row containing date-like / long-ID patterns is far more likely data.
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


def _extract_row_features(cells: List[str]) -> Dict[str, float]:
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
    non_empty_ratio = len(non_empty) / max(1, total)

    text_like = 0
    long_digit = 0
    date_like = 0
    numeric_like = 0

    for v in non_empty:
        if _DATE_LIKE_RE.match(v):
            date_like += 1
            numeric_like += 1
            continue
        if _LONG_DIGITS_RE.match(v):
            long_digit += 1
            numeric_like += 1
            continue
        if _PURE_NUMBER_RE.match(v):
            numeric_like += 1
            continue
        # Count as text-like only when it's alphabetic/Chinese and not mixed with digits.
        if _ALPHA_LIKE_RE.search(v) and not re.search(r"\d", v):
            text_like += 1

    denom = max(1, len(non_empty))
    return {
        "non_empty_ratio": non_empty_ratio,
        "text_ratio": text_like / denom,
        "long_digit_ratio": long_digit / denom,
        "date_ratio": date_like / denom,
        "numeric_ratio": numeric_like / denom,
    }


def _header_score(features: Dict[str, float]) -> float:
    # Strongly reward text_ratio; strongly penalize data-like ratios.
    text_ratio = features.get("text_ratio", 0.0)
    long_digit_ratio = features.get("long_digit_ratio", 0.0)
    date_ratio = features.get("date_ratio", 0.0)
    numeric_ratio = features.get("numeric_ratio", 0.0)
    non_empty_ratio = features.get("non_empty_ratio", 0.0)

    score = 3.5 * text_ratio
    score -= 2.5 * long_digit_ratio
    score -= 2.0 * date_ratio
    score -= 1.5 * numeric_ratio
    score += 0.2 * non_empty_ratio
    return score


def _is_header_like_row(features: Dict[str, float]) -> bool:
    text_ratio = features.get("text_ratio", 0.0)
    date_ratio = features.get("date_ratio", 0.0)
    long_digit_ratio = features.get("long_digit_ratio", 0.0)
    numeric_ratio = features.get("numeric_ratio", 0.0)
    return (
        text_ratio >= 0.35
        and long_digit_ratio <= 0.02
        and date_ratio <= 0.02
        and numeric_ratio <= 0.15
    )


def _select_header_row_index(df: pd.DataFrame) -> Tuple[int, Dict[str, Any]]:
    total_rows = len(df)
    if total_rows == 0:
        return -1, {
            "scanned_rows": [],
            "chosen_header_row_idx": -1,
            "chosen_reason": "no_header_row_detected",
        }

    scan_rows = min(20, total_rows)
    scanned_rows = []
    header_like_rows = []
    num_cols = df.shape[1] if df is not None and len(df.shape) > 1 else 0
    for i in range(scan_rows):
        row = df.iloc[i].tolist()
        row_str = [_cell_to_str(cell) for cell in row]
        features = _extract_row_features(row_str)
        score = _header_score(features)
        raw_preview: List[Tuple[str, str]] = []
        str_preview: List[str] = []
        for col_idx in range(num_cols):
            raw_value = df.iat[i, col_idx]
            if raw_value is None or pd.isna(raw_value):
                continue
            if isinstance(raw_value, str) and raw_value == "":
                continue
            raw_preview.append((repr(raw_value), type(raw_value).__name__))
            str_preview.append(_cell_to_str(raw_value))
            if len(raw_preview) >= 12:
                break
        scanned_rows.append({
            "row_idx": i,
            "score": score,
            "text_ratio": features.get("text_ratio", 0.0),
            "date_ratio": features.get("date_ratio", 0.0),
            "long_digit_ratio": features.get("long_digit_ratio", 0.0),
            "non_empty_ratio": features.get("non_empty_ratio", 0.0),
            "raw_preview": raw_preview,
            "str_preview": str_preview,
        })
        if _is_header_like_row(features):
            header_like_rows.append((i, score))

    if header_like_rows:
        header_like_rows.sort(key=lambda x: (-x[1], x[0]))
        best_idx = header_like_rows[0][0]
        return best_idx, {
            "scanned_rows": scanned_rows,
            "chosen_header_row_idx": best_idx,
            "chosen_reason": "score_max_header_like",
        }

    return -1, {
        "scanned_rows": scanned_rows,
        "chosen_header_row_idx": -1,
        "chosen_reason": "no_header_row_detected",
    }


def _first_non_empty_row_idx(df: pd.DataFrame, scan_rows: int = 20) -> int:
    total_rows = len(df)
    if total_rows == 0:
        return 0
    limit = min(scan_rows, total_rows)
    for idx in range(limit):
        row = df.iloc[idx].tolist()
        row_str = [_cell_to_str(cell) for cell in row]
        features = _extract_row_features(row_str)
        if features.get("non_empty_ratio", 0.0) > 0:
            return idx
    return 0


def _choose_best_sheet(file_path: str, preferred_sheet: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
    suffix = Path(file_path).suffix.lower()
    if suffix == ".xls":
        import xlrd
        wb = xlrd.open_workbook(file_path)
        sheetnames = wb.sheet_names() or []
        debug = {"sheets": [], "chosen_reason": "", "selected_sheet_name": ""}
        if not sheetnames:
            return "Sheet1", debug
        if preferred_sheet and preferred_sheet in sheetnames:
            debug["chosen_reason"] = "profile_sheet"
            debug["selected_sheet_name"] = preferred_sheet
            return preferred_sheet, debug
        if preferred_sheet and preferred_sheet not in sheetnames:
            debug["profile_sheet"] = preferred_sheet
            debug["profile_sheet_not_found"] = True
        for sheet_name in sheetnames:
            ws = wb.sheet_by_name(sheet_name)
            best_header_like_score = None
            non_empty_cells_count = 0
            max_row = min(30, ws.nrows)
            max_col = min(80, ws.ncols)
            for row_idx in range(max_row):
                row_values = ws.row_values(row_idx, 0, max_col)
                row_str = [_cell_to_str(cell) for cell in row_values]
                features = _extract_row_features(row_str)
                if _is_header_like_row(features):
                    score = _header_score(features)
                    if best_header_like_score is None or score > best_header_like_score:
                        best_header_like_score = score
                for cell in row_values:
                    if not _is_empty_cell(cell):
                        non_empty_cells_count += 1
            debug["sheets"].append({
                "sheet_name": sheet_name,
                "best_header_like_score": best_header_like_score,
                "non_empty_cells_count": non_empty_cells_count,
            })
        sheets_with_header = [s for s in debug["sheets"] if s["best_header_like_score"] is not None]
        if sheets_with_header:
            sheets_with_header.sort(
                key=lambda s: (-s["best_header_like_score"], sheetnames.index(s["sheet_name"]))
            )
            chosen = sheets_with_header[0]
            debug["chosen_reason"] = "best_header_like_score"
        else:
            debug["sheets"].sort(
                key=lambda s: (-s["non_empty_cells_count"], sheetnames.index(s["sheet_name"]))
            )
            chosen = debug["sheets"][0]
            debug["chosen_reason"] = "non_empty_cells_count"
        debug["selected_sheet_name"] = chosen["sheet_name"]
        return chosen["sheet_name"], debug
    wb = load_workbook(file_path, read_only=True, data_only=True)
    sheetnames = wb.sheetnames or []
    debug = {"sheets": [], "chosen_reason": "", "selected_sheet_name": ""}

    if not sheetnames:
        return "Sheet1", debug
    if preferred_sheet and preferred_sheet in sheetnames:
        debug["chosen_reason"] = "profile_sheet"
        debug["selected_sheet_name"] = preferred_sheet
        return preferred_sheet, debug
    if preferred_sheet and preferred_sheet not in sheetnames:
        debug["profile_sheet"] = preferred_sheet
        debug["profile_sheet_not_found"] = True

    for sheet_name in sheetnames:
        ws = wb[sheet_name]
        best_header_like_score = None
        non_empty_cells_count = 0

        for row in ws.iter_rows(min_row=1, max_row=30, min_col=1, max_col=80, values_only=True):
            row_values = list(row) if row is not None else []
            row_str = [_cell_to_str(cell) for cell in row_values]
            features = _extract_row_features(row_str)
            if _is_header_like_row(features):
                score = _header_score(features)
                if best_header_like_score is None or score > best_header_like_score:
                    best_header_like_score = score
            for cell in row_values:
                if not _is_empty_cell(cell):
                    non_empty_cells_count += 1

        debug["sheets"].append({
            "sheet_name": sheet_name,
            "best_header_like_score": best_header_like_score,
            "non_empty_cells_count": non_empty_cells_count,
        })

    sheets_with_header = [s for s in debug["sheets"] if s["best_header_like_score"] is not None]
    if sheets_with_header:
        sheets_with_header.sort(
            key=lambda s: (-s["best_header_like_score"], sheetnames.index(s["sheet_name"]))
        )
        chosen = sheets_with_header[0]
        debug["chosen_reason"] = "best_header_like_score"
    else:
        debug["sheets"].sort(
            key=lambda s: (-s["non_empty_cells_count"], sheetnames.index(s["sheet_name"]))
        )
        chosen = debug["sheets"][0]
        debug["chosen_reason"] = "non_empty_cells_count"

    debug["selected_sheet_name"] = chosen["sheet_name"]
    return chosen["sheet_name"], debug


def _read_excel_df(
    file_path: str,
    sheet_name: Any = 0,
    warnings: Optional[List[str]] = None
) -> Tuple[pd.DataFrame, str, bool, bool]:
    """
    Read Excel via pandas; fallback to full openpyxl if first row looks missing.
    Returns (df, read_backend, fallback_used, row_index_zero_detected).
    """
    row_index_zero_detected = False
    suffix = Path(file_path).suffix.lower()
    df, read_backend, df_warnings = _extract_sheet_df(file_path, sheet_name, suffix)
    if warnings is not None and df_warnings:
        warnings.extend(df_warnings)
    fallback_used = False
    if suffix == ".xls":
        return df, read_backend, fallback_used, row_index_zero_detected

    if len(df) > 0:
        top_row = df.iloc[0].tolist()
        top_row_all_empty = all(_cell_to_str(cell) == "" for cell in top_row)

        next_rows_have_data = False
        max_check = min(5, len(df) - 1)
        for idx in range(1, max_check + 1):
            row = df.iloc[idx].tolist()
            row_str = [_cell_to_str(cell) for cell in row]
            features = _extract_row_features(row_str)
            if features.get("non_empty_ratio", 0.0) > 0.2:
                next_rows_have_data = True
                break

        if top_row_all_empty and next_rows_have_data:
            try:
                df = pd.read_excel(
                    file_path,
                    sheet_name=sheet_name,
                    header=None,
                    keep_default_na=False,
                    engine="openpyxl",
                    engine_kwargs={"read_only": False, "data_only": False},
                )
            except TypeError:
                df = pd.read_excel(
                    file_path,
                    sheet_name=sheet_name,
                    header=None,
                    keep_default_na=False,
                    engine="openpyxl",
                )
            read_backend = "pandas_openpyxl_full"
            fallback_used = True

    if df.shape[0] >= 2:
        row0 = df.iloc[0].tolist()
        row1 = df.iloc[1].tolist()
        total_cells = max(1, len(row0))
        empty_cells = sum(1 for cell in row0 if _cell_to_str(cell) == "")
        empty_ratio = empty_cells / total_cells
        row1_features = _extract_row_features([_cell_to_str(cell) for cell in row1])
        if empty_ratio > 0.95 and row1_features.get("non_empty_ratio", 0.0) > 0.3:
            try:
                resolved_sheet_name = sheet_name
                if isinstance(sheet_name, int):
                    wb = load_workbook(file_path, data_only=False, read_only=False)
                    try:
                        if 0 <= sheet_name < len(wb.sheetnames):
                            resolved_sheet_name = wb.sheetnames[sheet_name]
                        else:
                            resolved_sheet_name = wb.sheetnames[0]
                    finally:
                        try:
                            wb.close()
                        except Exception:
                            pass
                df = _read_excel_df_openpyxl_values(
                    file_path,
                    str(resolved_sheet_name),
                    MAX_ROWS_TO_PROCESS + 20
                )
                read_backend = "openpyxl_values_full"
                fallback_used = True
                if warnings is not None:
                    warnings.append("excel_pandas_openpyxl_header_row_lost_fallback_openpyxl_values")
                if df.shape[0] >= 2:
                    row0 = df.iloc[0].tolist()
                    row0_empty = all(_cell_to_str(cell) == "" for cell in row0)
                    later_has_data = False
                    for row_idx in range(1, min(df.shape[0], 10)):
                        row_vals = df.iloc[row_idx].tolist()
                        if any(_cell_to_str(cell) != "" for cell in row_vals):
                            later_has_data = True
                            break
                    if row0_empty and later_has_data:
                        row_index_zero_detected = True
            except Exception:
                pass

    return df, read_backend, fallback_used, row_index_zero_detected


def _list_sheet_names(file_path: str) -> Tuple[List[str], str]:
    suffix = Path(file_path).suffix.lower()
    if suffix == ".xls":
        import xlrd
        wb = xlrd.open_workbook(file_path)
        return wb.sheet_names(), "xlrd"
    wb = load_workbook(file_path, read_only=True, data_only=False)
    return wb.sheetnames or [], "openpyxl"


def _extract_sheet_df(
    file_path: str,
    sheet_name: Any,
    suffix: str
) -> Tuple[pd.DataFrame, str, List[str]]:
    if suffix == ".xls":
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=None,
            engine="xlrd",
            keep_default_na=False
        )
        return df, "pandas_xlrd", []
    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=None,
        engine="openpyxl",
        keep_default_na=False
    )
    return df, "pandas_openpyxl", []


def _read_excel_df_openpyxl_values(
    file_path: str,
    sheet_name: str,
    max_rows: Optional[int] = None
) -> pd.DataFrame:
    wb = load_workbook(file_path, data_only=False, read_only=False)
    try:
        ws = wb[sheet_name]
        max_col = ws.max_column
        max_row = min(ws.max_row, max_rows) if max_rows else ws.max_row
        rows = list(ws.iter_rows(min_row=1, max_row=max_row, max_col=max_col, values_only=True))
        df = pd.DataFrame(rows)
        return df
    finally:
        try:
            wb.close()
        except Exception:
            pass


def _xlsx_has_row_index_zero(file_path: str) -> bool:
    """
    Fast check for row index zero in worksheet XML.
    """
    try:
        with zipfile.ZipFile(file_path, "r") as zf:
            for name in zf.namelist():
                if not name.startswith("xl/worksheets/sheet") or not name.endswith(".xml"):
                    continue
                with zf.open(name) as fh:
                    content = fh.read()
                if b'<row r="0"' in content:
                    return True
    except Exception:
        return False
    return False


def _get_header_rows(
    df: pd.DataFrame,
    header_row_idx: int,
    warnings: Optional[List[str]] = None,
) -> Tuple[List[Any], List[Any], int]:
    total_rows = len(df)
    if total_rows == 0:
        return [], [], 0

    header1 = df.iloc[header_row_idx].tolist()
    header2 = []
    data_start_idx = header_row_idx + 1
    if header_row_idx + 1 < total_rows:
        row2 = df.iloc[header_row_idx + 1].tolist()
        row2_str = [_cell_to_str(v) for v in row2]
        row2_features = _extract_row_features(row2_str)
        # Only allow 2-line header if the second row also looks like a header.
        if _is_header_like_row(row2_features):
            header2 = row2
            data_start_idx = header_row_idx + 2
        elif warnings is not None and "second_header_row_rejected_as_data" not in warnings:
            warnings.append("second_header_row_rejected_as_data")

    return header1, header2, data_start_idx


def _pick_header_layout(
    df: pd.DataFrame,
    preferred_header_idx: int,
    warnings: Optional[List[str]] = None,
) -> Tuple[int, List[Any], List[Any], int]:
    total_rows = len(df)
    if total_rows == 0:
        return 0, [], [], 0
    
    candidate_indices = [preferred_header_idx]
    candidate_indices.extend(list(range(min(3, total_rows))))
    seen = set()
    candidates = []
    for idx in candidate_indices:
        if idx in seen or idx < 0 or idx >= total_rows:
            continue
        seen.add(idx)
        for use_second in (True, False):
            header1 = df.iloc[idx].tolist()
            header2 = []
            data_start_idx = idx + 1
            if use_second and idx + 1 < total_rows:
                row2 = df.iloc[idx + 1].tolist()
                row2_str = [_cell_to_str(v) for v in row2]
                row2_features = _extract_row_features(row2_str)
                if _is_header_like_row(row2_features):
                    header2 = row2
                    data_start_idx = idx + 2
                else:
                    # If next row looks like data, do not treat as 2-line header candidate.
                    if warnings is not None and "second_header_row_rejected_as_data" not in warnings:
                        warnings.append("second_header_row_rejected_as_data")
                    continue
            _, _, data_row_count = _build_column_summaries(
                df, header1, header2, data_start_idx
            )
            candidates.append((data_row_count, idx, use_second, header1, header2, data_start_idx))
    
    if not candidates:
        return preferred_header_idx, [], [], 0
    
    candidates.sort(key=lambda x: (-x[0], x[1], not x[2]))
    _, header_idx, _, header1, header2, data_start_idx = candidates[0]
    return header_idx, header1, header2, data_start_idx


def _make_header_path(header1: str, header2: str, fallback: str) -> str:
    h1 = str(header1).strip() if header1 is not None else ""
    h2 = str(header2).strip() if header2 is not None else ""
    if h1 and h2:
        return f"{h1} / {h2}"
    if h1:
        return h1
    if h2:
        return h2
    return fallback


def _evenly_spaced_indices(start: int, end_exclusive: int, count: int) -> List[int]:
    if count <= 0 or end_exclusive <= start:
        return []
    if count == 1:
        return [start]
    span = end_exclusive - start - 1
    if span <= 0:
        return [start]
    step = span / (count - 1)
    indices = [start + int(round(i * step)) for i in range(count)]
    return sorted(set(idx for idx in indices if start <= idx < end_exclusive))


def _estimate_csv_chars(
    df: pd.DataFrame,
    header1: List[Any],
    header2: List[Any],
    data_start_idx: int,
    sample_size: int = 20
) -> int:
    header_lines = []
    if header1:
        header_lines.append(",".join(_cell_to_str(cell) for cell in header1))
    if header2:
        header_lines.append(",".join(_cell_to_str(cell) for cell in header2))

    header_len = sum(len(line) for line in header_lines) + max(0, len(header_lines) - 1)
    total_rows = len(df)
    data_rows = max(0, total_rows - data_start_idx)
    if data_rows == 0:
        return header_len

    sample_end = min(total_rows, data_start_idx + sample_size)
    sample_lengths = []
    for idx in range(data_start_idx, sample_end):
        row = df.iloc[idx].tolist()
        row_text = ",".join(_cell_to_str(cell) for cell in row)
        sample_lengths.append(len(row_text))
    avg_row_len = int(sum(sample_lengths) / max(1, len(sample_lengths)))
    return header_len + (avg_row_len + 1) * data_rows


def _build_column_summaries(
    df: pd.DataFrame,
    header1: List[Any],
    header2: List[Any],
    data_start_idx: int,
    head_rows: int = SAMPLE_HEAD_ROWS,
    spread_rows: int = SAMPLE_SPREAD_ROWS,
    max_samples: int = SAMPLE_MAX_PER_COLUMN
) -> Tuple[List[Dict[str, Any]], List[str], int]:
    total_rows = len(df)
    num_cols = df.shape[1] if total_rows > 0 else max(len(header1), len(header2))
    header1 = header1 + [""] * (num_cols - len(header1))
    header2 = header2 + [""] * (num_cols - len(header2))

    data_rows = max(0, total_rows - data_start_idx)
    head_indices = list(range(data_start_idx, min(total_rows, data_start_idx + head_rows)))
    spread_indices = _evenly_spaced_indices(data_start_idx, total_rows, spread_rows)
    sample_indices = sorted(set(head_indices + spread_indices))

    header_paths = []
    summaries = []

    for col_idx in range(num_cols):
        header_path = _make_header_path(header1[col_idx], header2[col_idx], f"col_{col_idx + 1}")
        header_paths.append(header_path)

        samples = []
        for idx in sample_indices:
            value = _cell_to_str(df.iat[idx, col_idx])
            if value and value not in samples:
                samples.append(value)
            if len(samples) >= max_samples:
                break

        unique_count = None
        if data_rows > 0:
            sample_end = min(total_rows, data_start_idx + MAX_ROWS_TO_PROCESS)
            col_values = df.iloc[data_start_idx:sample_end, col_idx].tolist()
            non_empty_values = [_cell_to_str(v) for v in col_values if not _is_empty_cell(v)]
            non_empty_count = len(non_empty_values)
            non_empty_ratio = non_empty_count / max(1, len(col_values))
            if non_empty_values:
                unique_count = len(set(non_empty_values))
        else:
            non_empty_ratio = 0.0

        summaries.append({
            "column_index": col_idx + 1,
            "header_path": header_path,
            "non_empty_ratio": round(non_empty_ratio, 4),
            "unique_count": unique_count,
            "samples": samples
        })

    return summaries, header_paths, data_rows


def _normalize_header_paths_and_summaries(
    header_paths: List[str],
    summaries: List[Dict[str, Any]]
) -> Tuple[List[str], List[Dict[str, Any]]]:
    normalized_paths: List[str] = []
    for idx, header_path in enumerate(header_paths):
        normalized = _normalize_header_for_semantic_key(header_path)
        normalized = normalized or header_path
        normalized_paths.append(normalized)
        if idx < len(summaries):
            summaries[idx]["header_path"] = normalized
    return normalized_paths, summaries


def _apply_row_filter(
    record: Dict[str, str],
    row_values: List[str],
    row_filter: Dict[str, Any]
) -> bool:
    if not row_values or all(value == "" for value in row_values):
        return False

    min_ratio = row_filter.get("min_nonempty_ratio")
    if isinstance(min_ratio, (int, float)):
        non_empty_count = sum(1 for value in row_values if value != "")
        ratio = non_empty_count / max(1, len(row_values))
        if ratio < float(min_ratio):
            return False

    required_any = row_filter.get("required_fields_any") or []
    if isinstance(required_any, list) and required_any:
        if not any(record.get(field, "") != "" for field in required_any):
            return False

    exclude_terms = row_filter.get("exclude_if_contains_any") or []
    if isinstance(exclude_terms, list) and exclude_terms:
        lowered_values = [value.lower() for value in row_values if value]
        for term in exclude_terms:
            if not term:
                continue
            term_lower = str(term).lower()
            if any(term_lower in value for value in lowered_values):
                return False

    return True


def _sanitize_semantic_key_by_header(
    semantic_key_by_header: Dict[str, Any],
    header_paths: List[str]
) -> Dict[str, str]:
    cleaned = {}
    for header_path in header_paths:
        raw_value = semantic_key_by_header.get(header_path)
        if isinstance(raw_value, str):
            cleaned_value = raw_value.strip()
        else:
            cleaned_value = ""
        cleaned[header_path] = cleaned_value
    return cleaned


def _propose_semantic_key_from_header_for_disambiguation(header_path: str) -> str:
    """
    Deterministic remapping rules for resolving semantic key conflicts.
    This is intentionally keyword/regex-based and does NOT depend on sample values.
    """
    header = str(header_path or "").strip()
    if not header:
        return ""

    # Position attribute / level / sequence
    if re.search(r"(岗位属性|一线|二线|三线|职级|层级|序列)", header):
        return "position_attribute"
    # Position name
    if re.search(r"(职位名称|岗位名称)", header):
        return "position"
    # Contract company (party A / labor contract)
    if re.search(r"(甲方公司|劳动合同)", header):
        return "contract_company"
    # Organizational unit
    if re.search(r"(公司名称|所属组织单位|组织单位|组织)", header):
        return "company"

    return ""


def _header_keyword_rank(header_path: str, keywords: List[str]) -> Optional[int]:
    header = str(header_path or "").strip()
    if not header:
        return None
    text = header.lower()
    text_compact = re.sub(r"[\s\W_]+", "", text)
    for idx, keyword in enumerate(keywords):
        if keyword in text_compact:
            return idx
    return None


def _remap_non_core_key_from_header(header_path: str) -> str:
    header = str(header_path or "").strip()
    if not header:
        return ""
    text = header.lower()
    text_compact = re.sub(r"[\s\W_]+", "", text)

    if "雇佣状态" in text_compact or "在职状态" in text_compact:
        return "employment_status"
    if "员工子组" in text_compact:
        return "subgroup"
    if "岗" in text_compact and "岗位" not in text_compact and "职位" not in text_compact:
        return "job_family"
    if "合同期限单位" in text_compact:
        return "duration_unit"
    if "试用期" in text_compact and "单位" in text_compact:
        return "probation_unit"
    if "岗位职能类" in text_compact or "岗位职能" in text_compact:
        return "job_function"
    if "供应商" in text_compact:
        return "vendor"
    if "所属组织单位" in text_compact or "组织单位" in text_compact:
        return "org_unit"

    return ""


def _resolve_core_key_collisions(
    semantic_key_by_header: Dict[str, str],
    header_paths: List[str],
) -> Tuple[Dict[str, str], bool]:
    core_key_rules = {
        "employee_id": ["员工工号", "工号", "人员编号"],
        "name": ["姓名", "员工姓名"],
        "company": ["公司名称"],
        "position": ["职位名称", "岗位名称"],
        "start_date": ["入职日期", "到岗日期"],
        "date": ["日期"],
    }

    updated = dict(semantic_key_by_header)
    changed = False
    for core_key, keywords in core_key_rules.items():
        candidates = [h for h in header_paths if (updated.get(h) or "").strip() == core_key]
        if len(candidates) <= 1:
            continue
        ranked = []
        for header_path in candidates:
            rank = _header_keyword_rank(header_path, keywords)
            if rank is not None:
                ranked.append((rank, header_path))
        ranked.sort(key=lambda x: (x[0], candidates.index(x[1])))
        keep_header = ranked[0][1] if ranked else None
        for header_path in candidates:
            if keep_header and header_path == keep_header:
                continue
            remap = _remap_non_core_key_from_header(header_path)
            if remap and remap != core_key:
                updated[header_path] = remap
            else:
                updated[header_path] = ""
            changed = True
    return updated, changed


def _resolve_semantic_key_conflicts(
    semantic_key_by_header: Dict[str, str],
    header_paths: List[str],
    warnings: List[str],
) -> Dict[str, str]:
    """
    Conflict disambiguation step (before de-dup in _build_semantic_keys):
    Only triggers when multiple columns map to the same semantic key.
    """
    if not isinstance(semantic_key_by_header, dict) or not header_paths:
        return semantic_key_by_header

    updated, core_changed = _resolve_core_key_collisions(semantic_key_by_header, header_paths)

    counts: Dict[str, int] = {}
    for header_path in header_paths:
        key = (updated.get(header_path) or "").strip()
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1

    conflict_keys = {k for k, v in counts.items() if v > 1}
    if not conflict_keys:
        return updated

    changed = core_changed
    for header_path in header_paths:
        cur = (updated.get(header_path) or "").strip()
        if not cur or cur not in conflict_keys:
            continue
        proposed = _propose_semantic_key_from_header_for_disambiguation(header_path)
        if proposed and proposed != cur:
            updated[header_path] = proposed
            changed = True

    if changed:
        warnings.append("semantic_key_collision_resolved")
        warnings.append("semantic_core_key_collision_resolved")

    return updated


def _compute_coverage(semantic_key_by_header: Dict[str, str], header_paths: List[str]) -> float:
    if not header_paths:
        return 0.0
    relevant_headers = [
        header
        for header in header_paths
        if header and not re.match(r"^col_\d+$", str(header).strip())
    ]
    if not relevant_headers:
        relevant_headers = header_paths
    mapped = sum(1 for header in relevant_headers if semantic_key_by_header.get(header, "").strip())
    return mapped / max(1, len(relevant_headers))


def _normalize_header_compact(text: Any) -> str:
    if text is None:
        return ""
    raw = str(text)
    raw = re.sub(r"\([^)]*\)", "", raw)
    raw = re.sub(r"（[^）]*）", "", raw)
    raw = raw.replace("／", "/")
    raw = raw.replace("\u3000", "")
    raw = re.sub(r"[\s]+", "", raw)
    raw = re.sub(r"[，,。.:：;；/\\\-\_|]+", "", raw)
    return raw.strip().lower()


def _normalize_header_text(text: str) -> str:
    return _normalize_header_compact(text)


def _normalize_header_for_semantic_key(text: Any) -> str:
    normalized = _normalize_header_compact(text)
    if "参加保险情况养老" in normalized:
        normalized = normalized.replace("参加保险情况养老", "参加保险情况")
    return normalized


def _normalize_reason_text(text: Any) -> str:
    return _normalize_header_compact(text)


def _infer_remove_intent(sheet_name: Any, header_paths: List[str], semantic_key_by_header: Dict[str, str]) -> bool:
    name_text = _normalize_reason_text(sheet_name)
    header_text = _normalize_reason_text(" ".join(header_paths or []))
    if any(k in name_text for k in _REMOVE_INTENT_KEYWORDS):
        return True
    if any(k in header_text for k in _REMOVE_INTENT_KEYWORDS):
        return True
    for value in (semantic_key_by_header or {}).values():
        if (value or "").strip() in _TERMINATION_KEYS:
            return True
    return False


def _column_text_profile(values: List[str]) -> Tuple[int, float, float, float]:
    non_empty = [v for v in values if v]
    if not non_empty:
        return 0, 0.0, 0.0, 0.0
    text_like = 0
    long_digit = 0
    pure_number = 0
    for v in non_empty:
        if _LONG_DIGITS_RE.match(v):
            long_digit += 1
        if _PURE_NUMBER_RE.match(v):
            pure_number += 1
        if _ALPHA_LIKE_RE.search(v) and not _PURE_NUMBER_RE.match(v) and not _LONG_DIGITS_RE.match(v):
            text_like += 1
    denom = max(1, len(non_empty))
    return len(non_empty), text_like / denom, long_digit / denom, pure_number / denom


def _ensure_termination_reason_mapping(
    df: pd.DataFrame,
    header_paths: List[str],
    semantic_key_by_header: Dict[str, str],
    data_start_idx: int,
    sheet_name: Any
) -> Dict[str, str]:
    if not header_paths:
        return semantic_key_by_header
    if not _infer_remove_intent(sheet_name, header_paths, semantic_key_by_header):
        return semantic_key_by_header
    updated = dict(semantic_key_by_header or {})
    total_rows = len(df)
    end_idx = min(total_rows, data_start_idx + MAX_ROWS_TO_PROCESS)
    for col_idx, header_path in enumerate(header_paths):
        header_text = _normalize_reason_text(header_path)
        if "原因" not in header_path and "原因" not in header_text and "reason" not in header_text:
            continue
        if "reason" in header_text or any(k in header_text for k in _REMOVE_INTENT_KEYWORDS) or "原因" in header_path:
            col_values = [_cell_to_str(df.iat[row_idx, col_idx]) for row_idx in range(data_start_idx, end_idx)]
            non_empty_count, text_ratio, long_ratio, number_ratio = _column_text_profile(col_values)
            if non_empty_count == 0:
                continue
            if text_ratio < 0.3 or long_ratio > 0.3 or number_ratio > 0.6:
                continue
            updated[header_path] = "termination_reason"
    return updated


def _apply_header_normalization_and_forced_mappings(
    header_paths: List[str],
    semantic_key_by_header: Dict[str, str]
) -> Tuple[List[str], Dict[str, str]]:
    normalized_headers: List[str] = []
    updated_semantic: Dict[str, str] = {}
    force_map = {
        "姓名": "name",
        "性别": "gender",
        "个人电脑编号": "employee_id",
        "身份证号码": "id_number",
        "证件号码": "id_number",
        "民族": "ethnicity",
        "学历": "education_level",
        "职工身份": "employee_status",
        "参加保险年月": "start_date",
        "参保年月": "start_date",
        "缴费年月": "start_date",
        "月缴费工资": "monthly_contribution",
        "户口情况": "household_registration",
        "参加保险情况": "pension_insurance_status",
        "减员年月": "termination_date",
        "离职年月": "termination_date",
        "备注": "remark",
    }
    for header_path in header_paths:
        normalized = _normalize_header_for_semantic_key(header_path)
        normalized_headers.append(normalized)
        existing_key = (semantic_key_by_header.get(header_path) or "").strip()
        forced_key = force_map.get(normalized)
        if normalized in updated_semantic:
            if not updated_semantic[normalized] and (forced_key or existing_key):
                updated_semantic[normalized] = forced_key or existing_key
        else:
            updated_semantic[normalized] = forced_key or existing_key
    return normalized_headers, updated_semantic


def _parse_month_start(value: Any) -> Optional[datetime]:
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
    match = re.match(r"^\s*(\d{4})[./-](\d{1,2})\s*$", text)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        if 1 <= month <= 12:
            return datetime(year, month, 1)
    match = re.match(r"^\s*(\d{4})[./-](\d{1,2})[./-]\d{1,2}\s*$", text)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        if 1 <= month <= 12:
            return datetime(year, month, 1)
    match = re.match(r"^\s*(\d{4})\s*年\s*(\d{1,2})\s*月?\s*$", text)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        if 1 <= month <= 12:
            return datetime(year, month, 1)
    return None


def _normalize_value(value: Any) -> str:
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
        return _cell_to_str(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        match = re.match(r"^\s*(\d{4})[./-](\d{1,2})\s*$", text)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            if 1 <= month <= 12:
                return date(year, month, 1).isoformat()
        match = re.match(r"^\s*(\d{4})[./-](\d{1,2})[./-](\d{1,2})\s*$", text)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                return _cell_to_str(text)
        match = re.match(r"^\s*(\d{4})\s*年\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?\s*$", text)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3) or 1)
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                return _cell_to_str(text)
        if text.isdigit():
            num = int(text)
            if 1 <= num <= 60000:
                try:
                    dt = pd.to_datetime(num, unit="d", origin="1899-12-30", errors="coerce")
                    if pd.notna(dt):
                        return dt.to_pydatetime().date().isoformat()
                except Exception:
                    pass
        return _cell_to_str(text)
    return _cell_to_str(value)


def deterministic_header_map(header_text: str) -> Optional[str]:
    header = str(header_text or "").strip()
    if not header:
        return None
    text_compact = _normalize_header_compact(header)

    if "供应商发薪编码名称" in text_compact:
        return "vendor_pay_code_name"
    if "供应商发薪编码" in text_compact:
        return "vendor_pay_code"
    if "个人电脑编号" in text_compact:
        return "employee_id"
    if "员工工号" in text_compact or "人员编号" in text_compact or "工号" in text_compact:
        return "employee_id"
    if "个人电脑编号" in text_compact:
        return "employee_id"
    if "员工姓名" in text_compact or "姓名" in text_compact:
        return "name"
    if "性别" in text_compact:
        return "gender"
    if "身份证号码" in text_compact or "证件号码" in text_compact:
        return "id_number"
    if "民族" in text_compact:
        return "ethnicity"
    if "学历" in text_compact:
        return "education_level"
    if "职工身份" in text_compact:
        return "employee_status"
    if "入职日期" in text_compact or "到岗日期" in text_compact:
        return "start_date"
    if "参加保险年月" in text_compact:
        return "start_date"
    if "月缴费工资" in text_compact:
        return "monthly_contribution"
    if "户口情况" in text_compact:
        return "household_registration"
    if "参加保险情况" in text_compact:
        return "pension_insurance_status"
    if "备注" in text_compact:
        return "remark"
    if "甲方公司" in text_compact and ("劳动合同" in text_compact or "合同" in text_compact):
        return "contract_company"
    if "合同起始日期" in text_compact:
        return "contract_start_date"
    if "合同终止日期" in text_compact:
        return "contract_end_date"
    if "签订日期" in text_compact:
        return "sign_date"
    if "高峰期预计到期日期" in text_compact:
        return "peak_end_date"
    if "培训服务协议开始日期" in text_compact:
        return "training_start_date"
    if "培训协议结束日期" in text_compact:
        return "training_end_date"
    if "竞业禁止协议签订日期" in text_compact:
        return "noncompete_sign_date"
    if "竞业禁止解除日期" in text_compact:
        return "noncompete_end_date"
    if "所属组织单位" in text_compact or "组织单位" in text_compact:
        return "org_unit"
    if "公司名称" in text_compact:
        return "company"
    if "工作地点" in text_compact or "工作地" in text_compact:
        return "work_location"
    if "所属地区" in text_compact:
        return "region"
    if "职位名称" in text_compact or "岗位名称" in text_compact:
        return "position"
    if "岗位属性" in text_compact:
        return "position_attribute"
    if "雇佣状态" in text_compact or "在职状态" in text_compact:
        return "employment_status"
    if "平台供应商" in text_compact or "供应商" in text_compact:
        return "vendor"
    if "参加保险情况" in text_compact:
        return "pension_insurance_status"

    return None


def _apply_deterministic_overrides(
    semantic_key_by_header: Dict[str, str],
    header_paths: List[str]
) -> Tuple[Dict[str, str], int]:
    updated = dict(semantic_key_by_header or {})
    overrides = 0
    for header_path in header_paths:
        deterministic_key = deterministic_header_map(header_path)
        if not deterministic_key:
            continue
        current = (updated.get(header_path) or "").strip()
        if current != deterministic_key:
            updated[header_path] = deterministic_key
            overrides += 1
    return updated, overrides


def _infer_key_from_values(samples: List[str]) -> str:
    if not samples:
        return ""
    joined = " ".join(samples)
    if re.search(r"\b1\d{10}\b", joined):
        return "phone"
    if re.search(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b", joined) or re.search(r"\d{4}年\d{1,2}月\d{1,2}日", joined):
        return "date"
    if re.search(r"\b\d{15}(\d{2}[0-9Xx])?\b", joined):
        return "id_number"
    if re.search(r"[¥￥$]|金额|费用|amount|price", joined, re.IGNORECASE):
        return "amount"
    return ""


def _infer_key_from_samples(samples: List[str]) -> str:
    if not samples:
        return ""

    total = 0
    long_digit = 0
    name_like = 0
    gender_like = 0
    date_like = 0
    company_like = 0
    position_attr_like = 0
    employment_status_like = 0

    for raw in samples:
        if raw is None:
            continue
        value = str(raw).strip()
        if not value:
            continue
        total += 1

        if _LONG_DIGITS_RE.match(value):
            long_digit += 1
        if _DATE_LIKE_RE.match(value):
            date_like += 1
        if value in {"男", "女"}:
            gender_like += 1
        if re.fullmatch(r"[\u4e00-\u9fff]{2,4}", value):
            name_like += 1
        if "有限公司" in value:
            company_like += 1
        if value in {"一线", "二线", "三线"} or "线" in value:
            position_attr_like += 1
        if value in {"在职", "离职", "执行中", "已签署"}:
            employment_status_like += 1

    if total == 0:
        return ""

    long_digit_ratio = long_digit / total
    name_ratio = name_like / total
    gender_ratio = gender_like / total
    date_ratio = date_like / total
    company_ratio = company_like / total
    position_attr_ratio = position_attr_like / total
    employment_status_ratio = employment_status_like / total

    if gender_ratio >= 0.6:
        return "gender"
    if name_ratio >= 0.6:
        return "name"
    if long_digit_ratio >= 0.6:
        return "employee_id"
    if date_ratio >= 0.5:
        return "date"
    if position_attr_ratio >= 0.5:
        return "position_attribute"
    if employment_status_ratio >= 0.5:
        return "employment_status"
    if company_ratio >= 0.5:
        return "company"

    return _infer_key_from_values([str(s) for s in samples if s is not None])


def _infer_key_from_header(header: str) -> str:
    text = _normalize_header_text(header)
    if not text:
        return ""
    # More specific disambiguation rules (must come before broader matches)
    # e.g. "岗位属性/岗位类别/岗位序列/一线/二线/三线/职级/层级" should not be mapped to "position"
    if any(token in text for token in ["岗位属性", "岗位类别", "岗位序列", "一线", "二线", "三线", "职级", "层级"]) or (
        "属性" in text and any(token in text for token in ["岗位", "职位", "职务", "position", "title"])
    ):
        return "position_attribute"
    # e.g. "职位名称/岗位名称" should map to position deterministically
    if any(token in text for token in ["职位名称", "岗位名称"]):
        return "position"
    # e.g. "甲方公司(劳动合同)" should not be mapped to generic "company"
    if any(token in text for token in ["甲方公司", "甲方", "劳动合同", "合同"]) and any(token in text for token in ["公司", "company"]):
        return "contract_company"
    # e.g. "所属组织单位" should be treated as organizational company/unit
    if any(token in text for token in ["所属组织", "组织单位", "组织", "归属单位"]):
        return "company"
    if any(token in text for token in ["姓名", "name"]):
        return "name"
    if any(token in text for token in ["个人电脑编号"]):
        return "employee_id"
    if any(token in text for token in ["工号", "员工编号", "employee", "emp no", "employee id", "staff id"]):
        return "employee_id"
    if any(token in text for token in ["证件", "身份证", "id", "证号", "document"]):
        return "id_number"
    if any(token in text for token in ["民族"]):
        return "ethnicity"
    if any(token in text for token in ["学历"]):
        return "education_level"
    if any(token in text for token in ["职工身份"]):
        return "employee_status"
    if any(token in text for token in ["手机号", "电话", "联系方式", "mobile", "phone", "tel"]):
        return "phone"
    if any(token in text for token in ["入职", "onboard", "start date", "参加保险年月"]):
        return "start_date"
    if any(token in text for token in ["离职", "退场", "offboard", "end date", "离岗"]):
        return "end_date"
    if any(token in text for token in ["合同起始日期"]):
        return "contract_start_date"
    if any(token in text for token in ["合同终止日期"]):
        return "contract_end_date"
    if any(token in text for token in ["签订日期"]):
        return "sign_date"
    if any(token in text for token in ["高峰期预计到期日期"]):
        return "peak_end_date"
    if any(token in text for token in ["培训服务协议开始日期"]):
        return "training_start_date"
    if any(token in text for token in ["培训协议结束日期"]):
        return "training_end_date"
    if any(token in text for token in ["竞业禁止协议签订日期"]):
        return "noncompete_sign_date"
    if any(token in text for token in ["竞业禁止解除日期"]):
        return "noncompete_end_date"
    if any(token in text for token in ["出生", "birth"]):
        return "birth_date"
    if any(token in text for token in ["邮箱", "email"]):
        return "email"
    if any(token in text for token in ["部门", "department"]):
        return "department"
    if any(token in text for token in ["岗位", "职位", "职务", "position", "title"]):
        return "position"
    if any(token in text for token in ["月缴费工资"]):
        return "monthly_contribution"
    if any(token in text for token in ["户口情况"]):
        return "household_registration"
    if any(token in text for token in ["参加保险情况"]):
        return "pension_insurance_status"
    if any(token in text for token in ["备注"]):
        return "remark"
    if any(token in text for token in ["公司名称"]):
        return "company"
    if any(token in text for token in ["公司", "单位", "company"]):
        return "company"
    if any(token in text for token in ["工作地", "所属地区", "地区"]):
        return "address"
    if any(token in text for token in ["地址", "address"]):
        return "address"
    if any(token in text for token in ["金额", "费用", "薪资", "amount", "price", "fee"]):
        return "amount"
    if any(token in text for token in ["备注", "说明", "comment", "remark"]):
        return "remark"
    return ""


def _fallback_semantic_key_by_header(
    header_paths: List[str],
    column_summaries: List[Dict[str, Any]]
) -> Dict[str, str]:
    summary_by_header = {c.get("header_path"): c for c in column_summaries if isinstance(c, dict)}
    mapping = {}
    for header in header_paths:
        inferred = _infer_key_from_header(header)
        samples = summary_by_header.get(header, {}).get("samples") or []
        header_is_generic = not header or re.match(r"^col_\d+$", str(header).strip())
        if not inferred or header_is_generic:
            inferred = _infer_key_from_samples([str(s) for s in samples if s is not None])
        mapping[header] = inferred
    return mapping


def _build_semantic_keys(
    header_paths: List[str],
    semantic_key_by_header: Dict[str, str],
    warnings: List[str]
) -> List[str]:
    semantic_keys = []
    used_keys: Dict[str, int] = {}
    for idx, header_path in enumerate(header_paths):
        raw_key = semantic_key_by_header.get(header_path, "").strip()
        if not raw_key:
            semantic_keys.append("")
            continue
        if raw_key in used_keys:
            used_keys[raw_key] += 1
            deduped_key = f"{raw_key}__{idx + 1}"
        else:
            used_keys[raw_key] = 1
            deduped_key = raw_key
        semantic_keys.append(deduped_key)
    if any(count > 1 for count in used_keys.values()):
        warnings.append("列语义存在重复键，已自动去重")
    return semantic_keys


class ExcelExtractor(BaseExtractor):
    """
    Extractor for Excel files (.xlsx, .xls).
    
    Optimizations for large files:
    1. Limits processed rows to MAX_ROWS_TO_PROCESS
    2. Limits CSV text length to MAX_CSV_CHARS
    3. Adds warnings if data is truncated
    """
    
    def __init__(self, llm: LLMClient, prompts: Optional[dict] = None):
        """
        Initialize the Excel extractor.
        
        Args:
            llm: The LLM client for AI-powered extraction.
            prompts: Dictionary of prompts, must include EXCEL_SCHEMA_INFER_PROMPT.
        """
        super().__init__(llm, prompts)
    
    def safe_extract(self, file_path: str, extract_all_sheets: bool = False, preferred_sheet: Optional[str] = None) -> SourceDoc:
        try:
            result = self.extract(file_path, extract_all_sheets=extract_all_sheets, preferred_sheet=preferred_sheet)
            return result
        except Exception as e:
            logger.error("Extraction failed for %s: %s", file_path, e, exc_info=True)
            return self._create_error_source_doc(file_path, e)
    
    def _extract_sheet(
        self,
        file_path: str,
        sheet_name: Any,
        filename: str,
        source_id: str,
        selected_sheet_name: str,
        sheet_selection_debug: Dict[str, Any],
        add_sheet_name: bool
    ) -> Tuple[Dict[str, Any], Optional[str], Dict[str, Any], Optional[Dict[str, Any]]]:
        warnings: List[str] = []
        df, read_backend, read_fallback_used, row_index_zero_detected = _read_excel_df(
            file_path,
            sheet_name=sheet_name,
            warnings=warnings
        )
        if read_fallback_used and "excel_read_fallback_full_openpyxl" not in warnings:
            warnings.append("excel_read_fallback_full_openpyxl")
        if row_index_zero_detected and "xlsx_row_index_zero_detected" not in warnings:
            warnings.append("xlsx_row_index_zero_detected")
        
        total_rows = len(df)
        
        header_row_idx, header_selection_debug = _select_header_row_index(df)
        header_mode = "header"
        if header_row_idx >= 0:
            header_row = df.iloc[header_row_idx].tolist()
            header_row_str = [_cell_to_str(cell) for cell in header_row]
            header_features = _extract_row_features(header_row_str)
            if (
                (header_features.get("long_digit_ratio", 0.0) > 0.03
                 or header_features.get("date_ratio", 0.0) > 0.03)
                and header_features.get("text_ratio", 0.0) >= 0.6
            ):
                header_row_idx = -1
                header_mode = "no_header"
                if "header_row_looks_like_data_forced_no_header" not in warnings:
                    warnings.append("header_row_looks_like_data_forced_no_header")
        if header_row_idx < 0:
            if "no_header_row_detected" not in warnings:
                warnings.append("no_header_row_detected")
            header_mode = "no_header"
            num_cols = df.shape[1] if df is not None and len(df.shape) > 1 else 0
            header1 = [f"col_{idx}" for idx in range(num_cols)]
            header2 = []
            data_start_idx = _first_non_empty_row_idx(df)
        else:
            header1, header2, data_start_idx = _get_header_rows(df, header_row_idx, warnings)
        column_summaries, header_paths, data_row_count = _build_column_summaries(
            df,
            header1,
            header2,
            data_start_idx
        )
        header_paths, column_summaries = _normalize_header_paths_and_summaries(
            header_paths,
            column_summaries
        )
        if data_row_count == 0 and total_rows >= 2:
            header_row_idx, header1, header2, data_start_idx = _pick_header_layout(
                df,
                header_row_idx,
                warnings
            )
            column_summaries, header_paths, data_row_count = _build_column_summaries(
                df,
                header1,
                header2,
                data_start_idx
            )
            header_paths, column_summaries = _normalize_header_paths_and_summaries(
                header_paths,
                column_summaries
            )
        if data_row_count == 0 and total_rows > 0:
            header_row_idx = -1
            header_mode = "no_header"
            num_cols = df.shape[1] if df is not None and len(df.shape) > 1 else 0
            header1 = [f"col_{idx}" for idx in range(num_cols)]
            header2 = []
            data_start_idx = _first_non_empty_row_idx(df)
            column_summaries, header_paths, data_row_count = _build_column_summaries(
                df,
                header1,
                header2,
                data_start_idx
            )
            header_paths, column_summaries = _normalize_header_paths_and_summaries(
                header_paths,
                column_summaries
            )
        schema_input = {
            "total_rows": total_rows,
            "data_rows": data_row_count,
            "columns": len(header_paths),
            "header_row_1": [str(cell) for cell in header1],
            "header_row_2": [str(cell) for cell in header2],
            "column_summaries": column_summaries
        }
        
        schema_infer = None
        fallback_used = False
        row_filter = None
        normalization = None
        semantic_key_by_header = {}
        
        try:
            prompt = self.prompts["EXCEL_SCHEMA_INFER_PROMPT"] + "\n\nINPUT_JSON:\n" + json.dumps(
                schema_input,
                ensure_ascii=False
            )
            schema_infer = self.llm.chat_json(
                prompt,
                system=None,
                step="excel_schema_infer",
                filename=filename,
                source_id=source_id,
                mode="schema_infer"
            )
        except Exception as e:
            warnings.append(f"schema_infer_failed: {str(e)}")
        
        if isinstance(schema_infer, dict):
            semantic_key_by_header = schema_infer.get("semantic_key_by_header") or schema_infer.get("column_semantics") or {}
            if not isinstance(semantic_key_by_header, dict):
                semantic_key_by_header = {}
            row_filter = schema_infer.get("row_filter")
            normalization = schema_infer.get("normalization")
            if not isinstance(row_filter, dict):
                row_filter = None
            if not isinstance(normalization, dict):
                normalization = None
        
        semantic_key_by_header = _sanitize_semantic_key_by_header(semantic_key_by_header, header_paths)
        semantic_key_by_header, deterministic_overrides_count = _apply_deterministic_overrides(
            semantic_key_by_header,
            header_paths
        )
        coverage = _compute_coverage(semantic_key_by_header, header_paths)
        
        if coverage < SCHEMA_INFER_COVERAGE_THRESHOLD:
            fallback_used = True
            warnings.append("schema_infer_low_coverage")
            semantic_key_by_header = _fallback_semantic_key_by_header(header_paths, column_summaries)
            semantic_key_by_header, deterministic_overrides_count = _apply_deterministic_overrides(
                semantic_key_by_header,
                header_paths
            )
            coverage = _compute_coverage(semantic_key_by_header, header_paths)
        
        semantic_key_by_header = _resolve_semantic_key_conflicts(semantic_key_by_header, header_paths, warnings)
        header_paths, semantic_key_by_header = _apply_header_normalization_and_forced_mappings(
            header_paths,
            semantic_key_by_header
        )
        semantic_key_by_header = _ensure_termination_reason_mapping(
            df,
            header_paths,
            semantic_key_by_header,
            data_start_idx,
            sheet_name
        )
        
        if coverage < SCHEMA_INFER_COVERAGE_THRESHOLD:
            logger.info(
                "Excel mode=fallback_failed | filename=%s | source_id=%s | rows=%d | cols=%d | coverage=%.3f",
                filename,
                source_id,
                total_rows,
                len(header_paths),
                coverage
            )
            warnings.append("schema_infer_failed")
            extracted_json = {
                "data": [],
                "metadata": {
                    "mode": "schema_infer",
                    "coverage": round(coverage, 4),
                    "total_rows": total_rows,
                    "processed_rows": 0,
                    "data_rows": data_row_count,
                    "header_row_idx": header_row_idx,
                    "data_start_idx": data_start_idx,
                    "header_mode": header_mode,
                    "truncated": False,
                    "fallback_used": fallback_used,
                    "semantic_keys_count": 0,
                    "deterministic_overrides_count": deterministic_overrides_count,
                    "semantic_key_by_header": semantic_key_by_header,
                    "header_selection_debug": header_selection_debug,
                    "read_backend": read_backend,
                    "selected_sheet_name": selected_sheet_name,
                    "sheet_selection_debug": sheet_selection_debug,
                },
                "warnings": warnings
            }
            logger.info(
                "Excel schema infer failed: coverage=%.3f keys=%d records=0 fallback=%s",
                coverage,
                0,
                fallback_used
            )
            sheet_summary = {
                "sheet_name": sheet_name,
                "header_row_idx": header_row_idx,
                "data_rows": data_row_count,
                "coverage": round(coverage, 4),
                "warnings": list(warnings)
            }
            return extracted_json, None, sheet_summary, None
        
        semantic_keys = _build_semantic_keys(header_paths, semantic_key_by_header, warnings)
        logger.info(
            "Excel mode=%s | filename=%s | source_id=%s | rows=%d | cols=%d | coverage=%.3f",
            "fallback" if fallback_used else "schema_infer",
            filename,
            source_id,
            total_rows,
            len(header_paths),
            coverage
        )
        
        records = []
        truncated = False
        end_idx = min(total_rows, data_start_idx + MAX_ROWS_TO_PROCESS)
        if data_row_count > MAX_ROWS_TO_PROCESS:
            truncated = True
            warnings.append(
                f"数据行 {data_row_count} 行，仅处理前 {MAX_ROWS_TO_PROCESS} 行"
            )
        
        for row_idx in range(data_start_idx, end_idx):
            row_values = [_cell_to_str(df.iat[row_idx, col_idx]) for col_idx in range(len(semantic_keys))]
            if not row_values or all(value == "" for value in row_values):
                continue
            record = {}
            for col_idx, semantic_key in enumerate(semantic_keys):
                if semantic_key:
                    raw_value = df.iat[row_idx, col_idx]
                    record[semantic_key] = _normalize_value(raw_value)
            
            if row_filter and not _apply_row_filter(record, row_values, row_filter):
                continue
            
            if record:
                record["__source_file__"] = filename
                if add_sheet_name:
                    record["__sheet_name__"] = sheet_name
                records.append(record)
        
        sample_indices = _evenly_spaced_indices(data_start_idx, end_idx, SAMPLE_HEAD_ROWS)
        sample_lines = []
        if header1:
            sample_lines.append(",".join(_cell_to_str(cell) for cell in header1))
        if header2:
            sample_lines.append(",".join(_cell_to_str(cell) for cell in header2))
        for idx in sample_indices:
            row = df.iloc[idx].tolist()
            sample_lines.append(",".join(_cell_to_str(cell) for cell in row))
        sample_csv_text = "\n".join(sample_lines)
        
        extracted_json = {
            "data": records,
            "metadata": {
                "mode": "schema_infer",
                "coverage": round(coverage, 4),
                "total_rows": total_rows,
                "processed_rows": len(records),
                "data_rows": data_row_count,
                "header_row_idx": header_row_idx,
                "data_start_idx": data_start_idx,
                "header_mode": header_mode,
                "truncated": truncated,
                "fallback_used": fallback_used,
                "semantic_keys_count": sum(1 for key in semantic_keys if key),
                "deterministic_overrides_count": deterministic_overrides_count,
                "semantic_key_by_header": semantic_key_by_header,
                "header_selection_debug": header_selection_debug,
                "read_backend": read_backend,
                "selected_sheet_name": selected_sheet_name,
                "sheet_selection_debug": sheet_selection_debug,
            },
            "warnings": warnings
        }
        
        logger.info(
            "Excel schema infer: coverage=%.3f keys=%d records=%d fallback=%s",
            coverage,
            sum(1 for key in semantic_keys if key),
            len(records),
            fallback_used
        )
        
        sheet_summary = {
            "sheet_name": sheet_name,
            "header_row_idx": header_row_idx,
            "data_rows": data_row_count,
            "coverage": round(coverage, 4),
            "warnings": list(warnings)
        }
        table_meta = {
            "total_rows": total_rows,
            "truncated": truncated,
            "mode": "schema_infer",
            "sampled_rows": len(sample_indices)
        }
        return extracted_json, sample_csv_text, sheet_summary, table_meta
    
    def extract(self, file_path: str, extract_all_sheets: bool = False, preferred_sheet: Optional[str] = None) -> SourceDoc:
        """
        Extract content from an Excel file.
        
        Args:
            file_path: Path to the Excel file.
        
        Returns:
            SourceDoc with extracted content.
        
        Raises:
            Exception: If extraction fails critically.
        """
        self.clear_derived_files()
        
        filename = Path(file_path).name
        source_id = str(uuid.uuid4())
        selected_sheet_name, sheet_selection_debug = _choose_best_sheet(file_path, preferred_sheet=preferred_sheet)
        sheet_names = None
        top_level_warnings: List[str] = []
        if sheet_selection_debug.get("profile_sheet_not_found"):
            top_level_warnings.append("sheet_not_found_fallback_to_auto")
        if extract_all_sheets:
            try:
                sheet_names, _ = _list_sheet_names(file_path)
            except Exception as e:
                top_level_warnings.append(f"sheet_names_read_failed: {str(e)}")
                sheet_names = None
            if not sheet_names:
                extract_all_sheets = False
        
        if not extract_all_sheets:
            extracted_json, sample_csv_text, sheet_summary, table_meta = self._extract_sheet(
                file_path=file_path,
                sheet_name=selected_sheet_name,
                filename=filename,
                source_id=source_id,
                selected_sheet_name=selected_sheet_name,
                sheet_selection_debug=sheet_selection_debug,
                add_sheet_name=False
            )
            warnings = extracted_json.get("warnings") or []
            for warning in top_level_warnings:
                if warning not in warnings:
                    warnings.append(warning)
            extracted_json["warnings"] = warnings
            if sample_csv_text is None:
                blocks = [
                    SourceBlock(order=1, type=BlockType.EXTRACTED_JSON, content=extracted_json, meta={})
                ]
            else:
                blocks = [
                    SourceBlock(
                        order=1,
                        type=BlockType.TABLE_CSV,
                        content=sample_csv_text,
                        meta=table_meta or {}
                    ),
                    SourceBlock(order=2, type=BlockType.EXTRACTED_JSON, content=extracted_json, meta={})
                ]
            return SourceDoc(
                source_id=source_id,
                filename=filename,
                file_path=file_path,
                source_type="excel",
                blocks=blocks,
                extracted=extracted_json
            )
        
        combined_records: List[Dict[str, Any]] = []
        combined_warnings: List[str] = []
        sheet_summaries: List[Dict[str, Any]] = []
        records_count_by_sheet: Dict[str, int] = {}
        per_sheet: Dict[str, Dict[str, Any]] = {}
        truncated_workbook_level = False
        truncation_reason = None
        total_records_seen = 0
        selected_sheet_extracted = None
        selected_sheet_sample_csv_text = None
        selected_sheet_table_meta = None
        first_sheet_extracted = None
        first_sheet_sample_csv_text = None
        first_sheet_table_meta = None
        
        for sheet_name in sheet_names:
            sheet_extracted, sample_csv_text, sheet_summary, table_meta = self._extract_sheet(
                file_path=file_path,
                sheet_name=sheet_name,
                filename=filename,
                source_id=source_id,
                selected_sheet_name=selected_sheet_name,
                sheet_selection_debug=sheet_selection_debug,
                add_sheet_name=True
            )
            data_records = sheet_extracted.get("data")
            sheet_record_count = 0
            skipped_due_to_cap = 0
            if isinstance(data_records, list):
                for record in data_records:
                    if not isinstance(record, dict):
                        continue
                    total_records_seen += 1
                    if MAX_RECORDS_PER_WORKBOOK and len(combined_records) >= MAX_RECORDS_PER_WORKBOOK:
                        truncated_workbook_level = True
                        truncation_reason = "workbook_records_limit"
                        skipped_due_to_cap += 1
                        continue
                    if "__source_file__" not in record:
                        record["__source_file__"] = filename
                    if "__sheet_name__" not in record:
                        record["__sheet_name__"] = sheet_name
                    combined_records.append(record)
                    sheet_record_count += 1
            records_count_by_sheet[str(sheet_name)] = sheet_record_count
            sheet_metadata = sheet_extracted.get("metadata") if isinstance(sheet_extracted, dict) else {}
            sheet_warnings = list(sheet_extracted.get("warnings") or []) if isinstance(sheet_extracted, dict) else []
            no_records_reason = None
            if sheet_record_count == 0:
                data_rows = (sheet_metadata or {}).get("data_rows")
                header_row_idx = (sheet_metadata or {}).get("header_row_idx")
                if skipped_due_to_cap > 0:
                    no_records_reason = "workbook_records_limit_reached"
                elif "schema_infer_failed" in sheet_warnings:
                    no_records_reason = "schema_infer_failed_and_no_fallback"
                elif "no_header_row_detected" in sheet_warnings and (data_rows == 0 or header_row_idx is None or header_row_idx < 0):
                    no_records_reason = "no_header_detected"
                elif isinstance(data_rows, int) and data_rows == 0:
                    no_records_reason = "no_data_rows"
                elif isinstance(data_rows, int) and data_rows > 0:
                    no_records_reason = "filtered_all_rows"
                else:
                    no_records_reason = "read_failed"
            per_sheet[str(sheet_name)] = {
                "header_row_idx": (sheet_metadata or {}).get("header_row_idx"),
                "data_start_idx": (sheet_metadata or {}).get("data_start_idx"),
                "data_rows": (sheet_metadata or {}).get("data_rows"),
                "coverage": (sheet_metadata or {}).get("coverage"),
                "semantic_key_by_header": (sheet_metadata or {}).get("semantic_key_by_header"),
                "warnings": sheet_warnings,
                "records_count": sheet_record_count,
                "read_backend": (sheet_metadata or {}).get("read_backend"),
                "no_records_reason": no_records_reason,
            }
            sheet_warnings = sheet_extracted.get("warnings")
            if isinstance(sheet_warnings, list):
                combined_warnings.extend(sheet_warnings)
            sheet_summaries.append(sheet_summary)
            if first_sheet_extracted is None:
                first_sheet_extracted = sheet_extracted
                first_sheet_sample_csv_text = sample_csv_text
                first_sheet_table_meta = table_meta
            if sheet_name == selected_sheet_name:
                selected_sheet_extracted = sheet_extracted
                selected_sheet_sample_csv_text = sample_csv_text
                selected_sheet_table_meta = table_meta
        
        if selected_sheet_extracted is None:
            selected_sheet_extracted = first_sheet_extracted or {"data": [], "metadata": {}, "warnings": []}
            selected_sheet_sample_csv_text = first_sheet_sample_csv_text
            selected_sheet_table_meta = first_sheet_table_meta
        
        workbook_metadata = {
            "sheets": sheet_summaries,
            "processed_rows": len(combined_records),
            "records_count_by_sheet": records_count_by_sheet,
            "per_sheet": per_sheet,
            "truncated_workbook_level": truncated_workbook_level,
            "workbook_records_seen": total_records_seen,
            "selected_sheet_name": selected_sheet_name,
            "sheet_selection_debug": sheet_selection_debug,
        }
        if MAX_RECORDS_PER_WORKBOOK:
            workbook_metadata["workbook_records_limit"] = MAX_RECORDS_PER_WORKBOOK
        if truncated_workbook_level and truncation_reason:
            workbook_metadata["truncation_reason"] = truncation_reason

        extracted_json = {
            "data": combined_records,
            "metadata": workbook_metadata,
            "warnings": list(selected_sheet_extracted.get("warnings") or [])
        }
        
        warnings = extracted_json["warnings"]
        for warning in combined_warnings + top_level_warnings:
            if warning not in warnings:
                warnings.append(warning)
        extracted_json["warnings"] = warnings
        
        if selected_sheet_sample_csv_text is None:
            blocks = [
                SourceBlock(order=1, type=BlockType.EXTRACTED_JSON, content=extracted_json, meta={})
            ]
        else:
            blocks = [
                SourceBlock(
                    order=1,
                    type=BlockType.TABLE_CSV,
                    content=selected_sheet_sample_csv_text,
                    meta=selected_sheet_table_meta or {}
                ),
                SourceBlock(order=2, type=BlockType.EXTRACTED_JSON, content=extracted_json, meta={})
            ]
        
        return SourceDoc(
            source_id=source_id,
            filename=filename,
            file_path=file_path,
            source_type="excel",
            blocks=blocks,
            extracted=extracted_json
        )

# Backward compatibility function
def extract_excel(path: str, llm: LLMClient, prompts: dict) -> Tuple[List[SourceBlock], Any]:
    """
    Legacy function for backward compatibility.
    
    Extracts Excel file content and returns blocks and extracted JSON.
    
    Args:
        path: Path to the Excel file.
        llm: LLM client for extraction.
        prompts: Dictionary of prompts.
    
    Returns:
        Tuple of (blocks, extracted_json).
    """
    extractor = ExcelExtractor(llm, prompts)
    try:
        source_doc = extractor.extract(path)
        return source_doc.blocks, source_doc.extracted
    except Exception as e:
        error_msg = f"Failed to read Excel file: {e}"
        logger.error(error_msg)
        extracted_json = {
            "error": error_msg,
            "warnings": [f"无法读取Excel文件: {str(e)}"]
        }
        blocks = [
            SourceBlock(order=1, type=BlockType.ERROR, content=error_msg, meta={}),
            SourceBlock(order=2, type=BlockType.EXTRACTED_JSON, content=extracted_json, meta={})
        ]
        return blocks, extracted_json
