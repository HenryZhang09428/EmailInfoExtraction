from openpyxl import load_workbook
from openpyxl.utils import get_column_letter, column_index_from_string, range_boundaries
from datetime import datetime
from typing import Any, Dict, List, Optional
import re
from core.logger import get_logger

logger = get_logger(__name__)

# Supported date formats for parsing
DATE_FORMATS = [
    # ISO formats
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    # Chinese date formats
    "%Y年%m月%d日",
    "%Y年%m月%d号",
    "%Y年%-m月%-d日",  # Without leading zeros (may not work on all platforms)
    # Slash formats
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    # Other common formats
    "%Y.%m.%d",
    "%d.%m.%Y",
    "%Y%m%d",
]

def apply_fill_plan(template_path: str, fill_plan: Dict[str, Any], output_path: str) -> int:
    wb = load_workbook(template_path, data_only=False)
    
    target = fill_plan.get("target", {})
    sheet_name = None
    if isinstance(target, dict):
        sheet_name = target.get("sheet_name") or target.get("sheet")
    if not sheet_name:
        sheet_name = fill_plan.get("sheet_name") or fill_plan.get("sheet")
    if isinstance(sheet_name, str):
        sheet_name = sheet_name.strip()
    if not sheet_name:
        sheet_name = wb.sheetnames[0] if wb.sheetnames else None
    if not sheet_name or sheet_name not in wb.sheetnames:
        available = ", ".join(wb.sheetnames) if wb.sheetnames else "none"
        raise ValueError(f"Sheet '{sheet_name}' not found in template. Available sheets: {available}")
    
    ws = wb[sheet_name]
    
    clear_ranges = fill_plan.get("clear_ranges", [])
    for range_str in clear_ranges:
        _clear_range(ws, range_str)
    
    cells_written = 0
    row_writes = fill_plan.get("row_writes", [])
    logger.debug("apply_fill_plan: %d row_writes, %d writes", len(row_writes), len(fill_plan.get('writes', [])))
    
    for rw_idx, row_write in enumerate(row_writes):
        start_cell = row_write.get("start_cell")
        if not start_cell:
            logger.debug("apply_fill_plan: row_write %d has no start_cell, skip", rw_idx)
            continue
        
        start_col, start_row = _parse_cell(start_cell)
        rows = row_write.get("rows", [])
        column_mapping = row_write.get("column_mapping", {})
        
        logger.debug("apply_fill_plan: row_write %d (%s): %d rows, column_mapping has %d keys", rw_idx, start_cell, len(rows), len(column_mapping))
        
        if not rows:
            logger.debug("row_write %d (%s): 0 rows, skip", rw_idx, start_cell)
            continue
        
        first_row_data = rows[0] if rows else None
        
        if isinstance(first_row_data, dict) and "column_letter" in first_row_data:
            for cell_data in rows:
                if not isinstance(cell_data, dict):
                    continue
                col_letter = cell_data.get("column_letter")
                value = cell_data.get("value")
                if not col_letter:
                    continue
                if value is not None and isinstance(value, str) and not value.strip():
                    value = None
                try:
                    col_idx = column_index_from_string(col_letter)
                    cell = ws.cell(start_row, col_idx)
                    if _is_formula_cell(cell):
                        continue
                    v = _convert_value(value, cell.number_format)
                    cell.value = v
                    cells_written += 1
                except Exception as e:
                    logger.warning("Failed to write cell %s%d: %s", col_letter, start_row, e)
        elif isinstance(first_row_data, list):
            current_row = start_row
            for row_data in rows:
                if not isinstance(row_data, list):
                    current_row += 1
                    continue
                for cell_data in row_data:
                    if isinstance(cell_data, dict):
                        col_letter = cell_data.get("column_letter")
                        value = cell_data.get("value")
                        if col_letter:
                            try:
                                col_idx = column_index_from_string(col_letter)
                                cell = ws.cell(current_row, col_idx)
                                if _is_formula_cell(cell):
                                    continue
                                v = _convert_value(value, cell.number_format)
                                cell.value = v
                                cells_written += 1
                            except Exception as e:
                                logger.warning("Failed to write %s%d: %s", col_letter, current_row, e)
                current_row += 1
        elif column_mapping and isinstance(first_row_data, dict) and "column_letter" not in first_row_data:
            current_row = start_row
            logger.debug("apply_fill_plan: Writing %d rows starting at row %d using column_mapping", len(rows), current_row)
            logger.debug("apply_fill_plan: column_mapping has %d keys: %s", len(column_mapping), list(column_mapping.keys())[:10])
            for row_idx, row_data in enumerate(rows):
                row_cells_written = 0
                if isinstance(row_data, list):
                    for cell_data in row_data:
                        if isinstance(cell_data, dict):
                            col_letter = cell_data.get("column_letter")
                            value = cell_data.get("value")
                            if col_letter:
                                try:
                                    col_idx = column_index_from_string(col_letter)
                                    cell = ws.cell(current_row, col_idx)
                                    if _is_formula_cell(cell):
                                        continue
                                    v = _convert_value(value, cell.number_format)
                                    cell.value = v
                                    cells_written += 1
                                    row_cells_written += 1
                                except Exception as e:
                                    logger.warning("Failed to write %s%d: %s", col_letter, current_row, e)
                elif isinstance(row_data, dict):
                    for key, col_letter in column_mapping.items():
                        if key in row_data:
                            value = row_data[key]
                            if value is None or (isinstance(value, str) and not value.strip()):
                                continue
                            try:
                                col_idx = column_index_from_string(col_letter)
                                cell = ws.cell(current_row, col_idx)
                                if _is_formula_cell(cell):
                                    continue
                                v = _convert_value(value, cell.number_format)
                                cell.value = v
                                cells_written += 1
                                row_cells_written += 1
                            except Exception as e:
                                logger.warning("Failed to write %s to %s%d: %s", key, col_letter, current_row, e)
                
                if row_idx < 3 or row_idx == len(rows) - 1:
                    logger.debug("apply_fill_plan: Row %d/%d (excel row %d): %d cells written", row_idx+1, len(rows), current_row, row_cells_written)
                current_row += 1
            logger.info("apply_fill_plan: Finished writing %d rows, total cells_written=%d", len(rows), cells_written)
        else:
            logger.debug("row_write %d: unhandled format first_row_data=%s keys=%s", rw_idx, type(first_row_data).__name__, list(first_row_data.keys()) if isinstance(first_row_data, dict) else 'n/a')
    
    writes = fill_plan.get("writes", [])
    for write in writes:
        cell_ref = write.get("cell")
        value = write.get("value")
        if not cell_ref:
            continue
        col, row = _parse_cell(cell_ref)
        cell = ws.cell(row, col)
        if _is_formula_cell(cell):
            continue
        v = _convert_value(value, cell.number_format)
        cell.value = v
        cells_written += 1
    
    logger.info("apply_fill_plan: total cells_written=%d", cells_written)
    wb.save(output_path)
    wb.close()
    return cells_written

def _clear_range(ws, range_str: str):
    try:
        min_col, min_row, max_col, max_row = range_boundaries(range_str)
        
        for row in range(min_row, max_row + 1):
            for col_idx in range(min_col, max_col + 1):
                cell = ws.cell(row, col_idx)
                if not _is_formula_cell(cell):
                    cell.value = None
    except Exception:
        pass

def _parse_cell(cell_ref: str) -> tuple:
    match = re.match(r'([A-Z]+)(\d+)', cell_ref.upper())
    if not match:
        raise ValueError(f"Invalid cell reference: {cell_ref}")
    
    col_letter = match.group(1)
    row = int(match.group(2))
    col_idx = column_index_from_string(col_letter)
    return col_idx, row

def _is_formula_cell(cell) -> bool:
    if cell.data_type == 'f':
        return True
    if isinstance(cell.value, str) and cell.value.startswith('='):
        return True
    return False

def _convert_value(value: Any, number_format: str) -> Any:
    """
    Convert a value to the appropriate type for Excel cell.
    
    - ID numbers (15+ digits) are preserved as strings
    - Dates are parsed from multiple formats (ISO, Chinese, slash formats)
    - Numbers are converted to int/float when appropriate
    """
    if value is None:
        return None
    
    if isinstance(value, (int, float)):
        return value
    
    if isinstance(value, datetime):
        return value
    
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        
        # 身份证号 / 证件号等长数字 ID：必须作为文本处理，不能转成数字
        # 纯数字或最后一位为 X/x，且长度较长（>= 15），认为是证件号/ID
        if (v.isdigit() or (len(v) > 1 and v[:-1].isdigit() and v[-1] in ("X", "x"))) and len(v) >= 15:
            return v
        
        # 手机号码也保留为字符串 (11位纯数字)
        if v.isdigit() and len(v) == 11:
            return v
        
        # 尝试解析日期 (如果目标单元格是日期格式或值看起来像日期)
        if _is_date_format(number_format) or _looks_like_date(v):
            parsed_date = _parse_date(v)
            if parsed_date:
                return parsed_date
            else:
                # 解析失败，记录警告但保留原始字符串
                logger.warning("Date parsing failed for value '%s', keeping as string", v)
                return v
        
        # 尝试转换数字
        if _looks_like_number(v):
            try:
                if '.' in v:
                    return float(v)
                return int(v)
            except Exception:
                pass
    
    return value


def _parse_date(value: str) -> Optional[datetime]:
    """
    Parse a date string using multiple format patterns.
    
    Supports:
    - ISO format: 2024-01-15, 2024-01-15T10:30:00
    - Chinese format: 2024年1月15日, 2024年01月15号
    - Slash format: 2024/01/15, 15/01/2024, 01/15/2024
    - Dot format: 2024.01.15
    - Compact format: 20240115
    
    Returns:
        datetime object if parsing succeeds, None otherwise
    """
    if not value or not isinstance(value, str):
        return None
    
    v = value.strip()
    if not v:
        return None
    
    # 首先尝试 ISO 格式 (fromisoformat 处理更好)
    try:
        # 处理带时区的 ISO 格式
        normalized = v.replace('Z', '+00:00')
        return datetime.fromisoformat(normalized)
    except (ValueError, TypeError):
        pass
    
    # 处理中文日期格式中不带前导零的情况
    # 例如: "2024年1月5日" -> 尝试手动解析
    chinese_match = re.match(r'^(\d{4})年(\d{1,2})月(\d{1,2})[日号]?$', v)
    if chinese_match:
        try:
            year = int(chinese_match.group(1))
            month = int(chinese_match.group(2))
            day = int(chinese_match.group(3))
            return datetime(year, month, day)
        except (ValueError, TypeError):
            pass
    
    # 尝试其他预定义格式
    for fmt in DATE_FORMATS:
        try:
            # 跳过可能不兼容的格式 (%-m 在 Windows 上不支持)
            if '%-' in fmt:
                continue
            return datetime.strptime(v, fmt)
        except (ValueError, TypeError):
            continue
    
    return None


def _looks_like_date(value: str) -> bool:
    """
    Check if a string looks like it might be a date.
    """
    if not value:
        return False
    
    # ISO date pattern
    if re.match(r'^\d{4}-\d{2}-\d{2}', value):
        return True
    
    # Chinese date pattern
    if re.match(r'^\d{4}年\d{1,2}月\d{1,2}[日号]?', value):
        return True
    
    # Slash date pattern
    if re.match(r'^\d{1,4}/\d{1,2}/\d{1,4}$', value):
        return True
    
    # Dot date pattern
    if re.match(r'^\d{4}\.\d{2}\.\d{2}$', value):
        return True
    
    # Compact date pattern (YYYYMMDD)
    if re.match(r'^\d{8}$', value):
        return True
    
    return False

def _is_iso_date(s: str) -> bool:
    iso_pattern = r'^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$'
    return bool(re.match(iso_pattern, s))

def _is_date_format(fmt: str) -> bool:
    if not fmt:
        return False
    date_indicators = ['d', 'm', 'y', 'h', 's', 'D', 'M', 'Y', 'H', 'S']
    return any(indicator in fmt for indicator in date_indicators)

def _looks_like_number(s: str) -> bool:
    s = s.strip()
    if not s:
        return False
    try:
        float(s.replace(',', '').replace(' ', ''))
        return True
    except:
        return False
