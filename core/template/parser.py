from openpyxl import load_workbook
from openpyxl.utils import get_column_letter, column_index_from_string, range_boundaries
from openpyxl.cell.cell import Cell
from typing import List, Tuple, Optional
from core.template.schema import (
    TemplateSchema, SheetSchema, RegionSchema, TableInfo, TableHeader,
    Constraints, KeyValuePair
)

def parse_template_xlsx(path: str) -> TemplateSchema:
    wb = load_workbook(path, data_only=False)
    sheet_schemas = []
    
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        regions = []
        
        if ws.tables:
            for table_name, table in ws.tables.items():
                ref = table.ref
                region = _parse_table_region(ws, ref, f"table_{table_name}")
                if region:
                    regions.append(region)
        
        if not regions:
            if ws.max_row > 0 and ws.max_column > 0:
                region = _detect_region_from_used_range(ws)
                if region:
                    regions.append(region)
        
        if regions:
            sheet_schemas.append(SheetSchema(sheet=sheet_name, regions=regions))
    
    wb.close()
    return TemplateSchema(sheet_schemas=sheet_schemas)

def _parse_table_region(ws, ref: str, region_id: str) -> Optional[RegionSchema]:
    try:
        min_col, min_row, max_col, max_row = range_boundaries(ref)
        min_col_idx = min_col
        max_col_idx = max_col
        
        header_rows = [min_row]
        if min_row + 1 <= max_row:
            second_row_has_content = any(
                ws.cell(min_row + 1, col).value for col in range(min_col_idx, max_col_idx + 1)
            )
            if second_row_has_content:
                header_rows.append(min_row + 1)
        
        table_info, constraints = _extract_table_info(ws, min_row, max_row, min_col_idx, max_col_idx, header_rows)
        
        return RegionSchema(
            region_id=region_id,
            layout_type="table",
            header_rows=header_rows,
            table=table_info,
            constraints=constraints
        )
    except Exception:
        return None

def _detect_region_from_used_range(ws) -> Optional[RegionSchema]:
    try:
        min_row = 1
        min_col = 1
        max_row = ws.max_row
        max_col = ws.max_column
        min_col_idx = min_col
        max_col_idx = max_col
        
        header_row = _find_header_row(ws, min_row, max_row, min_col_idx, max_col_idx)
        if header_row is None:
            return None
        
        header_rows = [header_row]
        if header_row + 1 <= max_row:
            second_row_has_content = any(
                ws.cell(header_row + 1, col).value for col in range(min_col_idx, max_col_idx + 1)
            )
            if second_row_has_content and _is_header_row(ws, header_row + 1, min_col_idx, max_col_idx):
                header_rows.append(header_row + 1)
        
        data_start_row = max(header_rows) + 1
        max_data_row = min(max_row, header_row + 500)
        
        table_info, constraints = _extract_table_info(ws, header_row, max_data_row, min_col_idx, max_col_idx, header_rows)
        
        return RegionSchema(
            region_id=f"region_{header_row}",
            layout_type="table",
            header_rows=header_rows,
            table=table_info,
            constraints=constraints
        )
    except Exception:
        return None

def _find_header_row(ws, start_row: int, end_row: int, start_col: int, end_col: int) -> Optional[int]:
    best_row = None
    best_count = 0
    
    for row in range(start_row, min(end_row + 1, start_row + 100)):
        non_empty_count = sum(
            1 for col in range(start_col, end_col + 1)
            if ws.cell(row, col).value
        )
        if non_empty_count > best_count:
            best_count = non_empty_count
            best_row = row
        
        if best_count >= 3 and row + 1 <= end_row:
            has_data_below = any(
                ws.cell(row + 1, col).value for col in range(start_col, end_col + 1)
            )
            if has_data_below:
                return best_row
    
    return best_row if best_count >= 2 else None

def _is_header_row(ws, row: int, start_col: int, end_col: int) -> bool:
    non_empty = sum(1 for col in range(start_col, end_col + 1) if ws.cell(row, col).value)
    return non_empty >= 2

def _extract_table_info(ws, start_row: int, end_row: int, start_col: int, end_col: int, header_rows: List[int]) -> Tuple[TableInfo, Constraints]:
    headers = []
    formula_cells = []
    number_formats = {}
    validations = []
    
    max_header_row = max(header_rows)
    data_start_row = max_header_row + 1
    
    # Build header path lookup for sample rows
    header_paths = []
    
    for col_idx in range(start_col, end_col + 1):
        col_letter = get_column_letter(col_idx)
        header_path = _build_header_path(ws, header_rows, col_idx)
        header_paths.append(header_path)
        
        sample_values = []
        for sample_row in range(data_start_row, min(data_start_row + 3, end_row + 1)):
            cell = ws.cell(sample_row, col_idx)
            if cell.value is not None:
                sample_values.append(str(cell.value))
        
        if data_start_row <= end_row:
            sample_cell = ws.cell(data_start_row, col_idx)
            if sample_cell.number_format and sample_cell.number_format != "General":
                number_formats[col_letter] = sample_cell.number_format
        
        headers.append(TableHeader(
            col_letter=col_letter,
            col_index=col_idx,
            header_path=header_path,
            sample_values=sample_values
        ))
    
    # Extract complete sample rows as few-shot examples (up to 3 rows)
    sample_rows = []
    max_sample_rows = 3
    for row_idx in range(data_start_row, min(data_start_row + max_sample_rows, end_row + 1)):
        row_data = {}
        has_data = False
        for col_offset, col_idx in enumerate(range(start_col, end_col + 1)):
            cell = ws.cell(row_idx, col_idx)
            header_path = header_paths[col_offset] if col_offset < len(header_paths) else f"Col_{col_idx}"
            if header_path and cell.value is not None:
                # Skip formula cells in samples
                if cell.data_type != 'f' and not (isinstance(cell.value, str) and cell.value.startswith('=')):
                    row_data[header_path] = str(cell.value)
                    has_data = True
        if has_data:
            sample_rows.append(row_data)
    
    for row in range(start_row, end_row + 1):
        for col_idx in range(start_col, end_col + 1):
            cell = ws.cell(row, col_idx)
            if cell.data_type == 'f' or (isinstance(cell.value, str) and cell.value.startswith('=')):
                col_letter = get_column_letter(col_idx)
                formula_cells.append(f"{col_letter}{row}")
    
    range_str = f"{get_column_letter(start_col)}{start_row}:{get_column_letter(end_col)}{end_row}"
    
    table_info = TableInfo(
        range=range_str,
        header=headers,
        sample_rows=sample_rows if sample_rows else None
    )
    
    constraints = Constraints(
        has_formulas=len(formula_cells) > 0,
        formula_cells=formula_cells,
        validations=validations,
        number_formats=number_formats
    )
    
    return table_info, constraints

def _build_header_path(ws, header_rows: List[int], col_idx: int) -> str:
    if len(header_rows) == 1:
        cell = ws.cell(header_rows[0], col_idx)
        return str(cell.value) if cell.value else ""
    
    parts = []
    prev_value = ""
    for row in header_rows:
        cell = ws.cell(row, col_idx)
        value = str(cell.value) if cell.value else prev_value
        if value:
            parts.append(value)
            prev_value = value
    
    return "/".join(parts) if parts else ""
