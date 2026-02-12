from typing import Literal, Optional, List, Dict, Any
from pydantic import BaseModel

class KeyValuePair(BaseModel):
    key_cell: str
    value_cell: str
    key_text: str

class TableHeader(BaseModel):
    col_letter: str
    col_index: int
    header_path: str
    sample_values: List[str]

class TableInfo(BaseModel):
    range: str
    header: List[TableHeader]
    sample_rows: Optional[List[Dict[str, Any]]] = None  # Few-shot examples from template

class Constraints(BaseModel):
    has_formulas: bool
    formula_cells: List[str]
    validations: List[Dict[str, Any]]
    number_formats: Dict[str, str]

class RegionSchema(BaseModel):
    region_id: str
    layout_type: Literal["table", "form"]
    header_rows: List[int]
    key_value_pairs: Optional[List[KeyValuePair]] = None
    table: Optional[TableInfo] = None
    constraints: Constraints

class SheetSchema(BaseModel):
    sheet: str
    regions: List[RegionSchema]

class TemplateSchema(BaseModel):
    sheet_schemas: List[SheetSchema]
