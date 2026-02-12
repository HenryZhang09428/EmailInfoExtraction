from typing import Literal, Any, Optional, List
from enum import Enum
from pydantic import BaseModel

SourceType = Literal["excel", "email", "image", "text", "other", "error"]


class BlockType(str, Enum):
    """Enum for SourceBlock types ensuring type safety across the codebase."""
    TEXT = "text"
    TABLE_CSV = "table_csv"
    OCR_TEXT = "ocr_text"
    EMAIL_TEXT = "email_text"
    EMAIL_BODY_TEXT = "email_body_text"
    IMAGE_METADATA = "image_metadata"
    BINARY = "binary"
    # Additional types used in extractors
    ERROR = "error"
    EXTRACTED_JSON = "extracted_json"
    EMAIL_HEADERS = "email_headers"
    EMAIL_HTML = "email_html"
    EMAIL_PARTS_SUMMARY = "email_parts_summary"
    EML_IMAGE_FILE = "eml_image_file"
    EML_FILE_PART = "eml_file_part"
    INLINE_IMAGE_REF = "inline_image_ref"
    INLINE_IMAGE_FILE = "inline_image_file"
    IMAGE_PATH = "image_path"
    VISION_EXTRACTED_JSON = "vision_extracted_json"
    # For docx content types
    TABLE = "table"
    IMAGE_PLACEHOLDER = "image_placeholder"


class SourceBlock(BaseModel):
    order: int
    type: BlockType
    content: Any
    meta: dict

    class Config:
        use_enum_values = True  # Serialize enum as its value


class SourceDoc(BaseModel):
    source_id: str
    filename: str
    file_path: str  # Mandatory field for unique document identification
    source_type: SourceType
    blocks: List[SourceBlock]
    extracted: Any
    parent_source_id: Optional[str] = None

class Fact(BaseModel):
    name: str
    value: Any
    sources: List[dict]


class FillPlanTarget(BaseModel):
    """Target specification for a fill plan."""
    sheet: Optional[str] = None
    region_id: Optional[str] = None
    layout_type: Optional[str] = None
    clear_policy: Optional[str] = None


class RowWrite(BaseModel):
    """A row write operation in a fill plan."""
    start_cell: str
    rows: List[dict]
    column_mapping: dict

class CellWrite(BaseModel):
    """A single cell write operation."""
    cell: str
    value: Any
    sheet: Optional[str] = None


class FillPlan(BaseModel):
    """
    Standardized output for template fill planning.
    Ensures type-safe, validated fill plan structure.
    """
    target: FillPlanTarget
    clear_ranges: List[str] = []
    row_writes: List[RowWrite] = []
    writes: List[CellWrite] = []
    warnings: List[str] = []
    llm_used: bool = False  # Whether LLM was successfully used for planning
    constant_values_count: int = 0  # Number of constant values inferred
    debug: Optional[dict] = None

    @classmethod
    def from_dict(cls, data: dict) -> "FillPlan":
        """Create a FillPlan from a dictionary, handling nested objects."""
        target_data = data.get("target", {})
        target = FillPlanTarget(**target_data) if isinstance(target_data, dict) else FillPlanTarget()
        
        row_writes = []
        for rw in data.get("row_writes", []):
            if isinstance(rw, dict):
                row_writes.append(RowWrite(**rw))
        
        writes = []
        for w in data.get("writes", []):
            if isinstance(w, dict):
                writes.append(CellWrite(**w))
        
        return cls(
            target=target,
            clear_ranges=data.get("clear_ranges", []),
            row_writes=row_writes,
            writes=writes,
            warnings=data.get("warnings", []),
            llm_used=data.get("llm_used", False),
            constant_values_count=data.get("constant_values_count", 0),
            debug=data.get("debug")
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for backward compatibility."""
        return {
            "target": self.target.model_dump(exclude_none=True),
            "clear_ranges": self.clear_ranges,
            "row_writes": [rw.model_dump() for rw in self.row_writes],
            "writes": [w.model_dump(exclude_none=True) for w in self.writes],
            "warnings": self.warnings,
            "llm_used": self.llm_used,
            "constant_values_count": self.constant_values_count,
            "debug": self.debug
        }


class IntermediateRepresentation(BaseModel):
    sources: List[SourceDoc]
    facts: List[Fact]
    target_schema: Optional[Any] = None
    output: Optional[Any] = None
    scores: Optional[dict] = None

def new_ir() -> IntermediateRepresentation:
    return IntermediateRepresentation(
        sources=[],
        facts=[],
        target_schema=None,
        output=None,
        scores=None
    )
