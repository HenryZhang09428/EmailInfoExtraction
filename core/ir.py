"""
中间表示模块 (Intermediate Representation Module)
================================================

定义提取与填充流程中的核心数据结构：SourceDoc、Fact、FillPlan 等。
"""

from typing import Literal, Any, Optional, List
from enum import Enum
from pydantic import BaseModel

# 文档来源类型
SourceType = Literal["excel", "email", "image", "text", "other", "error"]


class BlockType(str, Enum):
    """
    SourceBlock 类型枚举，确保全代码库中的类型安全。
    涵盖文本、表格、OCR、邮件、图片、二进制等多种块类型。
    """
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
    """
    源文档块模型，表示文档中的一个逻辑块（如一段文本、一个表格）。

    属性:
        order: 块在文档中的顺序
        type: 块类型（BlockType 枚举）
        content: 块内容
        meta: 元数据
    """
    order: int
    type: BlockType
    content: Any
    meta: dict

    class Config:
        use_enum_values = True  # 序列化时使用枚举值


class SourceDoc(BaseModel):
    """
    源文档模型，表示一个输入文件及其提取结果。

    属性:
        source_id: 文档唯一标识
        filename: 文件名
        file_path: 文件绝对路径（必填，用于唯一标识）
        source_type: 来源类型
        blocks: 文档块列表
        extracted: 提取结果（任意结构）
        parent_source_id: 父文档 ID（如从邮件附件衍生）
    """
    source_id: str
    filename: str
    file_path: str  # 必填字段，用于唯一标识文档
    source_type: SourceType
    blocks: List[SourceBlock]
    extracted: Any
    parent_source_id: Optional[str] = None


class Fact(BaseModel):
    """
    事实模型，表示一条从提取结果中得到的结构化事实。

    属性:
        name: 事实名称/键
        value: 事实值
        sources: 来源引用列表
    """
    name: str
    value: Any
    sources: List[dict]


class FillPlanTarget(BaseModel):
    """
    填充计划目标模型，指定填充的目标工作表、区域等。
    """
    sheet: Optional[str] = None
    region_id: Optional[str] = None
    layout_type: Optional[str] = None
    clear_policy: Optional[str] = None


class RowWrite(BaseModel):
    """
    行写入操作模型，表示填充计划中的批量行写入。
    """
    start_cell: str
    rows: List[dict]
    column_mapping: dict

class CellWrite(BaseModel):
    """
    单元格写入操作模型，表示单格写入。
    """
    cell: str
    value: Any
    sheet: Optional[str] = None


class FillPlan(BaseModel):
    """
    填充计划模型，模板填充规划的标准输出。

    确保类型安全、结构校验的填充计划。
    属性包括目标、清除范围、行写入、单元格写入、警告等。
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
        """从字典创建 FillPlan，处理嵌套对象。"""
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
        """转换为字典，保持向后兼容。"""
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
    """
    中间表示模型，贯穿提取与填充流程的核心数据结构。

    属性:
        sources: 源文档列表
        facts: 事实列表
        target_schema: 目标模式（可选）
        output: 输出结果（可选）
        scores: 评分信息（可选）
    """
    sources: List[SourceDoc]
    facts: List[Fact]
    target_schema: Optional[Any] = None
    output: Optional[Any] = None
    scores: Optional[dict] = None


def new_ir() -> IntermediateRepresentation:
    """创建空的中间表示实例。"""
    return IntermediateRepresentation(
        sources=[],
        facts=[],
        target_schema=None,
        output=None,
        scores=None
    )
