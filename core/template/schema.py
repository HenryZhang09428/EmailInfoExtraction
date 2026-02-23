"""
模板模式定义模块 (Template Schema Module)
========================================

本模块定义了 Excel 模板的结构化描述，用于解析和填充模板。
所有数据结构均基于 Pydantic 模型，确保类型安全和数据校验。
"""

from typing import Literal, Optional, List, Dict, Any
from pydantic import BaseModel


class KeyValuePair(BaseModel):
    """
    键值对模型，用于表单式布局中的键-值单元格映射。

    属性:
        key_cell: 键所在单元格引用（如 "A1"）
        value_cell: 值所在单元格引用（如 "B1"）
        key_text: 键的文本内容
    """
    key_cell: str
    value_cell: str
    key_text: str


class TableHeader(BaseModel):
    """
    表头列信息模型，描述表格中每一列的元数据。

    属性:
        col_letter: 列字母标识（如 "A", "B", "C"）
        col_index: 列索引（从 1 开始）
        header_path: 表头路径，多行表头时用 "/" 连接（如 "一级/二级"）
        sample_values: 该列在模板中的示例值列表，用于语义推断
    """
    col_letter: str
    col_index: int
    header_path: str
    sample_values: List[str]


class TableInfo(BaseModel):
    """
    表格信息模型，描述表格区域的结构和约束。

    属性:
        range: 表格区域范围（如 "A1:E10"）
        header: 表头列列表
        sample_rows: 模板中的示例数据行，用作 few-shot 示例；可选
    """
    range: str
    header: List[TableHeader]
    sample_rows: Optional[List[Dict[str, Any]]] = None  # 模板中的 few-shot 示例


class Constraints(BaseModel):
    """
    单元格约束模型，描述表格中的公式、校验和数字格式。

    属性:
        has_formulas: 是否包含公式单元格
        formula_cells: 公式单元格引用列表
        validations: 数据校验规则列表
        number_formats: 列字母到数字格式的映射
    """
    has_formulas: bool
    formula_cells: List[str]
    validations: List[Dict[str, Any]]
    number_formats: Dict[str, str]


class RegionSchema(BaseModel):
    """
    区域模式模型，描述工作表中一个逻辑区域的结构。

    属性:
        region_id: 区域唯一标识
        layout_type: 布局类型，"table" 表示表格，"form" 表示表单
        header_rows: 表头行号列表
        key_value_pairs: 表单式布局的键值对列表；表格布局时为 None
        table: 表格信息；表单布局时为 None
        constraints: 该区域的单元格约束
    """
    region_id: str
    layout_type: Literal["table", "form"]
    header_rows: List[int]
    key_value_pairs: Optional[List[KeyValuePair]] = None
    table: Optional[TableInfo] = None
    constraints: Constraints


class SheetSchema(BaseModel):
    """
    工作表模式模型，描述单个 Excel 工作表的区域结构。

    属性:
        sheet: 工作表名称
        regions: 该工作表中的区域列表
    """
    sheet: str
    regions: List[RegionSchema]


class TemplateSchema(BaseModel):
    """
    模板模式模型，描述整个 Excel 模板的完整结构。

    属性:
        sheet_schemas: 各工作表的模式列表
    """
    sheet_schemas: List[SheetSchema]
