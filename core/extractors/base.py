"""
提取器基类模块 (Base Extractor Module)
=====================================

为所有提取器提供统一接口和错误处理。
"""
import uuid
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

from core.ir import SourceDoc, SourceBlock, BlockType
from core.llm import LLMClient
from core.logger import get_logger

logger = get_logger(__name__)


class BaseExtractor(ABC):
    """
    所有提取器的抽象基类。

    提供标准接口:
    - extract(): 抽象方法，子类必须实现
    - safe_extract(): 包装方法，捕获异常并返回错误 SourceDoc
    """
    
    def __init__(self, llm: LLMClient, prompts: Optional[dict] = None):
        """
        初始化提取器。

        参数:
            llm: 用于 AI 提取的 LLM 客户端
            prompts: 提取用提示词字典，可选
        """
        self.llm = llm
        self.prompts = prompts or {}
        self.derived_files: List[str] = []
    
    @abstractmethod
    def extract(self, file_path: str) -> SourceDoc:
        """
        从文件提取内容并返回 SourceDoc。

        参数:
            file_path: 待提取文件路径

        返回:
            包含提取内容的 SourceDoc

        抛出:
            Exception: 提取失败时（由 safe_extract 捕获）
        """
        pass
    
    def safe_extract(self, file_path: str) -> SourceDoc:
        """
        安全提取：捕获异常，防止流水线崩溃。

        失败时记录错误并返回标准化的错误 SourceDoc。

        参数:
            file_path: 待提取文件路径

        返回:
            成功时返回包含内容的 SourceDoc，失败时返回错误 SourceDoc
        """
        try:
            result = self.extract(file_path)
            return result
        except Exception as e:
            logger.error("Extraction failed for %s: %s", file_path, e, exc_info=True)
            return self._create_error_source_doc(file_path, e)
    
    def _create_error_source_doc(self, file_path: str, error: Exception) -> SourceDoc:
        """
        创建标准化的错误 SourceDoc。

        参数:
            file_path: 提取失败的文件路径
            error: 发生的异常

        返回:
            包含错误信息的 SourceDoc
        """
        filename = Path(file_path).name if file_path else "unknown"
        source_id = str(uuid.uuid4())
        
        error_block = SourceBlock(
            order=1,
            type=BlockType.ERROR,
            content={"error": str(error), "error_type": type(error).__name__},
            meta={}
        )
        
        extracted_json_block = SourceBlock(
            order=2,
            type=BlockType.EXTRACTED_JSON,
            content={"error": str(error)},
            meta={}
        )
        
        return SourceDoc(
            source_id=source_id,
            filename=filename,
            file_path=file_path or "",
            source_type="error",
            blocks=[error_block, extracted_json_block],
            extracted={"error": str(error)}
        )
    
    def get_derived_files(self) -> List[str]:
        """
        获取上次提取产生的衍生文件列表（如邮件附件）。

        返回:
            衍生文件路径列表
        """
        return self.derived_files
    
    def clear_derived_files(self) -> None:
        """清空衍生文件列表。"""
        self.derived_files = []
