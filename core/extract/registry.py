"""
ExtractorRegistry: maps source_type -> extraction handler.

Each handler returns an ExtractResult (blocks, extracted, derived_files)
so the pipeline never needs to know which extractor class to instantiate.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from core.ir import SourceDoc, SourceBlock, BlockType
from core.llm import LLMClient
from core.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data contract returned by every extraction handler
# ---------------------------------------------------------------------------

@dataclass
class ExtractResult:
    """Uniform output produced by every extraction handler."""
    blocks: List[SourceBlock]
    extracted: Any
    derived_files: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# Type alias for extraction handlers.
# Signature: (source_doc, *, excel_sheet) -> ExtractResult
ExtractHandler = Callable[..., ExtractResult]


class ExtractorRegistry:
    """
    Registry that maps *source_type* to an extraction handler.

    Built-in handlers are registered at construction time; callers can
    override or extend via :meth:`register`.
    """

    def __init__(self, llm: LLMClient, prompts: dict):
        self._llm = llm
        self._prompts = prompts
        self._handlers: Dict[str, ExtractHandler] = {}
        self._register_defaults()

    # -- public API ----------------------------------------------------------

    def register(self, source_type: str, handler: ExtractHandler) -> None:
        self._handlers[source_type] = handler

    def extract(
        self,
        source_doc: SourceDoc,
        *,
        excel_sheet: Optional[str] = None,
    ) -> ExtractResult:
        """Dispatch extraction to the registered handler for *source_doc.source_type*."""
        handler = self._handlers.get(source_doc.source_type)
        if handler is None:
            logger.warning("No handler for source_type=%s (%s)", source_doc.source_type, source_doc.filename)
            return ExtractResult(blocks=[], extracted=None)
        return handler(source_doc, excel_sheet=excel_sheet)

    # -- built-in handlers ---------------------------------------------------

    def _register_defaults(self) -> None:
        self.register("excel", self._handle_excel)
        self.register("email", self._handle_email)
        self.register("image", self._handle_image)
        self.register("text", self._handle_text)
        self.register("other", self._handle_other)

    # ---- Excel (includes 0-row retry) ----

    def _handle_excel(self, source_doc: SourceDoc, *, excel_sheet: Optional[str] = None) -> ExtractResult:
        from core.extractors import ExcelExtractor

        extractor = ExcelExtractor(self._llm, self._prompts)
        result = extractor.safe_extract(
            source_doc.file_path,
            extract_all_sheets=False,
            preferred_sheet=excel_sheet,
        )

        # Retry with all sheets when single-sheet extraction returns 0 rows
        payload = result.extracted if isinstance(result.extracted, dict) else {}
        rows = payload.get("data") if isinstance(payload, dict) else None
        if (isinstance(rows, list) and len(rows) == 0) or rows is None:
            if excel_sheet is None:
                logger.info("Excel 0-row retry with extract_all_sheets=True for %s", source_doc.filename)
                retry = extractor.safe_extract(
                    source_doc.file_path,
                    extract_all_sheets=True,
                    preferred_sheet=excel_sheet,
                )
                retry_payload = retry.extracted if isinstance(retry.extracted, dict) else {}
                retry_rows = retry_payload.get("data") if isinstance(retry_payload, dict) else None
                if isinstance(retry_rows, list) and len(retry_rows) > 0:
                    logger.info("Excel all-sheets retry recovered %d rows for %s", len(retry_rows), source_doc.filename)
                    result = retry

        extracted = result.extracted
        if extracted is None:
            logger.warning("ExcelExtractor returned None for %s, creating default structure", source_doc.filename)
            extracted = {
                "error": "Extraction returned None",
                "warnings": ["提取结果为空，可能是文件格式问题或LLM调用失败"],
                "filename": source_doc.filename,
                "file_path": source_doc.file_path,
            }

        return ExtractResult(
            blocks=result.blocks,
            extracted=extracted,
            derived_files=extractor.get_derived_files(),
        )

    # ---- Email ----

    def _handle_email(self, source_doc: SourceDoc, **_kw: Any) -> ExtractResult:
        from core.extractors import EmailExtractor

        extractor = EmailExtractor(self._llm, self._prompts, source_doc.source_id)
        result = extractor.safe_extract(source_doc.file_path)
        return ExtractResult(
            blocks=result.blocks,
            extracted=result.extracted,
            derived_files=extractor.get_derived_files(),
        )

    # ---- Image ----

    def _handle_image(self, source_doc: SourceDoc, **_kw: Any) -> ExtractResult:
        from core.extractors import ImageExtractor

        extractor = ImageExtractor(self._llm, self._prompts)
        result = extractor.safe_extract(source_doc.file_path)
        return ExtractResult(
            blocks=result.blocks,
            extracted=result.extracted,
            derived_files=extractor.get_derived_files(),
        )

    # ---- Plain text ----

    def _handle_text(self, source_doc: SourceDoc, **_kw: Any) -> ExtractResult:
        with open(source_doc.file_path, "r", encoding="utf-8", errors="ignore") as fh:
            text = fh.read()
        blocks = [SourceBlock(order=1, type=BlockType.TEXT, content=text, meta={})]
        extracted = {"text": text, "file_path": source_doc.file_path}
        return ExtractResult(blocks=blocks, extracted=extracted)

    # ---- Other / binary ----

    def _handle_other(self, source_doc: SourceDoc, **_kw: Any) -> ExtractResult:
        file_size = os.path.getsize(source_doc.file_path) if os.path.exists(source_doc.file_path) else 0
        blocks = [SourceBlock(
            order=1,
            type=BlockType.BINARY,
            content={"note": "Binary or unknown file format, content not extracted"},
            meta={"file_size": file_size},
        )]
        extracted = {
            "binary": True,
            "filename": source_doc.filename,
            "file_path": source_doc.file_path,
            "file_size": file_size,
            "warnings": ["Binary or unknown file format, content not extracted"],
        }
        return ExtractResult(blocks=blocks, extracted=extracted)
