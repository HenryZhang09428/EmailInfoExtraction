"""
提取器注册表模块 (Extractor Registry Module)
==========================================

将 source_type 映射到对应的提取处理器。
每个处理器返回 ExtractResult（blocks、extracted、derived_files），
流水线无需知道具体使用哪个提取器类。
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
    """
    提取结果数据类，所有提取处理器统一返回此结构。

    属性:
        blocks: 源文档块列表
        extracted: 提取的 JSON 等结构化数据
        derived_files: 衍生文件路径列表（如邮件附件）
    """
    blocks: List[SourceBlock]
    extracted: Any
    derived_files: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# Type alias for extraction handlers.
# Signature: (source_doc) -> ExtractResult
ExtractHandler = Callable[..., ExtractResult]


class ExtractorRegistry:
    """
    提取器注册表：将 source_type 映射到提取处理器。

    构造时注册内置处理器；调用方可通过 register 覆盖或扩展。
    """

    def __init__(
        self,
        llm: LLMClient,
        prompts: dict,
        extractor_options: Optional[Dict[str, Any]] = None,
    ):
        self._llm = llm
        self._prompts = prompts
        self._extractor_options = extractor_options or {}
        self._handlers: Dict[str, ExtractHandler] = {}
        self._register_defaults()

    # -- public API ----------------------------------------------------------

    def register(self, source_type: str, handler: ExtractHandler) -> None:
        """注册或覆盖指定 source_type 的处理器。"""
        self._handlers[source_type] = handler

    def extract(self, source_doc: SourceDoc) -> ExtractResult:
        """根据 source_doc.source_type 分发到对应处理器执行提取。"""
        handler = self._handlers.get(source_doc.source_type)
        if handler is None:
            logger.warning("No handler for source_type=%s (%s)", source_doc.source_type, source_doc.filename)
            return ExtractResult(blocks=[], extracted=None)
        return handler(source_doc)

    # -- built-in handlers ---------------------------------------------------

    def _register_defaults(self) -> None:
        self.register("excel", self._handle_excel)
        self.register("email", self._handle_email)
        self.register("image", self._handle_image)
        self.register("text", self._handle_text)
        self.register("other", self._handle_other)

    # ---- Excel (smart automatic multi-sheet) ----

    def _handle_excel(self, source_doc: SourceDoc) -> ExtractResult:
        from core.extractors import ExcelExtractor
        from core.extractors.excel.reader import ExcelReader

        header_force_map = self._extractor_options.get("header_force_map")
        preferred_sheet_cfg = self._extractor_options.get("excel_preferred_sheet")
        preferred_sheet = preferred_sheet_cfg.strip() if isinstance(preferred_sheet_cfg, str) and preferred_sheet_cfg.strip() else None
        extractor = ExcelExtractor(
            self._llm,
            self._prompts,
            header_force_map=header_force_map if isinstance(header_force_map, dict) else None,
        )

        # ------------------------------------------------------------------
        # Multi-sheet extraction strategy (configurable via profile YAML).
        #
        # extract_mode (from profile excel.extract_mode):
        #   "auto"   — (default) smart automatic: scan all sheets, extract
        #               ≥ 2 useful sheets and merge; 0-row fallback retry.
        #   "single" — only extract one sheet (best or preferred).
        #   "all"    — always extract ALL sheets and merge records.
        # ------------------------------------------------------------------

        extract_mode_cfg = self._extractor_options.get("excel_extract_mode")
        extract_mode = (
            extract_mode_cfg.strip().lower()
            if isinstance(extract_mode_cfg, str) and extract_mode_cfg.strip().lower() in {"auto", "single", "all"}
            else "auto"
        )
        logger.info("Excel extract_mode=%s for %s", extract_mode, source_doc.filename)

        if extract_mode == "single":
            # ---- Single sheet mode: only extract best / preferred sheet ----
            result = extractor.safe_extract(
                source_doc.file_path,
                extract_all_sheets=False,
                preferred_sheet=preferred_sheet,
            )

        elif extract_mode == "all":
            # ---- All sheets mode: always extract every sheet ----
            result = extractor.safe_extract(
                source_doc.file_path,
                extract_all_sheets=True,
                preferred_sheet=preferred_sheet,
            )

        else:
            # ---- Auto mode (default): smart multi-sheet detection ----
            # 1. Cheaply scan all sheets (first 30 rows × 80 cols), keep
            #    every sheet that has a header-like row OR ≥ 3 non-empty
            #    cells.  The threshold is deliberately very low: we prefer
            #    over-extraction to missing useful data.
            #    - 1 useful sheet  → single-sheet path (fast)
            #    - ≥ 2 useful sheets → extract all and merge records
            # 2. Final fallback: if the merged result contains 0 rows,
            #    retry with truly ALL sheets (no filter) so nothing is lost.

            reader = ExcelReader()
            try:
                useful_sheets, selection_debug = reader.select_useful_sheets(
                    source_doc.file_path,
                )
            except Exception as e:
                logger.warning(
                    "select_useful_sheets failed for %s: %s — falling back to single best sheet",
                    source_doc.filename, e,
                )
                useful_sheets = []

            if len(useful_sheets) <= 1:
                result = extractor.safe_extract(
                    source_doc.file_path,
                    extract_all_sheets=False,
                    preferred_sheet=preferred_sheet or (useful_sheets[0] if useful_sheets else None),
                )
            else:
                logger.info(
                    "Auto multi-sheet: extracting %d sheets for %s: %s",
                    len(useful_sheets), source_doc.filename, useful_sheets,
                )
                sheet_names = list(useful_sheets)
                if preferred_sheet and preferred_sheet in sheet_names:
                    sheet_names = [preferred_sheet] + [s for s in sheet_names if s != preferred_sheet]
                result = extractor.safe_extract(
                    source_doc.file_path,
                    extract_all_sheets=True,
                    preferred_sheet=preferred_sheet or useful_sheets[0],
                    sheet_names=sheet_names,
                )

        # Final fallback (auto & all modes): if result has 0 rows,
        # retry with ALL sheets (no filter) so nothing is lost.
        if extract_mode != "single":
            payload = result.extracted if isinstance(result.extracted, dict) else {}
            rows = payload.get("data") if isinstance(payload, dict) else None
            if (isinstance(rows, list) and len(rows) == 0) or rows is None:
                logger.info("Excel 0-row retry with extract_all_sheets=True (no filter) for %s", source_doc.filename)
                retry = extractor.safe_extract(
                    source_doc.file_path,
                    extract_all_sheets=True,
                    preferred_sheet=None,
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
