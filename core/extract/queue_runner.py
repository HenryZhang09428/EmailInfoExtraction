"""
队列运行器模块 (Queue Runner Module)
===================================

对源文档进行 BFS 遍历处理。

职责:
- 已访问路径追踪（防止循环）
- 文件存在性校验
- 衍生文件（如附件）入队
- 提取错误恢复

不负责构建 facts 或任何后处理。
"""

from __future__ import annotations

import os
import traceback
from collections import deque
from typing import Deque, List, Optional, Set

from core.ir import SourceDoc, SourceBlock, BlockType
from core.router import route_files
from core.extract.registry import ExtractorRegistry
from core.logger import get_logger

logger = get_logger(__name__)


class QueueRunner:
    """
    广度优先队列处理器，处理源文档列表。

    通过 ExtractorRegistry 提取每个文档。
    衍生文件（如邮件附件）自动入队继续处理。
    """

    def __init__(self, registry: ExtractorRegistry, router_options: Optional[dict] = None):
        self._registry = registry
        self._router_options = router_options or None

    def run(
        self,
        initial_docs: List[SourceDoc],
    ) -> List[SourceDoc]:
        """
        处理 initial_docs 及其产生的衍生文件。

        返回完整处理后的 SourceDoc 列表（含衍生文档）。
        """
        queue: Deque[SourceDoc] = deque(initial_docs)
        processed: List[SourceDoc] = []
        visited: Set[str] = set()

        while queue:
            doc = queue.popleft()
            path = doc.file_path

            if path in visited:
                logger.debug("Skipping already visited: %s", path)
                continue

            # Missing file → error doc
            if not path or not os.path.exists(path):
                logger.warning("File not found: %s (%s)", doc.filename, path)
                visited.add(path)
                self._mark_file_not_found(doc)
                processed.append(doc)
                continue

            visited.add(path)
            logger.debug("Processing: %s (type=%s)", doc.filename, doc.source_type)

            try:
                result = self._registry.extract(doc)
                doc.blocks = result.blocks
                doc.extracted = result.extracted

                # Enqueue derived files (attachments, etc.)
                for derived_path in result.derived_files:
                    abs_path = os.path.abspath(derived_path)
                    if not os.path.exists(abs_path):
                        logger.warning("Derived file missing: %s (parent=%s)", abs_path, doc.source_id)
                        continue
                    if abs_path in visited:
                        continue
                    derived_doc = self._create_derived_doc(abs_path, doc.source_id, router_options=self._router_options)
                    if derived_doc:
                        queue.append(derived_doc)
                    else:
                        logger.warning("Could not route derived file: %s", derived_path)

            except Exception as exc:
                self._mark_extraction_error(doc, exc)

            processed.append(doc)
            logger.debug("Done: %s (queue remaining: %d)", doc.filename, len(queue))

        logger.info("Queue complete: %d docs processed", len(processed))
        return processed

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _mark_file_not_found(doc: SourceDoc) -> None:
        doc.extracted = {"error": "File not found", "file_path": doc.file_path, "filename": doc.filename}
        doc.blocks = [SourceBlock(
            order=1,
            type=BlockType.ERROR,
            content={"error": "File not found", "file_path": doc.file_path},
            meta={},
        )]

    @staticmethod
    def _mark_extraction_error(doc: SourceDoc, exc: Exception) -> None:
        tb = traceback.format_exc()
        logger.error("Extraction failed for %s: %s", doc.filename, exc)
        logger.debug("Traceback: %s", tb)
        doc.extracted = {
            "error": str(exc),
            "error_type": type(exc).__name__,
            "warnings": [f"提取过程中发生错误: {exc}"],
            "filename": doc.filename,
            "file_path": doc.file_path,
            "source_type": doc.source_type,
        }
        if not doc.blocks:
            doc.blocks = []
        doc.blocks.append(SourceBlock(
            order=len(doc.blocks) + 1,
            type=BlockType.ERROR,
            content={"error": str(exc), "traceback": tb},
            meta={},
        ))
        doc.blocks.append(SourceBlock(
            order=len(doc.blocks) + 1,
            type=BlockType.EXTRACTED_JSON,
            content=doc.extracted,
            meta={},
        ))

    @staticmethod
    def _create_derived_doc(path: str, parent_source_id: str, router_options: Optional[dict] = None) -> Optional[SourceDoc]:
        docs = route_files([path], router_options=router_options)
        if not docs:
            return None
        doc = docs[0]
        doc.parent_source_id = parent_source_id
        return doc
