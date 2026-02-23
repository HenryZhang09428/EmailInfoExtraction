"""
规范化层 (Normalize Layer)
========================

将原始提取结果转换为结构化的 IR 输出。

FactsBuilder  – 将 SourceDoc.extracted 转为 List[Fact]（按 source_type 采用不同策略）
PayloadBuilder – 将 IR.sources 转为 {sources, merged}（供填充规划器使用）
"""

from __future__ import annotations

import copy
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from core.ir import Fact, SourceDoc
from core.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# FactsBuilder
# ---------------------------------------------------------------------------

class FactsBuilder:
    """
    事实构建器：将 SourceDoc.extracted 转为扁平的 List[Fact]。

    不同 source_type 采用不同策略，调用方无需大量 if/elif 分支。
    """

    @classmethod
    def build(cls, sources: List[SourceDoc]) -> List[Fact]:
        """
        从 SourceDoc 列表构建 Fact 列表。
        图片类型使用 _build_image_facts，其他类型使用 _build_generic_facts。
        """
        facts: List[Fact] = []
        for doc in sources:
            if not doc.extracted or not isinstance(doc.extracted, dict):
                continue
            src_ref = [{"source_id": doc.source_id, "filename": doc.filename}]
            if doc.source_type == "image":
                cls._build_image_facts(doc.extracted, src_ref, facts)
            else:
                cls._build_generic_facts(doc.extracted, src_ref, facts)
        return facts

    # -- per-type strategies -------------------------------------------------

    @staticmethod
    def _build_generic_facts(
        extracted: dict,
        src_ref: List[dict],
        out: List[Fact],
    ) -> None:
        """将通用提取结果（键值对）转为 Fact 列表。"""
        for key, value in extracted.items():
            out.append(Fact(name=key, value=value, sources=src_ref))

    @staticmethod
    def _build_image_facts(
        extracted: dict,
        src_ref: List[dict],
        out: List[Fact],
    ) -> None:
        """
        将图片提取结果转为 Fact 列表。
        处理 extracted_fields（每字段一 fact）、tables（每单元格一 fact）、
        numbers（每数字一 fact）及其他顶层键。
        """
        fields = extracted.get("extracted_fields", {})
        if isinstance(fields, dict):
            for name, value in fields.items():
                out.append(Fact(name=name, value=value, sources=src_ref))

        # tables → one fact per cell
        tables = extracted.get("tables", [])
        if isinstance(tables, list):
            for t_idx, table in enumerate(tables):
                rows = table.get("rows", []) if isinstance(table, dict) else []
                if not isinstance(rows, list):
                    continue
                for r_idx, row in enumerate(rows):
                    if not isinstance(row, dict):
                        continue
                    for col, val in row.items():
                        out.append(Fact(
                            name=f"table_{t_idx + 1}_row_{r_idx + 1}_{col}",
                            value=val,
                            sources=src_ref,
                        ))

        # numbers → one fact per number
        numbers = extracted.get("numbers", [])
        if isinstance(numbers, list):
            for n_idx, info in enumerate(numbers):
                if isinstance(info, dict):
                    out.append(Fact(
                        name=f"number_{n_idx + 1}_{info.get('type', 'unknown')}",
                        value=info.get("text", ""),
                        sources=src_ref,
                    ))

        # remaining top-level keys (summary, warnings, …)
        skip = {"extracted_fields", "tables", "numbers"}
        for key, value in extracted.items():
            if key not in skip:
                out.append(Fact(name=key, value=value, sources=src_ref))


# ---------------------------------------------------------------------------
# PayloadBuilder
# ---------------------------------------------------------------------------

class PayloadBuilder:
    """
    载荷构建器：构建填充规划器所需的 {sources, merged} 载荷。

    merged 仅包含标量上下文值（公司名、日期等），用于常量推断。
    容器类键（data、records 等）通过可配置的 skip-set 排除。
    """

    # Keys whose values are record containers or large structures that must
    # never participate in the scalar "merged" dict.
    # Stage-3 note: a future improvement is to flip this to a *whitelist*
    # driven by TemplateSchema ("which fields need constant inference?").
    DEFAULT_SKIP_KEYS: FrozenSet[str] = frozenset({
        # record containers
        "data", "records", "rows", "items",
        # bookkeeping / non-scalar
        "metadata", "warnings",
        # other large keys
        "extracted_data", "table_data", "tables", "numbers", "extracted_fields",
    })

    def __init__(self, skip_keys: Optional[FrozenSet[str]] = None):
        self._skip_keys = skip_keys if skip_keys is not None else self.DEFAULT_SKIP_KEYS

    def build(self, sources: List[SourceDoc]) -> dict:
        """
        构建载荷，返回 {"sources": [...], "merged": {...}}。
        按 parent_source_id、source_type、filename 排序 sources。
        """
        sorted_sources = sorted(
            (s for s in sources if s.extracted is not None),
            key=lambda s: (s.parent_source_id or "", s.source_type or "", s.filename or ""),
        )

        sources_payload: List[dict] = []
        merged: Dict[str, Any] = {}

        for src in sorted_sources:
            enriched = self._inject_source_metadata(copy.deepcopy(src.extracted), src)
            sources_payload.append({
                "filename": src.filename,
                "source_type": src.source_type,
                "extracted": enriched,
                "parent_source_id": src.parent_source_id,
            })
            if isinstance(enriched, dict):
                self._merge_scalars(enriched, merged)

        logger.debug("PayloadBuilder: merged keys=%s", list(merged.keys()))
        return {"sources": sources_payload, "merged": merged}

    # -- internals -----------------------------------------------------------

    def _merge_scalars(self, extracted: dict, merged: Dict[str, Any]) -> None:
        """将首次出现的标量值收集到 merged，跳过容器类键。"""
        for key, value in extracted.items():
            if not isinstance(key, str) or key in self._skip_keys:
                continue
            if not self._is_scalar(value):
                continue
            if key not in merged or not merged[key]:
                merged[key] = value

    @staticmethod
    def _is_scalar(value: Any) -> bool:
        """判断值是否为标量（None、str、int、float、bool）。"""
        return value is None or isinstance(value, (str, int, float, bool))

    @staticmethod
    def _inject_source_metadata(data: Any, source: SourceDoc) -> Any:
        """为每条记录字典注入 __source_file__、__source_type__ 等元数据。"""
        if data is None:
            return data
        if isinstance(data, list):
            if data and isinstance(data[0], dict):
                out = []
                for item in data:
                    if isinstance(item, dict):
                        new = dict(item)
                        new["__source_file__"] = source.filename
                        new["__source_type__"] = source.source_type
                        new["__parent_source_id__"] = source.parent_source_id
                        out.append(new)
                    else:
                        out.append(item)
                return out
            return data
        if isinstance(data, dict):
            enriched = dict(data)
            for key, value in data.items():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    enriched[key] = PayloadBuilder._inject_source_metadata(value, source)
            return enriched
        return data
