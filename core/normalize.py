"""
Normalize layer: convert raw extraction results into structured IR outputs.

FactsBuilder  – SourceDoc.extracted → List[Fact]  (per-source_type strategy)
PayloadBuilder – IR.sources → {sources, merged}    (planner input)
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
    Convert ``SourceDoc.extracted`` into a flat ``List[Fact]``.

    Each *source_type* has its own strategy so the caller does not need
    giant ``if/elif`` trees.
    """

    @classmethod
    def build(cls, sources: List[SourceDoc]) -> List[Fact]:
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
        for key, value in extracted.items():
            out.append(Fact(name=key, value=value, sources=src_ref))

    @staticmethod
    def _build_image_facts(
        extracted: dict,
        src_ref: List[dict],
        out: List[Fact],
    ) -> None:
        # extracted_fields → one fact per field
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
    Build the ``{sources, merged}`` payload consumed by the fill planner.

    *merged* contains only scalar context values (company name, date, etc.)
    that are useful for constant inference.  Container keys (data, records, …)
    are excluded via a configurable skip-set.
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
        """Return ``{"sources": [...], "merged": {...}}``."""
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
        """Collect first-seen scalar values into *merged*, skipping containers."""
        for key, value in extracted.items():
            if not isinstance(key, str) or key in self._skip_keys:
                continue
            if not self._is_scalar(value):
                continue
            if key not in merged or not merged[key]:
                merged[key] = value

    @staticmethod
    def _is_scalar(value: Any) -> bool:
        return value is None or isinstance(value, (str, int, float, bool))

    @staticmethod
    def _inject_source_metadata(data: Any, source: SourceDoc) -> Any:
        """Tag every record-dict with ``__source_file__`` etc."""
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
