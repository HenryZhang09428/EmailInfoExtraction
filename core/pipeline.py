"""
Pipeline: thin orchestrator that composes the extract → normalize → fill layers.

run_extract   – file_paths → IntermediateRepresentation
fill_template – IR + template → (output_path, schema, fill_plan_dict)

Heavy lifting is delegated to:
  core.extract   – ExtractorRegistry, QueueRunner
  core.normalize – FactsBuilder, PayloadBuilder
  core.fill      – PlanRunner, WriterRunner
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Callable, List, Optional, Tuple

from core.llm import get_llm_client
from core.prompts_loader import get_prompts
from core.router import route_files
from core.ir import IntermediateRepresentation, SourceDoc
from core.extract.registry import ExtractorRegistry
from core.extract.queue_runner import QueueRunner
from core.normalize import FactsBuilder, PayloadBuilder
from core.fill import PlanRunner, WriterRunner
from core.logger import get_logger

logger = get_logger(__name__)

# Template module is optional (not all deployments need it).
try:
    from core.template.parser import parse_template_xlsx
    from core.template.schema import TemplateSchema
    _TEMPLATE_MODULE_AVAILABLE = True
except ImportError:
    _TEMPLATE_MODULE_AVAILABLE = False
    TemplateSchema = None  # type: ignore[assignment,misc]


# ---------------------------------------------------------------------------
# Extraction pipeline
# ---------------------------------------------------------------------------

def run_extract(
    file_paths: List[str],
    excel_sheet: Optional[str] = None,
) -> IntermediateRepresentation:
    """
    Extract content from *file_paths* and return an IR.

    Steps:
      1. Route files → SourceDoc list
      2. QueueRunner (BFS) → extracted SourceDocs (incl. derived files)
      3. FactsBuilder → Fact list
      4. Assemble IR
    """
    llm = get_llm_client()
    prompts = get_prompts()

    logger.info("run_extract: %d files: %s", len(file_paths), file_paths)

    # 1. Route
    initial_docs = route_files(file_paths)
    logger.debug("route_files → %d docs", len(initial_docs))

    # 2. BFS extraction
    registry = ExtractorRegistry(llm, prompts)
    runner = QueueRunner(registry)
    processed = runner.run(initial_docs, excel_sheet=excel_sheet)

    # 3. Build facts
    facts = FactsBuilder.build(processed)

    # 4. Assemble
    _log_sources(processed)
    ir = IntermediateRepresentation(
        sources=processed,
        facts=facts,
        target_schema=None,
        output=None,
        scores=None,
    )
    logger.info("IR created with %d sources", len(ir.sources))
    return ir


# ---------------------------------------------------------------------------
# Template fill pipeline
# ---------------------------------------------------------------------------

def fill_template(
    ir: IntermediateRepresentation,
    template_path: str,
    require_llm: bool = False,
    fill_plan_postprocess: Optional[Callable[[dict], Optional[dict]]] = None,
) -> Tuple[str, "TemplateSchema", dict]:
    """
    Fill a template with data from *ir*.

    Steps:
      1. Parse template schema
      2. PayloadBuilder → {sources, merged}
      3. PlanRunner → FillPlan (guaranteed type)
      4. WriterRunner → output file + enriched plan dict
    """
    if not _TEMPLATE_MODULE_AVAILABLE:
        raise ImportError("Template module is not available. Please check dependencies.")

    llm = get_llm_client()

    # 1. Parse template
    template_schema = parse_template_xlsx(template_path)

    # 2. Build planner payload
    payload = PayloadBuilder().build(ir.sources)

    # 3. Plan
    from pathlib import Path
    template_filename = Path(template_path).name
    fill_plan = PlanRunner.run(
        template_schema,
        payload,
        llm,
        template_filename,
        require_llm=require_llm,
    )

    total_rows = sum(len(rw.rows) for rw in fill_plan.row_writes)
    logger.info(
        "fill_template: plan llm_used=%s, constant_values_count=%d, rows=%d",
        fill_plan.llm_used, fill_plan.constant_values_count, total_rows,
    )

    # 4. Write
    output_path, plan_dict = WriterRunner.run(
        template_path,
        template_schema,
        fill_plan,
        payload,
        require_llm=require_llm,
        fill_plan_postprocess=fill_plan_postprocess,
    )

    return output_path, template_schema, plan_dict


# ---------------------------------------------------------------------------
# Stable IR signature (deterministic hashing for caching)
# ---------------------------------------------------------------------------

def build_stable_ir_signature(ir: Any) -> str:
    """Produce a deterministic SHA-256 hash over the IR's extracted data."""
    ir_obj = _to_plain_obj(ir)
    sources_value = getattr(ir, "sources", None)
    if sources_value is None and isinstance(ir_obj, dict):
        sources_value = ir_obj.get("sources", [])
    sources = [_to_plain_obj(s) for s in (sources_value or [])]
    sources = sorted(
        sources,
        key=lambda s: (
            (s.get("parent_source_id") or "") if isinstance(s, dict) else (getattr(s, "parent_source_id", None) or ""),
            (s.get("source_type") or "") if isinstance(s, dict) else (getattr(s, "source_type", None) or ""),
            (s.get("filename") or "") if isinstance(s, dict) else (getattr(s, "filename", None) or ""),
        ),
    )
    payload = []
    for source in sources:
        src = _to_plain_obj(source)
        if isinstance(src, dict):
            payload.append({
                "filename": src.get("filename"),
                "source_type": src.get("source_type"),
                "parent_source_id": src.get("parent_source_id"),
                "extracted": _canonicalize_for_hash(src.get("extracted")),
            })
        else:
            payload.append({
                "filename": getattr(source, "filename", None),
                "source_type": getattr(source, "source_type", None),
                "parent_source_id": getattr(source, "parent_source_id", None),
                "extracted": _canonicalize_for_hash(getattr(source, "extracted", None)),
            })
    json_text = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_plain_obj(value: Any) -> Any:
    if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
        return value.model_dump(mode="json")
    if hasattr(value, "dict") and callable(getattr(value, "dict")):
        return value.dict()
    return value


def _canonicalize_for_hash(value: Any) -> Any:
    value = _to_plain_obj(value)
    if isinstance(value, dict):
        return {k: _canonicalize_for_hash(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        items = [_canonicalize_for_hash(v) for v in value]
        try:
            items.sort(key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False))
        except TypeError:
            items.sort(key=lambda x: str(x))
        return items
    return value


def _extract_single_doc(
    source_doc: SourceDoc,
    llm: Any,
    prompts: dict,
    excel_sheet: Optional[str] = None,
) -> List[str]:
    """Backward-compatible wrapper used by tests.

    Delegates to :class:`ExtractorRegistry` and mutates *source_doc* in place
    (just like the old implementation).  Returns derived file paths.
    """
    registry = ExtractorRegistry(llm, prompts)
    result = registry.extract(source_doc, excel_sheet=excel_sheet)
    source_doc.blocks = result.blocks
    source_doc.extracted = result.extracted
    return result.derived_files


def _log_sources(docs: List[SourceDoc]) -> None:
    logger.debug("Total sources after processing: %d", len(docs))
    for i, sd in enumerate(docs):
        logger.debug(
            "Source %d: %s (type=%s, parent=%s, extracted=%s)",
            i, sd.filename, sd.source_type, sd.parent_source_id, sd.extracted is not None,
        )
