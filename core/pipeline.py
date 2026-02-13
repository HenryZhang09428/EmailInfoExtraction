import copy
import hashlib
import json
import os
import traceback
from collections import deque
from pathlib import Path
from typing import List, Union, Tuple, Deque, Set, Any, Optional, Callable
from core.config import get_settings
from core.llm import get_llm_client, LLMClient
from core.prompts_loader import get_prompts
from core.router import route_files
from core.ir import IntermediateRepresentation, Fact, SourceDoc, SourceBlock, BlockType
from core.extractors import ExcelExtractor, EmailExtractor, ImageExtractor
from core.logger import get_logger

logger = get_logger(__name__)

try:
    from core.template.parser import parse_template_xlsx
    from core.template.fill_planner import plan_fill, build_fallback_fill_plan
    from core.template.writer import apply_fill_plan
    from core.template.schema import TemplateSchema
    _TEMPLATE_MODULE_AVAILABLE = True
except ImportError as e:
    _TEMPLATE_MODULE_AVAILABLE = False
    TemplateSchema = None


def _extract_single_doc(
    source_doc: SourceDoc,
    llm: LLMClient,
    prompts: dict,
    excel_sheet: Optional[str] = None,
) -> List[str]:
    """
    Extract content from a single SourceDoc based on its source_type.
    
    Uses the class-based extractors with safe_extract for consistent error handling.
    
    Returns a list of derived file paths (attachments) that should be processed separately.
    Does NOT merge attachment data into the parent doc.
    """
    file_path = source_doc.file_path
    derived_files: List[str] = []
    
    if source_doc.source_type == "excel":
        logger.debug("Extracting Excel: %s", file_path)
        extractor = ExcelExtractor(llm, prompts)
        result = extractor.safe_extract(
            file_path,
            extract_all_sheets=False,
            preferred_sheet=excel_sheet,
        )
        extracted_payload = result.extracted if isinstance(result.extracted, dict) else {}
        extracted_rows = extracted_payload.get("data") if isinstance(extracted_payload, dict) else None
        extracted_rows_count = len(extracted_rows) if isinstance(extracted_rows, list) else 0
        if extracted_rows_count == 0 and excel_sheet is None:
            logger.info(
                "Excel extraction returned 0 rows for %s; retrying with extract_all_sheets=True",
                source_doc.filename
            )
            retry_result = extractor.safe_extract(
                file_path,
                extract_all_sheets=True,
                preferred_sheet=excel_sheet,
            )
            retry_payload = retry_result.extracted if isinstance(retry_result.extracted, dict) else {}
            retry_rows = retry_payload.get("data") if isinstance(retry_payload, dict) else None
            retry_rows_count = len(retry_rows) if isinstance(retry_rows, list) else 0
            if retry_rows_count > 0:
                logger.info(
                    "Excel all-sheets retry recovered %d rows for %s",
                    retry_rows_count,
                    source_doc.filename
                )
                result = retry_result
        
        source_doc.blocks = result.blocks
        source_doc.extracted = result.extracted
        
        if source_doc.extracted is None:
            logger.warning("ExcelExtractor returned None for %s, creating default structure", source_doc.filename)
            source_doc.extracted = {
                "error": "Extraction returned None",
                "warnings": ["提取结果为空，可能是文件格式问题或LLM调用失败"],
                "filename": source_doc.filename,
                "file_path": source_doc.file_path
            }
        
        logger.info("Excel extraction completed for %s", source_doc.filename)
        
    elif source_doc.source_type == "email":
        logger.debug("Extracting Email: %s", file_path)
        extractor = EmailExtractor(llm, prompts, source_doc.source_id)
        result = extractor.safe_extract(file_path)
        
        source_doc.blocks = result.blocks
        source_doc.extracted = result.extracted
        derived_files = extractor.get_derived_files()
        
        logger.info("Email extraction completed for %s", source_doc.filename)
        logger.debug("Email %s returned %d derived files", source_doc.filename, len(derived_files))
        
    elif source_doc.source_type == "image":
        logger.debug("Extracting Image: %s", file_path)
        extractor = ImageExtractor(llm, prompts)
        result = extractor.safe_extract(file_path)
        
        source_doc.blocks = result.blocks
        source_doc.extracted = result.extracted
        
        logger.info("Image extraction completed for %s", source_doc.filename)
        
    elif source_doc.source_type == "text":
        logger.debug("Extracting Text: %s", file_path)
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
        source_doc.blocks = [SourceBlock(order=1, type=BlockType.TEXT, content=text, meta={})]
        source_doc.extracted = {"text": text, "file_path": source_doc.file_path}
        logger.info("Text extraction completed for %s", source_doc.filename)
        
    elif source_doc.source_type == "other":
        # Binary/unknown file type: explicitly skip text reading/decoding to prevent UTF-8 errors
        logger.debug("Skipping binary/unknown file: %s", file_path)
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        source_doc.blocks = [SourceBlock(
            order=1,
            type=BlockType.BINARY,
            content={"note": "Binary or unknown file format, content not extracted"},
            meta={"file_size": file_size}
        )]
        source_doc.extracted = {
            "binary": True,
            "filename": source_doc.filename,
            "file_path": source_doc.file_path,
            "file_size": file_size,
            "warnings": ["Binary or unknown file format, content not extracted"]
        }
        logger.info("Binary file handling completed for %s", source_doc.filename)
        
    else:
        logger.warning("Unknown source type: %s for %s", source_doc.source_type, source_doc.filename)
    
    return derived_files


def _create_derived_source_doc(
    derived_file_path: str,
    parent_source_id: str
) -> SourceDoc:
    """
    Create a new SourceDoc for a derived file (attachment).
    Uses route_files to determine the correct source_type.
    """
    derived_docs = route_files([derived_file_path])
    if not derived_docs:
        return None
    
    derived_doc = derived_docs[0]
    derived_doc.parent_source_id = parent_source_id
    return derived_doc


def _handle_extraction_error(source_doc: SourceDoc, error: Exception) -> None:
    """Handle extraction errors by setting error information on the source_doc."""
    error_trace = traceback.format_exc()
    logger.error("Failed to extract %s: %s", source_doc.filename, error)
    logger.debug("Traceback: %s", error_trace)
    
    source_doc.extracted = {
        "error": str(error),
        "error_type": type(error).__name__,
        "warnings": [f"提取过程中发生错误: {str(error)}"],
        "filename": source_doc.filename,
        "file_path": source_doc.file_path,
        "source_type": source_doc.source_type
    }
    
    if not source_doc.blocks:
        source_doc.blocks = []
    
    source_doc.blocks.append(SourceBlock(
        order=len(source_doc.blocks) + 1,
        type=BlockType.ERROR,
        content={"error": str(error), "traceback": error_trace},
        meta={}
    ))
    
    source_doc.blocks.append(SourceBlock(
        order=len(source_doc.blocks) + 1,
        type=BlockType.EXTRACTED_JSON,
        content=source_doc.extracted,
        meta={}
    ))


def run_extract(file_paths: List[str], excel_sheet: Optional[str] = None) -> IntermediateRepresentation:
    """
    Extract content from files using a queue-based processing mechanism.
    
    This approach resolves iteration instability by:
    1. Using a deque for stable queue-based processing
    2. Tracking visited paths to prevent cyclic processing
    3. Processing attachments as separate SourceDocs (no merging into parent)
    4. Using the singleton LLM client for dependency injection
    """
    # Dependency injection: get singleton instances
    llm = get_llm_client()  # Singleton LLM instance
    prompts = get_prompts()
    
    logger.info("run_extract: Processing %d files: %s", len(file_paths), file_paths)
    
    # Initialize processing queue with routed source docs
    initial_docs = route_files(file_paths)
    logger.debug("route_files returned %d source_docs", len(initial_docs))
    
    # Queue-based processing mechanism
    processing_queue: Deque[SourceDoc] = deque(initial_docs)
    processed_docs: List[SourceDoc] = []
    visited_paths: Set[str] = set()
    
    # Process documents from the queue
    while processing_queue:
        source_doc = processing_queue.popleft()
        file_path = source_doc.file_path
        
        # Skip if already visited (prevents cycles)
        if file_path in visited_paths:
            logger.debug("Skipping already visited path: %s", file_path)
            continue
        
        # Validate file exists
        if not file_path or not os.path.exists(file_path):
            logger.warning("File path does not exist for %s: %s", source_doc.filename, file_path)
            # Still mark as visited and add to processed (with error state)
            visited_paths.add(file_path)
            source_doc.extracted = {
                "error": "File not found",
                "file_path": file_path,
                "filename": source_doc.filename
            }
            source_doc.blocks = [SourceBlock(
                order=1,
                type=BlockType.ERROR,
                content={"error": "File not found", "file_path": file_path},
                meta={}
            )]
            processed_docs.append(source_doc)
            continue
        
        # Mark as visited before processing
        visited_paths.add(file_path)
        logger.debug("Processing source_doc: %s (type: %s)", source_doc.filename, source_doc.source_type)
        
        try:
            # Extract content and get derived files (attachments)
            derived_files = _extract_single_doc(source_doc, llm, prompts, excel_sheet=excel_sheet)
            
            # Attachment Handling: Create new SourceDocs for derived files
            # and append them to the END of the processing queue
            # DO NOT merge attachment data into the current doc
            for derived_file in derived_files:
                abs_derived_path = os.path.abspath(derived_file)
                if not os.path.exists(abs_derived_path):
                    logger.warning(
                        "Derived file missing, skip enqueue | parent=%s | path=%s",
                        source_doc.source_id,
                        abs_derived_path
                    )
                    continue
                
                # Skip if already visited
                if abs_derived_path in visited_paths:
                    logger.debug("Skipping already visited derived file: %s", abs_derived_path)
                    continue
                
                # Create new SourceDoc for the attachment
                derived_doc = _create_derived_source_doc(abs_derived_path, source_doc.source_id)
                if derived_doc:
                    logger.debug("Queuing derived file: %s (type: %s, parent: %s)",
                                derived_doc.filename, derived_doc.source_type, source_doc.source_id)
                    # Append to END of queue for breadth-first processing
                    processing_queue.append(derived_doc)
                else:
                    logger.warning("Could not create SourceDoc for derived file: %s", derived_file)
            
        except Exception as e:
            _handle_extraction_error(source_doc, e)
        
        # Add processed doc to results
        processed_docs.append(source_doc)
        logger.debug("Processed doc added: %s (queue remaining: %d)", 
                    source_doc.filename, len(processing_queue))
    
    logger.info("Queue processing complete. Total processed: %d docs", len(processed_docs))
    
    # Build facts from processed documents
    facts = []
    for source_doc in processed_docs:
        if source_doc.extracted and isinstance(source_doc.extracted, dict):
            # Special handling for image sources: expand extracted_fields and tables
            if source_doc.source_type == "image":
                # Extract fields from extracted_fields
                extracted_fields = source_doc.extracted.get("extracted_fields", {})
                if isinstance(extracted_fields, dict):
                    for field_name, field_value in extracted_fields.items():
                        fact = Fact(
                            name=field_name,
                            value=field_value,
                            sources=[{"source_id": source_doc.source_id, "filename": source_doc.filename}]
                        )
                        facts.append(fact)
                
                # Extract table data
                tables = source_doc.extracted.get("tables", [])
                if isinstance(tables, list):
                    for table_idx, table in enumerate(tables):
                        rows = table.get("rows", []) if isinstance(table, dict) else []
                        if isinstance(rows, list):
                            for row_idx, row in enumerate(rows):
                                if isinstance(row, dict):
                                    for col_name, col_value in row.items():
                                        fact_name = f"table_{table_idx+1}_row_{row_idx+1}_{col_name}"
                                        fact = Fact(
                                            name=fact_name,
                                            value=col_value,
                                            sources=[{"source_id": source_doc.source_id, "filename": source_doc.filename}]
                                        )
                                        facts.append(fact)
                
                # Extract numbers
                numbers = source_doc.extracted.get("numbers", [])
                if isinstance(numbers, list):
                    for num_idx, num_info in enumerate(numbers):
                        if isinstance(num_info, dict):
                            num_text = num_info.get("text", "")
                            num_type = num_info.get("type", "unknown")
                            fact_name = f"number_{num_idx+1}_{num_type}"
                            fact = Fact(
                                name=fact_name,
                                value=num_text,
                                sources=[{"source_id": source_doc.source_id, "filename": source_doc.filename}]
                            )
                            facts.append(fact)
                
                # Add other fields (summary, warnings) as regular facts
                for key, value in source_doc.extracted.items():
                    if key not in ["extracted_fields", "tables", "numbers"]:
                        fact = Fact(
                            name=key,
                            value=value,
                            sources=[{"source_id": source_doc.source_id, "filename": source_doc.filename}]
                        )
                        facts.append(fact)
            else:
                # For non-image sources, use the original logic
                for key, value in source_doc.extracted.items():
                    fact = Fact(
                        name=key,
                        value=value,
                        sources=[{"source_id": source_doc.source_id, "filename": source_doc.filename}]
                    )
                    facts.append(fact)
    
    logger.debug("Total sources after processing: %d", len(processed_docs))
    for i, sd in enumerate(processed_docs):
        logger.debug("Source %d: %s (type: %s, parent: %s, extracted: %s)", 
                    i, sd.filename, sd.source_type, sd.parent_source_id, sd.extracted is not None)
    
    ir = IntermediateRepresentation(
        sources=processed_docs,
        facts=facts,
        target_schema=None,
        output=None,
        scores=None
    )
    
    logger.info("IR created with %d sources", len(ir.sources))
    return ir

def fill_template(
    ir: IntermediateRepresentation,
    template_path: str,
    require_llm: bool = False,
    fill_plan_postprocess: Optional[Callable[[dict], Optional[dict]]] = None,
) -> Tuple[str, TemplateSchema, dict]:
    if not _TEMPLATE_MODULE_AVAILABLE:
        raise ImportError("Template module is not available. Please check dependencies.")
    
    from core.ir import FillPlan
    
    settings = get_settings()
    llm = get_llm_client()
    
    template_schema = parse_template_xlsx(template_path)
    
    def _sort_key(src: SourceDoc) -> Tuple[str, str, str]:
        return (src.parent_source_id or "", src.source_type or "", src.filename or "")
    
    sorted_sources = sorted([s for s in ir.sources if s.extracted is not None], key=_sort_key)
    
    def _inject_source_metadata(extracted_data: Any, source: SourceDoc) -> Any:
        if extracted_data is None:
            return extracted_data
        if isinstance(extracted_data, list):
            if extracted_data and isinstance(extracted_data[0], dict):
                enriched = []
                for item in extracted_data:
                    if isinstance(item, dict):
                        new_item = dict(item)
                        new_item["__source_file__"] = source.filename
                        new_item["__source_type__"] = source.source_type
                        new_item["__parent_source_id__"] = source.parent_source_id
                        enriched.append(new_item)
                    else:
                        enriched.append(item)
                return enriched
            return extracted_data
        if isinstance(extracted_data, dict):
            enriched = dict(extracted_data)
            for key, value in extracted_data.items():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    enriched[key] = _inject_source_metadata(value, source)
            return enriched
        return extracted_data
    
    def _is_scalar_value(value: Any) -> bool:
        return value is None or isinstance(value, (str, int, float, bool))
    
    def _should_skip_merged_key(key: str) -> bool:
        # merged is ONLY for scalar "context" fields used for constant inference.
        # Never merge record containers or nested structures into merged.
        skip_keys = {
            # Record container keys (must never participate in merged)
            "data", "records", "rows", "items",
            # Common non-record but non-scalar / bookkeeping keys
            "metadata", "warnings",
            # Other known container/large keys
            "extracted_data", "table_data", "tables", "numbers", "extracted_fields",
        }
        return key in skip_keys
    
    all_extracted_data = {}
    sources_payload = []
    for source in sorted_sources:
        extracted_copy = copy.deepcopy(source.extracted)
        enriched_extracted = _inject_source_metadata(extracted_copy, source)
        sources_payload.append({
            "filename": source.filename,
            "source_type": source.source_type,
            "extracted": enriched_extracted,
            "parent_source_id": source.parent_source_id
        })
        
        if isinstance(enriched_extracted, dict):
            for key, value in enriched_extracted.items():
                if not isinstance(key, str) or _should_skip_merged_key(key):
                    continue
                if not _is_scalar_value(value):
                    continue
                if key not in all_extracted_data or not all_extracted_data[key]:
                    all_extracted_data[key] = value
    
    extracted = {
        "sources": sources_payload,
        "merged": all_extracted_data
    }
    
    logger.debug("fill_template: Merged extracted data keys: %s", list(all_extracted_data.keys()))
    
    template_filename = Path(template_path).name
    fill_plan = None
    plan_fill_error = None
    try:
        fill_plan = plan_fill(
            template_schema,
            extracted,
            llm,
            template_filename,
            require_llm=require_llm
        )
    except Exception as e:
        plan_fill_error = f"{type(e).__name__}: {str(e)[:200]}"
        logger.warning("fill_template: plan_fill raised exception: %s", plan_fill_error)
    
    fill_plan_dict: dict = {}
    if fill_plan is None:
        fill_plan_dict = {
            "target": {"sheet_name": None},
            "row_writes": [],
            "writes": [],
            "clear_ranges": [],
            "warnings": ["plan_fill returned None" if not plan_fill_error else f"plan_fill failed: {plan_fill_error}"],
            "llm_used": False,
            "constant_values_count": 0,
            "debug": {
                "plan_fill_error": plan_fill_error,
                "plan_fill_returned_none": plan_fill_error is None,
            }
        }
        logger.warning("fill_template: plan_fill returned None, using empty fill plan")
    elif isinstance(fill_plan, FillPlan):
        fill_plan_dict = fill_plan.to_dict()
    elif isinstance(fill_plan, dict):
        fill_plan_dict = fill_plan
    else:
        fill_plan_dict = {
            "target": {"sheet_name": None},
            "row_writes": [],
            "writes": [],
            "clear_ranges": [],
            "warnings": [f"plan_fill returned unexpected type: {type(fill_plan).__name__}"],
            "llm_used": False,
            "constant_values_count": 0,
            "debug": {
                "plan_fill_returned_type": type(fill_plan).__name__,
            }
        }
        logger.warning("fill_template: plan_fill returned unexpected type %s, using empty fill plan", type(fill_plan).__name__)
    
    if not isinstance(fill_plan_dict, dict):
        fill_plan_dict = {
            "target": {"sheet_name": None},
            "row_writes": [],
            "writes": [],
            "clear_ranges": [],
            "warnings": ["fill_plan_dict coercion failed"],
            "llm_used": False,
            "constant_values_count": 0,
            "debug": {}
        }
    
    llm_used = fill_plan_dict.get("llm_used", False)
    constant_count = fill_plan_dict.get("constant_values_count", 0)
    row_writes = fill_plan_dict.get("row_writes", [])
    total_rows = sum(len(r.get("rows", [])) for r in row_writes) if isinstance(row_writes, list) else 0
    logger.info("fill_template: plan llm_used=%s, constant_values_count=%d, rows=%d", 
                llm_used, constant_count, total_rows)
    
    output_dir = Path(template_path).parent
    output_path = output_dir / "filled_template.xlsx"
    if output_path.resolve() == Path(template_path).resolve():
        output_path = output_dir / "filled_output.xlsx"
    output_path = str(output_path)
    
    if fill_plan_postprocess:
        try:
            updated = fill_plan_postprocess(fill_plan_dict)
            if updated is not None:
                fill_plan_dict = updated
        except Exception as e:
            logger.warning("fill_template: fill_plan_postprocess failed: %s", e)

    cells_written = apply_fill_plan(template_path, fill_plan_dict, output_path)
    if fill_plan_dict is None:
        fill_plan_dict = {}
    dbg = fill_plan_dict.get("debug")
    if not isinstance(dbg, dict):
        fill_plan_dict["debug"] = {}
    fill_plan_dict["debug"]["cells_written"] = cells_written
    if cells_written == 0 and not require_llm:
        planner_mode = None
        dbg = fill_plan_dict.get("debug")
        if isinstance(dbg, dict):
            planner_mode = dbg.get("planner_mode")
        if planner_mode == "insurance_constrained_llm":
            return output_path, template_schema, fill_plan_dict
        from core.template.fill_planner import build_fallback_fill_plan
        previous_debug = dict(fill_plan_dict.get("debug", {})) if isinstance(fill_plan_dict, dict) else {}
        fallback = build_fallback_fill_plan(template_schema, extracted)
        if fallback:
            logger.info("fill_template: LLM plan wrote 0 cells; retrying with fallback")
            fallback["llm_used"] = False
            fallback["constant_values_count"] = 0
            fill_plan_dict = fallback
            if previous_debug:
                dbg = fill_plan_dict.get("debug")
                if not isinstance(dbg, dict):
                    fill_plan_dict["debug"] = {}
                fill_plan_dict["debug"].update(previous_debug)
            cells_written = apply_fill_plan(template_path, fill_plan_dict, output_path)
            dbg = fill_plan_dict.get("debug")
            if not isinstance(dbg, dict):
                fill_plan_dict["debug"] = {}
            fill_plan_dict["debug"]["cells_written"] = cells_written
            logger.debug("fill_template: fallback cells_written=%d", cells_written)
    
    logger.info("fill_template: Final plan llm_used=%s", fill_plan_dict.get("llm_used", False))
    if cells_written == 0:
        warnings = fill_plan_dict.get("warnings", [])
        if "0 cells written" not in warnings:
            warnings.append("0 cells written")
        fill_plan_dict["warnings"] = warnings
        dbg = fill_plan_dict.get("debug")
        if not isinstance(dbg, dict):
            fill_plan_dict["debug"] = {}
        fill_plan_dict["debug"]["fill_status"] = "failed"
    return output_path, template_schema, fill_plan_dict


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
        canonical_items = [_canonicalize_for_hash(v) for v in value]
        try:
            canonical_items.sort(key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False))
        except TypeError:
            canonical_items.sort(key=lambda x: str(x))
        return canonical_items
    return value


def build_stable_ir_signature(ir: Any) -> str:
    ir_obj = _to_plain_obj(ir)
    sources_value = getattr(ir, "sources", None)
    if sources_value is None and isinstance(ir_obj, dict):
        sources_value = ir_obj.get("sources", [])
    sources = sources_value or []
    sources = [_to_plain_obj(s) for s in sources]
    sources = sorted(
        sources,
        key=lambda s: (
            (s.get("parent_source_id") or "") if isinstance(s, dict) else (getattr(s, "parent_source_id", None) or ""),
            (s.get("source_type") or "") if isinstance(s, dict) else (getattr(s, "source_type", None) or ""),
            (s.get("filename") or "") if isinstance(s, dict) else (getattr(s, "filename", None) or "")
        )
    )
    payload = []
    for source in sources:
        src = _to_plain_obj(source)
        if isinstance(src, dict):
            filename = src.get("filename")
            source_type = src.get("source_type")
            parent_source_id = src.get("parent_source_id")
            extracted = _canonicalize_for_hash(src.get("extracted"))
        else:
            filename = getattr(source, "filename", None)
            source_type = getattr(source, "source_type", None)
            parent_source_id = getattr(source, "parent_source_id", None)
            extracted = _canonicalize_for_hash(getattr(source, "extracted", None))
        payload.append({
            "filename": filename,
            "source_type": source_type,
            "parent_source_id": parent_source_id,
            "extracted": extracted
        })
    json_text = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_text.encode("utf-8")).hexdigest()
