import json
from typing import Union, List, Optional, Dict
from core.ir import IntermediateRepresentation, BlockType, SourceDoc
from core.llm import LLMClient

# Token safety constants
DEFAULT_MAX_BLOCK_CHARS = 10000  # Maximum characters per block
DEFAULT_MAX_TOTAL_CHARS = 100000  # Maximum total characters for raw data


def truncate_content(text: str, max_chars: int = DEFAULT_MAX_BLOCK_CHARS) -> str:
    """
    Truncate content to prevent exceeding LLM context window limits.
    
    Args:
        text: The text content to truncate
        max_chars: Maximum number of characters allowed
        
    Returns:
        Truncated text with marker if data was cut off
    """
    if not isinstance(text, str):
        text = str(text)
    
    if len(text) <= max_chars:
        return text
    
    # Truncate and add marker
    truncated = text[:max_chars]
    return f"{truncated}\n...(truncated, {len(text) - max_chars} chars omitted)"


def _group_sources_by_hierarchy(sources: List[SourceDoc]) -> Dict[str, List[SourceDoc]]:
    """
    Group sources by parent-child relationship for hierarchical context assembly.
    
    Returns a dict where:
    - Keys are parent source_ids (or "root" for top-level sources)
    - Values are lists of child sources (attachments)
    """
    hierarchy: Dict[str, List[SourceDoc]] = {"root": []}
    
    # First pass: identify all sources and their parents
    for source in sources:
        if source.parent_source_id is None:
            hierarchy["root"].append(source)
        else:
            parent_id = source.parent_source_id
            if parent_id not in hierarchy:
                hierarchy[parent_id] = []
            hierarchy[parent_id].append(source)
    
    return hierarchy


def _build_source_context(source: SourceDoc, attachments: List[SourceDoc], max_chars: int = DEFAULT_MAX_BLOCK_CHARS) -> str:
    """
    Build context for a source and its attachments.
    
    Ensures both parent source and attachment content are included in the prompt.
    """
    parts = []
    
    # Build parent source context
    parent_header = f"=== Source: {source.filename} (ID: {source.source_id}, Type: {source.source_type}) ===\n"
    source_content = _extract_source_content(source, max_chars)
    
    if source_content:
        parts.append(parent_header + source_content)
    
    # Build attachment contexts
    for attachment in attachments:
        attachment_header = f"  --- Attachment: {attachment.filename} (ID: {attachment.source_id}, Type: {attachment.source_type}, Parent: {source.source_id}) ---\n"
        attachment_content = _extract_source_content(attachment, max_chars)
        
        if attachment_content:
            # Indent attachment content for clarity
            indented_content = "  " + attachment_content.replace("\n", "\n  ")
            parts.append(attachment_header + indented_content)
    
    return "\n".join(parts)


def _extract_source_content(source: SourceDoc, max_chars: int = DEFAULT_MAX_BLOCK_CHARS) -> str:
    """
    Extract content from a single source with truncation applied.
    """
    content_parts = []
    
    for block in source.blocks:
        block_type = block.type
        
        # Use BlockType enum values for type-safe comparisons
        if block_type == BlockType.TABLE_CSV.value:
            truncated = truncate_content(str(block.content), max_chars)
            content_parts.append(f"[Table CSV]\n{truncated}\n")
            
        elif block_type in [BlockType.TEXT.value, BlockType.EMAIL_TEXT.value]:
            truncated = truncate_content(str(block.content), max_chars)
            content_parts.append(f"[Text]\n{truncated}\n")
            
        elif block_type == BlockType.OCR_TEXT.value:
            truncated = truncate_content(str(block.content), max_chars)
            content_parts.append(f"[OCR Text]\n{truncated}\n")
            
        elif block_type == BlockType.EMAIL_HEADERS.value:
            # Include email headers as they contain important metadata
            headers_text = json.dumps(block.content, ensure_ascii=False) if isinstance(block.content, dict) else str(block.content)
            truncated = truncate_content(headers_text, max_chars // 2)
            content_parts.append(f"[Email Headers]\n{truncated}\n")
            
        elif block_type == BlockType.VISION_EXTRACTED_JSON.value:
            # Include vision extraction results
            vision_text = json.dumps(block.content, ensure_ascii=False, indent=2) if isinstance(block.content, dict) else str(block.content)
            truncated = truncate_content(vision_text, max_chars)
            content_parts.append(f"[Vision Extracted]\n{truncated}\n")
    
    # Also include extracted fields for image sources
    if source.extracted and isinstance(source.extracted, dict):
        if source.source_type == "image":
            extracted_fields = source.extracted.get("extracted_fields", {})
            if extracted_fields:
                fields_text = "\n".join([f"{k}: {v}" for k, v in extracted_fields.items()])
                truncated = truncate_content(fields_text, max_chars // 2)
                content_parts.append(f"[Image Extracted Fields]\n{truncated}\n")
            
            tables = source.extracted.get("tables", [])
            if tables:
                for i, table in enumerate(tables):
                    rows = table.get("rows", [])
                    if rows:
                        table_text = "\n".join([str(row) for row in rows])
                        truncated = truncate_content(table_text, max_chars // 2)
                        content_parts.append(f"[Image Table {i+1}]\n{truncated}\n")
    
    return "\n".join(content_parts)


def map_to_schema(ir: IntermediateRepresentation, attribute_set: List[dict], fewshot: Optional[str], llm: LLMClient, prompts: dict) -> Union[dict, list]:
    """
    Map extracted data to target schema using LLM.
    
    This function:
    1. Groups sources hierarchically (emails with their attachments)
    2. Applies token safety truncation to all content blocks
    3. Ensures all sources (including attachments) are included in the context
    """
    raw_data_parts = []
    
    # Group sources by hierarchy to ensure attachments are associated with their parent
    hierarchy = _group_sources_by_hierarchy(ir.sources)
    
    # Process root-level sources with their attachments
    for source in hierarchy.get("root", []):
        attachments = hierarchy.get(source.source_id, [])
        context = _build_source_context(source, attachments, DEFAULT_MAX_BLOCK_CHARS)
        if context:
            raw_data_parts.append(context)
    
    # Also include any orphaned sources (attachments without parents in this IR)
    processed_ids = {s.source_id for s in hierarchy.get("root", [])}
    for source in ir.sources:
        if source.source_id not in processed_ids and source.parent_source_id not in processed_ids:
            # This is an orphaned source, include it directly
            orphan_header = f"=== Source: {source.filename} (ID: {source.source_id}, Type: {source.source_type}) ===\n"
            orphan_content = _extract_source_content(source, DEFAULT_MAX_BLOCK_CHARS)
            if orphan_content:
                raw_data_parts.append(orphan_header + orphan_content)
    
    raw_data_text = "\n\n".join(raw_data_parts)
    
    # Apply total content truncation to prevent context overflow
    raw_data_text = truncate_content(raw_data_text, DEFAULT_MAX_TOTAL_CHARS)
    
    # Build extracted info with parent-child relationships preserved
    extracted_info = {}
    for source in ir.sources:
        if source.extracted is not None:
            extracted_info[source.source_id] = {
                "filename": source.filename,
                "file_path": source.file_path,
                "source_type": source.source_type,
                "parent_source_id": source.parent_source_id,
                "extracted": source.extracted
            }
    
    extracted_info_json = json.dumps(extracted_info, ensure_ascii=False, indent=2)
    # Apply truncation to extracted info to prevent context overflow
    extracted_info_json = truncate_content(extracted_info_json, DEFAULT_MAX_TOTAL_CHARS)
    
    attribute_set_json = json.dumps(attribute_set, ensure_ascii=False, indent=2)
    
    assembled_inputs = []
    assembled_inputs.append("=== Raw Data ===")
    assembled_inputs.append(raw_data_text)
    assembled_inputs.append("\n=== Extracted Info ===")
    assembled_inputs.append(extracted_info_json)
    assembled_inputs.append("\n=== Attribute Set ===")
    assembled_inputs.append(attribute_set_json)
    
    if fewshot:
        assembled_inputs.append("\n=== Few-shot Example ===")
        assembled_inputs.append(fewshot)
    else:
        assembled_inputs.append("\n=== Few-shot Example ===")
        assembled_inputs.append("[No few-shot example provided]")
    
    assembled_inputs_text = "\n".join(assembled_inputs)
    
    prompt = prompts["FINAL_MAPPING_PROMPT"] + "\n\n" + assembled_inputs_text
    
    try:
        final_json = llm.chat_json(prompt, system=None, step="legacy_map_to_schema")
        return final_json
    except Exception as e:
        error_msg = f"Failed to map to schema: {e}\nAssembled inputs:\n{assembled_inputs_text}"
        raise ValueError(error_msg) from e
