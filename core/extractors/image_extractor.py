"""
Image extractor module for extracting content from image files.
"""
import uuid
from pathlib import Path
from typing import List, Any, Optional

from core.extractors.base import BaseExtractor
from core.ir import SourceDoc, SourceBlock, BlockType
from core.llm import LLMClient
from core.logger import get_logger

logger = get_logger(__name__)


class ImageExtractor(BaseExtractor):
    """
    Extractor for image files (.png, .jpg, .jpeg, .gif, etc.).
    
    Uses OCR and vision models to extract structured information from images.
    """
    
    def __init__(self, llm: LLMClient, prompts: Optional[dict] = None):
        """
        Initialize the Image extractor.
        
        Args:
            llm: The LLM client for AI-powered extraction (with vision capability).
            prompts: Dictionary of prompts. Optional.
        """
        super().__init__(llm, prompts)
    
    def extract(self, file_path: str) -> SourceDoc:
        """
        Extract content from an image file.
        
        Args:
            file_path: Path to the image file.
        
        Returns:
            SourceDoc with extracted content.
        """
        self.clear_derived_files()
        
        filename = Path(file_path).name
        source_id = str(uuid.uuid4())
        
        blocks = []
        blocks.append(SourceBlock(order=1, type=BlockType.IMAGE_PATH, content=file_path, meta={}))
        
        # Try OCR extraction
        ocr_text = ""
        try:
            import pytesseract
            from PIL import Image
            img = Image.open(file_path)
            ocr_text = pytesseract.image_to_string(img) or ""
            logger.debug("OCR extracted %d characters from %s", len(ocr_text), file_path)
        except ImportError as e:
            logger.warning("OCR failed - pytesseract not installed: %s", e)
            ocr_text = ""
        except Exception as e:
            logger.warning("OCR failed for %s: %s", file_path, e)
            ocr_text = ""
        
        # Vision extraction prompt
        vision_prompt = """Extract information from this image and return a JSON object with the following structure:
{
  "summary": "Brief summary of the image content",
  "extracted_fields": {
    "name": "value",
    "phone": "value",
    "order_id": "value",
    "amount": "value",
    "date": "value",
    "address": "value",
    "product_name": "value",
    "quantity": "value",
    "unit": "value"
  },
  "tables": [
    {
      "rows": [
        {"col1": "value1", "col2": "value2", "col3": "value3", ...}
      ]
    }
  ],
  "numbers": [
    {"text": "123.45", "type": "amount"},
    {"text": "2024-01-01", "type": "date"},
    {"text": "13800138000", "type": "phone"},
    {"text": "ORD123456", "type": "id"}
  ],
  "warnings": []
}

IMPORTANT INSTRUCTIONS:
1. If the image contains a table, extract EVERY ROW with ALL COLUMNS. Each row should be a complete record with all column values.
2. For tables, use column names from the header row (e.g., "序号", "姓名", "身份证号码", "联系方式", "入职日期", "邮箱", "离职退场时间") as keys in each row object.
3. CRITICAL: If the image is primarily a table, leave extracted_fields EMPTY or only include fields that are OUTSIDE the table. DO NOT extract individual rows from the table into extracted_fields. All table data should ONLY be in the tables array.
4. Extract ALL table rows completely - do not skip any rows or columns. Each row must contain all column values.
5. Extract numbers with types (amount/date/id/phone/other) from the entire image.
6. If uncertain, add to warnings. Do not make up values."""
        
        if ocr_text.strip():
            vision_prompt += f"""

=== OCR Text ===
{ocr_text}

Refine your extraction using the provided OCR text below as a reference for verifying numbers, dates, and specific text fields."""
        
        # Vision extraction
        vision_json = None
        try:
            vision_result = self.llm.vision_json(vision_prompt, [file_path], system=None, step="image_vision_extract")
            if isinstance(vision_result, list):
                vision_json = vision_result[0] if vision_result else {}
            elif isinstance(vision_result, dict):
                vision_json = vision_result
            else:
                vision_json = {}
            logger.debug("Vision extraction completed for %s", file_path)
        except Exception as e:
            logger.warning("Vision extraction failed for %s: %s", file_path, e)
            vision_json = {}
        
        if not isinstance(vision_json, dict):
            vision_json = {}
        
        # Ensure all expected keys exist
        for key in ["summary", "extracted_fields", "tables", "numbers", "warnings"]:
            if key not in vision_json:
                if key == "extracted_fields":
                    vision_json[key] = {}
                elif key in ["tables", "numbers", "warnings"]:
                    vision_json[key] = []
                else:
                    vision_json[key] = ""
        
        blocks.append(SourceBlock(order=2, type=BlockType.VISION_EXTRACTED_JSON, content=vision_json, meta={}))
        
        # OCR-based extraction for enhancement
        ocr_json = None
        if ocr_text.strip():
            blocks.append(SourceBlock(order=3, type=BlockType.OCR_TEXT, content=ocr_text, meta={}))
            
            vision_tables = vision_json.get("tables", [])
            vision_table_rows = 0
            if vision_tables and len(vision_tables) > 0:
                vision_table_rows = len(vision_tables[0].get("rows", []))
            
            if not vision_json.get("extracted_fields") or len(vision_json.get("extracted_fields", {})) < 3 or vision_table_rows < 2:
                ocr_prompt = f"""Extract structured information from the following OCR text. If there is a table, extract EVERY ROW with ALL COLUMNS.

OCR Text:
{ocr_text}

Return JSON with this structure:
{{
  "extracted_fields": {{"field": "value"}},
  "tables": [
    {{
      "rows": [
        {{"column_name1": "value1", "column_name2": "value2", ...}}
      ]
    }}
  ]
}}

IMPORTANT: If there is a table, extract ALL rows completely. Use the header row column names as keys. Do not skip any rows or columns."""
                try:
                    ocr_result = self.llm.chat_json(ocr_prompt, system=None, step="image_ocr_enhance")
                    if isinstance(ocr_result, dict):
                        ocr_json = ocr_result
                        logger.debug("OCR-based extraction completed for %s", file_path)
                except Exception as e:
                    logger.warning("OCR-based extraction failed for %s: %s", file_path, e)
        
        # Merge OCR results with vision results
        extracted_json = vision_json.copy()
        if ocr_json:
            if ocr_json.get("extracted_fields"):
                ocr_fields = ocr_json["extracted_fields"]
                vision_fields = extracted_json.get("extracted_fields", {})
                for key, value in ocr_fields.items():
                    if key not in vision_fields:
                        vision_fields[key] = value
                        if "warnings" not in extracted_json:
                            extracted_json["warnings"] = []
                        if f"Field '{key}' from OCR" not in str(extracted_json["warnings"]):
                            extracted_json["warnings"].append(f"Field '{key}' extracted from OCR (not in vision)")
                extracted_json["extracted_fields"] = vision_fields
            
            if ocr_json.get("tables"):
                ocr_tables = ocr_json["tables"]
                vision_tables = extracted_json.get("tables", [])
                ocr_table_rows = 0
                if ocr_tables and len(ocr_tables) > 0:
                    ocr_table_rows = len(ocr_tables[0].get("rows", []))
                
                vision_table_rows = 0
                if vision_tables and len(vision_tables) > 0:
                    vision_table_rows = len(vision_tables[0].get("rows", []))
                
                if ocr_table_rows > vision_table_rows:
                    extracted_json["tables"] = ocr_tables
                    if "warnings" not in extracted_json:
                        extracted_json["warnings"] = []
                    extracted_json["warnings"].append(f"Table data enhanced from OCR ({ocr_table_rows} rows vs {vision_table_rows} rows)")
        
        return SourceDoc(
            source_id=source_id,
            filename=filename,
            file_path=file_path,
            source_type="image",
            blocks=blocks,
            extracted=extracted_json
        )
