"""
Extractors module for extracting content from various file types.

Provides:
- BaseExtractor: Abstract base class with standard interface and error handling
- ExcelExtractor: For Excel files (.xlsx, .xls)
- EmailExtractor: For email files (.eml, .txt, .docx)
- ImageExtractor: For image files (.png, .jpg, etc.)
"""

from core.extractors.base import BaseExtractor
from core.extractors.excel_extractor import ExcelExtractor
from core.extractors.email_extractor import EmailExtractor
from core.extractors.image_extractor import ImageExtractor

__all__ = [
    "BaseExtractor",
    "ExcelExtractor",
    "EmailExtractor",
    "ImageExtractor",
]
