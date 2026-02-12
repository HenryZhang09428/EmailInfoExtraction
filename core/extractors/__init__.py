"""
Extractors module for extracting content from various file types.

Provides:
- BaseExtractor: Abstract base class with standard interface and error handling
- ExcelExtractor: For Excel files (.xlsx, .xls)
- EmailExtractor: For email files (.eml, .txt, .docx)
- ImageExtractor: For image files (.png, .jpg, etc.)

Legacy functions are also exported for backward compatibility:
- extract_excel
- extract_email
- extract_image
"""

from core.extractors.base import BaseExtractor
from core.extractors.excel_extractor import ExcelExtractor, extract_excel
from core.extractors.email_extractor import EmailExtractor, extract_email
from core.extractors.image_extractor import ImageExtractor, extract_image

__all__ = [
    # Base class
    "BaseExtractor",
    # Extractor classes
    "ExcelExtractor",
    "EmailExtractor",
    "ImageExtractor",
    # Legacy functions (backward compatibility)
    "extract_excel",
    "extract_email",
    "extract_image",
]
