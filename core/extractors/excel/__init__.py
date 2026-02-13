"""
Excel extraction subpackage.

Public API:
  - ExcelExtractor         (main orchestrator, in excel_extractor.py)
  - DataCleaner            (cell/value normalisation)
  - HeaderDetector         (header row identification)
  - ExcelReader            (file I/O, sheet selection)
  - SchemaMapper           (semantic key inference)
  - ExtractorConfig        (tunable thresholds)
"""

from core.extractors.excel.config import ExtractorConfig, DEFAULT_CONFIG
from core.extractors.excel.data_cleaner import DataCleaner
from core.extractors.excel.header_detector import HeaderDetector
from core.extractors.excel.reader import ExcelReader
from core.extractors.excel.schema_mapper import SchemaMapper

__all__ = [
    "ExtractorConfig",
    "DEFAULT_CONFIG",
    "DataCleaner",
    "HeaderDetector",
    "ExcelReader",
    "SchemaMapper",
]
