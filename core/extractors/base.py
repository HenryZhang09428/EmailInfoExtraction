"""
Base extractor class providing a standard interface and error handling for all extractors.
"""
import uuid
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

from core.ir import SourceDoc, SourceBlock, BlockType
from core.llm import LLMClient
from core.logger import get_logger

logger = get_logger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class for all extractors.
    
    Provides a standard interface with:
    - extract(): Abstract method that subclasses must implement
    - safe_extract(): Wrapper that catches exceptions and returns an error SourceDoc
    """
    
    def __init__(self, llm: LLMClient, prompts: Optional[dict] = None):
        """
        Initialize the extractor.
        
        Args:
            llm: The LLM client for AI-powered extraction.
            prompts: Dictionary of prompts for extraction. Optional.
        """
        self.llm = llm
        self.prompts = prompts or {}
        self.derived_files: List[str] = []
    
    @abstractmethod
    def extract(self, file_path: str) -> SourceDoc:
        """
        Extract content from a file and return a SourceDoc.
        
        Args:
            file_path: Path to the file to extract.
        
        Returns:
            SourceDoc with extracted content.
        
        Raises:
            Exception: If extraction fails (caught by safe_extract).
        """
        pass
    
    def safe_extract(self, file_path: str) -> SourceDoc:
        """
        Safely extract content from a file, catching any exceptions.
        
        This method wraps extract() in a try/except block to prevent
        pipeline crashes. On failure, it logs the error and returns
        a standardized error SourceDoc.
        
        Args:
            file_path: Path to the file to extract.
        
        Returns:
            SourceDoc with extracted content, or an error SourceDoc on failure.
        """
        try:
            result = self.extract(file_path)
            return result
        except Exception as e:
            logger.error("Extraction failed for %s: %s", file_path, e, exc_info=True)
            return self._create_error_source_doc(file_path, e)
    
    def _create_error_source_doc(self, file_path: str, error: Exception) -> SourceDoc:
        """
        Create a standardized error SourceDoc.
        
        Args:
            file_path: Path to the file that failed extraction.
            error: The exception that occurred.
        
        Returns:
            SourceDoc with error information.
        """
        filename = Path(file_path).name if file_path else "unknown"
        source_id = str(uuid.uuid4())
        
        error_block = SourceBlock(
            order=1,
            type=BlockType.ERROR,
            content={"error": str(error), "error_type": type(error).__name__},
            meta={}
        )
        
        extracted_json_block = SourceBlock(
            order=2,
            type=BlockType.EXTRACTED_JSON,
            content={"error": str(error)},
            meta={}
        )
        
        return SourceDoc(
            source_id=source_id,
            filename=filename,
            file_path=file_path or "",
            source_type="error",
            blocks=[error_block, extracted_json_block],
            extracted={"error": str(error)}
        )
    
    def get_derived_files(self) -> List[str]:
        """
        Get the list of derived files (e.g., email attachments) from the last extraction.
        
        Returns:
            List of file paths for derived files.
        """
        return self.derived_files
    
    def clear_derived_files(self) -> None:
        """Clear the list of derived files."""
        self.derived_files = []
