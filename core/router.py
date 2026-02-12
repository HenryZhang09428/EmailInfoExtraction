import uuid
from typing import List
from pathlib import Path
from core.ir import SourceDoc, SourceType

# Known file extensions allowlist for proper type routing
EXCEL_EXTENSIONS = {'.xlsx', '.xls'}
EMAIL_EXTENSIONS = {'.docx', '.txt', '.eml'}
IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp'}
TEXT_EXTENSIONS = {'.md', '.csv', '.json', '.xml', '.html', '.htm', '.log', '.rtf'}

# Combined allowlist of all known extensions
KNOWN_EXTENSIONS = EXCEL_EXTENSIONS | EMAIL_EXTENSIONS | IMAGE_EXTENSIONS | TEXT_EXTENSIONS


def _get_source_type(ext: str) -> SourceType:
    """
    Determine SourceType based on file extension.
    
    Returns 'other' for unknown extensions to prevent binary misinterpretation.
    """
    if ext in EXCEL_EXTENSIONS:
        return "excel"
    elif ext in EMAIL_EXTENSIONS:
        return "email"
    elif ext in IMAGE_EXTENSIONS:
        return "image"
    elif ext in TEXT_EXTENSIONS:
        return "text"
    else:
        # Fallback: unknown extension treated as 'other' (binary)
        # This prevents UTF-8 decode errors on binary files
        return "other"


def route_files(file_paths: List[str]) -> List[SourceDoc]:
    """
    Route files to appropriate SourceDoc instances based on file extension.
    
    Populates the mandatory file_path field for unique document identification.
    Unknown file extensions are routed to 'other' type to avoid binary misinterpretation.
    """
    result = []
    
    for file_path in file_paths:
        path = Path(file_path)
        ext = path.suffix.lower()
        
        source_type = _get_source_type(ext)
        
        source_doc = SourceDoc(
            source_id=str(uuid.uuid4()),
            filename=path.name,
            file_path=str(path.resolve()),  # Mandatory field: absolute path as unique identifier
            source_type=source_type,
            blocks=[],
            extracted=None
        )
        result.append(source_doc)
    
    return result
