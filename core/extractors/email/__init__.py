"""
Email extraction subpackage.

Public API:
- ``EmailParser``         — MIME parsing and header decoding
- ``AttachmentHandler``   — disk I/O, dedup, extension guessing
- ``ContentCleaner``      — HTML→text, thread truncation
- ``DateParser``          — all date parsing strategies
- ``ResignationDetector`` — leave/departure detection
"""

from core.extractors.email.attachment_handler import AttachmentHandler
from core.extractors.email.content_cleaner import ContentCleaner
from core.extractors.email.date_parser import DateParser
from core.extractors.email.email_parser import EmailParser
from core.extractors.email.resignation_detector import ResignationDetector

__all__ = [
    "AttachmentHandler",
    "ContentCleaner",
    "DateParser",
    "EmailParser",
    "ResignationDetector",
]
