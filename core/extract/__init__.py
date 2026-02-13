"""
Extract layer: registry-based dispatching and BFS queue processing.
"""

from core.extract.registry import ExtractResult, ExtractorRegistry
from core.extract.queue_runner import QueueRunner

__all__ = [
    "ExtractResult",
    "ExtractorRegistry",
    "QueueRunner",
]
