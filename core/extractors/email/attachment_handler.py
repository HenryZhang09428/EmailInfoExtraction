"""
Attachment handling for email extraction.

Responsibilities:
- Determine whether a MIME part should be exported.
- Write payloads to disk with SHA-256 deduplication.
- Guess extensions and sanitise filenames.
"""

from __future__ import annotations

import hashlib
import mimetypes
import os
import re
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from core.extractors.email.config import (
    CONTENT_TYPE_EXTENSION_MAP,
    IMAGE_EXTENSIONS,
)
from core.logger import get_logger

logger = get_logger(__name__)


class AttachmentHandler:
    """Encapsulates all MIME-part export logic (disk I/O, dedup, naming)."""

    # ------------------------------------------------------------------
    # Image detection
    # ------------------------------------------------------------------

    @staticmethod
    def is_image_file(content_type: str, filename: Optional[str] = None) -> bool:
        """Check if a file is an image based on content-type and/or extension."""
        if content_type and content_type.lower().startswith("image/"):
            return True
        if filename:
            ext = Path(filename).suffix.lower()
            if ext in IMAGE_EXTENSIONS:
                return True
        return False

    # ------------------------------------------------------------------
    # Extension guessing
    # ------------------------------------------------------------------

    @staticmethod
    def guess_extension(content_type: str, filename: Optional[str] = None) -> str:
        """Best-effort extension guessing.

        Returns a lowercase extension (including leading dot), or ``""``
        if unknown.
        """
        if filename:
            ext = Path(filename).suffix.lower()
            if ext:
                return ext

        ct = (content_type or "").lower().strip()
        if not ct:
            return ""

        if ct in CONTENT_TYPE_EXTENSION_MAP:
            return CONTENT_TYPE_EXTENSION_MAP[ct]

        guessed = mimetypes.guess_extension(ct)
        if guessed:
            return guessed.lower()

        # Image fallbacks
        if ct.startswith("image/"):
            if "png" in ct:
                return ".png"
            if "jpeg" in ct or "jpg" in ct:
                return ".jpg"
            if "gif" in ct:
                return ".gif"
            if "webp" in ct:
                return ".webp"
            if "bmp" in ct:
                return ".bmp"

        return ""

    # ------------------------------------------------------------------
    # Filename safety
    # ------------------------------------------------------------------

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Drop directory parts, null bytes, and reject special names."""
        if not filename:
            return ""
        name = Path(filename).name
        name = name.replace("\x00", "").strip()
        if name in (".", ".."):
            return ""
        return name

    # ------------------------------------------------------------------
    # SHA-256 helper
    # ------------------------------------------------------------------

    @staticmethod
    def sha256_bytes(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    # ------------------------------------------------------------------
    # Part export
    # ------------------------------------------------------------------

    @classmethod
    def export_eml_part(
        cls,
        part,
        out_dir: str,
        seen_sha256_to_path: Dict[str, str],
    ) -> Optional[dict]:
        """Export a single MIME part to disk with SHA-256 dedup.

        Returns a metadata dict if the part was exported (or was a dup),
        otherwise ``None``.
        """
        content_disposition = part.get("Content-Disposition", "") or ""
        content_type = part.get_content_type()
        filename = part.get_filename()
        content_id = part.get("Content-ID", "") or ""
        if content_id:
            content_id = content_id.strip("<>")

        payload = part.get_payload(decode=True)
        size_bytes = len(payload) if payload else 0

        disposition = ""
        if content_disposition:
            m = re.match(r"^\s*(\w+)", content_disposition, re.IGNORECASE)
            if m:
                disposition = m.group(1).lower()

        # --- Should we export? ---
        should_export = False
        is_image = cls.is_image_file(content_type, filename)

        if content_type in ("text/plain", "text/html") and not filename and disposition not in ("attachment", "inline"):
            should_export = False
        else:
            if disposition in ("attachment", "inline"):
                should_export = True
            elif filename:
                should_export = True
            elif payload and is_image:
                should_export = True
            elif content_type and not content_type.startswith("text/"):
                should_export = True

        if not (should_export and payload):
            return None

        os.makedirs(out_dir, exist_ok=True)

        sha256 = cls.sha256_bytes(payload)

        # Dedup: same content already written
        existing = seen_sha256_to_path.get(sha256)
        if existing and os.path.exists(existing):
            return {
                "path": existing,
                "written_path": existing,
                "filename": Path(existing).name,
                "content_type": content_type,
                "size_bytes": size_bytes,
                "content_id": content_id,
                "disposition": disposition,
                "sha256": sha256,
                "is_duplicate": True,
            }

        ext = cls.guess_extension(content_type, filename)
        safe_filename = cls.sanitize_filename(filename or "")
        if safe_filename:
            if not Path(safe_filename).suffix and ext:
                safe_filename = f"{safe_filename}{ext}"
        else:
            file_id = str(uuid.uuid4())
            safe_filename = f"{file_id}{ext}" if ext else f"{file_id}.bin"

        file_path = os.path.join(out_dir, safe_filename)
        abs_file_path = os.path.abspath(file_path)

        # Avoid name collisions (same name, different bytes)
        if os.path.exists(abs_file_path):
            stem = Path(safe_filename).stem
            suffix = Path(safe_filename).suffix
            safe_filename = f"{stem}.{sha256[:8]}{suffix}" if suffix else f"{stem}.{sha256[:8]}"
            abs_file_path = os.path.abspath(os.path.join(out_dir, safe_filename))

        try:
            with open(abs_file_path, "wb") as f:
                f.write(payload)
        except Exception as e:
            logger.warning("Failed to write email part: %s (%s)", safe_filename, e)
            return {
                "path": abs_file_path,
                "written_path": None,
                "filename": safe_filename,
                "content_type": content_type,
                "size_bytes": size_bytes,
                "content_id": content_id,
                "disposition": disposition,
                "sha256": sha256,
                "is_duplicate": False,
                "write_error": str(e),
            }

        seen_sha256_to_path[sha256] = abs_file_path
        logger.info("Email attachment exported: %s (%d bytes)", safe_filename, size_bytes)
        return {
            "path": abs_file_path,
            "written_path": abs_file_path,
            "filename": safe_filename,
            "content_type": content_type,
            "size_bytes": size_bytes,
            "content_id": content_id,
            "disposition": disposition,
            "sha256": sha256,
            "is_duplicate": False,
        }
