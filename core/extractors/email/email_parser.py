"""
Low-level .eml parsing: RFC-2822 byte parsing, MIME header decoding,
and part traversal.
"""

from __future__ import annotations

from email import policy
from email.header import decode_header
from email.parser import BytesParser
from typing import List, Optional

from core.logger import get_logger

logger = get_logger(__name__)


class EmailParser:
    """Parse ``.eml`` files and return a structured dictionary."""

    # ------------------------------------------------------------------
    # Header decoding
    # ------------------------------------------------------------------

    @staticmethod
    def decode_mime_header(header_value: Optional[str]) -> str:
        """Decode a MIME header value into a plain string."""
        if not header_value:
            return ""
        decoded_parts = decode_header(header_value)
        decoded_string = ""
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                try:
                    decoded_string += part.decode(encoding if encoding else "utf-8", errors="ignore")
                except Exception:
                    decoded_string += part.decode("utf-8", errors="ignore")
            else:
                decoded_string += part
        return decoded_string

    # ------------------------------------------------------------------
    # Full parse
    # ------------------------------------------------------------------

    @classmethod
    def parse(cls, path: str) -> dict:
        """Parse an ``.eml`` file and return structured data.

        Returns::

            {
                "headers": { "from", "to", "cc", "bcc", "subject", "date", "message_id" },
                "bodies":  { "text": str | None, "html": str | None },
                "parts":   [ { "content_type", "filename", ... } ],
            }
        """
        logger.info("Email parse: start %s", path)
        with open(path, "rb") as f:
            parser = BytesParser(policy=policy.default)
            msg = parser.parse(f)

        headers = {
            "from": cls.decode_mime_header(msg.get("From", "")),
            "to": cls.decode_mime_header(msg.get("To", "")),
            "cc": cls.decode_mime_header(msg.get("Cc", "")),
            "bcc": cls.decode_mime_header(msg.get("Bcc", "")),
            "subject": cls.decode_mime_header(msg.get("Subject", "")),
            "date": msg.get("Date", ""),
            "message_id": msg.get("Message-ID", ""),
        }

        bodies: dict = {"text": None, "html": None}
        parts: List[dict] = []

        def _process_part(part, is_root: bool = False) -> None:
            content_type = part.get_content_type()
            filename = part.get_filename()
            content_disposition = part.get("Content-Disposition", "") or ""
            content_id = part.get("Content-ID", "") or ""
            content_transfer_encoding = part.get("Content-Transfer-Encoding", "") or ""

            payload = part.get_payload(decode=True)
            size_bytes = len(payload) if payload else 0

            part_info = {
                "content_type": content_type,
                "filename": filename,
                "content_disposition": content_disposition,
                "content_id": content_id,
                "content_transfer_encoding": content_transfer_encoding,
                "size_bytes": size_bytes,
            }

            if part.is_multipart():
                for subpart in part.iter_parts():
                    _process_part(subpart, is_root=False)
            else:
                parts.append(part_info)

            if content_type == "text/plain" and bodies["text"] is None:
                try:
                    charset = part.get_content_charset() or "utf-8"
                    if payload:
                        bodies["text"] = payload.decode(charset, errors="ignore")
                except Exception:
                    pass
            elif content_type == "text/html" and bodies["html"] is None:
                try:
                    charset = part.get_content_charset() or "utf-8"
                    if payload:
                        bodies["html"] = payload.decode(charset, errors="ignore")
                except Exception:
                    pass

        _process_part(msg, is_root=True)

        parsed = {"headers": headers, "bodies": bodies, "parts": parts}
        logger.info("Email parse: done %s (parts=%d)", path, len(parts))
        return parsed

    # ------------------------------------------------------------------
    # Re-parse for attachment export (returns the raw Message object)
    # ------------------------------------------------------------------

    @staticmethod
    def parse_raw(path: str):
        """Return the raw ``email.message.Message`` object for attachment export."""
        with open(path, "rb") as f:
            parser = BytesParser(policy=policy.default)
            return parser.parse(f)
