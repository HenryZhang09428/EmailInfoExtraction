"""
Email extractor module for extracting content from email files (.eml, .txt, .docx).
"""
import json
import re
import os
import uuid
import mimetypes
import hashlib
import html as _html
from datetime import datetime
from typing import List, Any, Tuple, Dict, Optional, Union
from email.parser import BytesParser
from email import policy
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path

from docx import Document

from core.extractors.base import BaseExtractor
from core.ir import SourceDoc, SourceBlock, BlockType
from core.llm import LLMClient
from core.logger import get_logger

logger = get_logger(__name__)


def decode_mime_header(header_value):
    """Decode a MIME header value."""
    if not header_value:
        return ""
    decoded_parts = decode_header(header_value)
    decoded_string = ""
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            try:
                decoded_string += part.decode(encoding if encoding else 'utf-8', errors='ignore')
            except:
                decoded_string += part.decode('utf-8', errors='ignore')
        else:
            decoded_string += part
    return decoded_string


# ========== Date Parsing Helpers ==========

def _parse_partial_cn_date(text: str) -> Optional[Tuple[int, int]]:
    """
    Parse Chinese partial date patterns like '11月1日', '11月01日'.
    Returns (month, day) tuple or None if not matched.
    """
    if not text:
        return None
    # Match patterns: 11月1日, 11月01日 (no year)
    pattern = re.compile(r'(\d{1,2})月(\d{1,2})日')
    match = pattern.search(text)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return (month, day)
    return None


def _parse_any_date(text: str) -> Optional[datetime]:
    """
    Parse common date formats and return a datetime object.
    Supports: YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD, YYYY年MM月DD日.
    Returns None if parsing fails.
    """
    if not text:
        return None
    
    text = text.strip()
    
    # Pattern 1: YYYY年MM月DD日
    cn_full_pattern = re.compile(r'(\d{4})年(\d{1,2})月(\d{1,2})日')
    match = cn_full_pattern.search(text)
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            pass
    
    # Pattern 2: YYYY-MM-DD or YYYY/MM/DD
    iso_pattern = re.compile(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})')
    match = iso_pattern.search(text)
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            pass
    
    # Pattern 3: YYYYMMDD (8 consecutive digits)
    compact_pattern = re.compile(r'(\d{4})(\d{2})(\d{2})')
    match = compact_pattern.search(text)
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            pass
    
    return None


def _infer_year_from_email_date(email_dt: datetime, month: int) -> int:
    """
    Infer the year for a partial date based on the email date.
    
    Rule:
    - Default year = email_dt.year
    - If email is in December and the target month is January, assume next year (cross-year forward)
    """
    year = email_dt.year
    # Cross-year forward: email in Dec, leave date in Jan
    if email_dt.month == 12 and month == 1:
        year += 1
    return year


def _to_iso_date(year: int, month: int, day: int) -> str:
    """
    Convert year, month, day to ISO format string 'YYYY-MM-DD'.
    """
    return f"{year:04d}-{month:02d}-{day:02d}"


def _parse_email_header_date(date_str: str) -> Optional[datetime]:
    """
    Parse the Date header from an email into a datetime object.
    """
    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        pass
    # Fallback: try common patterns
    patterns = [
        r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
        r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',
    ]
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    for pat in patterns:
        match = re.search(pat, date_str, re.IGNORECASE)
        if match:
            groups = match.groups()
            try:
                if len(groups) == 3 and groups[1] in month_map:
                    # DD Mon YYYY format
                    return datetime(int(groups[2]), month_map[groups[1]], int(groups[0]))
                elif len(groups) == 3:
                    # YYYY-MM-DD format
                    return datetime(int(groups[0]), int(groups[1]), int(groups[2]))
            except ValueError:
                pass
    return None


def is_image_file(content_type: str, filename: Optional[str] = None) -> bool:
    """Check if a file is an image based on content_type and/or filename extension."""
    if content_type and content_type.lower().startswith('image/'):
        return True
    
    if filename:
        ext = Path(filename).suffix.lower()
        image_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.svg']
        if ext in image_extensions:
            return True
    
    return False


def _guess_extension(content_type: str, filename: Optional[str] = None) -> str:
    """
    Best-effort extension guessing.
    Returns a lowercase extension (including leading dot), or empty string if unknown.
    """
    if filename:
        ext = Path(filename).suffix.lower()
        if ext:
            return ext

    ct = (content_type or "").lower().strip()
    if not ct:
        return ""

    # Fix common types not reliably covered by mimetypes on all platforms
    special = {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/vnd.ms-excel": ".xls",
        "text/csv": ".csv",
        "application/csv": ".csv",
        "application/pdf": ".pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/msword": ".doc",
    }
    if ct in special:
        return special[ct]

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


def _sanitize_filename(filename: str) -> str:
    """Prevent path traversal and keep a reasonable filename."""
    if not filename:
        return ""
    name = Path(filename).name  # drop any directory parts
    name = name.replace("\x00", "").strip()
    # Avoid weird empty/relative names
    if name in (".", ".."):
        return ""
    return name


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def export_eml_part(part, out_dir: str, seen_sha256_to_path: Dict[str, str]) -> Optional[dict]:
    """Export an email part (attachment/inline resource) to disk with sha256 dedup."""
    content_disposition = part.get('Content-Disposition', '') or ''
    content_type = part.get_content_type()
    filename = part.get_filename()
    content_id = part.get('Content-ID', '') or ''
    if content_id:
        content_id = content_id.strip('<>')
    
    payload = part.get_payload(decode=True)
    size_bytes = len(payload) if payload else 0
    
    disposition = ''
    if content_disposition:
        disposition_match = re.match(r'^\s*(\w+)', content_disposition, re.IGNORECASE)
        if disposition_match:
            disposition = disposition_match.group(1).lower()
    
    should_export = False
    is_image = is_image_file(content_type, filename)
    
    # Do not export the main text bodies unless explicitly marked as attachments
    if content_type in ("text/plain", "text/html") and not filename and disposition not in ["attachment", "inline"]:
        should_export = False
    else:
        if disposition in ["attachment", "inline"]:
            should_export = True
        elif filename:
            should_export = True
        elif payload and is_image:
            should_export = True
        elif content_type and not content_type.startswith("text/"):
            should_export = True
    
    if should_export and payload:
        os.makedirs(out_dir, exist_ok=True)

        sha256 = _sha256_bytes(payload)
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
                "is_duplicate": True
            }
        
        ext = _guess_extension(content_type, filename)
        safe_filename = _sanitize_filename(filename or "")
        if safe_filename:
            if not Path(safe_filename).suffix and ext:
                safe_filename = f"{safe_filename}{ext}"
        else:
            file_id = str(uuid.uuid4())
            safe_filename = f"{file_id}{ext}" if ext else f"{file_id}.bin"
        
        file_path = os.path.join(out_dir, safe_filename)
        abs_file_path = os.path.abspath(file_path)
        # Avoid collisions (same name, different bytes)
        if os.path.exists(abs_file_path):
            stem = Path(safe_filename).stem
            suffix = Path(safe_filename).suffix
            safe_filename = f"{stem}.{sha256[:8]}{suffix}" if suffix else f"{stem}.{sha256[:8]}"
            abs_file_path = os.path.abspath(os.path.join(out_dir, safe_filename))
        try:
            with open(abs_file_path, 'wb') as f:
                f.write(payload)
        except Exception as e:
            logger.warning("Failed to write email part: %s (%s)", safe_filename, e)
            return {
                'path': abs_file_path,
                'written_path': None,
                'filename': safe_filename,
                'content_type': content_type,
                'size_bytes': size_bytes,
                'content_id': content_id,
                'disposition': disposition,
                'sha256': sha256,
                'is_duplicate': False,
                'write_error': str(e)
            }

        seen_sha256_to_path[sha256] = abs_file_path
        logger.info("Email attachment exported: %s (%d bytes)", safe_filename, size_bytes)
        return {
            'path': abs_file_path,
            'written_path': abs_file_path,
            'filename': safe_filename,
            'content_type': content_type,
            'size_bytes': size_bytes,
            'content_id': content_id,
            'disposition': disposition,
            'sha256': sha256,
            'is_duplicate': False
        }
    
    return None


def parse_eml(path: str) -> dict:
    """Parse an .eml file and return structured data."""
    logger.info("Email parse: start %s", path)
    with open(path, 'rb') as f:
        parser = BytesParser(policy=policy.default)
        msg = parser.parse(f)
    
    headers = {}
    headers['from'] = decode_mime_header(msg.get('From', ''))
    headers['to'] = decode_mime_header(msg.get('To', ''))
    headers['cc'] = decode_mime_header(msg.get('Cc', ''))
    headers['bcc'] = decode_mime_header(msg.get('Bcc', ''))
    headers['subject'] = decode_mime_header(msg.get('Subject', ''))
    headers['date'] = msg.get('Date', '')
    headers['message_id'] = msg.get('Message-ID', '')
    
    bodies = {'text': None, 'html': None}
    parts = []
    
    def process_part(part, is_root=False):
        content_type = part.get_content_type()
        filename = part.get_filename()
        content_disposition = part.get('Content-Disposition', '') or ''
        content_id = part.get('Content-ID', '') or ''
        content_transfer_encoding = part.get('Content-Transfer-Encoding', '') or ''
        
        payload = part.get_payload(decode=True)
        size_bytes = len(payload) if payload else 0
        
        part_info = {
            'content_type': content_type,
            'filename': filename,
            'content_disposition': content_disposition,
            'content_id': content_id,
            'content_transfer_encoding': content_transfer_encoding,
            'size_bytes': size_bytes
        }
        
        if part.is_multipart():
            for subpart in part.iter_parts():
                process_part(subpart, is_root=False)
        else:
            parts.append(part_info)
        
        if content_type == 'text/plain' and bodies['text'] is None:
            try:
                charset = part.get_content_charset() or 'utf-8'
                if payload:
                    bodies['text'] = payload.decode(charset, errors='ignore')
            except:
                pass
        elif content_type == 'text/html' and bodies['html'] is None:
            try:
                charset = part.get_content_charset() or 'utf-8'
                if payload:
                    bodies['html'] = payload.decode(charset, errors='ignore')
            except:
                pass
    
    process_part(msg, is_root=True)
    
    parsed = {
        'headers': headers,
        'bodies': bodies,
        'parts': parts
    }
    logger.info("Email parse: done %s (parts=%d)", path, len(parts))
    return parsed


class EmailExtractor(BaseExtractor):
    """
    Extractor for email files (.eml, .txt, .docx).
    
    Handles:
    - .eml files: Full email parsing with attachments
    - .txt files: Plain text email content
    - .docx files: Word document content
    """
    
    def __init__(self, llm: LLMClient, prompts: Optional[dict] = None, source_id: Optional[str] = None):
        """
        Initialize the Email extractor.
        
        Args:
            llm: The LLM client for AI-powered extraction.
            prompts: Dictionary of prompts, should include EMAIL_TO_JSON_PROMPT.
            source_id: Optional source ID to use for this extraction.
        """
        super().__init__(llm, prompts)
        self.source_id = source_id
    
    def extract(self, file_path: str) -> SourceDoc:
        """
        Extract content from an email file.
        
        Args:
            file_path: Path to the email file.
        
        Returns:
            SourceDoc with extracted content.
        
        Raises:
            ValueError: If the file format is unsupported.
        """
        self.clear_derived_files()
        
        path = Path(file_path)
        ext = path.suffix.lower()
        filename = path.name
        
        source_id = self.source_id or str(uuid.uuid4())
        logger.info("Email extraction start: %s (%s)", filename, ext)
        
        if ext == '.eml':
            source_doc = self._extract_eml(file_path, filename, source_id)
        elif ext == '.txt':
            source_doc = self._extract_txt(file_path, filename, source_id)
        elif ext == '.docx':
            source_doc = self._extract_docx(file_path, filename, source_id)
        else:
            raise ValueError(f"Unsupported email file format: {ext}")
        logger.info("Email extraction end: %s (derived_files=%d)", filename, len(self.derived_files))
        return source_doc
    
    def _extract_eml(self, file_path: str, filename: str, source_id: str) -> SourceDoc:
        """Extract content from an .eml file."""
        def _strip_html_to_text(html_text: str) -> str:
            if not html_text:
                return ""
            text = html_text
            # Remove scripts/styles
            text = re.sub(r"<\s*script[^>]*>[\s\S]*?<\s*/\s*script\s*>", " ", text, flags=re.IGNORECASE)
            text = re.sub(r"<\s*style[^>]*>[\s\S]*?<\s*/\s*style\s*>", " ", text, flags=re.IGNORECASE)
            # Common line breaks
            text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", text)
            text = re.sub(r"(?i)</\s*p\s*>", "\n", text)
            # Strip tags
            text = re.sub(r"<[^>]+>", " ", text)
            text = _html.unescape(text)
            # Normalize whitespace
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n{3,}", "\n\n", text)
            return text.strip()

        def _truncate_email_thread(text: str) -> Tuple[str, dict]:
            """
            Keep only the latest message content, heuristically cutting quoted thread.
            Uses priority-based truncation:
            1. First, search for explicit separators (highest priority)
            2. Only if no explicit separators, detect quoted header blocks (3+ fields in 10 lines)
            
            Returns (truncated_text, truncation_meta).
            """
            if not text:
                return "", {"truncated": False, "rule": None, "marker": None}

            lines = text.split("\n")
            
            explicit_separators = [
                (r"^\s*-{3,}\s*原始邮件\s*-{3,}\s*$", "原始邮件_separator"),
                (r"^\s*-{3,}\s*Original\s+Message\s*-{3,}\s*$", "original_message_separator"),
                (r"^\s*-{3,}\s*Forwarded\s+message\s*-{3,}\s*$", "forwarded_message_separator"),
                (r"^\s*Begin\s+forwarded\s+message\s*[:：]?\s*$", "begin_forwarded"),
                (r"^\s*-{5,}\s*$", "dashes_separator"),
                (r"^\s*On\s.+wrote\s*[:：]\s*$", "on_wrote_separator"),
            ]
            
            for line_idx, line in enumerate(lines):
                for pattern, marker_name in explicit_separators:
                    if re.match(pattern, line, flags=re.IGNORECASE):
                        char_pos = sum(len(l) + 1 for l in lines[:line_idx])
                        if char_pos > 0:
                            return text[:char_pos].strip(), {
                                "truncated": True,
                                "rule": "explicit_separator",
                                "marker": marker_name,
                                "pattern": pattern,
                                "line_index": line_idx,
                            }

            header_field_patterns = [
                r"^\s*发件人\s*[:：]",
                r"^\s*From\s*[:：]",
                r"^\s*发送时间\s*[:：]",
                r"^\s*Sent\s*[:：]",
                r"^\s*Date\s*[:：]",
                r"^\s*收件人\s*[:：]",
                r"^\s*To\s*[:：]",
                r"^\s*主题\s*[:：]",
                r"^\s*Subject\s*[:：]",
                r"^\s*抄送\s*[:：]",
                r"^\s*Cc\s*[:：]",
            ]
            
            window_size = 10
            min_fields_required = 3
            
            for start_idx in range(len(lines)):
                end_idx = min(start_idx + window_size, len(lines))
                window_lines = lines[start_idx:end_idx]
                
                matched_fields = set()
                for win_line in window_lines:
                    for hp in header_field_patterns:
                        if re.match(hp, win_line, flags=re.IGNORECASE):
                            field_name = hp.split(r"\s*[:：]")[0].replace(r"^\s*", "").strip("\\")
                            matched_fields.add(field_name.lower())
                            break
                
                if len(matched_fields) >= min_fields_required:
                    char_pos = sum(len(l) + 1 for l in lines[:start_idx])
                    if char_pos > 100:
                        return text[:char_pos].strip(), {
                            "truncated": True,
                            "rule": "quoted_header_block",
                            "marker": f"header_block_{len(matched_fields)}_fields",
                            "fields_found": list(matched_fields),
                            "line_index": start_idx,
                            "window_size": window_size,
                        }

            return text.strip(), {"truncated": False, "rule": None, "marker": None}

        def _normalize_body_text(eml_data: dict, max_chars: int = 15000) -> Tuple[str, dict]:
            raw_text = (eml_data.get("bodies") or {}).get("text")
            raw_html = (eml_data.get("bodies") or {}).get("html")
            used = "text/plain" if raw_text else ("text/html" if raw_html else "none")
            base = raw_text or _strip_html_to_text(raw_html or "")
            truncated_thread, truncation_meta = _truncate_email_thread(base)
            truncated_to_max = False
            text = truncated_thread
            if len(text) > max_chars:
                text = text[:max_chars]
                truncated_to_max = True
            meta = {
                "used_body": used,
                "thread_truncated": truncation_meta.get("truncated", False),
                "thread_truncation_rule": truncation_meta.get("rule"),
                "thread_marker": truncation_meta.get("marker"),
                "max_chars": max_chars,
                "truncated_to_max_chars": truncated_to_max,
                "char_count": len(text),
            }
            if truncation_meta.get("fields_found"):
                meta["thread_fields_found"] = truncation_meta["fields_found"]
            return text, meta

        def _detect_leave_lines(body_text: str) -> Tuple[List[str], dict]:
            """
            Detect leave/removal lines using lightweight heuristics.
            Returns (matched_lines, detection_meta).
            
            A line matches if it contains:
            - A name followed by parentheses with digits like （42648073） or (42648073)
            - Keywords like 离职, 申请离职, 离职生效, 离岗, 解除, 退场
            - A date-like fragment such as 11月1日 or 2025-11-01 or 2025/11/01
            """
            if not body_text or not body_text.strip():
                return [], {"triggered": False, "reason": "empty_body"}

            lines = body_text.splitlines()
            
            name_id_pattern = re.compile(r'[\u4e00-\u9fa5]{2,4}[（(]\d{4,}[）)]')
            
            leave_keywords = ['离职', '申请离职', '离职生效', '离岗', '解除', '退场', '减员', '停保', '社保减员']
            leave_pattern = re.compile('|'.join(re.escape(kw) for kw in leave_keywords))
            
            date_patterns = [
                re.compile(r'\d{1,2}月\d{1,2}日'),
                re.compile(r'\d{4}年\d{1,2}月\d{1,2}日'),
                re.compile(r'\d{4}[-/]\d{1,2}[-/]\d{1,2}'),
            ]
            
            matched_lines = []
            high_confidence_count = 0
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                has_name_id = bool(name_id_pattern.search(line))
                has_leave_keyword = bool(leave_pattern.search(line))
                has_date = any(dp.search(line) for dp in date_patterns)
                
                if has_name_id and has_leave_keyword and has_date:
                    matched_lines.append(line)
                    high_confidence_count += 1
                elif has_name_id and (has_leave_keyword or has_date):
                    matched_lines.append(line)
                elif has_leave_keyword and has_date and len(line) < 100:
                    matched_lines.append(line)
            
            triggered = len(matched_lines) >= 2 or high_confidence_count >= 1
            
            meta = {
                "triggered": triggered,
                "total_lines": len(lines),
                "matched_count": len(matched_lines),
                "high_confidence_count": high_confidence_count,
            }
            
            return matched_lines, meta

        def _regex_extract_leave_records(lines: List[str], email_dt: Optional[datetime]) -> List[Dict[str, Any]]:
            """
            Deterministic regex-based extraction of leave records from matched lines.
            High recall extractor for patterns like:
            - 邝莲爱（42648073）申请离职，预计11月1日离职生效
            
            Returns list of records with: name, employee_id, leave_date_text, leave_date, intent
            """
            records: List[Dict[str, Any]] = []
            
            name_id_pattern = re.compile(r'([\u4e00-\u9fa5]{2,4})[（(](\d{4,})[）)]')
            
            date_patterns = [
                (re.compile(r'(\d{4})年(\d{1,2})月(\d{1,2})日'), "full_cn"),
                (re.compile(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'), "iso"),
                (re.compile(r'(\d{1,2})月(\d{1,2})日'), "partial_cn"),
            ]
            
            def _infer_year_extended(email_dt: Optional[datetime], month: int) -> int:
                if not email_dt:
                    return datetime.now().year
                year = email_dt.year
                if email_dt.month >= 10 and month <= 2:
                    year += 1
                return year
            
            def _extract_date_from_line(line: str, email_dt: Optional[datetime]) -> Tuple[str, str]:
                for pattern, fmt in date_patterns:
                    match = pattern.search(line)
                    if match:
                        groups = match.groups()
                        if fmt == "full_cn":
                            year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                            date_text = f"{year}年{month}月{day}日"
                        elif fmt == "iso":
                            year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                            date_text = match.group(0)
                        else:
                            month, day = int(groups[0]), int(groups[1])
                            year = _infer_year_extended(email_dt, month)
                            date_text = f"{month}月{day}日"
                        
                        try:
                            datetime(year, month, day)
                            iso_date = f"{year:04d}-{month:02d}-{day:02d}"
                            return date_text, iso_date
                        except ValueError:
                            pass
                return "", ""
            
            seen_ids: set = set()
            
            for line in lines:
                matches = name_id_pattern.findall(line)
                if not matches:
                    continue
                
                date_text, iso_date = _extract_date_from_line(line, email_dt)
                
                for name, emp_id in matches:
                    if emp_id in seen_ids:
                        continue
                    seen_ids.add(emp_id)
                    
                    records.append({
                        "name": name,
                        "employee_id": emp_id,
                        "leave_date_text": date_text,
                        "leave_date": iso_date,
                        "intent": "remove",
                        "note": "",
                        "_source": "regex_extraction",
                    })
            
            return records

        def _validate_llm_records(llm_records: List[Dict], regex_records: List[Dict]) -> List[Dict[str, Any]]:
            """
            Validate LLM records against basic constraints.
            - name must look like Chinese (2-4 characters)
            - employee_id must be digits
            - leave_date must parse or be empty
            
            If LLM returns empty but regex found candidates, use regex results.
            """
            chinese_name_pattern = re.compile(r'^[\u4e00-\u9fa5]{2,4}$')
            digit_pattern = re.compile(r'^\d{4,}$')
            
            def _is_valid_record(rec: Dict) -> bool:
                name = str(rec.get("name", "")).strip()
                emp_id = str(rec.get("employee_id", "")).strip()
                
                if not chinese_name_pattern.match(name):
                    return False
                if emp_id and not digit_pattern.match(emp_id):
                    return False
                
                leave_date = str(rec.get("leave_date", "")).strip()
                if leave_date:
                    if not re.match(r'^\d{4}-\d{2}-\d{2}$', leave_date):
                        return False
                    try:
                        parts = leave_date.split("-")
                        datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                    except (ValueError, IndexError):
                        return False
                
                return True
            
            valid_llm_records = [r for r in llm_records if _is_valid_record(r)]
            
            if not valid_llm_records and regex_records:
                return regex_records
            
            return valid_llm_records

        def _merge_regex_and_llm_records(
            regex_records: List[Dict],
            llm_records: List[Dict]
        ) -> List[Dict[str, Any]]:
            """
            Merge regex and LLM records, preferring LLM data when available.
            Use employee_id as key for matching.
            """
            llm_by_id: Dict[str, Dict] = {}
            for rec in llm_records:
                emp_id = str(rec.get("employee_id", "")).strip()
                if emp_id:
                    llm_by_id[emp_id] = rec
            
            merged: List[Dict[str, Any]] = []
            seen_ids: set = set()
            
            for llm_rec in llm_records:
                emp_id = str(llm_rec.get("employee_id", "")).strip()
                if emp_id:
                    seen_ids.add(emp_id)
                merged.append(llm_rec)
            
            for regex_rec in regex_records:
                emp_id = str(regex_rec.get("employee_id", "")).strip()
                if emp_id and emp_id not in seen_ids:
                    merged.append(regex_rec)
                    seen_ids.add(emp_id)
            
            return merged

        def _coerce_leave_lines_shape(obj: Any, meta: dict, warnings: List[str]) -> dict:
            """
            Ensure dict: {"data": [...], "metadata": {...}, "warnings": [...]}
            for leave lines extraction. Forces intent='remove' and filters fields.
            """
            base = _coerce_body_extracted_shape(obj, meta, warnings)
            
            # Ensure metadata.source is set
            if "source" not in base.get("metadata", {}):
                base["metadata"]["source"] = "email_body_leave_lines"
            
            # Filter and normalize records to only allowed fields
            # Note: leave_date will be added by _normalize_leave_dates after this
            allowed_fields = {"name", "employee_id", "leave_date_text", "leave_date", "intent", "note"}
            normalized_records = []
            for item in base.get("data", []):
                if isinstance(item, dict):
                    rec = {}
                    for k in allowed_fields:
                        val = item.get(k, "")
                        rec[k] = str(val) if val is not None else ""
                    # Force intent to 'remove'
                    rec["intent"] = "remove"
                    normalized_records.append(rec)
            base["data"] = normalized_records
            
            return base

        def _normalize_leave_dates(extracted: dict, email_dt: Optional[datetime], warnings: List[str]) -> None:
            """
            Normalize leave_date_text to leave_date (YYYY-MM-DD) for each record.
            Modifies records in-place and appends warnings for parse failures.
            
            Args:
                extracted: The leave_lines_extracted dict with 'data' list
                email_dt: The datetime parsed from email header Date (for year inference)
                warnings: List to append warnings to
            """
            records = extracted.get("data", [])
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                
                leave_date_text = rec.get("leave_date_text", "").strip()
                if not leave_date_text:
                    # No date text to parse
                    rec["leave_date"] = ""
                    continue
                
                # Try to parse as full date first
                full_dt = _parse_any_date(leave_date_text)
                if full_dt:
                    rec["leave_date"] = _to_iso_date(full_dt.year, full_dt.month, full_dt.day)
                    continue
                
                # Try to parse as partial Chinese date (month/day only)
                partial = _parse_partial_cn_date(leave_date_text)
                if partial:
                    month, day = partial
                    if email_dt:
                        year = _infer_year_from_email_date(email_dt, month)
                    else:
                        # Fallback: use current year if no email date available
                        year = datetime.now().year
                        warnings.append(
                            f"leave_date: no email date for year inference, using current year {year} "
                            f"for '{leave_date_text}'"
                        )
                    # Validate the date
                    try:
                        datetime(year, month, day)  # Raises ValueError if invalid
                        rec["leave_date"] = _to_iso_date(year, month, day)
                    except ValueError as e:
                        rec["leave_date"] = ""
                        warnings.append(
                            f"leave_date: invalid date {year}-{month}-{day} from '{leave_date_text}': {e}"
                        )
                    continue
                
                # Could not parse
                rec["leave_date"] = ""
                warnings.append(f"leave_date: failed to parse '{leave_date_text}'")

        def _coerce_body_extracted_shape(obj: Any, body_meta: dict, warnings: List[str]) -> dict:
            """
            Ensure dict: {"data": [...], "metadata": {...}, "warnings": [...]}
            Never raises; always returns a dict in the required shape.
            """
            if obj is None:
                warnings.append("body_extracted: llm returned None")
                return {"data": [], "metadata": dict(body_meta or {}), "warnings": list(warnings)}
            if isinstance(obj, list):
                warnings.append("body_extracted: llm returned array; wrapped into {data: ...}")
                return {"data": obj, "metadata": dict(body_meta or {}), "warnings": list(warnings)}
            if not isinstance(obj, dict):
                warnings.append(f"body_extracted: llm returned {type(obj).__name__}; wrapped into metadata.raw_output")
                return {
                    "data": [],
                    "metadata": {**dict(body_meta or {}), "raw_output": str(obj)[:2000]},
                    "warnings": list(warnings),
                }
            data = obj.get("data", [])
            meta = obj.get("metadata", {})
            w = obj.get("warnings", [])
            if not isinstance(data, list):
                warnings.append("body_extracted: 'data' is not a list; replaced with []")
                data = []
            if not isinstance(meta, dict):
                warnings.append("body_extracted: 'metadata' is not an object; replaced with {}")
                meta = {}
            if not isinstance(w, list):
                warnings.append("body_extracted: 'warnings' is not a list; replaced with []")
                w = []
            merged_meta = dict(body_meta or {})
            merged_meta.update(meta)
            merged_warnings = []
            for msg in list(warnings) + [str(x) for x in w if x is not None]:
                if msg and msg not in merged_warnings:
                    merged_warnings.append(msg)
            return {"data": data, "metadata": merged_meta, "warnings": merged_warnings}

        def _stringify_record_values(record: dict) -> dict:
            def _to_str(v: Any) -> str:
                if v is None:
                    return ""
                if isinstance(v, str):
                    return v
                if isinstance(v, (int, float, bool)):
                    return str(v)
                try:
                    return json.dumps(v, ensure_ascii=False)
                except Exception:
                    return str(v)
            out = {}
            for k, v in (record or {}).items():
                if not isinstance(k, str) or not k.strip():
                    continue
                out[k] = _to_str(v)
            return out

        logger.info("EML extract start: %s", filename)
        blocks = []
        eml_data = parse_eml(file_path)
        logger.info("EML parsed: %s (has_text=%s has_html=%s parts=%d)", filename, bool(eml_data.get("bodies", {}).get("text")), bool(eml_data.get("bodies", {}).get("html")), len(eml_data.get("parts") or []))
        
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / '.cache' / 'eml_parts' / source_id
        out_dir = str(cache_dir)
        
        parts_summary = []
        for part_info in eml_data['parts']:
            parts_summary.append({
                'content_type': part_info.get('content_type', ''),
                'filename': part_info.get('filename', ''),
                'content_disposition': part_info.get('content_disposition', ''),
                'content_id': part_info.get('content_id', ''),
                'size_bytes': part_info.get('size_bytes', 0)
            })
        
        order = 1
        blocks.append(SourceBlock(order=order, type=BlockType.EMAIL_HEADERS, content=eml_data['headers'], meta={}))
        order += 1
        
        if eml_data['bodies']['text']:
            blocks.append(SourceBlock(order=order, type=BlockType.EMAIL_TEXT, content=eml_data['bodies']['text'], meta={}))
            order += 1
        
        if eml_data['bodies']['html']:
            blocks.append(SourceBlock(order=order, type=BlockType.EMAIL_HTML, content=eml_data['bodies']['html'], meta={}))
            order += 1

        normalized_body_text, body_text_meta = _normalize_body_text(eml_data, max_chars=15000)
        blocks.append(SourceBlock(
            order=order,
            type=BlockType.EMAIL_BODY_TEXT,
            content=normalized_body_text,
            meta=body_text_meta
        ))
        order += 1
        
        blocks.append(SourceBlock(order=order, type=BlockType.EMAIL_PARTS_SUMMARY, content=parts_summary, meta={}))
        order += 1
        
        # Extract CIDs from HTML
        cid_list = []
        if eml_data['bodies']['html']:
            html_content = eml_data['bodies']['html']
            cid_pattern = r'<img[^>]+src=["\']cid:([^"\']+)["\']'
            cid_matches = re.finditer(cid_pattern, html_content, re.IGNORECASE)
            for match in cid_matches:
                cid = match.group(1)
                if cid not in cid_list:
                    cid_list.append(cid)
        
        # Re-parse for attachment export
        with open(file_path, 'rb') as f:
            parser = BytesParser(policy=policy.default)
            msg = parser.parse(f)
        
        inline_images = []
        attachments_summary = []
        cid_to_path = {}
        sha256_to_path: Dict[str, str] = {}
        derived_set = set()
        exported_sha_added = set()
        export_warnings = []
        part_counter = {"index": 0}

        def _record_export_warning(part, reason: str) -> None:
            part_index = part_counter.get("index", 0)
            export_warnings.append(
                f"export_failed idx={part_index} filename={part.get_filename() or ''} "
                f"content_type={part.get_content_type()} reason={reason}"
            )

        def _iter_rfc822_messages(part):
            payload = part.get_payload()
            if isinstance(payload, list):
                for item in payload:
                    if item:
                        yield item
            elif payload is not None:
                if hasattr(payload, "get_payload"):
                    yield payload
                elif isinstance(payload, (bytes, bytearray)):
                    try:
                        parser = BytesParser(policy=policy.default)
                        yield parser.parsebytes(payload)
                    except Exception:
                        pass
        
        def process_parts_for_export(part, current_order):
            part_counter["index"] += 1
            content_type = part.get_content_type()
            if content_type == "message/rfc822":
                nested_found = False
                for nested_msg in _iter_rfc822_messages(part):
                    nested_found = True
                    current_order = process_parts_for_export(nested_msg, current_order)
                if not nested_found:
                    _record_export_warning(part, "rfc822_payload_missing")

            exported = export_eml_part(part, out_dir, sha256_to_path)
            if exported:
                is_image = is_image_file(
                    exported.get('content_type', ''),
                    exported.get('filename', '')
                )
                
                # Always include exported file in derived_files (deduped)
                written_path = exported.get("written_path") or exported.get("path")
                if written_path and os.path.exists(written_path):
                    abs_path = os.path.abspath(written_path)
                    if abs_path not in derived_set:
                        derived_set.add(abs_path)
                        self.derived_files.append(abs_path)
                else:
                    _record_export_warning(part, "written_path_missing")

                if exported.get("content_id"):
                    cid_key = exported["content_id"].strip("<>")
                    cid_to_path[cid_key] = written_path or exported.get("path")

                if exported.get("disposition") == "attachment":
                    attachments_summary.append({
                        'filename': exported.get('filename', ''),
                        'content_type': exported.get('content_type', ''),
                        'size_bytes': exported.get('size_bytes', 0)
                    })

                # Avoid duplicate blocks for same payload
                sha256 = exported.get("sha256")
                if sha256 and sha256 in exported_sha_added:
                    # Duplicate payload: keep derived_files/cid mapping, but avoid noisy repeated blocks
                    pass
                else:
                    if sha256:
                        exported_sha_added.add(sha256)
                    if is_image:
                        logger.debug(
                            "Found image part: %s, content_type: %s, path: %s",
                            exported.get("filename", "no filename"),
                            exported.get("content_type"),
                            exported.get("path"),
                        )
                        blocks.append(SourceBlock(
                            order=current_order,
                            type=BlockType.EML_IMAGE_FILE,
                            content=exported,
                            meta={}
                        ))
                        current_order += 1
                    else:
                        blocks.append(SourceBlock(
                            order=current_order,
                            type=BlockType.EML_FILE_PART,
                            content=exported,
                            meta={}
                        ))
                        current_order += 1
            
            if part.is_multipart():
                for subpart in part.iter_parts():
                    current_order = process_parts_for_export(subpart, current_order)
            
            return current_order
        
        order = process_parts_for_export(msg, order)
        logger.info("EML exported parts: %s (derived_files=%d attachments=%d inline_images=%d)", filename, len(self.derived_files), len(attachments_summary), len(inline_images))
        
        # Process CID references
        for cid in cid_list:
            blocks.append(SourceBlock(
                order=order,
                type=BlockType.INLINE_IMAGE_REF,
                content={"cid": cid},
                meta={}
            ))
            order += 1
            
            cid_normalized = cid.strip('<>')
            matched_path = None
            if cid in cid_to_path:
                matched_path = cid_to_path[cid]
            elif cid_normalized in cid_to_path:
                matched_path = cid_to_path[cid_normalized]
            else:
                for stored_cid, path in cid_to_path.items():
                    if stored_cid.lower() == cid.lower() or stored_cid.lower() == cid_normalized.lower():
                        matched_path = path
                        break
            
            if matched_path:
                blocks.append(SourceBlock(
                    order=order,
                    type=BlockType.INLINE_IMAGE_FILE,
                    content={
                        "cid": cid,
                        "path": matched_path
                    },
                    meta={}
                ))
                order += 1
                inline_images.append({
                    "cid": cid,
                    "path": matched_path
                })
        
        warnings = []
        for cid in cid_list:
            cid_normalized = cid.strip('<>')
            found = (cid in cid_to_path or 
                    cid_normalized in cid_to_path or
                    any(stored_cid.lower() == cid.lower() or stored_cid.lower() == cid_normalized.lower() 
                        for stored_cid in cid_to_path.keys()))
            if not found:
                warnings.append(f"CID {cid} referenced in HTML but not found in email parts")
        if export_warnings:
            warnings.extend(export_warnings)
        
        sender_name, sender_email = parseaddr(eml_data['headers']['from'])
        recipients = []
        for addr in eml_data['headers']['to'].split(','):
            name, email_addr = parseaddr(addr.strip())
            if email_addr:
                recipients.append({"name": name, "email": email_addr})
        
        bodies_summary = {}
        if eml_data['bodies']['text']:
            bodies_summary['text_length'] = len(eml_data['bodies']['text'])
        if eml_data['bodies']['html']:
            bodies_summary['html_length'] = len(eml_data['bodies']['html'])
        
        email_content = {
            "headers": eml_data['headers'],
            "bodies_summary": bodies_summary,
            "inline_images": inline_images,
            "attachments_summary": attachments_summary,
            "warnings": warnings
        }

        # ===== Leave Lines Detection (Regex + LLM) =====
        leave_lines_extracted: Optional[dict] = None
        leave_matched_lines, leave_detect_meta = _detect_leave_lines(normalized_body_text)
        
        email_header_date = eml_data.get("headers", {}).get("date", "")
        email_dt = _parse_email_header_date(email_header_date)
        
        if leave_detect_meta.get("triggered"):
            logger.info(
                "Leave lines detector triggered: %s (matched=%d, high_conf=%d)",
                filename,
                leave_detect_meta.get("matched_count", 0),
                leave_detect_meta.get("high_confidence_count", 0)
            )
            
            regex_records = _regex_extract_leave_records(leave_matched_lines, email_dt)
            logger.info(
                "Regex extraction: %s (records=%d)",
                filename,
                len(regex_records)
            )
            
            leave_prompt_tpl = (self.prompts or {}).get("EMAIL_LEAVE_LINES_TO_JSON_PROMPT", "")
            leave_llm_raw: Any = None
            leave_warnings: List[str] = []
            llm_records: List[Dict] = []
            
            if leave_prompt_tpl.strip():
                snippet = "\n".join(leave_matched_lines[:50])
                leave_prompt = leave_prompt_tpl + "\n\nLEAVE_LINES:\n" + snippet
                try:
                    logger.info("Leave lines extraction: LLM start (%s)", filename)
                    if hasattr(self.llm, "chat_json_once"):
                        leave_llm_raw = self.llm.chat_json_once(
                            leave_prompt,
                            system=None,
                            temperature=0,
                            timeout=30.0,
                            step="email_leave_lines_to_json"
                        )
                    else:
                        leave_llm_raw = self.llm.chat_json(
                            leave_prompt,
                            system=None,
                            temperature=0,
                            step="email_leave_lines_to_json"
                        )
                    logger.info("Leave lines extraction: LLM done (%s)", filename)
                    
                    if isinstance(leave_llm_raw, dict):
                        llm_data = leave_llm_raw.get("data", [])
                        if isinstance(llm_data, list):
                            llm_records = [r for r in llm_data if isinstance(r, dict)]
                except Exception as e:
                    leave_warnings.append(f"leave_lines_extracted: llm_exception: {type(e).__name__}: {str(e)[:200]}")
                    leave_llm_raw = {"error": "llm_exception", "message": str(e)[:200]}
            
            validated_llm_records = _validate_llm_records(llm_records, regex_records)
            final_records = _merge_regex_and_llm_records(regex_records, validated_llm_records)
            
            extraction_source = "regex_only"
            if llm_records and validated_llm_records:
                extraction_source = "llm_validated"
            elif llm_records and not validated_llm_records:
                extraction_source = "regex_fallback_llm_invalid"
                leave_warnings.append("leave_lines: LLM records invalid, using regex fallback")
            elif not llm_records and regex_records:
                extraction_source = "regex_fallback_llm_empty"
            
            leave_meta = {
                **leave_detect_meta,
                "matched_lines_count": len(leave_matched_lines),
                "regex_records_count": len(regex_records),
                "llm_records_count": len(llm_records),
                "final_records_count": len(final_records),
                "extraction_source": extraction_source,
            }
            
            for rec in final_records:
                rec["intent"] = "remove"
                if "_source" in rec:
                    del rec["_source"]
            
            leave_lines_extracted = {
                "data": final_records,
                "metadata": {**leave_meta, "source": "email_body_leave_lines"},
                "warnings": leave_warnings,
            }
            
            date_norm_warnings: List[str] = []
            _normalize_leave_dates(leave_lines_extracted, email_dt, date_norm_warnings)
            
            if date_norm_warnings:
                existing_warnings = leave_lines_extracted.get("warnings", [])
                if not isinstance(existing_warnings, list):
                    existing_warnings = []
                existing_warnings.extend(date_norm_warnings)
                leave_lines_extracted["warnings"] = existing_warnings
            
            logger.info(
                "Leave lines extraction done: %s (records=%d, source=%s)",
                filename,
                len(leave_lines_extracted.get("data", [])),
                extraction_source
            )
        else:
            logger.debug(
                "Leave lines detector not triggered: %s (matched=%d)",
                filename,
                leave_detect_meta.get("matched_count", 0)
            )

        # ===== Body extraction via LLM (generic records) =====
        body_warnings: List[str] = []
        prompt_tpl = (self.prompts or {}).get("EML_BODY_TO_JSON_PROMPT", "")
        body_llm_raw: Any = None
        if not normalized_body_text.strip():
            body_warnings.append("body_extracted: empty normalized body text")
        elif not prompt_tpl.strip():
            body_warnings.append("body_extracted: missing prompt EML_BODY_TO_JSON_PROMPT")
        else:
            prompt = prompt_tpl + "\n\nEMAIL_BODY_TEXT:\n" + normalized_body_text
            try:
                logger.info("Email body extraction: LLM start (%s)", filename)
                # Fast-fail for UI: keep Extract responsive even if LLM is slow/unavailable
                if hasattr(self.llm, "chat_json_once"):
                    body_llm_raw = self.llm.chat_json_once(
                        prompt,
                        system=None,
                        temperature=0,
                        timeout=30.0,
                        step="email_body_to_json"
                    )
                else:
                    body_llm_raw = self.llm.chat_json(prompt, system=None, temperature=0, step="email_body_to_json")
                logger.info("Email body extraction: LLM done (%s)", filename)
            except Exception as e:
                body_warnings.append(f"body_extracted: llm_exception: {type(e).__name__}: {str(e)[:200]}")
                body_llm_raw = {"error": "llm_exception", "message": str(e)[:200]}

        body_extracted = _coerce_body_extracted_shape(body_llm_raw, body_text_meta, body_warnings)

        # Normalize records: dict-only items, string values, inject __source_file__
        normalized_records: List[dict] = []
        for item in body_extracted.get("data", []) if isinstance(body_extracted, dict) else []:
            if isinstance(item, dict):
                rec = _stringify_record_values(item)
                rec["__source_file__"] = filename
                normalized_records.append(rec)
        body_extracted["data"] = normalized_records

        top_warnings: List[str] = []
        for w in (email_content.get("warnings") or []) + (body_extracted.get("warnings") or []):
            if isinstance(w, str) and w and w not in top_warnings:
                top_warnings.append(w)

        extracted_json = {
            # Compatibility: expose records at top-level so fill_planner can discover them
            "data": body_extracted.get("data", []),
            "metadata": body_extracted.get("metadata", {}) if isinstance(body_extracted.get("metadata"), dict) else {},
            "warnings": top_warnings,
            # Backward compatible payloads
            "email_context": email_content,
            "body_extracted": body_extracted,
        }
        
        # Include leave_lines_extracted if triggered
        if leave_lines_extracted is not None:
            extracted_json["leave_lines_extracted"] = leave_lines_extracted
            # Add leave lines data to top-level data with __extraction_type__ marker
            for rec in leave_lines_extracted.get("data", []):
                if isinstance(rec, dict):
                    rec_copy = dict(rec)
                    rec_copy["__extraction_type__"] = "leave_lines"
                    rec_copy["__source_file__"] = filename
                    extracted_json["data"].append(rec_copy)

        blocks.append(SourceBlock(order=order, type=BlockType.EXTRACTED_JSON, content=extracted_json, meta={}))
        order += 1

        logger.debug("extract_email returning %d derived files: %s", len(self.derived_files), self.derived_files)

        return SourceDoc(
            source_id=source_id,
            filename=filename,
            file_path=file_path,
            source_type="email",
            blocks=blocks,
            extracted=extracted_json
        )
    
    def _extract_txt(self, file_path: str, filename: str, source_id: str) -> SourceDoc:
        """Extract content from a .txt email file."""
        blocks = []
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
        
        blocks.append(SourceBlock(order=1, type=BlockType.TEXT, content=text, meta={}))
        
        prompt = self.prompts["EMAIL_TO_JSON_PROMPT"] + "\n\nEMAIL_TEXT:\n" + text
        extracted_json = self.llm.chat_json(prompt, system=None, step="email_to_json")
        
        blocks.append(SourceBlock(order=2, type=BlockType.EXTRACTED_JSON, content=extracted_json, meta={}))
        
        return SourceDoc(
            source_id=source_id,
            filename=filename,
            file_path=file_path,
            source_type="email",
            blocks=blocks,
            extracted=extracted_json
        )
    
    def _extract_docx(self, file_path: str, filename: str, source_id: str) -> SourceDoc:
        """Extract content from a .docx file."""
        blocks = []
        
        doc = Document(file_path)
        docx_blocks = []
        
        for para in doc.paragraphs:
            if para.text.strip():
                docx_blocks.append({
                    "type": "text",
                    "content": para.text.strip()
                })
        
        for table in doc.tables:
            if len(table.rows) > 0:
                headers = [cell.text.strip() for cell in table.rows[0].cells]
                rows = []
                for row in table.rows[1:]:
                    row_data = {}
                    for i, cell in enumerate(row.cells):
                        key = headers[i] if i < len(headers) else f"col_{i}"
                        row_data[key] = cell.text.strip()
                    rows.append(row_data)
                docx_blocks.append({
                    "type": "table",
                    "content": rows
                })
        
        image_count = 0
        for rel in doc.part.rels.values():
            if "image" in rel.target_ref:
                docx_blocks.append({
                    "type": "image_placeholder",
                    "content": {
                        "note": "docx image detected",
                        "index": image_count
                    }
                })
                image_count += 1
        
        docx_type_map = {
            "text": BlockType.TEXT,
            "table": BlockType.TABLE,
            "image_placeholder": BlockType.IMAGE_PLACEHOLDER
        }
        for i, block_data in enumerate(docx_blocks, start=1):
            block_type = docx_type_map.get(block_data["type"], BlockType.TEXT)
            blocks.append(SourceBlock(order=i, type=block_type, content=block_data["content"], meta={}))
        
        blocks_json = json.dumps(docx_blocks, ensure_ascii=False, separators=(',', ':'))
        
        prompt = self.prompts["EMAIL_TO_JSON_PROMPT"] + "\n\nDOCX_BLOCKS_JSON:\n" + blocks_json
        extracted_json = self.llm.chat_json(prompt, system=None, step="email_to_json")
        
        blocks.append(SourceBlock(order=len(blocks) + 1, type=BlockType.EXTRACTED_JSON, content=extracted_json, meta={}))
        
        return SourceDoc(
            source_id=source_id,
            filename=filename,
            file_path=file_path,
            source_type="email",
            blocks=blocks,
            extracted=extracted_json
        )


# Backward compatibility function
def extract_email(path: str, llm: LLMClient, prompts: dict, source_id: Optional[str] = None) -> Tuple[List[SourceBlock], Any, List[str]]:
    """
    Legacy function for backward compatibility.
    
    Extracts email content and returns blocks, extracted JSON, and derived files.
    
    Args:
        path: Path to the email file.
        llm: LLM client for extraction.
        prompts: Dictionary of prompts.
        source_id: Optional source ID.
    
    Returns:
        Tuple of (blocks, extracted_json, derived_files).
    """
    extractor = EmailExtractor(llm, prompts, source_id)
    source_doc = extractor.extract(path)
    return source_doc.blocks, source_doc.extracted, extractor.get_derived_files()
