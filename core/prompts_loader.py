import re
import os
from pathlib import Path

PROMPT_KEYS = [
    "EXCEL_SCHEMA_INFER_PROMPT",
    "EMAIL_TO_JSON_PROMPT",
    "EML_BODY_TO_JSON_PROMPT",
]

_prompts_cache = None


def _resolve_prompt_file(project_root: Path) -> Path:
    configured = os.getenv("PROMPT_FILE", "").strip()
    if not configured:
        return project_root / "prompt.md"
    prompt_path = Path(configured).expanduser()
    if not prompt_path.is_absolute():
        prompt_path = (project_root / prompt_path).resolve()
    return prompt_path


def get_prompts() -> dict:
    global _prompts_cache
    if _prompts_cache is not None:
        return _prompts_cache
    
    project_root = Path(__file__).parent.parent
    prompt_file = _resolve_prompt_file(project_root)
    
    if not prompt_file.exists():
        raise FileNotFoundError(
            f"Prompt file not found: {prompt_file}\n"
            f"Please create the prompt file (or set PROMPT_FILE) with the following sections:\n"
            f"- ## EXCEL_SCHEMA_INFER_PROMPT\n"
            f"- ## EMAIL_TO_JSON_PROMPT\n"
            f"- ## EML_BODY_TO_JSON_PROMPT"
        )
    
    with open(prompt_file, "r", encoding="utf-8") as f:
        content = f.read()
    
    prompts = {}
    
    sections = re.split(r'^##\s+', content, flags=re.MULTILINE)
    section_map = {}
    
    for section in sections[1:]:
        lines = section.split('\n', 1)
        if len(lines) >= 2:
            title = lines[0].strip()
            body = lines[1].strip()
            section_map[title] = body
    
    for key in PROMPT_KEYS:
        prompts[key] = section_map.get(key, "")
    
    _prompts_cache = prompts
    return prompts
