import re
from pathlib import Path

PROMPT_KEYS = [
    "EXCEL_SCHEMA_INFER_PROMPT",
    "EMAIL_TO_JSON_PROMPT",
    "EML_BODY_TO_JSON_PROMPT",
    "ATTRIBUTE_SET_PROMPT",
    "FINAL_MAPPING_PROMPT"
]

_prompts_cache = None

def get_prompts() -> dict:
    global _prompts_cache
    if _prompts_cache is not None:
        return _prompts_cache
    
    project_root = Path(__file__).parent.parent
    prompt_file = project_root / "prompt.md"
    
    if not prompt_file.exists():
        raise FileNotFoundError(
            f"Prompt file not found: {prompt_file}\n"
            f"Please create prompt.md in the project root directory with the following sections:\n"
            f"- ## EXCEL_SCHEMA_INFER_PROMPT\n"
            f"- ## EMAIL_TO_JSON_PROMPT\n"
            f"- ## EML_BODY_TO_JSON_PROMPT\n"
            f"- ## ATTRIBUTE_SET_PROMPT\n"
            f"- ## FINAL_MAPPING_PROMPT"
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
