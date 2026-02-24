"""
提示词加载模块 (Prompts Loader Module)
=====================================

从 Markdown 文件加载 LLM 所需的各类提示词，支持环境变量指定文件路径。
"""

import re
import os
from pathlib import Path

# 需要从 prompt 文件中解析的提示词键名
PROMPT_KEYS = [
    "EXCEL_SCHEMA_INFER_PROMPT",
    "EMAIL_TO_JSON_PROMPT",
    "EML_BODY_TO_JSON_PROMPT",
    "EMAIL_LEAVE_LINES_TO_JSON_PROMPT",
    "TEMPLATE_COLUMN_MAPPING_PROMPT",
]

# 提示词缓存，避免重复读取文件
_prompts_cache = None


def _resolve_prompt_file(project_root: Path) -> Path:
    """
    解析提示词文件路径：优先使用 PROMPT_FILE 环境变量，否则使用项目根下的 prompt.md。
    """
    configured = os.getenv("PROMPT_FILE", "").strip()
    if not configured:
        return project_root / "prompt.md"
    prompt_path = Path(configured).expanduser()
    if not prompt_path.is_absolute():
        prompt_path = (project_root / prompt_path).resolve()
    return prompt_path


def get_prompts() -> dict:
    """
    获取所有提示词，按 PROMPT_KEYS 解析 prompt 文件中的 ## 标题段落。
    结果会缓存，避免重复读取。
    """
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
