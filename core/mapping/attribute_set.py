from typing import List
from core.llm import LLMClient

def build_attribute_set(header_rows: List[List[str]], sample_rows: List[List[str]], llm: LLMClient, prompts: dict) -> List[dict]:
    csv_lines = []
    
    for header_row in header_rows:
        csv_lines.append(",".join(str(cell) for cell in header_row))
    
    for sample_row in sample_rows:
        csv_lines.append(",".join(str(cell) for cell in sample_row))
    
    text = "\n".join(csv_lines)
    
    prompt = prompts["ATTRIBUTE_SET_PROMPT"] + "\n\nHEADERS_AND_SAMPLES:\n" + text
    
    try:
        result = llm.chat_json(prompt, system=None, step="legacy_attribute_set")
        if isinstance(result, list):
            return result
        elif isinstance(result, dict):
            return [result]
        else:
            return []
    except Exception as e:
        error_msg = f"Failed to build attribute set: {e}\nOriginal text:\n{text}"
        raise ValueError(error_msg) from e
