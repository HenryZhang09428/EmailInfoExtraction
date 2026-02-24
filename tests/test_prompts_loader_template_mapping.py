from core.prompts_loader import get_prompts


def test_template_column_mapping_prompt_is_loaded():
    prompts = get_prompts()
    assert "TEMPLATE_COLUMN_MAPPING_PROMPT" in prompts
    assert isinstance(prompts["TEMPLATE_COLUMN_MAPPING_PROMPT"], str)
    assert prompts["TEMPLATE_COLUMN_MAPPING_PROMPT"].strip()

