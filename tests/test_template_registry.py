import sys
import types

from core.template.schema import Constraints, RegionSchema, SheetSchema, TableHeader, TableInfo, TemplateSchema
from core.template.template_registry import resolve_strategy


def _build_social_security_template_schema() -> TemplateSchema:
    headers = [
        TableHeader(col_letter="A", col_index=1, header_path="参保人/姓名", sample_values=[]),
        TableHeader(col_letter="B", col_index=2, header_path="证件号码", sample_values=[]),
        TableHeader(col_letter="C", col_index=3, header_path="社保/申报类型", sample_values=[]),
        TableHeader(col_letter="D", col_index=4, header_path="社保/费用年月", sample_values=[]),
    ]
    table = TableInfo(range="A2:D20", header=headers, sample_rows=[])
    region = RegionSchema(
        region_id="region_1",
        layout_type="table",
        header_rows=[2],
        table=table,
        constraints=Constraints(has_formulas=False, formula_cells=[], validations=[], number_formats={}),
    )
    return TemplateSchema(sheet_schemas=[SheetSchema(sheet="Sheet1", regions=[region])])


def test_resolve_strategy_uses_builtin_template_key():
    resolved = resolve_strategy(
        template_schema=_build_social_security_template_schema(),
        template_filename="增员模板.xlsx",
        planner_options={"template": {"template_key": "social_security_default"}},
        require_llm=False,
    )
    assert resolved.strategy is not None
    assert resolved.context.template_key == "social_security_default"
    assert resolved.context.strategy_key == "social_security_llm_mapping"


def test_resolve_strategy_defaults_to_llm_mapping_for_social_security_template():
    resolved = resolve_strategy(
        template_schema=_build_social_security_template_schema(),
        template_filename="减员模板.xlsx",
        planner_options={},
        require_llm=False,
    )
    assert resolved.strategy is not None
    assert resolved.context.strategy_key == "social_security_llm_mapping"


def test_resolve_strategy_allows_user_registry_override():
    resolved = resolve_strategy(
        template_schema=None,
        template_filename="增员模板.xlsx",
        planner_options={
            "template": {
                "template_key": "my_custom_template",
                "registry": {
                    "my_custom_template": {
                        "strategy_key": "social_security_llm_mapping",
                        "prompt_key": "TEMPLATE_COLUMN_MAPPING_PROMPT",
                        "constraints": {"required_targets": ["name", "event_date"]},
                    }
                },
            }
        },
        require_llm=False,
    )
    assert resolved.strategy is not None
    assert resolved.registration is not None
    assert resolved.registration.template_key == "my_custom_template"
    assert resolved.context.strategy_key == "social_security_llm_mapping"


def test_resolve_strategy_plugin_takes_precedence(monkeypatch):
    module_name = "custom_strategies_demo_for_test"
    fn_name = "build_fill_plan"
    mod = types.ModuleType(module_name)

    def _plugin_strategy(template_schema, extracted_json, llm, template_filename, context):  # pragma: no cover
        return None

    setattr(mod, fn_name, _plugin_strategy)
    monkeypatch.setitem(sys.modules, module_name, mod)

    resolved = resolve_strategy(
        template_schema=None,
        template_filename="增员模板.xlsx",
        planner_options={
            "template": {
                "strategy_key": "social_security_llm_mapping",
                "strategy_plugin": f"{module_name}:{fn_name}",
            }
        },
        require_llm=False,
    )
    assert resolved.strategy is _plugin_strategy
    assert resolved.context.strategy_key == "social_security_llm_mapping"

