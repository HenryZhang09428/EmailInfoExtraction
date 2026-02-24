import sys
import types

from core.template.template_registry import resolve_strategy


def test_resolve_strategy_uses_builtin_template_key():
    resolved = resolve_strategy(
        template_schema=None,  # not used by resolver yet
        template_filename="增员模板.xlsx",
        planner_options={"template": {"template_key": "social_security_default"}},
        require_llm=False,
    )
    assert resolved.strategy is not None
    assert resolved.context.template_key == "social_security_default"
    assert resolved.context.strategy_key == "social_security_llm_mapping"


def test_resolve_strategy_explicit_strategy_key_overrides_registration_default():
    resolved = resolve_strategy(
        template_schema=None,
        template_filename="减员模板.xlsx",
        planner_options={
            "template": {
                "template_key": "social_security_default",
                "strategy_key": "social_security_legacy",
            }
        },
        require_llm=False,
    )
    assert resolved.strategy is not None
    assert resolved.context.strategy_key == "social_security_legacy"


def test_resolve_strategy_allows_user_registry_override():
    resolved = resolve_strategy(
        template_schema=None,
        template_filename="增员模板.xlsx",
        planner_options={
            "template": {
                "template_key": "my_custom_template",
                "registry": {
                    "my_custom_template": {
                        "strategy_key": "social_security_legacy",
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
    assert resolved.context.strategy_key == "social_security_legacy"


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
                "strategy_key": "social_security_legacy",
                "strategy_plugin": f"{module_name}:{fn_name}",
            }
        },
        require_llm=False,
    )
    assert resolved.strategy is _plugin_strategy
    assert resolved.context.strategy_key == "social_security_legacy"

