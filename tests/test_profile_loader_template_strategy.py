from pathlib import Path

from core.profile_loader import load_profile


def test_profile_loader_supports_template_strategy_fields(tmp_path: Path):
    profile_file = tmp_path / "profile.yaml"
    profile_file.write_text(
        """
profile_id: "strategy_test"
excel:
  sheet: "auto"
  extract_mode: "auto"
templates:
  - key: "add"
    template_path: "app/templates/目标1模版：社保增员表.xlsx"
    template_key: "social_security_default"
    strategy_key: "social_security_llm_mapping"
    strategy_plugin: "custom_strategies.demo:build_fill_plan"
    registry_source: "profiles/template_registry.yaml"
    mapping_constraints:
      required_targets: ["name", "event_date"]
      optional_targets: ["id_number"]
""".strip(),
        encoding="utf-8",
    )
    profile = load_profile(str(profile_file))
    assert isinstance(profile.get("templates"), list) and profile["templates"]
    job = profile["templates"][0]
    assert job.get("template_key") == "social_security_default"
    assert job.get("strategy_key") == "social_security_llm_mapping"
    assert job.get("strategy_plugin") == "custom_strategies.demo:build_fill_plan"
    assert job.get("registry_source") == "profiles/template_registry.yaml"
    assert isinstance(job.get("mapping_constraints"), dict)
