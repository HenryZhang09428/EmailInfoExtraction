"""
Template registry and strategy resolution.

Provides:
- Built-in template registrations
- Optional user-provided registration overrides
- Built-in strategy dispatch
- Optional plugin strategy loader (module:function)
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from core.ir import FillPlan, FillPlanTarget
from core.llm import LLMClient
from core.logger import get_logger
from core.template.schema import TemplateSchema
from core.template.strategy_base import StrategyContext, TemplateStrategy

logger = get_logger(__name__)


@dataclass(frozen=True)
class TemplateRegistration:
    template_key: str
    strategy_key: str
    prompt_key: Optional[str] = None
    constraints: Dict[str, Any] = field(default_factory=dict)
    signature: Dict[str, Any] = field(default_factory=dict)


def _social_security_llm_mapping_strategy(
    template_schema: TemplateSchema,
    extracted_json: Dict[str, Any],
    llm: LLMClient,
    template_filename: Optional[str],
    context: StrategyContext,
) -> FillPlan:
    from core.template.profiles.social_security import (
        build_social_security_fill_plan,
        detect_social_security_template,
    )

    profile = detect_social_security_template(template_schema)
    if not profile.is_detected:
        return FillPlan(
            target=FillPlanTarget(),
            warnings=["social_security_profile_not_detected"],
            llm_used=False,
            debug={"planner_mode": "social_security_llm_mapping_strategy"},
        )
    return build_social_security_fill_plan(
        template_schema,
        extracted_json,
        llm,
        template_filename or "",
        profile,
        profile.template_intent or _infer_intent_from_filename(template_filename),
        planner_options=context.planner_options,
        use_llm_mapping=True,
    )


_BUILTIN_STRATEGIES: Dict[str, TemplateStrategy] = {
    "social_security_llm_mapping": _social_security_llm_mapping_strategy,
}

_BUILTIN_TEMPLATE_REGISTRY: Dict[str, TemplateRegistration] = {
    "social_security_default": TemplateRegistration(
        template_key="social_security_default",
        strategy_key="social_security_llm_mapping",
        prompt_key="TEMPLATE_COLUMN_MAPPING_PROMPT",
        constraints={
            "required_targets": ["name", "event_date"],
            "optional_targets": ["id_number", "termination_reason"],
        },
        signature={
            "header_keywords_any": ["姓名", "证件", "申报类型", "费用年月"],
        },
    )
}


def _infer_intent_from_filename(template_filename: Optional[str]) -> str:
    from core.template.logic.insurance import InsuranceBusinessLogic
    from core.template.logic.config import CONFIG

    inferred = InsuranceBusinessLogic(CONFIG).infer_intent_from_filename(template_filename or "")
    return inferred or "add"


def _safe_template_options(planner_options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(planner_options, dict):
        return {}
    t = planner_options.get("template")
    return t if isinstance(t, dict) else {}


def _resolve_user_registry(template_options: Dict[str, Any]) -> Dict[str, TemplateRegistration]:
    raw = template_options.get("registry")
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, TemplateRegistration] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        strategy_key = value.get("strategy_key")
        if not isinstance(strategy_key, str) or not strategy_key.strip():
            continue
        out[key.strip()] = TemplateRegistration(
            template_key=key.strip(),
            strategy_key=strategy_key.strip(),
            prompt_key=value.get("prompt_key") if isinstance(value.get("prompt_key"), str) else None,
            constraints=value.get("constraints") if isinstance(value.get("constraints"), dict) else {},
            signature=value.get("signature") if isinstance(value.get("signature"), dict) else {},
        )
    return out


def _resolve_plugin_strategy(plugin_ref: str) -> Optional[TemplateStrategy]:
    if not isinstance(plugin_ref, str) or ":" not in plugin_ref:
        return None
    module_name, func_name = plugin_ref.split(":", 1)
    module_name = module_name.strip()
    func_name = func_name.strip()
    if not module_name or not func_name:
        return None
    module = importlib.import_module(module_name)
    strategy = getattr(module, func_name, None)
    if callable(strategy):
        return strategy  # type: ignore[return-value]
    return None


@dataclass
class ResolvedStrategy:
    strategy: Optional[TemplateStrategy]
    context: StrategyContext
    registration: Optional[TemplateRegistration] = None


def resolve_strategy(
    template_schema: TemplateSchema,
    template_filename: Optional[str],
    planner_options: Optional[Dict[str, Any]],
    require_llm: bool,
) -> ResolvedStrategy:
    template_options = _safe_template_options(planner_options)
    template_key = template_options.get("template_key")
    strategy_key = template_options.get("strategy_key")
    strategy_plugin = template_options.get("strategy_plugin")

    user_registry = _resolve_user_registry(template_options)
    merged_registry = dict(_BUILTIN_TEMPLATE_REGISTRY)
    merged_registry.update(user_registry)

    registration: Optional[TemplateRegistration] = None
    if isinstance(template_key, str) and template_key.strip():
        registration = merged_registry.get(template_key.strip())

    # explicit strategy_key wins
    resolved_strategy_key: Optional[str] = None
    if isinstance(strategy_key, str) and strategy_key.strip():
        resolved_strategy_key = strategy_key.strip()
    elif registration is not None:
        resolved_strategy_key = registration.strategy_key
    else:
        try:
            from core.template.profiles.social_security import detect_social_security_template

            profile = detect_social_security_template(template_schema)
            if profile.is_detected:
                resolved_strategy_key = "social_security_llm_mapping"
        except Exception:
            # Keep resolver robust: if profile detection fails, strategy remains unresolved.
            resolved_strategy_key = None

    if resolved_strategy_key == "social_security_legacy":
        logger.warning("strategy_key 'social_security_legacy' is deprecated, using social_security_llm_mapping")
        resolved_strategy_key = "social_security_llm_mapping"

    resolved_strategy: Optional[TemplateStrategy] = None
    if isinstance(strategy_plugin, str) and strategy_plugin.strip():
        try:
            resolved_strategy = _resolve_plugin_strategy(strategy_plugin.strip())
        except Exception as exc:
            logger.warning("Failed loading strategy plugin %s: %s", strategy_plugin, exc)

    if resolved_strategy is None and resolved_strategy_key:
        resolved_strategy = _BUILTIN_STRATEGIES.get(resolved_strategy_key)

    context = StrategyContext(
        template_key=template_key if isinstance(template_key, str) else None,
        strategy_key=resolved_strategy_key,
        template_options=template_options,
        planner_options=planner_options if isinstance(planner_options, dict) else {},
        require_llm=require_llm,
    )
    if template_filename and not context.template_options.get("template_filename"):
        context.template_options["template_filename"] = template_filename
    return ResolvedStrategy(strategy=resolved_strategy, context=context, registration=registration)

