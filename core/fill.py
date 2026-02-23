"""
填充层 (Fill Layer)
==================

负责填充计划生成和模板写入。

PlanRunner   – 调用 plan_fill，保证返回 FillPlan（Stage 2A 契约）
WriterRunner – 应用计划 → 写入 → 回退 → cells_written 簿记
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from core.ir import FillPlan, FillPlanTarget
from core.llm import LLMClient
from core.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# PlanRunner  (Stage 2A: plan_fill → FillPlan, always)
# ---------------------------------------------------------------------------

class PlanRunner:
    """
    计划运行器：包装 plan_fill，保证返回 FillPlan。

    若 plan_fill 抛出异常或返回非预期类型，则捕获并返回空 FillPlan，
    错误信息写入 warnings 和 debug 字段。
    """

    @staticmethod
    def run(
        template_schema: Any,
        extracted: dict,
        llm: LLMClient,
        template_filename: str,
        *,
        require_llm: bool = False,
        planner_options: Optional[dict] = None,
    ) -> FillPlan:
        """
        运行填充规划，返回 FillPlan。
        异常时返回带警告的空 FillPlan。
        """
        from core.template.fill_planner import plan_fill

        try:
            result = plan_fill(
                template_schema, extracted, llm,
                template_filename, require_llm=require_llm, planner_options=planner_options,
            )
        except Exception as exc:
            msg = f"{type(exc).__name__}: {str(exc)[:200]}"
            logger.warning("PlanRunner: plan_fill raised: %s", msg)
            return FillPlan(
                target=FillPlanTarget(),
                warnings=[f"plan_fill failed: {msg}"],
                debug={"plan_fill_error": msg},
            )

        # Enforce FillPlan contract
        if isinstance(result, FillPlan):
            return result

        # Unexpected type – should not happen but handle gracefully
        logger.warning("PlanRunner: plan_fill returned %s instead of FillPlan", type(result).__name__)
        return FillPlan(
            target=FillPlanTarget(),
            warnings=[f"plan_fill returned unexpected type: {type(result).__name__}"],
            debug={"plan_fill_returned_type": type(result).__name__},
        )


# ---------------------------------------------------------------------------
# WriterRunner  (apply + fallback + bookkeeping)
# ---------------------------------------------------------------------------

class WriterRunner:
    """
    写入运行器：将 FillPlan 应用到模板文件。

    处理:
    - 输出路径解析
    - 可选的 fill_plan_postprocess 钩子
    - LLM 计划写入 0 格时的回退计划
    - cells_written / fill_status 的 debug 簿记
    """

    @staticmethod
    def run(
        template_path: str,
        template_schema: Any,
        fill_plan: FillPlan,
        extracted: dict,
        *,
        require_llm: bool = False,
        fill_plan_postprocess: Optional[Callable[[dict], Optional[dict]]] = None,
    ) -> Tuple[str, dict]:
        """
        返回 (output_path, fill_plan_dict)，其中 fill_plan_dict
        会包含 debug.cells_written 和 debug.fill_status。
        """
        from core.template.writer import apply_fill_plan
        from core.template.fill_planner import build_fallback_fill_plan

        output_path = WriterRunner._resolve_output_path(template_path)
        plan_dict = fill_plan.to_dict()

        # Optional postprocess hook
        if fill_plan_postprocess:
            try:
                updated = fill_plan_postprocess(plan_dict)
                if updated is not None:
                    plan_dict = updated
            except Exception as exc:
                logger.warning("WriterRunner: postprocess hook failed: %s", exc)

        cells_written = apply_fill_plan(template_path, plan_dict, output_path)
        WriterRunner._set_debug(plan_dict, "cells_written", cells_written)

        # Fallback: if LLM plan wrote 0 cells, try deterministic fallback
        if cells_written == 0 and not require_llm:
            planner_mode = (plan_dict.get("debug") or {}).get("planner_mode")
            if planner_mode == "insurance_constrained_llm":
                # This planner already did its best; don't override.
                return output_path, plan_dict

            previous_debug = dict(plan_dict.get("debug", {}))
            fallback = build_fallback_fill_plan(template_schema, extracted)
            if fallback:
                logger.info("WriterRunner: LLM plan wrote 0 cells; retrying with fallback")
                fallback["llm_used"] = False
                fallback["constant_values_count"] = 0
                plan_dict = fallback
                if previous_debug:
                    WriterRunner._ensure_debug(plan_dict).update(previous_debug)
                cells_written = apply_fill_plan(template_path, plan_dict, output_path)
                WriterRunner._set_debug(plan_dict, "cells_written", cells_written)
                logger.debug("WriterRunner: fallback cells_written=%d", cells_written)

        logger.info("WriterRunner: final llm_used=%s", plan_dict.get("llm_used", False))

        if cells_written == 0:
            warnings = plan_dict.setdefault("warnings", [])
            if "0 cells written" not in warnings:
                warnings.append("0 cells written")
            WriterRunner._set_debug(plan_dict, "fill_status", "failed")

        return output_path, plan_dict

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _resolve_output_path(template_path: str) -> str:
        out_dir = Path(template_path).parent
        out = out_dir / "filled_template.xlsx"
        if out.resolve() == Path(template_path).resolve():
            out = out_dir / "filled_output.xlsx"
        return str(out)

    @staticmethod
    def _ensure_debug(plan_dict: dict) -> dict:
        dbg = plan_dict.get("debug")
        if not isinstance(dbg, dict):
            plan_dict["debug"] = {}
        return plan_dict["debug"]

    @staticmethod
    def _set_debug(plan_dict: dict, key: str, value: Any) -> None:
        WriterRunner._ensure_debug(plan_dict)[key] = value
