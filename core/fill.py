"""
Fill layer: plan generation and template writing.

PlanRunner   – invoke plan_fill, guarantee FillPlan output (Stage 2A contract)
WriterRunner – apply plan → write → fallback → cells_written bookkeeping
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
    Wrapper around ``plan_fill`` that **guarantees** a :class:`FillPlan` return.

    If ``plan_fill`` raises or returns an unexpected type, the runner
    catches it and returns an empty ``FillPlan`` carrying the error in
    its *warnings* and *debug* fields.
    """

    @staticmethod
    def run(
        template_schema: Any,
        extracted: dict,
        llm: LLMClient,
        template_filename: str,
        *,
        require_llm: bool = False,
    ) -> FillPlan:
        from core.template.fill_planner import plan_fill

        try:
            result = plan_fill(
                template_schema, extracted, llm,
                template_filename, require_llm=require_llm,
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
    Apply a :class:`FillPlan` to a template file.

    Handles:
    - output path resolution
    - optional ``fill_plan_postprocess`` hook
    - fallback plan when LLM plan writes 0 cells
    - ``cells_written`` / ``fill_status`` bookkeeping in *debug*
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
        Returns ``(output_path, fill_plan_dict)`` where *fill_plan_dict*
        is enriched with ``debug.cells_written`` and ``debug.fill_status``.
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
