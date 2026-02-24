"""
Template strategy SPI definitions.

This module defines a small, typed strategy contract used by template
planning. Strategies return a validated FillPlan and may be provided by:
- built-in callables (shipped in repository)
- optional user plugins (module:function)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Protocol

from core.ir import FillPlan
from core.llm import LLMClient
from core.template.schema import TemplateSchema


@dataclass
class StrategyContext:
    template_key: Optional[str] = None
    strategy_key: Optional[str] = None
    template_options: Dict[str, Any] = field(default_factory=dict)
    planner_options: Dict[str, Any] = field(default_factory=dict)
    require_llm: bool = False


class TemplateStrategy(Protocol):
    def __call__(
        self,
        template_schema: TemplateSchema,
        extracted_json: Dict[str, Any],
        llm: LLMClient,
        template_filename: Optional[str],
        context: StrategyContext,
    ) -> FillPlan:
        ...


StrategyFactory = Callable[[], TemplateStrategy]

