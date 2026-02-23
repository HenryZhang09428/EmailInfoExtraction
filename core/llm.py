"""
LLM 客户端模块 (LLM Client Module)
=================================

封装 OpenAI 兼容 API 的调用，支持 chat_json、vision_json 等，
包含重试、Token 统计、JSON 解析等能力。
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
import threading
import time
from dataclasses import dataclass
from functools import partial
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx
from openai import AsyncOpenAI
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from core.config import get_settings
from core.logger import get_logger

logger = get_logger(__name__)


RE_JSON_BLOCK = re.compile(r"```(?:json)?\s*(\{.*\}|\[.*\])\s*```", re.DOTALL)


@dataclass(frozen=True)
class RetryConfig:
    """
    重试配置数据类，不可变。
    属性: max_attempts, min_wait_seconds, max_wait_seconds, backoff_multiplier
    """
    max_attempts: int
    min_wait_seconds: float
    max_wait_seconds: float
    backoff_multiplier: float


class TokenTracker:
    """
    Token 使用追踪器，累计多次 LLM 请求的 token 消耗。
    """

    def __init__(self) -> None:
        self._usage: Dict[str, int] = {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "requests_count": 0,
        }

    def update(self, response: Any) -> None:
        """根据响应更新 token 统计。"""
        usage = getattr(response, "usage", None)
        if usage is None:
            return
        input_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
        total_tokens = int(getattr(usage, "total_tokens", 0) or (input_tokens + output_tokens))
        self._usage["input_tokens"] += input_tokens
        self._usage["output_tokens"] += output_tokens
        self._usage["total_tokens"] += total_tokens
        self._usage["requests_count"] += 1
        logger.debug(
            "Token usage: input=%d, output=%d, total=%d (cumulative=%d)",
            input_tokens,
            output_tokens,
            total_tokens,
            self._usage["total_tokens"],
        )

    def get(self) -> Dict[str, int]:
        """返回当前 token 使用统计的副本。"""
        return dict(self._usage)

    def reset(self) -> None:
        """重置统计。"""
        self._usage = {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "requests_count": 0,
        }


class JSONParser:
    """
    轻量 JSON 解析器，用于解析模型响应。

    主路径期望严格 JSON（通过 response_format=json_object 启用）。
    回退支持 Markdown 代码块和括号平衡切片。
    """

    @staticmethod
    def parse(text: str) -> Union[dict, list]:
        """
        解析文本为 JSON 对象或数组。
        失败时返回包含 error、raw_output、parse_error 的字典。
        """
        raw = (text or "").strip()
        if not raw:
            return {"error": "json_parse_error", "raw_output": "", "parse_error": "empty_output"}

        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

        block = RE_JSON_BLOCK.search(raw)
        if block:
            try:
                return json.loads(block.group(1))
            except json.JSONDecodeError:
                pass

        sliced = JSONParser._extract_balanced_json(raw)
        if sliced is not None:
            try:
                return json.loads(sliced)
            except json.JSONDecodeError:
                pass

        return {
            "error": "json_parse_error",
            "raw_output": raw[:5000],
            "parse_error": "unable_to_parse_json",
        }

    @staticmethod
    def _extract_balanced_json(text: str) -> Optional[str]:
        """从文本中提取括号平衡的 JSON 片段。"""
        start_positions = [idx for idx in (text.find("{"), text.find("[")) if idx != -1]
        if not start_positions:
            return None
        start = min(start_positions)
        opener = text[start]
        closer = "}" if opener == "{" else "]"
        depth = 0
        for idx in range(start, len(text)):
            char = text[idx]
            if char == opener:
                depth += 1
            elif char == closer:
                depth -= 1
                if depth == 0:
                    return text[start : idx + 1]
        return None


def _extract_retry_context(retry_state: Any) -> Dict[str, Any]:
    args = retry_state.args or []
    kwargs = retry_state.kwargs or {}
    self_obj = args[0] if args else None
    prompt = kwargs.get("prompt") if "prompt" in kwargs else (args[1] if len(args) > 1 else "")
    step = kwargs.get("step")
    model = getattr(self_obj, "model", None)
    timeout = getattr(self_obj, "timeout", None)
    return {
        "self": self_obj,
        "prompt_len": len(prompt) if isinstance(prompt, str) else 0,
        "step": step,
        "model": model,
        "timeout": timeout,
        "filename": kwargs.get("filename"),
        "source_id": kwargs.get("source_id"),
        "mode": kwargs.get("mode"),
        "coverage": kwargs.get("coverage"),
    }


def _log_retry(retry_state: Any, method: str) -> None:
    ctx = _extract_retry_context(retry_state)
    self_obj = ctx.get("self")
    if self_obj and hasattr(self_obj, "_set_last_call_retry"):
        self_obj._set_last_call_retry(retry_state.attempt_number)
    model = ctx.get("model")
    if method == "vision_json" and self_obj is not None:
        model = getattr(self_obj, "vision_model", model)
    logger.warning(
        "LLM %s retrying (attempt %d/3) | step=%s | model=%s | timeout=%s | prompt_chars=%d | filename=%s | source_id=%s | mode=%s | coverage=%s | error=%s",
        method,
        retry_state.attempt_number,
        ctx.get("step"),
        model,
        ctx.get("timeout"),
        ctx.get("prompt_len"),
        ctx.get("filename"),
        ctx.get("source_id"),
        ctx.get("mode"),
        ctx.get("coverage"),
        str(retry_state.outcome.exception()) if retry_state.outcome else "unknown",
    )

# Module-level singleton instance
_llm_client_instance: Optional["LLMClient"] = None


def get_llm_client() -> "LLMClient":
    """
    获取 LLMClient 单例。

    确保整个应用只创建一个 LLMClient 并复用，
    避免在 pipeline、fill_planner 等处重复实例化。
    """
    global _llm_client_instance
    if _llm_client_instance is None:
        logger.debug("Creating new LLMClient singleton instance")
        _llm_client_instance = LLMClient()
    return _llm_client_instance


def reset_llm_client() -> None:
    """
    重置 LLMClient 单例。

    用于测试或配置变更后需要重新创建客户端时。
    """
    global _llm_client_instance
    _llm_client_instance = None
    logger.debug("LLMClient singleton instance reset")


class LLMClient:
    """
    LLM 客户端，封装 OpenAI 兼容 API 的异步调用。

    支持 chat_json、chat_text、vision_json 等方法，
    内置重试、Token 统计、JSON 解析等。
    """
    def __init__(self) -> None:
        settings = get_settings()
        timeout = httpx.Timeout(settings.REQUEST_TIMEOUT, connect=10.0)
        self.client = AsyncOpenAI(
            api_key=settings.OPENAI_API_KEY,
            base_url=settings.OPENAI_BASE_URL,
            timeout=timeout,
        )
        self.model = settings.OPENAI_MODEL
        self.vision_model = settings.OPENAI_VISION_MODEL
        self.temperature = settings.TEMPERATURE
        self.timeout = settings.REQUEST_TIMEOUT
        self.retry_config = RetryConfig(
            max_attempts=int(getattr(settings, "LLM_RETRY_MAX_ATTEMPTS", 3)),
            min_wait_seconds=float(getattr(settings, "LLM_RETRY_MIN_WAIT_SECONDS", 2.0)),
            max_wait_seconds=float(getattr(settings, "LLM_RETRY_MAX_WAIT_SECONDS", 10.0)),
            backoff_multiplier=float(getattr(settings, "LLM_RETRY_BACKOFF_MULTIPLIER", 1.0)),
        )
        self.token_tracker = TokenTracker()
        self._last_call_info: Optional[Dict[str, Any]] = None
        logger.info("LLMClient initialized with model=%s, vision_model=%s", self.model, self.vision_model)

    def _set_last_call_start(
        self,
        *,
        step: Optional[str],
        method: str,
        model: str,
        timeout: Optional[float],
        prompt_len: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> float:
        start_time = time.time()
        info: Dict[str, Any] = {
            "step": step,
            "method": method,
            "model": model,
            "timeout": timeout,
            "prompt_chars": prompt_len,
            "status": "in_progress",
            "start_time": start_time,
            "retries": 0,
        }
        if extra:
            info.update(extra)
        self._last_call_info = info
        return start_time

    def _set_last_call_end(self, start_time: float, status: str, error: Optional[str] = None) -> None:
        if self._last_call_info is None:
            self._last_call_info = {}
        end_time = time.time()
        self._last_call_info.update(
            {"status": status, "end_time": end_time, "elapsed_ms": int((end_time - start_time) * 1000)}
        )
        if error:
            self._last_call_info["error"] = error

    def _set_last_call_retry(self, attempts: int) -> None:
        if self._last_call_info is None:
            self._last_call_info = {"retries": attempts}
        else:
            self._last_call_info["retries"] = attempts

    def get_last_call_info(self) -> Optional[Dict[str, Any]]:
        return dict(self._last_call_info) if self._last_call_info else None

    def get_token_usage(self) -> Dict[str, int]:
        return self.token_tracker.get()

    def reset_token_usage(self) -> None:
        self.token_tracker.reset()
        logger.debug("Token usage statistics reset")

    @staticmethod
    def _build_messages(prompt: str, system: Optional[str] = None) -> List[Dict[str, Any]]:
        messages: List[Dict[str, Any]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        return messages

    async def _build_vision_messages(
        self,
        prompt: str,
        image_paths: List[str],
        system: Optional[str],
    ) -> List[Dict[str, Any]]:
        messages: List[Dict[str, Any]] = []
        if system:
            messages.append({"role": "system", "content": system})

        content: List[Dict[str, Any]] = [{"type": "text", "text": prompt}]
        for image_path in image_paths:
            image_data = await asyncio.to_thread(self._read_image_base64, image_path)
            content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_data}"}})
        messages.append({"role": "user", "content": content})
        return messages

    @staticmethod
    def _read_image_base64(image_path: str) -> str:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")

    @staticmethod
    def _run_sync(coro: Any) -> Any:
        """
        Run a coroutine from sync context.

        - Preferred path: `asyncio.run` (script/worker context).
        - Fallback: if already inside a running loop, execute in a helper thread.
        """
        try:
            return asyncio.run(coro)
        except RuntimeError as exc:
            if "asyncio.run() cannot be called from a running event loop" not in str(exc):
                raise

            holder: Dict[str, Any] = {}

            def _runner() -> None:
                try:
                    holder["result"] = asyncio.run(coro)
                except Exception as thread_exc:  # noqa: BLE001
                    holder["error"] = thread_exc

            thread = threading.Thread(target=_runner, daemon=True)
            thread.start()
            thread.join()
            if "error" in holder:
                raise holder["error"]
            return holder.get("result")

    async def _chat_json_async(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        step: Optional[str] = None,
        filename: Optional[str] = None,
        source_id: Optional[str] = None,
        mode: Optional[str] = None,
        coverage: Optional[float] = None,
    ) -> Union[dict, list]:
        messages = self._build_messages(prompt, system=system)
        temp = self.temperature if temperature is None else temperature
        logger.info(
            "LLM chat_json start | step=%s | model=%s | timeout=%s | prompt_chars=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self.model,
            self.timeout,
            len(prompt),
            filename,
            source_id,
            mode,
            coverage,
        )
        start = self._set_last_call_start(
            step=step,
            method="chat_json",
            model=self.model,
            timeout=self.timeout,
            prompt_len=len(prompt),
        )
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temp,
                response_format={"type": "json_object"},
            )
        except Exception as exc:
            self._set_last_call_end(start, "error", error=str(exc))
            raise

        self.token_tracker.update(response)
        content = (response.choices[0].message.content or "").strip()
        self._set_last_call_end(start, "ok")
        logger.info(
            "LLM chat_json done | step=%s | elapsed_ms=%d | retries=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self._last_call_info.get("elapsed_ms", 0) if self._last_call_info else 0,
            self._last_call_info.get("retries", 0) if self._last_call_info else 0,
            filename,
            source_id,
            mode,
            coverage,
        )
        return JSONParser.parse(content)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=partial(_log_retry, method="chat_json"),
        reraise=True,
    )
    def chat_json(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        step: Optional[str] = None,
        filename: Optional[str] = None,
        source_id: Optional[str] = None,
        mode: Optional[str] = None,
        coverage: Optional[float] = None,
    ) -> Union[dict, list]:
        return self._run_sync(
            self._chat_json_async(
                prompt=prompt,
                system=system,
                temperature=temperature,
                step=step,
                filename=filename,
                source_id=source_id,
                mode=mode,
                coverage=coverage,
            )
        )

    async def _chat_json_once_async(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        step: Optional[str] = None,
        filename: Optional[str] = None,
        source_id: Optional[str] = None,
        mode: Optional[str] = None,
        coverage: Optional[float] = None,
    ) -> Union[dict, list]:
        messages = self._build_messages(prompt, system=system)
        temp = self.temperature if temperature is None else temperature
        logger.info(
            "LLM chat_json_once start | step=%s | model=%s | timeout=%s | prompt_chars=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self.model,
            timeout,
            len(prompt),
            filename,
            source_id,
            mode,
            coverage,
        )
        start = self._set_last_call_start(
            step=step,
            method="chat_json_once",
            model=self.model,
            timeout=timeout if isinstance(timeout, (int, float)) else self.timeout,
            prompt_len=len(prompt),
        )
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temp,
                timeout=timeout,
                response_format={"type": "json_object"},
            )
        except Exception as exc:
            self._set_last_call_end(start, "error", error=str(exc))
            raise
        self.token_tracker.update(response)
        content = (response.choices[0].message.content or "").strip()
        self._set_last_call_end(start, "ok")
        logger.info(
            "LLM chat_json_once done | step=%s | elapsed_ms=%d | retries=0 | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self._last_call_info.get("elapsed_ms", 0) if self._last_call_info else 0,
            filename,
            source_id,
            mode,
            coverage,
        )
        return JSONParser.parse(content)

    def chat_json_once(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        step: Optional[str] = None,
        filename: Optional[str] = None,
        source_id: Optional[str] = None,
        mode: Optional[str] = None,
        coverage: Optional[float] = None,
    ) -> Union[dict, list]:
        """Single-shot JSON call without retry, sync bridge for UI/fast-fail flows."""
        return self._run_sync(
            self._chat_json_once_async(
                prompt=prompt,
                system=system,
                temperature=temperature,
                timeout=timeout,
                step=step,
                filename=filename,
                source_id=source_id,
                mode=mode,
                coverage=coverage,
            )
        )

    async def _chat_text_async(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        step: Optional[str] = None,
        filename: Optional[str] = None,
        source_id: Optional[str] = None,
        mode: Optional[str] = None,
        coverage: Optional[float] = None,
    ) -> str:
        messages = self._build_messages(prompt, system=system)
        temp = self.temperature if temperature is None else temperature
        logger.info(
            "LLM chat_text start | step=%s | model=%s | timeout=%s | prompt_chars=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self.model,
            self.timeout,
            len(prompt),
            filename,
            source_id,
            mode,
            coverage,
        )
        start = self._set_last_call_start(
            step=step,
            method="chat_text",
            model=self.model,
            timeout=self.timeout,
            prompt_len=len(prompt),
        )
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temp,
            )
        except Exception as exc:
            self._set_last_call_end(start, "error", error=str(exc))
            raise

        self.token_tracker.update(response)
        self._set_last_call_end(start, "ok")
        logger.info(
            "LLM chat_text done | step=%s | elapsed_ms=%d | retries=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self._last_call_info.get("elapsed_ms", 0) if self._last_call_info else 0,
            self._last_call_info.get("retries", 0) if self._last_call_info else 0,
            filename,
            source_id,
            mode,
            coverage,
        )
        return response.choices[0].message.content or ""

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=partial(_log_retry, method="chat_text"),
        reraise=True,
    )
    def chat_text(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        step: Optional[str] = None,
        filename: Optional[str] = None,
        source_id: Optional[str] = None,
        mode: Optional[str] = None,
        coverage: Optional[float] = None,
    ) -> str:
        return self._run_sync(
            self._chat_text_async(
                prompt=prompt,
                system=system,
                temperature=temperature,
                step=step,
                filename=filename,
                source_id=source_id,
                mode=mode,
                coverage=coverage,
            )
        )

    async def _vision_json_async(
        self,
        prompt: str,
        image_paths: List[str],
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        step: Optional[str] = None,
        filename: Optional[str] = None,
        source_id: Optional[str] = None,
        mode: Optional[str] = None,
        coverage: Optional[float] = None,
    ) -> Union[dict, list]:
        messages = await self._build_vision_messages(prompt, image_paths=image_paths, system=system)
        temp = self.temperature if temperature is None else temperature
        logger.info(
            "LLM vision_json start | step=%s | model=%s | timeout=%s | prompt_chars=%d | images=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self.vision_model,
            self.timeout,
            len(prompt),
            len(image_paths),
            filename,
            source_id,
            mode,
            coverage,
        )
        start = self._set_last_call_start(
            step=step,
            method="vision_json",
            model=self.vision_model,
            timeout=self.timeout,
            prompt_len=len(prompt),
            extra={"images": len(image_paths)},
        )
        try:
            response = await self.client.chat.completions.create(
                model=self.vision_model,
                messages=messages,
                temperature=temp,
                response_format={"type": "json_object"},
            )
        except Exception as exc:
            self._set_last_call_end(start, "error", error=str(exc))
            raise

        self.token_tracker.update(response)
        content = (response.choices[0].message.content or "").strip()
        self._set_last_call_end(start, "ok")
        logger.info(
            "LLM vision_json done | step=%s | elapsed_ms=%d | retries=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self._last_call_info.get("elapsed_ms", 0) if self._last_call_info else 0,
            self._last_call_info.get("retries", 0) if self._last_call_info else 0,
            filename,
            source_id,
            mode,
            coverage,
        )
        return JSONParser.parse(content)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=partial(_log_retry, method="vision_json"),
        reraise=True,
    )
    def vision_json(
        self,
        prompt: str,
        image_paths: List[str],
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        step: Optional[str] = None,
        filename: Optional[str] = None,
        source_id: Optional[str] = None,
        mode: Optional[str] = None,
        coverage: Optional[float] = None,
    ) -> Union[dict, list]:
        return self._run_sync(
            self._vision_json_async(
                prompt=prompt,
                image_paths=image_paths,
                system=system,
                temperature=temperature,
                step=step,
                filename=filename,
                source_id=source_id,
                mode=mode,
                coverage=coverage,
            )
        )
