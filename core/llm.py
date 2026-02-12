import json
import re
import time
import httpx
from functools import partial
from typing import Union, Optional, List, Dict, Any
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from core.config import get_settings
from core.logger import get_logger

logger = get_logger(__name__)


def _extract_retry_context(retry_state) -> Dict[str, Any]:
    args = retry_state.args or []
    kwargs = retry_state.kwargs or {}
    self_obj = args[0] if args else None
    prompt = args[1] if len(args) > 1 else ""
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
        "coverage": kwargs.get("coverage")
    }


def _log_retry(retry_state, method: str) -> None:
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
        str(retry_state.outcome.exception()) if retry_state.outcome else "unknown"
    )

# Module-level singleton instance
_llm_client_instance: Optional["LLMClient"] = None


def get_llm_client() -> "LLMClient":
    """
    Get the singleton LLMClient instance.
    
    This ensures only one LLMClient is created and reused across the application,
    avoiding multiple instantiations in pipeline.py, fill_planner.py, etc.
    
    Returns:
        LLMClient: The singleton LLMClient instance.
    """
    global _llm_client_instance
    if _llm_client_instance is None:
        logger.debug("Creating new LLMClient singleton instance")
        _llm_client_instance = LLMClient()
    return _llm_client_instance


def reset_llm_client() -> None:
    """
    Reset the singleton LLMClient instance.
    
    Useful for testing or when settings change and a new client is needed.
    """
    global _llm_client_instance
    _llm_client_instance = None
    logger.debug("LLMClient singleton instance reset")


class LLMClient:
    def __init__(self):
        settings = get_settings()
        timeout = httpx.Timeout(settings.REQUEST_TIMEOUT, connect=10.0)
        self.client = OpenAI(
            api_key=settings.OPENAI_API_KEY,
            base_url=settings.OPENAI_BASE_URL,
            timeout=timeout
        )
        self.model = settings.OPENAI_MODEL
        self.vision_model = settings.OPENAI_VISION_MODEL
        self.temperature = settings.TEMPERATURE
        self.timeout = settings.REQUEST_TIMEOUT
        
        # Token usage tracking
        self.token_usage = {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "requests_count": 0
        }
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
        extra: Optional[Dict[str, Any]] = None
    ) -> float:
        start_time = time.time()
        info = {
            "step": step,
            "method": method,
            "model": model,
            "timeout": timeout,
            "prompt_chars": prompt_len,
            "status": "in_progress",
            "start_time": start_time,
            "retries": 0
        }
        if extra:
            info.update(extra)
        self._last_call_info = info
        return start_time

    def _set_last_call_end(self, start_time: float, status: str, error: Optional[str] = None) -> None:
        if not self._last_call_info:
            self._last_call_info = {}
        end_time = time.time()
        self._last_call_info.update({
            "status": status,
            "end_time": end_time,
            "elapsed_ms": int((end_time - start_time) * 1000)
        })
        if error:
            self._last_call_info["error"] = error

    def _set_last_call_retry(self, attempts: int) -> None:
        if not self._last_call_info:
            self._last_call_info = {"retries": attempts}
        else:
            self._last_call_info["retries"] = attempts

    def get_last_call_info(self) -> Optional[Dict[str, Any]]:
        return self._last_call_info.copy() if self._last_call_info else None
    
    def _update_token_usage(self, response) -> None:
        """Update token usage statistics from API response."""
        if hasattr(response, 'usage') and response.usage is not None:
            usage = response.usage
            input_tokens = getattr(usage, 'prompt_tokens', 0) or 0
            output_tokens = getattr(usage, 'completion_tokens', 0) or 0
            total_tokens = getattr(usage, 'total_tokens', 0) or (input_tokens + output_tokens)
            
            self.token_usage["input_tokens"] += input_tokens
            self.token_usage["output_tokens"] += output_tokens
            self.token_usage["total_tokens"] += total_tokens
            self.token_usage["requests_count"] += 1
            
            logger.debug("Token usage: input=%d, output=%d, total=%d (cumulative: %d)",
                        input_tokens, output_tokens, total_tokens, self.token_usage["total_tokens"])
    
    def get_token_usage(self) -> dict:
        """Get current token usage statistics."""
        return self.token_usage.copy()
    
    def reset_token_usage(self) -> None:
        """Reset token usage statistics."""
        self.token_usage = {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "requests_count": 0
        }
        logger.debug("Token usage statistics reset")

    def _extract_json(self, text: str) -> Union[dict, list]:
        """
        Extract and parse JSON from model output text.
        
        Attempts to extract JSON from markdown code blocks or raw text,
        with multiple fallback strategies for handling malformed JSON.
        
        Args:
            text: The raw text output from the model.
        
        Returns:
            The parsed JSON as a dict or list. On parse failure, returns a
            fallback dictionary with keys:
                - "error": "json_parse_error"
                - "raw_output": The original text (truncated to 5000 chars)
                - "parse_error": The error message from the parser
            
            This method never raises exceptions for JSON formatting issues.
        """
        text = text.strip()
        original_text = text
        
        json_match = re.search(r'```(?:json)?\s*(\{.*\}|\[.*\])\s*```', text, re.DOTALL)
        if json_match:
            text = json_match.group(1)
        else:
            brace_start = text.find('{')
            bracket_start = text.find('[')
            
            if brace_start != -1 and (bracket_start == -1 or brace_start < bracket_start):
                depth = 0
                start = brace_start
                for i in range(start, len(text)):
                    if text[i] == '{':
                        depth += 1
                    elif text[i] == '}':
                        depth -= 1
                        if depth == 0:
                            text = text[start:i+1]
                            break
            elif bracket_start != -1:
                depth = 0
                start = bracket_start
                for i in range(start, len(text)):
                    if text[i] == '[':
                        depth += 1
                    elif text[i] == ']':
                        depth -= 1
                        if depth == 0:
                            text = text[start:i+1]
                            break
        
        # 尝试解析原始 JSON
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            # 如果失败，尝试修复控制字符问题
            # JSON 规范要求字符串中的控制字符必须被转义
            # 我们删除未转义的控制字符（除了常见的 \n, \r, \t）
            try:
                # 方法1: 删除所有未转义的控制字符（保留已转义的）
                # 使用正则表达式，但要小心不要破坏转义序列
                def remove_unescaped_control_chars(s: str) -> str:
                    result = []
                    i = 0
                    while i < len(s):
                        if s[i] == '\\' and i + 1 < len(s):
                            # 转义序列，保留
                            result.append(s[i])
                            result.append(s[i+1])
                            i += 2
                        elif ord(s[i]) < 32 and s[i] not in ['\n', '\r', '\t']:
                            # 未转义的控制字符（除了常见的外），删除
                            i += 1
                        else:
                            result.append(s[i])
                            i += 1
                    return ''.join(result)
                
                fixed_text = remove_unescaped_control_chars(text)
                return json.loads(fixed_text)
            except json.JSONDecodeError:
                # 方法2: 更激进 - 删除所有控制字符
                try:
                    cleaned_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)
                    return json.loads(cleaned_text)
                except json.JSONDecodeError as e2:
                    # Log the error and return a fallback dictionary instead of raising
                    error_pos = getattr(e2, 'pos', None)
                    error_msg = f"Failed to parse JSON from model output: {e2}"
                    if error_pos and error_pos < len(original_text):
                        context_start = max(0, error_pos - 200)
                        context_end = min(len(original_text), error_pos + 200)
                        context = original_text[context_start:context_end]
                        error_msg += f"\nError position: {error_pos}\nContext around error:\n{context}"
                    
                    logger.error("%s", error_msg)
                    
                    return {
                        "error": "json_parse_error",
                        "raw_output": original_text[:5000],  # Truncate to avoid huge logs
                        "parse_error": str(e2)
                    }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=partial(_log_retry, method="chat_json"),
        reraise=True
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
        coverage: Optional[float] = None
    ) -> Union[dict, list]:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        
        logger.info(
            "LLM chat_json start | step=%s | model=%s | timeout=%s | prompt_chars=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self.model,
            self.timeout,
            len(prompt),
            filename,
            source_id,
            mode,
            coverage
        )
        start_time = self._set_last_call_start(
            step=step,
            method="chat_json",
            model=self.model,
            timeout=self.timeout,
            prompt_len=len(prompt)
        )
        temp = self.temperature if temperature is None else temperature
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temp
            )
        except Exception as e:
            self._set_last_call_end(start_time, "error", error=str(e))
            raise
        self._update_token_usage(response)
        content = response.choices[0].message.content
        self._set_last_call_end(start_time, "ok")
        logger.info(
            "LLM chat_json done | step=%s | elapsed_ms=%d | retries=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self._last_call_info.get("elapsed_ms", 0) if self._last_call_info else 0,
            self._last_call_info.get("retries", 0) if self._last_call_info else 0,
            filename,
            source_id,
            mode,
            coverage
        )
        return self._extract_json(content)

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
        coverage: Optional[float] = None
    ) -> Union[dict, list]:
        """
        Like chat_json, but:
        - NO retries (fast-fail for UI flows)
        - Supports per-call timeout override
        """
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        logger.info(
            "LLM chat_json_once start | step=%s | model=%s | timeout=%s | prompt_chars=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self.model,
            timeout,
            len(prompt),
            filename,
            source_id,
            mode,
            coverage
        )
        start_time = self._set_last_call_start(
            step=step,
            method="chat_json_once",
            model=self.model,
            timeout=timeout,
            prompt_len=len(prompt)
        )
        temp = self.temperature if temperature is None else temperature
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temp,
                timeout=timeout,
            )
        except Exception as e:
            self._set_last_call_end(start_time, "error", error=str(e))
            raise
        self._update_token_usage(response)
        content = response.choices[0].message.content
        self._set_last_call_end(start_time, "ok")
        logger.info(
            "LLM chat_json_once done | step=%s | elapsed_ms=%d | retries=0 | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self._last_call_info.get("elapsed_ms", 0) if self._last_call_info else 0,
            filename,
            source_id,
            mode,
            coverage
        )
        return self._extract_json(content)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=partial(_log_retry, method="chat_text"),
        reraise=True
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
        coverage: Optional[float] = None
    ) -> str:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        
        logger.info(
            "LLM chat_text start | step=%s | model=%s | timeout=%s | prompt_chars=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self.model,
            self.timeout,
            len(prompt),
            filename,
            source_id,
            mode,
            coverage
        )
        start_time = self._set_last_call_start(
            step=step,
            method="chat_text",
            model=self.model,
            timeout=self.timeout,
            prompt_len=len(prompt)
        )
        temp = self.temperature if temperature is None else temperature
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temp
            )
        except Exception as e:
            self._set_last_call_end(start_time, "error", error=str(e))
            raise
        self._update_token_usage(response)
        self._set_last_call_end(start_time, "ok")
        logger.info(
            "LLM chat_text done | step=%s | elapsed_ms=%d | retries=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self._last_call_info.get("elapsed_ms", 0) if self._last_call_info else 0,
            self._last_call_info.get("retries", 0) if self._last_call_info else 0,
            filename,
            source_id,
            mode,
            coverage
        )
        return response.choices[0].message.content

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=partial(_log_retry, method="vision_json"),
        reraise=True
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
        coverage: Optional[float] = None
    ) -> Union[dict, list]:
        import base64
        
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        
        content = [{"type": "text", "text": prompt}]
        for image_path in image_paths:
            with open(image_path, "rb") as image_file:
                image_data = base64.b64encode(image_file.read()).decode('utf-8')
                content.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{image_data}"
                    }
                })
        messages.append({"role": "user", "content": content})
        
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
            coverage
        )
        start_time = self._set_last_call_start(
            step=step,
            method="vision_json",
            model=self.vision_model,
            timeout=self.timeout,
            prompt_len=len(prompt),
            extra={"images": len(image_paths)}
        )
        temp = self.temperature if temperature is None else temperature
        try:
            response = self.client.chat.completions.create(
                model=self.vision_model,
                messages=messages,
                temperature=temp
            )
        except Exception as e:
            self._set_last_call_end(start_time, "error", error=str(e))
            raise
        self._update_token_usage(response)
        content_text = response.choices[0].message.content
        self._set_last_call_end(start_time, "ok")
        logger.info(
            "LLM vision_json done | step=%s | elapsed_ms=%d | retries=%d | filename=%s | source_id=%s | mode=%s | coverage=%s",
            step,
            self._last_call_info.get("elapsed_ms", 0) if self._last_call_info else 0,
            self._last_call_info.get("retries", 0) if self._last_call_info else 0,
            filename,
            source_id,
            mode,
            coverage
        )
        return self._extract_json(content_text)
