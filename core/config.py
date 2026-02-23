"""
配置模块 (Configuration Module)
==============================

从环境变量和 .env 文件加载应用配置，包括 OpenAI API、LLM 重试等设置。
"""

from pydantic_settings import BaseSettings
from pydantic import field_validator
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """
    应用配置类，继承自 Pydantic BaseSettings，支持从环境变量自动加载。

    属性:
        OPENAI_BASE_URL: OpenAI API 基础 URL，默认官方地址
        OPENAI_API_KEY: OpenAI API 密钥（必填）
        OPENAI_MODEL: 默认文本模型
        OPENAI_VISION_MODEL: 视觉模型，用于图片理解
        TEMPERATURE: 生成温度，0 表示确定性输出
        REQUEST_TIMEOUT: 请求超时秒数
        LLM_RETRY_*: 重试相关配置
    """
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4o-mini"
    OPENAI_VISION_MODEL: str = "gpt-4o-mini"
    TEMPERATURE: float = 0.0
    REQUEST_TIMEOUT: int = 180  # 增加到180秒，以处理大数据量的情况
    LLM_RETRY_MAX_ATTEMPTS: int = 3
    LLM_RETRY_MIN_WAIT_SECONDS: float = 2.0
    LLM_RETRY_MAX_WAIT_SECONDS: float = 10.0
    LLM_RETRY_BACKOFF_MULTIPLIER: float = 1.0

    @field_validator("OPENAI_API_KEY")
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        """
        校验 OPENAI_API_KEY 已设置且非空。

        若未配置则抛出 ValueError，提示用户检查 .env 文件。
        """
        if v is None or v.strip() == "":
            raise ValueError(
                "OPENAI_API_KEY is not set or empty. "
                "Please check your .env file and ensure OPENAI_API_KEY is configured."
            )
        return v

    class Config:
        env_file = ".env"
        case_sensitive = False


# 全局单例，避免重复加载配置
_settings_instance = None


def get_settings() -> Settings:
    """
    获取配置单例。

    首次调用时创建 Settings 实例并缓存，后续调用返回同一实例。
    """
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance
