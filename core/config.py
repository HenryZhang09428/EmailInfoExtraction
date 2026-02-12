from pydantic_settings import BaseSettings
from pydantic import field_validator
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4o-mini"
    OPENAI_VISION_MODEL: str = "gpt-4o-mini"
    TEMPERATURE: float = 0.0
    REQUEST_TIMEOUT: int = 180  # 增加到180秒，以处理大数据量的情况

    @field_validator("OPENAI_API_KEY")
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        """Validate that OPENAI_API_KEY is set and not empty."""
        if v is None or v.strip() == "":
            raise ValueError(
                "OPENAI_API_KEY is not set or empty. "
                "Please check your .env file and ensure OPENAI_API_KEY is configured."
            )
        return v

    class Config:
        env_file = ".env"
        case_sensitive = False

_settings_instance = None

def get_settings() -> Settings:
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance
