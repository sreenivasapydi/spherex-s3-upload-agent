from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    """Configuration settings for the application."""
    
    SERVICE_NAME: str = "spherex-s3-upload-agent"
    SERVICE_DESCRIPTION: str = "Spherex S3 Upload Service Agent"
    SERVICE_VERSION: str = "0.1.0"

    SPHEREX_UPLOAD_SERVICE_URL: str
    S3_BUCKET_NAME: str
    AWS_UNSIGNED: bool = False
    AWS_PROFILE: str | None = None
    MAX_POOL_CONNECTIONS: int = 100
    WORKER_CONCURRENCY: int = 50
    S3_MAX_CONCURRENCY: int = 20

    APP_ENV: str = Field(
        description="Application environment", default="development"
    )
    APP_PORT : int = Field(
        description="Application port", default=8080
    )

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=True, extra="ignore"
    )

    
settings = Settings() # pyright: ignore[reportCallIssue]