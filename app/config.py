from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration settings for the application."""
    SPHEREX_UPLOAD_SERVICE_URL: str
    S3_BUCKET_NAME: str
    AWS_UNSIGNED: bool = False
    AWS_PROFILE: str | None = None
    MAX_POOL_CONNECTIONS: int = 100
    WORKER_CONCURRENCY: int = 50
    S3_MAX_CONCURRENCY: int = 20

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=True, extra="ignore"
    )

    
settings = Settings() # pyright: ignore[reportCallIssue]