from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration settings for the application."""
    
    SERVICE_NAME: str = "spherex-s3-upload-agent"
    SERVICE_DESCRIPTION: str = "Spherex S3 Upload Service Agent"
    SERVICE_VERSION: str = "0.1.0"

    SPHEREX_UPLOAD_SERVICE_URL: str
    S3_BUCKET_NAME: str
    AWS_UNSIGNED: bool = False
    AWS_PROFILE: str | None = None
    
    # --- Pipelined Upload Settings ---
    # File I/O concurrency: disk-bound, optimal values:
    #   - NVMe SSD: 32-64
    #   - SATA SSD: 16-32  
    #   - HDD: 4-8
    IO_CONCURRENCY: int = 32
    
    # Network upload concurrency: latency-bound, can be much higher
    # Depends on bandwidth and S3 endpoint capacity
    NETWORK_CONCURRENCY: int = 100
    
    # Buffer queue size between I/O and network stages
    # Higher = more memory, better throughput smoothing
    @computed_field
    @property
    def BUFFER_QUEUE_SIZE(self) -> int:
        return max(self.IO_CONCURRENCY, self.NETWORK_CONCURRENCY)*2

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