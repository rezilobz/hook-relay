from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_pending: str = "hookrelay.events.pending"
    kafka_topic_retry: str = "hookrelay.events.retry"
    kafka_topic_dlq: str = "hookrelay.events.dlq"
    kafka_consumer_group: str = "hookrelay-workers"

    # Database
    database_url: str = "postgresql+asyncpg://hookrelay:hookrelay@localhost:5432/hookrelay"

    # Worker
    worker_concurrency: int = 10
    max_retry_attempts: int = 15
    delivery_timeout_seconds: int = 10

    # Security
    signature_header: str = "X-HookRelay-Signature"
    api_key_header: str = "X-API-Key"
    api_key: str

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Server
    host: str = "0.0.0.0"  # noqa: S104
    port: int = 8000
    log_level: str = "INFO"


settings = Settings()
