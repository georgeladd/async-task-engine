"""Application settings and configuration module."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration settings for async task engine service.

    Attributes:
        app_name: Name of the microservice.
        environment: Current runtime environment (local, dev, prod).
        log_level: Logging level threshold.
        rabbitmq_host: Hostname of RabbitMQ broker.
        rabbitmq_port: Port of RabbitMQ broker.
        rabbitmq_user: Username for RabbitMQ broker.
        rabbitmq_password: Password for RabbitMQ broker.
        rabbitmq_main_queue: Name of the primary task queue.
        rabbitmq_dlq_queue: Name of the dead-letter queue.
        redis_host: Hostname of Redis server.
        redis_port: Port of Redis server.
        redis_db: Redis database number.
        redis_lock_ttl_seconds: TTL for distributed lock in seconds.
        batch_chunk_size: Default item count per processing batch chunk.
        max_task_retries: Maximum retry attempts for failing tasks.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "async-task-engine"
    environment: str = "local"
    log_level: str = "INFO"

    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_main_queue: str = "tasks_primary"
    rabbitmq_dlq_queue: str = "tasks_dead_letter"

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_lock_ttl_seconds: int = 300

    batch_chunk_size: int = 100
    max_task_retries: int = 3

    @property
    def rabbitmq_uri(self) -> str:
        """Constructs AMQP connection URI.

        Returns:
            Formatted AMQP URI string.
        """
        return (
            f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}"
            f"@{self.rabbitmq_host}:{self.rabbitmq_port}/"
        )

    @property
    def redis_uri(self) -> str:
        """Constructs Redis connection URI.

        Returns:
            Formatted Redis URI string.
        """
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


settings = Settings()
