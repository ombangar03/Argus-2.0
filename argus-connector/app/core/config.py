from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "argus-connector"
    redis_url: str = "redis://localhost:6379/0"
    connector_stream: str = "queue:connector:rss"
    result_stream: str = "connector:rss:results"
    consumer_group: str = "rss_connectors"
    consumer_name: str = "rss_worker-1"

    model_config = SettingsConfigDict(
        env_file = ".env",
        case_sensitive=False
    )

settings = Settings()