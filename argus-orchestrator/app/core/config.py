from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "argus-orchestrator"
    app_env: str = "development"
    mongo_url:str = "mongodb://localhost:27017"
    mongo_database: str = "argus"
    redis_url: str = "redis://localhost:6379/0"
    connector_stream: str = "queue:connector:rss"
    result_stream: str = "connector:rss:results"
    consumer_group: str = "orchestrator-results"
    consumer_name: str = "orchestrator-1"

    # Redis pending-result recovery
    result_claim_idle_ms: int = 30000
    result_recovery_interval: int = 10

    dispatch_tick_seconds: int = 5

    model_config = SettingsConfigDict(
        env_file = ".env",
        case_sensitive=False,
        extra="ignore"
    )
    
settings = Settings()