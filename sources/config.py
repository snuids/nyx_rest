from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    WELCOMEMESSAGE: str = ""
    ICON: str = ""

    # Feature flags
    USE_LOGSTASH: bool = False
    COOKIESECURE: bool = True

    # Redis
    REDIS_IP: str = ""

    # Output
    OUTPUT_FOLDER: str = ""
    OUTPUT_URL: str = ""

    # PostgreSQL
    PG_LOGIN: str = ""
    PG_PASSWORD: str = ""
    PG_HOST: str = ""
    PG_PORT: str = "5432"
    PG_DATABASE: str = ""

    # Elasticsearch / OpenSearch
    ELK_URL: str = ""
    ELK_SSL: bool = False
    ELK_LOGIN: str = ""
    ELK_PASSWORD: str = ""

    # AMQC (ActiveMQ / STOMP)
    AMQC_URL: str = ""
    AMQC_PORT: str = "61613"
    AMQC_LOGIN: str = ""
    AMQC_PASSWORD: str = ""

    # Google OAuth (optional)
    CLIENT_SECRET_FILE: str = ""

    # Grafana (optional)
    GRAFANA_URL: str = ""
    GRAFANA_API_KEY: str = ""

    # SMTP (helper service)
    SMTP_ADDRESS: str = ""
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM: str = ""
    SMTP_PORT: int = 25
    SMTP_SSL: bool = False
    SMTP_TLS: bool = False

    # SQL Server
    SQLSERVER_HOST: str = "NA"
    SQLSERVER_LOGIN: str = "NA"
    SQLSERVER_PASSWORD: str = "NA"
    SQLSERVER_PORT: int = 1433

    # Onfleet
    ONFLEET_APIKEY: str = ""


settings = Settings()
