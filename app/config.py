from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    '''
        This class defines all configuration settings for the application, with defaults and environment variable aliases.
        Application configuration loaded from environment variables or .env file.
    '''
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",          # silently ignore any extra env vars not defined here
    )

    # Application
    environment: str = Field(default="development", alias="ENVIRONMENT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    secret_key: str = Field(default="dev-secret", alias="SECRET_KEY")

    # JWT
    jwt_secret_key: str = Field(default="dev-jwt-secret", alias="JWT_SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    jwt_expire_hours: int = Field(default=8, alias="JWT_EXPIRE_HOURS")

    # MIS Database (Supabase / PostgreSQL — async asyncpg URL)
    database_url: str = Field(alias="DATABASE_URL")

    # MIS Database — optional sync URL (used by SQLAlchemy sync engine if needed)
    # If not set in .env it is auto-derived from database_url
    database_url_sync: str = Field(default="", alias="DATABASE_URL_SYNC")

    # OpenCart MySQL (website source — read-only)
    opencart_db_host: str = Field(default="", alias="OPENCART_DB_HOST")
    opencart_db_port: int = Field(default=3306, alias="OPENCART_DB_PORT")
    opencart_db_name: str = Field(default="bookshop", alias="OPENCART_DB_NAME")
    opencart_db_user: str = Field(default="", alias="OPENCART_DB_USER")
    opencart_db_password: str = Field(default="", alias="OPENCART_DB_PASSWORD")

    @property
    def opencart_db_url(self) -> str:
        # MySQL connection URL for SQLAlchemy (using PyMySQL driver)
        return (
            f"mysql+pymysql://{self.opencart_db_user}:{self.opencart_db_password}"
            f"@{self.opencart_db_host}:{self.opencart_db_port}/{self.opencart_db_name}"
            f"?charset=utf8mb4"
        )

    # Rozetka API
    rozetka_api_base_url: str = Field(
        default="https://api.seller.rozetka.com.ua",
        alias="ROZETKA_API_BASE_URL",
    )
    rozetka_api_username: str = Field(default="", alias="ROZETKA_API_USERNAME")
    rozetka_api_password_b64: str = Field(default="", alias="ROZETKA_API_PASSWORD_B64")

    # Email alerts
    smtp_host: str = Field(default="smtp.gmail.com", alias="SMTP_HOST")
    smtp_port: int = Field(default=587, alias="SMTP_PORT")
    smtp_username: str = Field(default="", alias="SMTP_USERNAME")
    smtp_password: str = Field(default="", alias="SMTP_PASSWORD")
    smtp_from_name: str = Field(default="MyEnglishBooks MIS", alias="SMTP_FROM_NAME")
    alert_to_email: str = Field(default="", alias="ALERT_TO_EMAIL")

    # Scheduler
    ingestion_interval_hours: int = Field(default=6, alias="INGESTION_INTERVAL_HOURS")
    alert_check_interval_minutes: int = Field(
        default=30, alias="ALERT_CHECK_INTERVAL_MINUTES"
    )



@lru_cache
def get_settings() -> Settings:
    # Cached function to return a Settings instance, ensuring environment variables are read only once.
    return Settings()

