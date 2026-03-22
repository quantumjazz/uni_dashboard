from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Application settings loaded from environment variables and defaults."""

    app_name: str = "University Leadership Dashboard"
    app_env: str = "development"
    debug: bool = True
    eurostat_base_url: str = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
    database_url: str = f"sqlite:///{ROOT_DIR / 'data' / 'cache' / 'dashboard.db'}"
    cache_ttl_hours: int = 24
    indicators_config_path: Path = ROOT_DIR / "config" / "indicators.yaml"
    frontend_dir: Path = ROOT_DIR / "frontend" / "src"
    default_countries: str = "BG,EU27_2020,DE,FR,RO,PL,CZ"
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
