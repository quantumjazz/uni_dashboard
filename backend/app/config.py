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
    openalex_base_url: str = "https://api.openalex.org"
    openalex_contact_email: str | None = None
    research_default_institution_id: str = "I122492136"
    research_featured_institution_ids: list[str] = [
        "I122492136",
        "I58918642",
        "I192878625",
        "I31151848",
        "I30147112",
        "I178498160",
        "I211070993",
    ]
    database_url: str = f"sqlite:///{ROOT_DIR / 'data' / 'cache' / 'dashboard.db'}"
    cache_ttl_hours: int = 24
    cordis_base_url: str = "https://cordis.europa.eu/api/dataextractions"
    cordis_api_key: str | None = None
    deqar_base_url: str = "https://backend.deqar.eu/webapi/v2"
    deqar_api_key: str | None = None
    deqar_reports_csv_path: Path = ROOT_DIR / "data" / "deqar" / "deqar-reports.csv"
    deqar_institutions_csv_path: Path = ROOT_DIR / "data" / "deqar" / "deqar-institutions.csv"
    deqar_agencies_csv_path: Path = ROOT_DIR / "data" / "deqar" / "deqar-agencies.csv"
    deqar_report_limit: int = 0
    neaa_base_url: str = "https://www.neaa.government.bg"
    neaa_higher_institutions_path: str = "/en/accredited-higher-education-institutions/higher-institutions"
    neaa_timeout_seconds: float = 30.0
    neaa_cache_ttl_hours: int = 12
    eheso_eter_institutions_csv_path: Path = ROOT_DIR / "data" / "eheso" / "eter-institutions.csv"
    indicators_config_path: Path = ROOT_DIR / "config" / "indicators.yaml"
    pages_config_path: Path = ROOT_DIR / "config" / "pages.yaml"
    frontend_dir: Path = ROOT_DIR / "frontend" / "src"
    default_countries: str = "BG,EU27_2020,DE,FR,RO,PL,CZ"
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
