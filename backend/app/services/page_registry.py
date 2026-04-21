from pathlib import Path

import yaml

from backend.app.config import get_settings
from backend.app.models.schemas import PageDefinition


class PageRegistry:
    """Loads page definitions from YAML for frontend navigation and layout metadata."""

    def __init__(self, config_path: Path | None = None) -> None:
        settings = get_settings()
        self.config_path = config_path or settings.pages_config_path
        self._pages = self._load()

    def _load(self) -> list[PageDefinition]:
        with self.config_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        pages = payload.get("pages", [])
        return [PageDefinition(**item) for item in pages]

    def list(self) -> list[PageDefinition]:
        return list(self._pages)
