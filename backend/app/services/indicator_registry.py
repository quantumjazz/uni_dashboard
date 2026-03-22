from pathlib import Path

import yaml

from backend.app.config import get_settings
from backend.app.models.schemas import IndicatorDefinition


class IndicatorRegistry:
    """Loads indicator definitions from YAML so new metrics can be added declaratively."""

    def __init__(self, config_path: Path | None = None) -> None:
        settings = get_settings()
        self.config_path = config_path or settings.indicators_config_path
        self._indicators = self._load()

    def _load(self) -> dict[str, IndicatorDefinition]:
        with self.config_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        indicators = payload.get("indicators", [])
        return {item["id"]: IndicatorDefinition(**item) for item in indicators}

    def list(self) -> list[IndicatorDefinition]:
        return list(self._indicators.values())

    def get(self, indicator_id: str) -> IndicatorDefinition:
        return self._indicators[indicator_id]

    def metadata(self) -> dict:
        panels = sorted({indicator.panel for indicator in self._indicators.values()})
        strategic_questions = sorted({indicator.strategic_question for indicator in self._indicators.values()})
        return {"panels": panels, "strategic_questions": strategic_questions, "count": len(self._indicators)}
