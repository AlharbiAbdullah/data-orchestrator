#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
"""Data Orchestrator's README diagrams: architecture (the solution) and tech stack (where
each tool lives), on one layout. Run it to regenerate both .excalidraw.svg files here.
The kit is Rai's /media → diagram; RAI_DIAGRAM_KIT points elsewhere if needed.
"""

import os
import sys
from collections.abc import Callable
from pathlib import Path

DEFAULT_KIT = Path.home() / "helm/03-rai/skills/media/scripts/diagram"
KIT = Path(os.environ.get("RAI_DIAGRAM_KIT", DEFAULT_KIT))
sys.path.insert(0, str(KIT))
from lib import Scene, render  # type: ignore[import-not-found]  # noqa: E402

HERE = Path(__file__).parent
DAGSTER = "url:https://dagster.io/site-icons/icon-512.png"
HTTPX = "url:https://www.python-httpx.org/img/butterfly.png"
# zone: (x, y, w, h, title)
Z = {
    "source": (0, 330, 190, 300, "SOURCE"),
    "orchestrate": (370, 150, 890, 530, "ORCHESTRATE"),
    "extract": (410, 330, 330, 300, "EXTRACT"),
    "warehouse": (860, 330, 360, 300, "WAREHOUSE"),
}
MID = 480


def frame(s: Scene, header: Callable[[Scene], None]) -> None:
    """Zones, the project box and the labelled arrows: the same in both diagrams."""
    s.zone(330, 0, 970, 720, None, dotted=True)
    header(s)
    for x, y, w, h, title in Z.values():
        s.zone(x, y, w, h, title)
    s.arrow([(192, MID), (408, MID)], label="fetch\nweather JSON", at=(194, MID - 60, 132))
    s.arrow([(742, MID), (858, MID)], label="load\ninto DuckDB", at=(740, MID - 60, 120))


def row(s: Scene, zone: str, items: list[tuple[str, str]], top: int | None = None) -> None:
    """Logos with names, spread evenly across a zone."""
    x, y, w, _, _ = Z[zone]
    slot = w / len(items)
    top = y + 90 if top is None else top
    for j, (key, name) in enumerate(items):
        s.tool(x + slot * j + slot / 2, top, key, name, w=slot)


def tech_stack() -> None:
    s = Scene()

    def header(s: Scene) -> None:
        s.tool(406, 14, "si:uv", "uv", w=110)
        s.tool(516, 14, "si:docker", "Docker", w=110)

    frame(s, header)
    row(s, "source", [("si:json", "Open-Meteo API")])
    row(s, "orchestrate", [(DAGSTER, "Dagster")], top=196)
    row(s, "extract", [(HTTPX, "HTTPX")])
    row(s, "warehouse", [("gh:duckdb", "DuckDB"), ("si:dbt#FF694B", "dbt")])
    s.save("tech-stack")


def architecture() -> None:
    s = Scene()

    frame(s, lambda s: None)

    def parts(zone: str, labels: list[str], top: int, h: int = 52, gap: int = 14) -> None:
        x, _, w, _, _ = Z[zone]
        for label in labels:
            s.part(x + 20, top, w - 40, h, label)
            top += h + gap

    parts("source", ["weather API", "no key"], top=405)
    parts("extract", ["read city list", "fetch each city", "raw JSON file"], top=405)
    parts("warehouse", ["raw, upsert by key", "staging, intermediate", "marts, 19 tests"], top=405)
    x, y, w, _, _ = Z["orchestrate"]
    jobs = ["daily schedule", "asset lineage", "run history"]
    slot = (w - 40) / len(jobs)
    for j, label in enumerate(jobs):
        s.part(x + 20 + slot * j + 8, y + 55, slot - 16, 52, label)
    s.save("architecture")


tech_stack()
architecture()
render(HERE, "architecture", "tech-stack")
