from __future__ import annotations

from pathlib import Path

PROJECT_PACKAGE_NAME = "the_light_house_project_777"
PROJECT_DISPLAY_NAME = "the_light_house_project+777"

PROJECT_ROOT = Path(__file__).resolve().parent
SOURCE_ROOT = PROJECT_ROOT.parent
WORKSPACE_ROOT = SOURCE_ROOT.parent


def project_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


def source_path(*parts: str) -> Path:
    return SOURCE_ROOT.joinpath(*parts)
