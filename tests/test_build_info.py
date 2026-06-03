from pathlib import Path

import toml
from tools.write_build_info import write_build_info

ROOT_DIR = Path(__file__).parent.parent


def test_hatch_build_hook_writes_static_version_build_info(tmp_path: Path):
    pyproject = toml.load(ROOT_DIR / "pyproject.toml")
    project_version = pyproject["project"]["version"]
    build_hook = pyproject["tool"]["hatch"]["build"]["hooks"]["custom"]

    assert project_version == "3.6.27+growthloop"
    assert build_hook["path"] == "tools/write_build_info.py"
    assert build_hook["build-info-path"] == "src/prefect/_build_info.py"
    assert (
        f"/{build_hook['path']}"
        in pyproject["tool"]["hatch"]["build"]["targets"]["sdist"]["include"]
    )

    write_build_info(
        tmp_path,
        {"version": project_version, "build_date": "2026-06-02T00:00:00+00:00"},
        {"path": build_hook["build-info-path"]},
    )

    build_info = (tmp_path / build_hook["build-info-path"]).read_text()
    assert '__version__ = "3.6.27+growthloop"' in build_info
    assert '__build_date__ = "2026-06-02T00:00:00+00:00"' in build_info
    assert "__dirty__ = False" in build_info
