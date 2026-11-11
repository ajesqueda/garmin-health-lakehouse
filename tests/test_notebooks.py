"""Notebooks run cell by cell, top to bottom, and ruff skips undefined names in them
(F821 is ignored for notebooks/ and pipelines/ because of dbutils and spark). This
catches the mistake that broke ingest on 2026-11-10: using a name one cell before
the cell that imports it."""

import ast
import builtins
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
NOTEBOOKS = sorted([*ROOT.glob("notebooks/*.py"), *ROOT.glob("pipelines/*.py")])
PROVIDED = {"dbutils", "spark", "display", "displayHTML"}  # injected by Databricks


def cells(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    out = []
    for cell in text.split("# COMMAND ----------"):
        lines = [ln for ln in cell.splitlines()
                 if not ln.startswith("# MAGIC") and not ln.startswith("# Databricks")]
        out.append("\n".join(lines))
    return out


def undefined_names(path: Path) -> list[str]:
    defined = set(dir(builtins)) | PROVIDED
    problems = []
    for i, code in enumerate(cells(path)):
        tree = ast.parse(code)
        # Everything this cell binds counts for the cell itself (functions can refer
        # to names assigned later in the same cell) and for every later cell.
        for node in ast.walk(tree):
            if isinstance(node, ast.Import | ast.ImportFrom):
                defined.update((a.asname or a.name).split(".")[0] for a in node.names)
            elif isinstance(node, ast.FunctionDef | ast.ClassDef):
                defined.add(node.name)
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                defined.add(node.id)
            elif isinstance(node, ast.arg):
                defined.add(node.arg)
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                if node.id not in defined:
                    problems.append(f"cell {i}: {node.id}")
    return problems


def test_notebooks_were_found():
    assert any(p.parent.name == "notebooks" for p in NOTEBOOKS)


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_no_name_is_used_before_its_cell_defines_it(path):
    assert undefined_names(path) == []


def test_the_check_catches_use_before_import(tmp_path):
    nb = tmp_path / "nb.py"
    nb.write_text("# Databricks notebook source\nx = Path('a')\n\n"
                  "# COMMAND ----------\n\nfrom pathlib import Path\n")
    assert undefined_names(nb) == ["cell 0: Path"]
