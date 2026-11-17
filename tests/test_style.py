from __future__ import annotations

import ast
import io
import subprocess
import tokenize
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SKIP = {".venv", "data", "build", "dist", "__pycache__"}


def python_files() -> list[Path]:
    try:
        listed = subprocess.run(
            ["git", "ls-files", "*.py"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
        files = [PROJECT_ROOT / name for name in listed]
    except (subprocess.CalledProcessError, FileNotFoundError):
        files = [
            path
            for path in PROJECT_ROOT.rglob("*.py")
            if not SKIP.intersection(path.relative_to(PROJECT_ROOT).parts)
        ]
    assert files
    return files


def test_no_comments_anywhere():
    offenders = []
    for path in python_files():
        source = path.read_text(encoding="utf-8")
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type == tokenize.COMMENT:
                offenders.append(f"{path.name}:{token.start[0]}")
    assert offenders == []


def test_no_docstrings_anywhere():
    offenders = []
    for path in python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(
                node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
            ) and ast.get_docstring(node):
                offenders.append(f"{path.name}:{getattr(node, 'name', 'module')}")
    assert offenders == []
