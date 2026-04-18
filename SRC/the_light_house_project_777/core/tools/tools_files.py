import os
import shutil
from pathlib import Path
from typing import List

from project_meta import PROJECT_ROOT, SOURCE_ROOT

# Allowed root path list injected from main.py.
# File access is restricted to these directories for safety.
ALLOWED_ROOTS: List[Path] = []

def _format_roots() -> str:
    return ", ".join(str(root) for root in ALLOWED_ROOTS) if ALLOWED_ROOTS else "(empty)"

def _init_default_roots() -> None:
    global ALLOWED_ROOTS
    if ALLOWED_ROOTS:
        return
    env_roots = os.getenv("PYQLE_ALLOWED_ROOTS", "").strip()
    if env_roots:
        roots = [Path(r.strip()) for r in env_roots.split(";") if r.strip()]
    else:
        roots = [
            PROJECT_ROOT,
            PROJECT_ROOT / "logs",
            SOURCE_ROOT / "pyqle_core" / "reforming" / "logs",
        ]
    ALLOWED_ROOTS = [r.resolve() for r in roots if r]

_init_default_roots()

def set_allowed_roots(roots: List[str]):
    """
    Set the list of root directories allowed for file access.
    """
    global ALLOWED_ROOTS
    if not roots:
        print("Warning: No allowed file roots set. File operations will be disabled.")
    ALLOWED_ROOTS = [Path(r).resolve() for r in roots]

def _is_path_allowed(path: Path) -> bool:
    """
    Check whether the given path is inside one of the allowed roots.
    """
    if not ALLOWED_ROOTS:
        return False
    
    resolved_path = path.resolve()
    for root in ALLOWED_ROOTS:
        if root in resolved_path.parents or root == resolved_path:
            return True
    return False

def write_text(file_path: str, content: str) -> str:
    """
    Write a text file to the specified path.
    The path must be inside ALLOWED_ROOTS.
    """
    if not ALLOWED_ROOTS:
        raise PermissionError(
            f"Cannot write file: No allowed root directories are set. Allowed roots: {_format_roots()}. Requested: {file_path}"
        )
    
    p = Path(file_path)
    if not p.is_absolute():
        p = ALLOWED_ROOTS[0] / p

    if not _is_path_allowed(p):
        raise PermissionError(
            f"File path '{file_path}' is outside the allowed directories. Allowed roots: {_format_roots()}"
        )

    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding='utf-8')
    print(f"File written to: {p.resolve()}")
    return str(p.resolve())

def read_file(file_path: str) -> str:
    """
    Read the text contents of the specified file path.
    The path must be inside ALLOWED_ROOTS.
    """
    if not ALLOWED_ROOTS:
        raise PermissionError(
            f"Cannot read file: No allowed root directories are set. Allowed roots: {_format_roots()}. Requested: {file_path}"
        )

    p = Path(file_path)
    if not p.is_absolute():
        p = ALLOWED_ROOTS[0] / p

    if not _is_path_allowed(p):
        raise PermissionError(
            f"File path '{file_path}' is outside the allowed directories. Allowed roots: {_format_roots()}"
        )

    if not p.is_file():
        return f"ERROR: File not found at '{file_path}'"

    try:
        return p.read_text(encoding='utf-8')
    except Exception as e:
        return f"ERROR: Could not read file '{file_path}': {e}"

def delete_path(path: str) -> str:
    """
    Deletes a file or a directory at the specified path.
    The path must be within ALLOWED_ROOTS.
    """
    if not ALLOWED_ROOTS:
        raise PermissionError(
            f"Cannot delete path: No allowed root directories are set. Allowed roots: {_format_roots()}. Requested: {path}"
        )

    p = Path(path)
    if not p.is_absolute():
        p = ALLOWED_ROOTS[0] / p

    if not _is_path_allowed(p):
        raise PermissionError(
            f"Path '{path}' is outside the allowed directories. Allowed roots: {_format_roots()}"
        )

    if not p.exists():
        return f"ERROR: Path not found at '{path}'"

    try:
        if p.is_file():
            p.unlink()
            message = f"Successfully deleted file: {p.resolve()}"
        elif p.is_dir():
            shutil.rmtree(p)
            message = f"Successfully deleted directory: {p.resolve()}"
        else:
            raise ValueError("Path is not a file or directory.")
        
        print(message)
        return message
    except Exception as e:
        error_message = f"ERROR: Could not delete path '{path}': {e}"
        print(error_message)
        raise type(e)(error_message) from e
