import sys

import pytest

# ---------------------------------------------------------------------------
# Windows: suppress PermissionError from pytest's atexit symlink cleanup.
# On Windows, directory symlinks in the pytest temp dir (pytest-current)
# require admin privileges to delete.  This patches the cleanup helper
# so the harmless error no longer prints to stderr.
# ---------------------------------------------------------------------------
if sys.platform == "win32":
    try:
        import _pytest.pathlib as _pp
        _orig_cleanup = _pp.cleanup_dead_symlinks

        def _safe_cleanup(root):  # type: ignore[no-untyped-def]
            try:
                _orig_cleanup(root)
            except PermissionError:
                pass

        _pp.cleanup_dead_symlinks = _safe_cleanup
    except Exception:
        pass


def pytest_addoption(parser):
    parser.addoption(
        "--integration", action="store_true", default=False,
        help="Run integration tests (requires valid Fantrax cookies)",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: marks tests as integration (need --integration flag)")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration"):
        return
    skip = pytest.mark.skip(reason="Need --integration flag to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
