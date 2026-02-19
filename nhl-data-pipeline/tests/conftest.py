import pytest


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
