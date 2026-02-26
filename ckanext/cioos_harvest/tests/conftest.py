"""pytest configuration for cioos_harvest tests."""


def pytest_addoption(parser):
    parser.addoption(
        "--regenerate-fixtures",
        action="store_true",
        default=False,
        help=(
            "Overwrite expected JSON fixture files with freshly parsed output. "
            "Use this after intentional parser changes to update golden files."
        ),
    )
