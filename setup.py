# Package metadata lives in pyproject.toml. setup.py only remains because Babel
# reads message_extractors from it (python setup.py extract_messages).
from setuptools import setup

setup(
    message_extractors={
        "ckanext": [
            ("**.py", "python", None),
            ("**.js", "javascript", None),
            ("**/templates/**.html", "ckan", None),
        ],
    },
)
