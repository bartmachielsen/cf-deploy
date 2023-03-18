import argparse
import re
import sys
import subprocess
from pathlib import Path
from typing import List

import toml

VERSION_REGEX = re.compile(r"(\d+\.\d+\.\d+)")


def increment_version(version: str) -> str:
    major, minor, patch = map(int, version.split("."))
    return f"{major}.{minor}.{patch + 1}"


def version_in_git_last_commit(file_path: str) -> str:
    try:
        last_commit_content = subprocess.check_output(
            ["git", "show", f"HEAD:{file_path}"], text=True
        )
    except subprocess.CalledProcessError:
        return None

    data = toml.loads(last_commit_content)
    if "tool" not in data or "poetry" not in data["tool"]:
        return None

    return data["tool"]["poetry"]["version"]


def process_pyproject_toml(file_path: str) -> None:
    with open(file_path, "r") as file:
        content = file.read()

    data = toml.loads(content)

    if "tool" not in data or "poetry" not in data["tool"]:
        print(f"No version found in {file_path}")
        sys.exit(1)

    current_version = data["tool"]["poetry"]["version"]
    last_commit_version = version_in_git_last_commit(file_path)

    if current_version == last_commit_version:
        new_version = increment_version(current_version)
        with open(file_path, "w") as file:
            file.write(content.replace(current_version, new_version))
        print(f"Version updated in {file_path}: {current_version} -> {new_version}")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="*", help="File paths to process")
    args = parser.parse_args(argv)

    for file_path in args.files:
        process_pyproject_toml(Path(file_path))

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
