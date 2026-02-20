from pathlib import Path
import json


def read_json(file: Path):
    with open(file, "r") as f:
        return json.load(f)
