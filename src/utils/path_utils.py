import os
from pathlib import Path

def create_if_not_exists(path: str):
    if not os.path.isdir(path):
        os.makedirs(path)

    return path