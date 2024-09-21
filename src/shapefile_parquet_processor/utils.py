# src/shapefile_processor/utils.py
import os
import shutil

def clean_dir(directory: str) -> None:
    """
    Clean the contents of a directory.

    :param directory: Directory to clean.
    """
    if os.path.isdir(directory):
        shutil.rmtree(directory)
    elif os.path.exists(directory):
        os.remove(directory)
    os.makedirs(directory, exist_ok=True)
