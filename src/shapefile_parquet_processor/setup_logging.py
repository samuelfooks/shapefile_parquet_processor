# src/shapefile_processor/logging_setup.py
import os
import logging

def setup_logging(log_dir: str):
    """
    Configure logging settings.

    :param log_dir: Directory to store log files.
    """
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_dir, 'shapefile_processing.log'),
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s',
        filemode='w'
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Also log to console
