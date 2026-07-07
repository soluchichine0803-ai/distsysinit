import logging
import os
import sys
from datetime import datetime
from shared.config import LOG_DIR

def setup_logger(name: str) -> logging.Logger:
    """
    Sets up a logger that outputs to both stdout and a file in the logs/ directory.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if setup_logger is called multiple times for the same name
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File handler
        log_file = os.path.join(LOG_DIR, f"{name}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def get_timestamp() -> datetime:
    """Returns the current timestamp."""
    return datetime.now()

def ensure_dir(path: str):
    """Ensures a directory exists."""
    os.makedirs(path, exist_ok=True)
