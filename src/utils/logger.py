"""Logging and utility functions."""
import logging
import sys
from config.settings import settings


def setup_logger(name: str = "lakehouse") -> logging.Logger:
    """Configures and returns a standardized logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(settings.LOG_LEVEL.upper())
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(settings.LOG_LEVEL.upper())
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
