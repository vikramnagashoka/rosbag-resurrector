"""Structured logging configuration for RosBag Resurrector."""

import logging
import sys


def setup_logging(
    level: str = "WARNING",
    log_file: str | None = None,
    verbose: bool = False,
) -> None:
    """Configure logging for the resurrector package.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR).
        log_file: Optional path to a log file.
        verbose: If True, sets level to DEBUG.
    """
    if verbose:
        level = "DEBUG"

    root_logger = logging.getLogger("resurrector")
    root_logger.setLevel(getattr(logging, level.upper(), logging.WARNING))

    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Always log to stderr (doesn't interfere with Rich CLI output)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    root_logger.addHandler(stderr_handler)

    # Optional file handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
