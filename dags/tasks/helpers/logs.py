import logging
import sys
from pathlib import Path
from typing import Union


def _set_level(level: str) -> int:
    level = level.lower()
    levels_lookup = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    if level not in levels_lookup:
        raise ValueError("Invalid level")
    return levels_lookup.get(level)


def make_logger(
    name: str,
    level: str = "debug",
    path: Union[str, Path] = None,
    mode: str = "w",
    add_handler: bool = False,
) -> logging.Logger:
    level = _set_level(level)

    logger = logging.getLogger(name)

    for handler in logger.handlers:
        logger.removeHandler(handler)

    logger.setLevel(level)
    log_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)-5.5s/%(name)s] %(message)s"
    )

    if path is not None:
        path = Path(path)
        file_handler = logging.FileHandler(path, mode)
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

    if add_handler:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)

    return logger


def silence_loggers(names: list[str]) -> None:
    for name in names:
        l = logging.getLogger(name)
        l.setLevel(logging.WARNING)
