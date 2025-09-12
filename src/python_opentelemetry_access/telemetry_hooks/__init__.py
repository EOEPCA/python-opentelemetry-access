from functools import cache
from inspect import isfunction
import logging
import pathlib
from typing import Callable
import os
from plugin_utils.loader import (
    load_plugins,
    convert_file_based_hooks_to_name_based_hooks,
)

logger = logging.getLogger("OTEL_ACCESS")

Hooks = list[Callable]


@cache
def load_hooks(
    hooks_dir: pathlib.Path | str | None = None,
) -> dict[str, Hooks]:
    """
    Each hook might have multiple functions. The files with earlier alphanumeric names
    will have their hooks called earlier.
    """
    hooks_dir = hooks_dir or os.environ.get("RH_TELEMETRY_HOOK_DIR_PATH")

    if hooks_dir is None:
        logger.warning("No hook path specified")
        return {}

    file_to_hooks: dict[str, dict[str, Callable]] = load_plugins(
        pathlib.Path(hooks_dir),
        value=(lambda x: x if isfunction(x) else None),
        logger=logger,
        perfile=True,
    )
    return convert_file_based_hooks_to_name_based_hooks(file_to_hooks)
