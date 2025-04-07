from functools import cache
from inspect import isfunction
import logging
import pathlib
from typing import Callable, Any
import os
from plugin_utils.loader import load_plugins
import inspect

logger = logging.getLogger("OTEL_ACCESS")

Hook = Callable[..., Any]


@cache
def load_hooks(hooks_dir: pathlib.Path | str | None = None) -> dict[str, Hook]:
    hooks_dir = hooks_dir or os.environ.get("RH_TELEMETRY_HOOK_DIR_PATH")

    if hooks_dir is None:
        logger.warn("No hook path specified")
        return {}

    return load_plugins(
        pathlib.Path(hooks_dir),
        value=(lambda x: x if isfunction(x) else None),
        logger=logger,
        perfile=False,
    )


async def run_hook_async(hook_to_run: Hook, *args: Any, **kvargs: Any) -> Any:
    x = hook_to_run(*args, **kvargs)

    if inspect.isawaitable(x):
        return await x

    return x
