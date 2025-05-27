# -*- coding: utf-8 -*-
from os import getenv
from typing import Union

from iplanrio.pipelines_utils.logging import log


def getenv_or_action(
    key: str, default: str = None, action: str = "raise"
) -> Union[str, None]:
    """
    Gets an environment variable or executes an action.

    Args:
        key (str): The environment variable key.
        default (str, optional): The default value. Defaults to `None`.
        action (str, optional): The action to execute. Must be one of `'raise'`,
            `'warn'` or `'ignore'`. Defaults to `'raise'`.

    Returns:
        Union[str, None]: The environment variable value or the default value.
    """
    if action not in ("raise", "warn", "ignore"):
        raise ValueError(
            f"Invalid action '{action}'. Must be one of 'raise', 'warn' or 'ignore'."
        )
    value = getenv(key, default)
    if value is None:
        if action == "raise":
            raise ValueError(f"Environment variable '{key}' not found.")
        elif action == "warn":
            log.warning(f"Environment variable '{key}' not found.")
    return value
