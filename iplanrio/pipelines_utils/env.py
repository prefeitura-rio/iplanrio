# -*- coding: utf-8 -*-
import base64
import json
from os import getenv
from typing import List, Union

from google.oauth2 import service_account

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


def get_database_username_and_password_from_secret_env(path: str = None) -> dict:
    return {
        "DB_USERNAME": getenv_or_action(f"{path}.DB_USERNAME"),
        "DB_PASSWORD": getenv_or_action(f"{path}.DB_PASSWORD"),
    }


def get_bd_credentials_from_env(
    mode: str = None, scopes: List[str] = None
) -> service_account.Credentials:
    """
    Gets credentials from env vars
    """

    if mode not in ["prod", "staging"]:
        raise ValueError("Mode must be 'prod' or 'staging'")
    env: str = getenv(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()}", "")
    if env == "":
        raise ValueError(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()} env var not set!")
    info: dict = json.loads(base64.b64decode(env))
    cred: service_account.Credentials = (
        service_account.Credentials.from_service_account_info(info)
    )
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred
