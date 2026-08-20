# -*- coding: utf-8 -*-
"""Utilitários para gerenciamento de variáveis de ambiente e credenciais.

Fornece funções para obtenção de credenciais do Google Cloud, Base dos Dados,
bancos de dados e injeção de service accounts. Inclui validação e tratamento
de erros para variáveis de ambiente ausentes.
"""
import base64
import json
from os import environ, getenv
from typing import List, Optional, Union

from google.oauth2 import service_account
from prefect import task

from iplanrio.pipelines_utils.logging import log


def getenv_or_action(
    key: str, default: Optional[str] = None, action: str = "raise"
) -> Union[str, None]:
    """Obtém variável de ambiente com tratamento de erro configurável.

    Args:
        key: Nome da variável de ambiente.
        default: Valor padrão se variável não existir.
        action: Ação ao não encontrar variável ("raise", "warn" ou "ignore").

    Returns:
        Valor da variável de ambiente ou valor padrão.

    Raises:
        ValueError: Se action for "raise" e variável não for encontrada.
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
            log(f"Environment variable '{key}' not found.")
    return value


def get_database_username_and_password_from_secret_env(
    secret_path: str,
) -> dict:
    """Obtém credenciais de banco de dados a partir de variáveis de ambiente.

    Converte o caminho do secret para formato de variável de ambiente e
    busca DB_USERNAME e DB_PASSWORD.

    Args:
        secret_path: Caminho do secret (será convertido para formato de env var).

    Returns:
        Dicionário com chaves DB_USERNAME e DB_PASSWORD.
    """
    secret_path = secret_path.upper().replace("-", "_").replace("/", "")
    return {
        "DB_USERNAME": getenv_or_action(f"{secret_path}__DB_USERNAME"),
        "DB_PASSWORD": getenv_or_action(f"{secret_path}__DB_PASSWORD"),
    }


def validate_bd_credentials():
    """Valida presença de credenciais Base dos Dados nas variáveis de ambiente.

    Raises:
        ValueError: Se alguma credencial necessária não for encontrada.
    """
    creds_name = [
        "BASEDOSDADOS_CREDENTIALS_PROD",
        "BASEDOSDADOS_CREDENTIALS_STAGING",
        "BASEDOSDADOS_CONFIG",
    ]

    for cred_name in creds_name:
        _ = getenv_or_action(cred_name, action="raise")


def get_bd_credentials_from_env(
    mode: str, scopes: Optional[List[str]] = None
) -> service_account.Credentials:
    """Obtém credenciais Google Cloud a partir de variáveis de ambiente.

    Decodifica credenciais Base64 e cria objeto Credentials do Google.

    Args:
        mode: Modo do ambiente ("prod" ou "staging").
        scopes: Escopos de acesso (padrão: drive e cloud-platform).

    Returns:
        Objeto Credentials do Google Cloud autenticado.

    Raises:
        ValueError: Se mode inválido ou credenciais não encontradas.
    """
    validate_bd_credentials()
    if mode not in ["prod", "staging"]:
        raise ValueError("Mode must be 'prod' or 'staging'")
    env: str = getenv(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()}", "")
    if env == "":
        raise ValueError(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()} env var not set!")
    info: dict = json.loads(base64.b64decode(env))
    cred: service_account.Credentials = (
        service_account.Credentials.from_service_account_info(info)
    )
    if not scopes:
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform",
        ]
    cred = cred.with_scopes(scopes)
    return cred


def inject_bd_credentials(environment: str = "prod"):
    """Injeta credenciais Base dos Dados como arquivo temporário.

    Decodifica credenciais Base64, salva em /tmp/credentials.json e
    configura GOOGLE_APPLICATION_CREDENTIALS.

    Args:
        environment: Ambiente das credenciais ("prod" ou "staging").

    Raises:
        ValueError: Se credenciais não forem encontradas.
    """
    validate_bd_credentials()
    service_account_name = f"BASEDOSDADOS_CREDENTIALS_{environment.upper()}"
    service_account_b64 = getenv_or_action(service_account_name)
    if service_account_b64:
        service_account = base64.b64decode(service_account_b64)
    else:
        raise ValueError(f"{service_account_name} env var not set!")

    with open("/tmp/credentials.json", "wb") as credentials_file:
        credentials_file.write(service_account)
    environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"
    log(f"INJECTED: {service_account_name}")


@task
def inject_bd_credentials_task(environment: str = "prod"):
    inject_bd_credentials(environment=environment)


def get_database_username_and_password_from_secret(infisical_secret_path: str):
    """Wrapper para obter credenciais de banco via secret path.

    Args:
        infisical_secret_path: Caminho do secret no Infisical.

    Returns:
        Dicionário com DB_USERNAME e DB_PASSWORD.
    """
    return get_database_username_and_password_from_secret_env(
        secret_path=infisical_secret_path
    )


def get_credentials_from_env(
    mode: str = "prod", scopes: List[str] = None
) -> service_account.Credentials:
    """Obtém credenciais Google Cloud com escopos opcionais.

    Similar a get_bd_credentials_from_env, mas aplica escopos apenas se fornecidos.

    Args:
        mode: Modo do ambiente ("prod" ou "staging").
        scopes: Escopos de acesso opcionais.

    Returns:
        Objeto Credentials do Google Cloud.

    Raises:
        ValueError: Se mode inválido ou credenciais não encontradas.
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
