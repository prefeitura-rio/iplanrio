# -*- coding: utf-8 -*-
"""Utilitários para integração com DBT (Data Build Tool) via Prefect.

Fornece funções para download de repositórios Git contendo projetos DBT
e execução de comandos DBT (run, test, build, source freshness, etc.)
com suporte a targets, seleção de modelos e flags customizados.
"""
import os
import shutil

import git
from prefect import task
from prefect_dbt import PrefectDbtRunner

from iplanrio.pipelines_utils.logging import log


def download_repository(git_repository_path: str) -> str:
    """Clona repositório Git contendo projeto DBT.

    Remove diretório existente se houver, clona o repositório e verifica
    presença de pasta 'queries'.

    Args:
        git_repository_path: URL do repositório Git.

    Returns:
        Caminho do diretório 'queries' se existir, senão caminho raiz do repositório.

    Raises:
        ValueError: Se git_repository_path não for fornecido.
        Exception: Se houver erro ao criar diretório ou clonar repositório.
    """
    if not git_repository_path:
        raise ValueError("git_repository_path is required")

    # Create repository folder
    try:
        repository_path = os.path.join(os.getcwd(), "dbt_repository")

        if os.path.exists(repository_path):
            shutil.rmtree(repository_path, ignore_errors=False)
        os.makedirs(repository_path)

        log(f"Repository folder created: {repository_path}", level="info")

    except Exception as e:
        raise Exception(f"Error when creating repository folder: {e}")

    # Download repository
    try:
        git.Repo.clone_from(git_repository_path, repository_path)
        log(f"Repository downloaded: {git_repository_path}", level="info")
    except git.GitCommandError as e:
        raise Exception(f"Error when downloading repository: {e}")

    # check for 'queries' folder
    queries_path = os.path.join(repository_path, "queries")
    if os.path.isdir(queries_path):
        log(f"'queries' folder found at: {queries_path}", level="info")
        return queries_path

    return repository_path


@task
def execute_dbt_task(
    command: str = "run",
    target: str = "dev",
    select: str = "",
    exclude: str = "",
    state: str = "",
    flag: str = "",
    git_repository_path: str = "https://github.com/prefeitura-rio/queries-rj-iplanrio",
):
    """Executa comando DBT via Prefect usando PrefectDbtRunner.

    Baixa o repositório, instala dependências DBT e executa o comando especificado
    com os argumentos fornecidos.

    Args:
        command: Comando DBT a executar ("run", "test", "build", "source freshness", etc.).
        target: Ambiente de destino DBT ("dev", "prod", etc.).
        select: Argumento select do DBT para filtrar models.
        exclude: Argumento exclude do DBT para filtrar models.
        state: Argumento state do DBT para processamento incremental.
        flag: Flags adicionais do DBT.
        git_repository_path: URL do repositório Git com projeto DBT.

    Returns:
        Resultado da execução do comando DBT.

    Raises:
        Exception: Se houver erro ao instalar dependências ou executar comando.
    """
    _ = download_repository(git_repository_path=git_repository_path)

    # Build the command arguments
    if command == "source freshness":
        command_args = ["source", "freshness"]
    else:
        command_args = [command]

    # Add common arguments for most DBT commands
    if command in ("build", "run", "test", "source freshness", "seed", "snapshot"):
        command_args.extend(["--target", target])

        if select:
            command_args.extend(["--select", select])
        if exclude:
            command_args.extend(["--exclude", exclude])
        if state:
            command_args.extend(["--state", state])
        if flag:
            command_args.extend([flag])

    log(f"Executing dbt command: {' '.join(command_args)}", level="info")

    # Initialize PrefectDbtRunner
    runner = PrefectDbtRunner(
        raise_on_failure=False  # Allow the flow to handle failures gracefully
    )
    # Execute the dbt deps command
    try:
        deps_result = runner.invoke(["deps"])
        log("✅ DBT dependencies installed successfully", level="info")
        log(msg=str(deps_result))
    except Exception as e:
        log(f"❌ Error installing DBT dependencies: {e}", level="error")
        raise

    # Execute the dbt command with the constructed arguments
    try:
        running_result = runner.invoke(command_args)
        log(
            f"DBT command completed with success: {running_result.success}",
            level="info",
        )
    except Exception as e:
        log(f"Error executing DBT command: {e}", level="error")
        raise

    log(msg=str(running_result))
