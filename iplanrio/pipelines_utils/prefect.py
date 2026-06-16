# -*- coding: utf-8 -*-
"""Utilitários para integração e gerenciamento de fluxos Prefect.

Fornece funções para renomeação de flow runs, geração de schedules para dumps
de bancos de dados, deleção em lote de execuções e criação de configurações
YAML para deployments com múltiplos schedules escalonados.
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Union

import yaml
from prefect import task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import DeploymentFilter, FlowFilter, FlowRunFilter
from prefect.client.schemas.sorting import FlowRunSort
from prefect.context import get_run_context
from prefect.schedules import Interval

from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.io import query_to_line
from iplanrio.pipelines_utils.logging import log


@task
def rename_current_flow_run_task(new_name: str):
    """Atualiza o nome da execução atual do fluxo Prefect.

    Args:
        new_name: Novo nome para a execução do fluxo.
    """

    # Pega o contexto da execução atual para obter o ID
    context = get_run_context()
    flow_run_id = context.task_run.flow_run_id
    log(f"Obtido o ID da execução do fluxo: {flow_run_id}")

    # Usa o cliente assíncrono do Prefect para interagir com a API
    # 1. Define uma função async interna para fazer o trabalho com o cliente
    async def _update_run_name():
        async with get_client() as client:
            await client.update_flow_run(flow_run_id=flow_run_id, name=new_name)

    asyncio.run(_update_run_name())

    log(f"Nome da execução do fluxo atualizado para {new_name}!")


def generate_dump_db_schedules(
    interval: timedelta,
    start_date: datetime,
    db_database: str,
    db_host: str,
    db_port: Union[str, int],
    db_type: str,
    dataset_id: str,
    infisical_secret_path: str,
    table_parameters: dict,
    biglake_table: bool = True,
    db_charset: str = NOT_SET,
    batch_size: int = 50000,
    runs_interval_minutes: int = 15,
    timezone: str = "America/Sao_Paulo",
) -> List[Interval]:
    """Gera múltiplos schedules Prefect para dump de tabelas de banco de dados.

    Cria schedules escalonados para evitar sobrecarga, com cada tabela
    iniciando em horários diferentes.

    Args:
        interval: Intervalo entre execuções.
        start_date: Data/hora de início do primeiro schedule.
        db_database: Nome do banco de dados.
        db_host: Host do banco de dados.
        db_port: Porta do banco de dados.
        db_type: Tipo do banco ("mysql", "sqlserver", "oracle", "postgres").
        dataset_id: ID do dataset de destino.
        infisical_secret_path: Caminho do secret com credenciais.
        table_parameters: Lista de dicionários com parâmetros por tabela.
        biglake_table: Se True, cria tabela BigLake.
        db_charset: Charset do banco de dados.
        batch_size: Tamanho do lote para extração.
        runs_interval_minutes: Minutos entre início de cada schedule.
        timezone: Timezone para os schedules.

    Returns:
        Lista de objetos Interval configurados.
    """
    other_parameters = {
        "retry_dump_upload_attempts": 1,
        "batch_data_type": "csv",
        "log_number_of_batches": 100,
        "break_query_frequency": None,
        "break_query_start": None,
        "break_query_end": None,
        "partition_columns": None,
        "partition_date_format": None,
        "partition_columns": None,
        "lower_bound_date": None,
        "break_query_frequency": None,
        "break_query_start": None,
        "break_query_end": None,
    }

    db_port = str(db_port)
    clocks = []
    for count, parameters in enumerate(table_parameters):
        parameter_defaults = {
            "batch_size": batch_size,
            "infisical_secret_path": infisical_secret_path,
            "db_database": db_database,
            "db_host": db_host,
            "db_port": db_port,
            "db_type": db_type,
            "dataset_id": dataset_id,
            "table_id": parameters["table_id"],
            "db_charset": db_charset,
            "biglake_table": biglake_table,
            "dump_mode": parameters["dump_mode"],
            "execute_query": query_to_line(parameters["execute_query"]),
        }

        # Add remaining parameters if value is not None
        for key, value in parameters.items():
            if value is not None and key not in ["interval", "start_date"]:
                parameter_defaults[key] = value

        if "dbt_alias" in parameters:
            parameter_defaults["dbt_alias"] = parameters["dbt_alias"]
        if "dataset_id" in parameters:
            parameter_defaults["dataset_id"] = parameters["dataset_id"]
        new_interval = parameters["interval"] if "interval" in parameters else interval
        new_start_date = (
            parameters["start_date"]
            if "start_date" in parameters
            else start_date + timedelta(minutes=runs_interval_minutes * count)
        )

        for key, value in other_parameters.items():
            if key not in parameters:
                parameter_defaults[key] = value

        clocks.append(
            Interval(
                new_interval,
                anchor_date=new_start_date,
                parameters=parameter_defaults,
                slug=parameters["table_id"],
                timezone=timezone,
            )
        )
    return clocks


async def delete_flow_run_batch(
    number_of_runs: int,
    flow_name: str = None,
    deployment_name: str = None,
    states: list[str] | None = None,
    concurrency_limit: int = 20,
) -> int:
    """Deleta execuções de fluxo Prefect em lote de forma assíncrona.

    Busca e deleta execuções de fluxo que correspondam aos filtros especificados,
    processando em lotes para evitar sobrecarga da API.

    Args:
        number_of_runs: Número máximo de execuções a deletar.
        flow_name: Nome do fluxo para filtrar (opcional).
        deployment_name: Nome do deployment para filtrar (opcional).
        states: Lista de estados para filtrar (ex: ["Failed", "Cancelled"]).
        concurrency_limit: Número máximo de deleções simultâneas.

    Returns:
        Número total de execuções deletadas.

    Note:
        Estados possíveis: "Scheduled", "Late", "AwaitingRetry", "Pending",
        "Running", "Retrying", "Paused", "Cancelling", "Cancelled",
        "Completed", "Cached", "RolledBack", "Failed", "Crashed"
    """
    API_FETCH_LIMIT = 200

    if not states:
        states = []

    states = [state.capitalize() for state in states]
    batches_int = int(number_of_runs / API_FETCH_LIMIT)
    total_estimated_batches = (
        batches_int if number_of_runs % API_FETCH_LIMIT == 0 else batches_int
    )

    total_deleted_count = 0
    batch_number = 0

    print(f"Iniciando processo para deletar até {number_of_runs} execuções de fluxo.")
    print(f"Filtros: flow_name='{flow_name}', states={states}")
    print(
        f"Estimativa: {total_estimated_batches} lotes de no máximo {API_FETCH_LIMIT} execuções cada."
    )
    total_time = 0
    async with get_client() as client:
        while total_deleted_count < number_of_runs:
            batch_number += (
                1  # Incrementa o contador do lote no início de cada iteração
            )
            runs_to_fetch = min(API_FETCH_LIMIT, number_of_runs - total_deleted_count)
            start_time = time.time()
            try:
                flow_runs_in_batch = await client.read_flow_runs(
                    flow_filter=(
                        FlowFilter(name={"any_": [flow_name]}) if flow_name else None
                    ),
                    deployment_filter=(
                        DeploymentFilter(name={"any_": [deployment_name]})
                        if deployment_name
                        else None
                    ),
                    flow_run_filter=FlowRunFilter(state={"name": {"any_": states}}),
                    sort=FlowRunSort.END_TIME_DESC,
                    limit=runs_to_fetch,
                )
            except Exception as e:
                print(f"Erro ao buscar lote da API: {e}. Interrompendo.")
                break

            if not flow_runs_in_batch:
                print(
                    "Nenhuma execução de fluxo adicional foi encontrada. O processo será finalizado."
                )
                break

            total_in_batch = len(flow_runs_in_batch)
            print(
                f"Lote: {batch_number}/{total_estimated_batches} with {total_in_batch} runs."
            )
            semaphore = asyncio.Semaphore(concurrency_limit)

            async def delete_run_with_semaphore(run):
                async with semaphore:
                    try:
                        await client.delete_flow_run(run.id)
                        return True
                    except Exception as e:
                        return e

            delete_tasks = [
                delete_run_with_semaphore(run) for run in flow_runs_in_batch
            ]
            results = await asyncio.gather(*delete_tasks)
            deleted_in_this_batch = sum(1 for r in results if r is True)
            # failures_in_this_batch = total_in_batch - deleted_in_this_batch
            total_deleted_count += deleted_in_this_batch
            batch_time = time.time() - start_time
            total_time += batch_time
            estimated_time_to_finish = str(
                timedelta(
                    seconds=(total_time / batch_number)
                    * (total_estimated_batches - batch_number)
                )
            )[:7]
            print(
                f"  Deleted: {total_deleted_count}/{number_of_runs} - {round(100 * total_deleted_count/number_of_runs, 2)}% | {round(batch_time, 2)}s / {estimated_time_to_finish}"
            )
            if total_in_batch < runs_to_fetch:
                print(
                    "Último lote de execuções disponível foi processado. O processo será finalizado."
                )
                break

    print(f"\nOperação finalizada após processar {batch_number} lote(s).")
    print(f"Total de {total_deleted_count} execuções deletadas.")
    return total_deleted_count


def create_schedules(
    schedules_parameters: list,
    base_interval_seconds: int,
    base_anchor_date_str: str,
    runs_interval_minutes: int,
    timezone: str,
    slug_field: str = None,
):
    """Gera configuração YAML de schedules Prefect escalonados.

    Cria múltiplos schedules com horários de início defasados para evitar
    execuções simultâneas.

    Args:
        schedules_parameters: Lista de dicionários com parâmetros por schedule.
        base_interval_seconds: Intervalo base entre execuções em segundos.
        base_anchor_date_str: Data/hora de início do primeiro schedule (ISO format).
        runs_interval_minutes: Minutos de defasagem entre schedules.
        timezone: Timezone IANA (ex: "America/Sao_Paulo").
        slug_field: Campo dos parâmetros a usar como slug (opcional).

    Returns:
        String YAML com configuração de schedules.
    """
    base_anchor_date = datetime.fromisoformat(base_anchor_date_str)
    schedules = []

    for i, table_params in enumerate(schedules_parameters):
        # Calculate the staggered anchor date for this schedule
        anchor_date = base_anchor_date + timedelta(minutes=runs_interval_minutes * i)
        flow_run_parameters = {}
        for key, value in table_params.items():
            flow_run_parameters[key] = value
            # Create the final schedule object for the YAML

        schedule_config = {
            "interval": base_interval_seconds,
            "anchor_date": anchor_date.isoformat(),
            "timezone": timezone,
        }

        if slug_field:
            schedule_config["slug"] = flow_run_parameters[slug_field]
        schedule_config["parameters"] = flow_run_parameters

        schedules.append(schedule_config)
    # Assemble the final deployment structure
    return yaml.dump(
        {
            "schedules": schedules,
        },
        sort_keys=False,
        indent=2,
        width=120,
    )


def create_dump_db_schedules(
    table_parameters_list: list,
    base_interval_seconds: int,
    base_anchor_date_str: str,
    runs_interval_minutes: int,
    timezone: str,
):
    """Gera YAML de schedules Prefect específico para dumps de banco de dados.

    Processa queries SQL para linha única e cria schedules escalonados
    por table_id.

    Args:
        table_parameters_list: Lista de dicionários com parâmetros de tabelas.
        base_interval_seconds: Intervalo base entre execuções em segundos.
        base_anchor_date_str: Data/hora de início (ISO format).
        runs_interval_minutes: Minutos de defasagem entre schedules.
        timezone: Timezone IANA.

    Returns:
        String YAML com configuração de schedules.

    Raises:
        ValueError: Se algum parâmetro não contiver 'table_id'.
    """
    base_anchor_date = datetime.fromisoformat(base_anchor_date_str)
    schedules = []

    for i, table_params in enumerate(table_parameters_list):
        # Calculate the staggered anchor date for this schedule
        anchor_date = base_anchor_date + timedelta(minutes=runs_interval_minutes * i)

        # Start with a base set of parameters for the flow run
        flow_run_parameters = {}

        # Merge the specific parameters for this table
        # This includes table_id, execute_query, dump_mode, etc.
        for key, value in table_params.items():
            if key == "execute_query":
                flow_run_parameters[key] = query_to_line(value).strip()
            else:
                flow_run_parameters[key] = value

        # Ensure required parameters from the list are set
        if "table_id" not in flow_run_parameters:
            raise ValueError(f"Missing 'table_id' in table parameters at index {i}")

        # Create the final schedule object for the YAML
        schedule_config = {
            "interval": base_interval_seconds,
            "anchor_date": anchor_date.isoformat(),
            "timezone": timezone,
            "slug": flow_run_parameters["table_id"],
            "parameters": flow_run_parameters,
        }
        schedules.append(schedule_config)

    # Assemble the final deployment structure
    return yaml.dump(
        {
            "schedules": schedules,
        },
        sort_keys=False,
        indent=2,
        width=120,
    )
