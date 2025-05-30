# -*- coding: utf-8 -*-
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Union

from prefect import get_client
from prefect.client.schemas.filters import DeploymentFilter, FlowFilter, FlowRunFilter
from prefect.client.schemas.sorting import FlowRunSort
from prefect.schedules import Interval

from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.io import query_to_line


def generate_dump_db_schedules(  # pylint: disable=too-many-arguments,too-many-locals
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
    """
    Generates multiple schedules for database dumping.
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
    batch_size: int,
    flow_name: str = None,
    deployment_name: str = None,
    states: list[str] | None = None,
) -> int:
    """
    Busca até 'batch_size' execuções de fluxo que correspondam aos estados
    e as deleta. Retorna o número de execuções realmente deletadas neste batch.

    Possible states:
        "Scheduled",
        "Late",
        "AwaitingRetry",
        "Pending",
        "Running",
        "Retrying",
        "Paused",
        "Cancelling",
        "Cancelled",
        "Completed",
        "Cached",
        "RolledBack",
        "Failed",
        "Crashed"

    """
    # capitalize states
    states = [state.capitalize() for state in states]

    async with get_client() as client:
        print(f"  Buscando até {batch_size} execuções de fluxo para deleção...")
        flow_runs = await client.read_flow_runs(
            flow_filter=FlowFilter(name={"any_": [flow_name]}) if flow_name else None,
            deployment_filter=(
                DeploymentFilter(name={"any_": [deployment_name]})
                if deployment_name
                else None
            ),
            flow_run_filter=FlowRunFilter(state={"name": {"any_": states}}),
            sort=FlowRunSort.END_TIME_DESC,
            limit=batch_size,
        )
        if not flow_runs:
            print(
                "  Nenhuma execução de fluxo encontrada neste batch que corresponda aos critérios."
            )
            return 0

        print(f"  Encontradas {len(flow_runs)} execuções para deletar neste batch.")
        deleted_count_in_batch = 0
        for i, run in enumerate(flow_runs):
            print(
                f"    Deletando run {i+1}/{len(flow_runs)} (ID: {run.id}, Estado: {run.state_name})..."
            )
            try:
                await client.delete_flow_run(flow_run_id=run.id)
                deleted_count_in_batch += 1
            except Exception as e:
                print(f"    Erro ao deletar a execução de fluxo {run.id}: {e}")
                # Decide if you want to continue or break on error

        return deleted_count_in_batch


def delete_all_eligible_flow_runs(
    batch_size: int,
    states: list[str],
    flow_name: str = None,
    deployment_name: str = None,
):
    """
    Deleta todas as execuções de fluxo que correspondem aos estados especificados
    em batches, sem precisar de um número total inicial.

    Possible states:
        "Scheduled",
        "Late",
        "AwaitingRetry",
        "Pending",
        "Running",
        "Retrying",
        "Paused",
        "Cancelling",
        "Cancelled",
        "Completed",
        "Cached",
        "RolledBack",
        "Failed",
        "Crashed"


    """
    total_deleted_overall = 0
    batch_number = 0

    print(
        f"Iniciando a deleção de execuções de fluxo com estados: {states} em batches de {batch_size}."
    )

    while True:
        batch_number += 1
        print(f"\n--- Processando batch {batch_number} ---")

        deleted_in_current_batch = asyncio.run(
            delete_flow_run_batch(
                batch_size=batch_size,
                states=states,
                flow_name=flow_name,
                deployment_name=deployment_name,
            )
        )
        total_deleted_overall += deleted_in_current_batch

        if deleted_in_current_batch == 0:
            print(
                f"\nNenhuma outra execução de fluxo elegível encontrada. Total deletado: {total_deleted_overall}"
            )
            break

        if deleted_in_current_batch < batch_size:
            print(
                f"\nMenos de {batch_size} execuções foram encontradas neste batch, indicando que todas as restantes foram processadas."
            )
            print(f"Total deletado no geral: {total_deleted_overall}")
            break

        print(
            f"Batch {batch_number} concluído. Total deletado até agora: {total_deleted_overall}. Pausando por 1 segundo..."
        )
        time.sleep(1)
