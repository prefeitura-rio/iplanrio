# -*- coding: utf-8 -*-
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Union

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import DeploymentFilter, FlowFilter, FlowRunFilter
from prefect.client.schemas.sorting import FlowRunSort
from prefect.schedules import Interval

from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.io import query_to_line
import yaml


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
        response = await client.hello()
        print(response.json())

        # flow_runs = await client.read_flow_runs(
        #     flow_filter=FlowFilter(name={"any_": [flow_name]}) if flow_name else None,
        #     deployment_filter=(
        #         DeploymentFilter(name={"any_": [deployment_name]})
        #         if deployment_name
        #         else None
        #     ),
        #     flow_run_filter=FlowRunFilter(state={"name": {"any_": states}}),
        #     sort=FlowRunSort.END_TIME_DESC,
        #     limit=batch_size,
        # )
        # if not flow_runs:
        #     print(
        #         "  Nenhuma execução de fluxo encontrada neste batch que corresponda aos critérios."
        #     )
        #     return 0

        # print(f"  Encontradas {len(flow_runs)} execuções para deletar neste batch.")
        # deleted_count_in_batch = 0
        # for i, run in enumerate(flow_runs):
        #     print(
        #         f"    Deletando run {i+1}/{len(flow_runs)} (ID: {run.id}, Estado: {run.state_name})..."
        #     )
        #     try:
        #         await client.delete_flow_run(flow_run_id=run.id)
        #         deleted_count_in_batch += 1
        #     except Exception as e:
        #         print(f"    Erro ao deletar a execução de fluxo {run.id}: {e}")
        #         # Decide if you want to continue or break on error

        # return deleted_count_in_batch


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


def create_schedules(
    schedules_parameters: list,
    base_interval_seconds: int,
    base_anchor_date_str: str,
    runs_interval_minutes: int,
    timezone: str,
    slug_field: str = None,
):
    """
    Generates a full Prefect deployment YAML.

        Args:
            schedules_parameters (list): A list of dictionaries, each defining a table to dump.
            deployment_name (str): The name for the Prefect deployment.
            entrypoint (str): The entrypoint for the flow (e.g., 'path/to/flow.py:flow_name').
            base_interval_seconds (int): The base interval for schedules.
            base_anchor_date_str (str): The anchor date for the first schedule.
            runs_interval_minutes (int): The number of minutes to wait between starting each schedule.
            timezone (str): The IANA timezone for all schedules.
            work_pool_name (str): The name of the work pool.
            work_queue_name (str): The name of the work queue.
            job_image (str): The Docker image for the job.
            job_command (str): The command to execute the flow run.

        Returns:
            dict: A dictionary representing the complete Prefect deployment YAML.
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
    db_type: str,
    db_database: str,
    db_host: str,
    db_port: int,
    dataset_id: str,
    infisical_secret_path: str,
    default_biglake_table: bool = True,
    default_batch_size: int = 50000,
):
    """
    Generates a full Prefect deployment YAML for database dump tasks.

    Args:
        deployment_name (str): The name for the Prefect deployment.
        entrypoint (str): The entrypoint for the flow (e.g., 'path/to/flow.py:flow_name').
        table_parameters_list (list): A list of dictionaries, each defining a table to dump.
        base_interval_seconds (int): The base interval for schedules.
        base_anchor_date_str (str): The anchor date for the first schedule.
        runs_interval_minutes (int): The number of minutes to wait between starting each schedule.
        timezone (str): The IANA timezone for all schedules.
        db_type (str): The database type (e.g., 'oracle').
        db_database (str): The name of the database.
        db_host (str): The database host.
        db_port (int): The database port.
        dataset_id (str): The default dataset ID for the dumps.
        infisical_secret_path (str): The path to secrets in Infisical.
        work_pool_name (str): The name of the work pool.
        work_queue_name (str): The name of the work queue.
        job_image (str): The Docker image for the job.
        job_command (str): The command to execute the flow run.
        default_biglake_table (bool): The default value for 'biglake_table'.
        default_batch_size (int): The default value for 'batch_size'.

    Returns:
        dict: A dictionary representing the complete Prefect deployment YAML.
    """
    base_anchor_date = datetime.fromisoformat(base_anchor_date_str)
    schedules = []

    for i, table_params in enumerate(table_parameters_list):
        # Calculate the staggered anchor date for this schedule
        anchor_date = base_anchor_date + timedelta(minutes=runs_interval_minutes * i)

        # Start with a base set of parameters for the flow run
        flow_run_parameters = {
            "db_type": db_type,
            "db_database": db_database,
            "db_host": db_host,
            "db_port": str(db_port),
            "dataset_id": table_params.get("dataset_id", dataset_id),
            "infisical_secret_path": infisical_secret_path,
            "biglake_table": default_biglake_table,
            "batch_size": default_batch_size,
        }

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
