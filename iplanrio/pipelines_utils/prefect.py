from datetime import datetime, timedelta
from typing import List, Union

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
