# -*- coding: utf-8 -*-
from typing import Optional

from prefect import flow

from iplanrio.pipelines_templates.dump_db.tasks import (
    dump_upload_batch_task,
    format_partitioned_query_task,
    get_database_username_and_password_from_secret_task,
    inject_bd_credentials_task,
    parse_comma_separated_string_to_list_task,
)
from iplanrio.pipelines_utils.constants import NOT_SET


@flow(log_prints=True)
def dump_db(
    db_database: str = "db_database",
    db_host: str = "db_host",
    db_port: str = "db_port",
    db_type: str = "db_type",
    execute_query: str = "execute_query",
    dataset_id: str = "dataset_id",
    table_id: str = "table_id",
    infisical_secret_path: str = "infisical_secret_path",
    dump_mode: str = "overwrite",
    partition_date_format: str = "%Y-%m-%d",
    partition_columns: Optional[str] = None,
    lower_bound_date: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    db_charset: str = NOT_SET,
    retry_dump_upload_attempts: int = 1,
    batch_size: int = 50000,
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
):
    crd = inject_bd_credentials_task(environment="prod")  # noqa
    secrets = get_database_username_and_password_from_secret_task(
        infisical_secret_path=infisical_secret_path
    )
    partition_columns = parse_comma_separated_string_to_list_task(
        text=partition_columns
    )

    formated_query = format_partitioned_query_task(
        query=execute_query,
        dataset_id=dataset_id,
        table_id=table_id,
        database_type=db_type,
        partition_columns=partition_columns,
        lower_bound_date=lower_bound_date,
        date_format=partition_date_format,
        break_query_start=break_query_start,
        break_query_end=break_query_end,
        break_query_frequency=break_query_frequency,
    )
    dump_upload = dump_upload_batch_task(  # noqa
        queries=formated_query,
        batch_size=batch_size,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        partition_columns=partition_columns,
        batch_data_type=batch_data_type,
        biglake_table=biglake_table,
        log_number_of_batches=log_number_of_batches,
        retry_dump_upload_attempts=retry_dump_upload_attempts,
        database_type=db_type,
        hostname=db_host,
        port=db_port,
        user=secrets["DB_USERNAME"],
        password=secrets["DB_PASSWORD"],
        database=db_database,
        charset=db_charset,
    )
