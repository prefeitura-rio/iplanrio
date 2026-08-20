# -*- coding: utf-8 -*-
from typing import Optional

from prefect import flow

from iplanrio.pipelines_templates.dump_db.tasks import (
    dump_upload_batch_task,
    format_partitioned_query_task,
    get_database_username_and_password_from_secret_task,
    parse_comma_separated_string_to_list_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task


@flow(log_prints=True)
def rj_segovi_dump_db_1746(
    db_database: str = "db_database",
    db_host: str = "db_host",
    db_port: str = "db_port",
    db_type: str = "db_type",
    db_charset: Optional[str] = "NOT_SET",
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
    retry_dump_upload_attempts: int = 3,
    batch_size: int = 50000,
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    max_concurrency: int = 1,
    only_staging_dataset: bool = False,
):
    """
    Flow Prefect para dump incremental de banco de dados para BigQuery.

    Orquestra a extração de dados de bancos relacionais (MySQL, Oracle, Postgres,
    SQL Server) para BigQuery com suporte a particionamento incremental, processamento
    paralelo e retry automático.

    Args:
        db_database: Nome do banco de dados.
        db_host: Hostname do servidor do banco.
        db_port: Porta de conexão.
        db_type: Tipo do banco ('mysql', 'oracle', 'postgres', 'sql_server').
        db_charset: Charset da conexão (default: 'NOT_SET').
        execute_query: Query SQL a ser executada.
        dataset_id: ID do dataset no BigQuery.
        table_id: ID da tabela no BigQuery.
        infisical_secret_path: Caminho do secret no Infisical com credenciais.
        dump_mode: Modo de escrita - 'overwrite' ou 'append' (default: 'overwrite').
        partition_date_format: Formato da coluna de partição (default: '%Y-%m-%d').
        partition_columns: Colunas de particionamento separadas por vírgula (default: None).
        lower_bound_date: Data mínima ou alias ('current_day', 'previous_month', etc).
        break_query_frequency: Frequência para quebra em chunks ('day', 'month', etc).
        break_query_start: Data inicial para quebra em chunks.
        break_query_end: Data final para quebra em chunks.
        retry_dump_upload_attempts: Tentativas de retry em caso de falha (default: 3).
        batch_size: Registros por lote (default: 50000).
        batch_data_type: Formato dos arquivos - 'csv' ou 'parquet' (default: 'csv').
        biglake_table: Se True, cria tabela BigLake (default: True).
        log_number_of_batches: Intervalo de batches para logging (default: 100).
        max_concurrency: Queries simultâneas (default: 1).
        only_staging_dataset: Se True, remove dataset de produção (default: False).
    """
    rename_current_flow_run_task(new_name=table_id)
    inject_bd_credentials_task(environment="prod")  # noqa
    secrets = get_database_username_and_password_from_secret_task(
        infisical_secret_path=infisical_secret_path
    )
    partition_columns_list = parse_comma_separated_string_to_list_task(
        text=partition_columns
    )

    formated_query = format_partitioned_query_task(
        query=execute_query,
        dataset_id=dataset_id,
        table_id=table_id,
        database_type=db_type,
        partition_columns=partition_columns_list,
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
        partition_columns=partition_columns_list,
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
        max_concurrency=max_concurrency,
        only_staging_dataset=only_staging_dataset,
    )
