# -*- coding: utf-8 -*-
import asyncio
import shutil
import traceback
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4
from dateutil.relativedelta import relativedelta
import basedosdados as bd
import pytz
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from iplanrio.pipelines_utils.bd import _delete_prod_dataset, get_storage_blobs
from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.database_sql import (
    Database,
    MySql,
    Oracle,
    Postgres,
    SqlServer,
)
from iplanrio.pipelines_utils.gcs import (
    delete_blobs_list,
    list_blobs_with_prefix,
    parse_blobs_to_partition_dict,
)
from iplanrio.pipelines_utils.io import (
    extract_last_partition_date,
    remove_tabs_from_query,
)
from iplanrio.pipelines_utils.logging import log, log_mod
from iplanrio.pipelines_utils.pandas import (
    add_ingestion_timestamp,
    batch_to_dataframe,
    build_query_new_columns,
    clean_dataframe,
    dataframe_to_csv,
    dataframe_to_parquet,
    dump_header_to_file,
    parse_date_columns,
    remove_columns_accents,
    to_partitions,
)


def parse_comma_separated_string_to_list(text: Optional[str]) -> List[str]:
    """
    Converte string separada por vírgulas em lista de strings limpa.

    Remove caracteres especiais (newlines, tabs, carriage returns), vírgulas duplicadas
    e strings vazias. Útil para parsing de parâmetros de configuração.

    Args:
        text: String com valores separados por vírgula. Pode ser None.

    Returns:
        Lista de strings sem espaços extras e sem valores vazios.
        Retorna lista vazia se text for None ou vazio.

    Examples:
        >>> parse_comma_separated_string_to_list("col1, col2,  col3")
        ['col1', 'col2', 'col3']
        >>> parse_comma_separated_string_to_list("data_particao,,")
        ['data_particao']
        >>> parse_comma_separated_string_to_list(None)
        []
    """
    if text is None or not text:
        return []
    # Remove extras.
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\t", "")
    while ",," in text:
        text = text.replace(",,", ",")
    while text.endswith(","):
        text = text[:-1]
    result = [x.strip() for x in text.split(",")]
    result = [item for item in result if item != "" and item is not None]
    return result


def database_get_db(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    charset: str = NOT_SET,
) -> Database:
    """
    Factory para criação de objetos de conexão com banco de dados.

    Retorna instância apropriada da classe Database baseada no tipo especificado.
    Suporta MySQL, Oracle, PostgreSQL e SQL Server.

    Args:
        database_type: Tipo do banco - 'mysql', 'oracle', 'postgres' ou 'sql_server'.
        hostname: Endereço do servidor (ex: 'localhost', '192.168.1.100').
        port: Porta de conexão (ex: 3306 para MySQL, 1521 para Oracle).
        user: Nome de usuário para autenticação.
        password: Senha para autenticação.
        database: Nome do banco de dados/schema.
        charset: Charset da conexão (opcional). Se NOT_SET, usa charset padrão.

    Returns:
        Instância de Database específica para o tipo solicitado
        (MySql, Oracle, Postgres ou SqlServer).

    Raises:
        ValueError: Se database_type não for um dos tipos suportados.

    Examples:
        >>> db = database_get_db(
        ...     database_type='mysql',
        ...     hostname='db.example.com',
        ...     port=3306,
        ...     user='etl_user',
        ...     password='secret',
        ...     database='production'
        ... )
    """

    DATABASE_MAPPING: Dict[str, type[Database]] = {
        "mysql": MySql,
        "oracle": Oracle,
        "postgres": Postgres,
        "sql_server": SqlServer,
    }

    if database_type not in DATABASE_MAPPING:
        raise ValueError(f"Unknown database type: {database_type}")
    return DATABASE_MAPPING[database_type](
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset if charset != NOT_SET else None,
    )


def database_execute(
    database,
    query: str,
) -> None:
    """
    Executa query SQL no banco de dados após limpeza.

    Remove tabs e executa a query no objeto de banco fornecido. Registra a query
    executada nos logs para rastreabilidade.

    Args:
        database: Objeto Database já conectado (MySql, Oracle, Postgres ou SqlServer).
        query: Query SQL a ser executada. Tabs serão removidos automaticamente.

    Note:
        Esta função apenas executa a query, não retorna resultados.
        Para buscar dados, use database.fetch_batch() após a execução.

    Examples:
        >>> db = database_get_db('mysql', 'localhost', 3306, 'user', 'pass', 'db')
        >>> database_execute(db, 'SELECT * FROM users WHERE created_at > "2024-01-01"')
    """
    # log(f"Query parsed: {query}")
    query = remove_tabs_from_query(query)
    log(f"Executing query line: {query}")
    database.execute_query(query)


def _process_single_query(
    # Parâmetros de Conexão
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    charset: str,
    # Parâmetros de Batch e Tabela
    query: str,
    batch_size: int,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    partition_columns: List[str],
    batch_data_type: str,
    biglake_table: bool,
    log_number_of_batches: int,
    # Estado e Informações
    cleared_partitions: Set[str],
    cleared_table: bool,
    log_prefix: str,
    only_staging_dataset: bool = False,
    add_timestamp_column: bool = False,
) -> Tuple[Set[str], bool, int, int]:
    """
    Processa uma única query, extrai dados em lotes e faz upload para BigQuery.

    Conecta ao banco de dados, executa a query, processa os dados em batches,
    aplica transformações (remoção de acentos, particionamento) e envia para GCS/BigQuery.

    Args:
        database_type: Tipo do banco ('mysql', 'oracle', 'postgres', 'sql_server').
        hostname: Endereço do servidor do banco.
        port: Porta de conexão.
        user: Usuário do banco.
        password: Senha do banco.
        database: Nome do banco de dados.
        charset: Charset da conexão.
        query: Query SQL a ser executada.
        batch_size: Número de registros por lote.
        dataset_id: ID do dataset no BigQuery.
        table_id: ID da tabela no BigQuery.
        dump_mode: Modo de escrita ('append' ou 'overwrite').
        partition_columns: Colunas usadas para particionamento.
        batch_data_type: Formato dos arquivos ('csv' ou 'parquet').
        biglake_table: Se True, cria tabela BigLake.
        log_number_of_batches: Intervalo de batches para logging.
        cleared_partitions: Set de partições já limpas (acumulador).
        cleared_table: Flag indicando se a tabela foi limpa.
        log_prefix: Prefixo para mensagens de log.
        only_staging_dataset: Se True, remove dataset de produção.
        add_timestamp_column: Se True, adiciona coluna de timestamp de ingestão.

    Returns:
        Tupla com (partições limpas, flag de tabela limpa, número de batches, total de linhas).
    """
    # Keep track of cleared stuff
    prepath = f"data/{uuid4()}/"
    db_object = database_get_db(
        database_type=database_type,
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
    )

    database_execute(
        database=db_object,
        query=query,
    )

    # Get data columns
    columns = db_object.get_columns()
    log(f"{log_prefix}: Got columns: {columns}")

    new_query_cols = build_query_new_columns(table_columns=columns)
    log(f"{log_prefix}: New query columns without accents: {new_query_cols}")

    prepath = Path(prepath)

    if not partition_columns or partition_columns[0] == "":
        partition_column = None
    else:
        partition_column = partition_columns[0]

    if not partition_column:
        log(f"{log_prefix}: NO partition column specified! Writing unique files")
    else:
        log(
            f"{log_prefix}: Partition column: {partition_column} FOUND!! Write to partitioned files"
        )

    # Now loop until we have no more data.
    batch = db_object.fetch_batch(batch_size)
    idx = 0
    batchs_len = 0
    while len(batch) > 0:
        prepath.mkdir(parents=True, exist_ok=True)
        # Log progress each 100 batches.
        log_mod(
            msg=f"{log_prefix}: Dumping batch {idx+1} with size {len(batch)}",
            index=idx,
            mod=log_number_of_batches,
        )
        batchs_len += len(batch)

        # Dump batch to file.
        dataframe = batch_to_dataframe(batch=batch, columns=columns)
        old_columns = dataframe.columns.tolist()
        dataframe.columns = remove_columns_accents(dataframe)
        new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
        dataframe = clean_dataframe(dataframe)
        if add_timestamp_column:
            dataframe = add_ingestion_timestamp(dataframe)
        saved_files = []
        if partition_column:
            dataframe, date_partition_columns = parse_date_columns(
                dataframe, new_columns_dict[partition_column]
            )
            partitions = date_partition_columns + [
                new_columns_dict[col] for col in partition_columns[1:]
            ]
            saved_files = to_partitions(
                data=dataframe,
                partition_columns=partitions,
                savepath=prepath,
                data_type=batch_data_type,
                suffix=f"{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            )
        elif batch_data_type == "csv":
            fname = prepath / f"{uuid4()}.csv"
            dataframe_to_csv(dataframe, fname)
            saved_files = [fname]
        elif batch_data_type == "parquet":
            fname = prepath / f"{uuid4()}.parquet"
            dataframe_to_parquet(dataframe, fname)
            saved_files = [fname]
        else:
            raise ValueError(f"Unknown data type: {batch_data_type}")

        # Log progress each 100 batches.

        log_mod(
            msg=f"{log_prefix}: Batch generated {len(saved_files)} files. Will now upload.",
            index=idx,
            mod=log_number_of_batches,
        )

        # Upload files.
        tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
        table_staging = f"{tb.table_full_name['staging']}"
        st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
        storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
        storage_path_link = (
            f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
            f"/staging/{dataset_id}/{table_id}"
        )
        dataset_is_public = tb.client["bigquery_prod"].project == "datario"
        # If we have a partition column
        if partition_column:
            # Extract the partition from the filenames
            partitions = []
            for saved_file in saved_files:
                # Remove the prepath and filename. This is the partition.
                partition = str(saved_file).replace(str(prepath), "")
                partition = partition.replace(saved_file.name, "")
                # Strip slashes from beginning and end.
                partition = partition.strip("/")
                # Add to list.
                partitions.append(partition)
            # Remove duplicates.
            partitions = list(set(partitions))
            log_mod(
                msg=f"{log_prefix}: Got partitions: {partitions}",
                index=idx,
                mod=log_number_of_batches,
            )
            # Loop through partitions and delete files from GCS.
            blobs_to_delete = []
            for partition in partitions:
                if partition not in cleared_partitions:
                    blobs = list_blobs_with_prefix(
                        bucket_name=st.bucket_name,
                        prefix=f"staging/{dataset_id}/{table_id}/{partition}",
                        mode="staging",
                    )
                    blobs_to_delete.extend(blobs)
                cleared_partitions.add(partition)
            if blobs_to_delete:
                delete_blobs_list(bucket_name=st.bucket_name, blobs=blobs_to_delete)
                log_mod(
                    msg=f"{log_prefix}: Deleted {len(blobs_to_delete)} blobs from GCS: {blobs_to_delete}",  # noqa
                    index=idx,
                    mod=log_number_of_batches,
                )
        if dump_mode == "append":
            if tb.table_exists(mode="staging"):
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE APPEND: Table ALREADY EXISTS:"
                        + f"\n{table_staging}"
                        + f"\n{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
            else:
                # the header is needed to create a table when dosen't exist
                log_mod(
                    msg=f"{log_prefix}: MODE APPEND: Table DOESN'T EXISTS\nStart to CREATE HEADER file",  # noqa
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    msg=f"{log_prefix}: MODE APPEND: Created HEADER file:\n"
                    f"{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                    set_biglake_connection_permissions=False,
                )

                log_mod(
                    msg=(
                        f"{log_prefix}: MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                        + f"{table_staging}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )

                if not cleared_table:
                    st.delete_table(
                        mode="staging",
                        bucket_name=st.bucket_name,
                        not_found_ok=True,
                    )
                    log_mod(
                        msg=(
                            f"{log_prefix}: MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"  # noqa
                            + f"{storage_path}\n"
                            + f"{storage_path_link}"
                        ),
                        index=idx,
                        mod=log_number_of_batches,
                    )
                    cleared_table = True
        elif dump_mode == "overwrite":
            if tb.table_exists(mode="staging") and not cleared_table:
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
                st.delete_table(
                    mode="staging",
                    bucket_name=st.bucket_name,
                    not_found_ok=True,
                )
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
                # delete only staging table and let DBT overwrite the prod table
                tb.delete(mode="staging")
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                        + f"{table_staging}\n"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )

            if not cleared_table:
                # the header is needed to create a table when dosen't exist
                # in overwrite mode the header is always created
                st.delete_table(
                    mode="staging",
                    bucket_name=st.bucket_name,
                    not_found_ok=True,
                )
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )

                log_mod(
                    msg=f"{log_prefix}: MODE OVERWRITE: Table DOSEN'T EXISTS\nStart to CREATE HEADER file",  # noqa
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    f"{log_prefix}: MODE OVERWRITE: Created HEADER file:\n"
                    f"{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                    set_biglake_connection_permissions=False,
                )

                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Sucessfully CREATED TABLE\n"
                        + f"{table_staging}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )

                st.delete_table(
                    mode="staging",
                    bucket_name=st.bucket_name,
                    not_found_ok=True,
                )
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"  # noqa
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
                cleared_table = True

        if only_staging_dataset:
            _delete_prod_dataset(
                only_staging_dataset=only_staging_dataset,
                dataset_id=dataset_id,
            )

        log_mod(
            msg="STARTING UPLOAD TO GCS",
            index=idx,
            mod=log_number_of_batches,
        )
        if tb.table_exists(mode="staging"):
            # Upload them all at once
            tb.append(filepath=prepath, if_exists="replace")
            log_mod(
                msg=f"{log_prefix}: STEP UPLOAD: Sucessfully uploaded batch {idx +1} file with size {len(batch)} to Storage",
                index=idx,
                mod=log_number_of_batches,
            )
            for saved_file in saved_files:
                # Delete the files
                saved_file.unlink()
        else:
            log_mod(
                msg=f"{log_prefix}: STEP UPLOAD: Table does not exist in STAGING, need to create first",  # noqa
                index=idx,
                mod=log_number_of_batches,
            )
        # Get next batch.
        batch = db_object.fetch_batch(batch_size)
        idx += 1

        # delete batch data from prepath
        shutil.rmtree(prepath)
    log(msg=f"{log_prefix}: --- Batchs: {idx}, Rows: {batchs_len} ---")

    return cleared_partitions, cleared_table, idx, batchs_len


def dump_upload_batch(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    queries: List[dict],
    batch_size: int,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    charset: str = NOT_SET,
    partition_columns: List[str] = [],
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    retry_dump_upload_attempts: int = 3,
    max_concurrency: int = 1,  # Novo parâmetro para definir o limite do semáforo
    only_staging_dataset: bool = False,
    add_timestamp_column: bool = False
):
    """
    Executa múltiplas queries em paralelo com controle de concorrência e retry automático.

    Orquestra a extração de dados de bancos relacionais para BigQuery, processando
    múltiplas queries concorrentemente com semáforo para controle de paralelismo
    e retry automático em caso de falhas.

    Args:
        database_type: Tipo do banco ('mysql', 'oracle', 'postgres', 'sql_server').
        hostname: Endereço do servidor do banco.
        port: Porta de conexão.
        user: Usuário do banco.
        password: Senha do banco.
        database: Nome do banco de dados.
        queries: Lista de dicts com 'query', 'start_date' e 'end_date'.
        batch_size: Número de registros por lote.
        dataset_id: ID do dataset no BigQuery.
        table_id: ID da tabela no BigQuery.
        dump_mode: Modo de escrita ('append' ou 'overwrite').
        charset: Charset da conexão (default: NOT_SET).
        partition_columns: Lista de colunas para particionamento (default: []).
        batch_data_type: Formato dos arquivos - 'csv' ou 'parquet' (default: 'csv').
        biglake_table: Se True, cria tabela BigLake (default: True).
        log_number_of_batches: Intervalo de batches para logging (default: 100).
        retry_dump_upload_attempts: Número de tentativas em caso de falha (default: 3).
        max_concurrency: Número máximo de queries simultâneas (default: 1).
        only_staging_dataset: Se True, remove dataset de produção (default: False).
        add_timestamp_column: Se True, adiciona timestamp de ingestão (default: False).

    Raises:
        RuntimeError: Se alguma query falhar após todas as tentativas de retry.
    """
    bd_version = bd.__version__
    log(f"Using basedosdados@{bd_version}")

    # --- Início da lógica assíncrona interna ---
    retry_attempts = retry_dump_upload_attempts
    retry_delay_seconds = 300

    async def _run_query_with_retries(
        semaphore: asyncio.Semaphore,
        log_prefix: str,
        **kwargs,
    ) -> Union[Tuple[Set[str], bool, int, int], Exception]:
        """
        Wrapper que gerencia o semáforo e a lógica de retry para uma única query.
        """
        for attempt in range(retry_attempts):
            try:
                async with semaphore:
                    # O log de início da query agora usa o prefixo, tornando-o mais claro
                    log(f"{log_prefix}: Iniciando processamento.")
                    log(f"{log_prefix}: Tentativa {attempt + 1}/{retry_attempts}.")

                    # Adiciona o `log_prefix` aos argumentos que serão passados para a função trabalhadora
                    kwargs["log_prefix"] = log_prefix  # NOVO

                    func_to_run = partial(_process_single_query, **kwargs)
                    result = await run_sync_in_worker_thread(func_to_run)

                    log(
                        f"{log_prefix}: Processamento concluído com sucesso na tentativa {attempt + 1}."
                    )
                    return result
            except Exception as e:
                log(f"{log_prefix}: Falha na tentativa {attempt + 1}. Erro: {e}")
                if attempt == retry_attempts - 1:
                    log(
                        f"{log_prefix}: Todas as {retry_attempts} tentativas falharam. Registrando o erro final."
                    )
                    return e
                await asyncio.sleep(retry_delay_seconds)

        return RuntimeError(
            f"{log_prefix}: A lógica de retry terminou inesperadamente."
        )

    async def _main_async_runner():
        """A corrotina principal que orquestra a execução concorrente."""
        semaphore = asyncio.Semaphore(max_concurrency)
        log(
            f"Controle de concorrência ativado. Máximo de {max_concurrency} tarefas simultâneas."
        )

        tasks_to_run = []
        initial_cleared_partitions = set()
        initial_cleared_table = False
        total_queries = len(queries)

        for n_query, query_info in enumerate(queries):
            progress = round(100 * (n_query + 1) / total_queries, 2)
            log_prefix = f"[Query {n_query + 1}/{total_queries} ({progress}%) | Datas: {query_info.get('start_date')} a {query_info.get('end_date')}]"
            task_args = {
                "database_type": database_type,
                "hostname": hostname,
                "port": port,
                "user": user,
                "password": password,
                "database": database,
                "charset": charset,
                "query": query_info["query"],
                "batch_size": batch_size,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "dump_mode": dump_mode,
                "partition_columns": partition_columns,
                "batch_data_type": batch_data_type,
                "biglake_table": biglake_table,
                "log_number_of_batches": log_number_of_batches,
                "cleared_partitions": initial_cleared_partitions,
                "cleared_table": initial_cleared_table,
                "only_staging_dataset": only_staging_dataset,
                "add_timestamp_column": add_timestamp_column
            }
            # Cria a tarefa com o wrapper de retry
            task = _run_query_with_retries(
                semaphore, log_prefix=log_prefix, **task_args
            )
            tasks_to_run.append(task)

        log(f"Iniciando a execução de {len(tasks_to_run)} queries em paralelo...")
        # `asyncio.gather` com `return_exceptions=True` é uma alternativa, mas retornar a exceção
        # no nosso wrapper nos dá mais controle sobre a lógica de retry.
        return await asyncio.gather(*tasks_to_run)

    # Inicia o loop de eventos asyncio a partir do nosso contexto síncrono.
    results = asyncio.run(_main_async_runner())

    # --- Agregação de resultados e tratamento de erros ---
    total_idx = 0
    total_batchs_len = 0
    final_cleared_partitions = set()
    failed_queries = []

    # `zip` garante a associação correta entre a query original e seu resultado/erro.
    for query_info, result in zip(queries, results):
        if isinstance(result, Exception):
            # Coleta as informações da query que falhou e a exceção.
            failed_queries.append(
                {
                    "start_date": query_info.get("start_date"),
                    "end_date": query_info.get("end_date"),
                    "error": str(result),
                    "traceback": traceback.format_exc(),
                }
            )
        else:
            # Processa o resultado de sucesso
            cleared_parts, _, idx, batchs_len = result
            total_idx += idx
            total_batchs_len += batchs_len
            final_cleared_partitions.update(cleared_parts)

    # --- Passo Final: Levantar erro se houver falhas ---
    if failed_queries:
        error_summary = "\n".join(
            [
                f"  - Datas: {fq['start_date']} a {fq['end_date']}\n  Erro: {fq['error']}"
                for fq in failed_queries
            ]
        )
        error_message = f"{len(failed_queries)} de {len(queries)} queries falharam após {retry_attempts} tentativas.\nResumo das falhas:\n{error_summary}"
        # Levanta uma única exceção com todas as informações.
        raise RuntimeError(error_message)

    log(
        msg=f"SUCESSO: Todas as {len(queries)} queries foram executadas. Total de Batchs: {total_idx}, Rows: {total_batchs_len}"
    )


def format_partitioned_query(
    query: str,
    dataset_id: str,
    table_id: str,
    database_type: str,
    partition_columns: Optional[List[str]] = None,
    lower_bound_date: Optional[str] = None,
    date_format: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    wait: Optional[str] = None,
    offset: Optional[int] = 1
) -> List[dict]:
    """
    Formata query para extração incremental baseada em partições de data.

    Analisa a última partição disponível no BigQuery e gera queries com filtros
    de data para extrair apenas dados novos. Opcionalmente, quebra o período em
    chunks menores (dia, mês, ano, etc) para evitar timeouts.

    Args:
        query: Query SQL base a ser formatada.
        dataset_id: ID do dataset no BigQuery.
        table_id: ID da tabela no BigQuery.
        database_type: Tipo do banco ('mysql', 'oracle', 'postgres', 'sql_server').
        partition_columns: Lista de colunas de particionamento (primeira deve ser data).
        lower_bound_date: Data mínima ou alias ('current_day', 'previous_month', etc).
        date_format: Formato da data (ex: '%Y-%m-%d').
        break_query_start: Data inicial para quebra em chunks.
        break_query_end: Data final para quebra em chunks.
        break_query_frequency: Frequência dos chunks ('day', 'week', 'month', 'year', etc).
        wait: Parâmetro não utilizado (compatibilidade).
        offset: Dias/meses de offset para datas relativas (default: 1).

    Returns:
        Lista de dicts com 'query', 'start_date' e 'end_date' para cada chunk.
    """
    if not partition_columns or partition_columns[0] == "":
        log("NO partition column specified. Returning query as is")
        return [{"query": query, "start_date": None, "end_date": None}]

    partition_column = partition_columns[0]
    last_partition_date = get_last_partition_date(dataset_id, table_id, date_format)

    if last_partition_date is None:
        log("NO partition blob was found.")

    # Check if the table already exists in BigQuery.
    table = bd.Table(dataset_id, table_id)

    # If it doesn't, return the query as is, so we can fetch the whole table.
    if not table.table_exists(mode="staging"):
        log("NO tables was found.")

    if not break_query_frequency:
        return [
            build_single_partition_query(
                query=query,
                partition_column=partition_column,
                lower_bound_date=lower_bound_date,
                last_partition_date=last_partition_date,
                date_format=date_format,
                database_type=database_type,
                offset=offset,
            )
        ]

    return build_chunked_queries(
        query=query,
        partition_column=partition_column,
        date_format=date_format,
        database_type=database_type,
        break_query_start=break_query_start,
        break_query_end=break_query_end,
        break_query_frequency=break_query_frequency,
        lower_bound_date=lower_bound_date,
        last_partition_date=last_partition_date,
        offset=offset
    )


def get_last_partition_date(
    dataset_id: str, table_id: str, date_format: Optional[str]
) -> Optional[str]:
    """
    Obtém a data da última partição disponível no GCS para a tabela.

    Lista blobs do Google Cloud Storage e extrai a data mais recente das partições
    existentes. Usado para determinar ponto de partida em dumps incrementais.

    Args:
        dataset_id: ID do dataset no BigQuery (ex: 'rj_segovi').
        table_id: ID da tabela no BigQuery (ex: 'ocorrencias').
        date_format: Formato da data para parsing (ex: '%Y-%m-%d').

    Returns:
        Data da última partição formatada (ex: '2024-06-15') ou None se a tabela
        ainda não possui partições no storage.

    Examples:
        >>> get_last_partition_date('rj_segovi', 'ocorrencias', '%Y-%m-%d')
        '2024-06-15'
        >>> get_last_partition_date('new_dataset', 'new_table', '%Y-%m-%d')
        None
    """
    blobs = get_storage_blobs(dataset_id=dataset_id, table_id=table_id)
    storage_partitions_dict = parse_blobs_to_partition_dict(blobs=blobs)
    return extract_last_partition_date(
        partitions_dict=storage_partitions_dict, date_format=date_format
    )


def get_last_date(
    lower_bound_date: Optional[str], date_format: str, last_partition_date: str, offset: Optional[int] = 1
) -> str:
    """
    Calcula a data de início para extração baseada em aliases ou data literal.

    Suporta aliases relativos ('current_day', 'previous_month', etc) e datas literais.
    Usa timezone de São Paulo para cálculos de datas atuais. Se uma data literal for
    fornecida junto com última partição, retorna a menor entre as duas.

    Args:
        lower_bound_date: Data literal ('2024-01-01') ou alias:
            - 'current_year': Primeiro dia do ano atual
            - 'current_month': Primeiro dia do mês atual
            - 'previous_month': Primeiro dia do mês anterior (offset controla quantos meses)
            - 'current_day': Data atual
            - 'previous_day': Data anterior (offset controla quantos dias)
        date_format: Formato de saída da data (ex: '%Y-%m-%d', '%Y%m%d').
        last_partition_date: Data da última partição existente (usada como fallback).
        offset: Número de dias/meses para aliases 'previous_*' (default: 1).

    Returns:
        Data formatada como string no formato especificado.

    Examples:
        >>> get_last_date('current_day', '%Y-%m-%d', '2024-01-01')
        '2026-06-16'
        >>> get_last_date('previous_month', '%Y-%m-%d', '2024-01-01', offset=1)
        '2026-05-01'
        >>> get_last_date('2024-01-15', '%Y-%m-%d', '2024-01-20')
        '2024-01-15'
    """
    brazil_timezone = pytz.timezone("America/Sao_Paulo")
    now: datetime = datetime.now(brazil_timezone)
    if lower_bound_date == "current_year":
        return now.replace(month=1, day=1).strftime(date_format)
    elif lower_bound_date == "current_month":
        return now.replace(day=1).strftime(date_format)
    elif lower_bound_date == "previous_month":
        return (now - relativedelta(months=offset)).replace(day=1).strftime(date_format)
    elif lower_bound_date == "current_day":
        return now.strftime(date_format)
    elif lower_bound_date == "previous_day":
        return (now - relativedelta(days=offset)).strftime(date_format)
    elif lower_bound_date:
        if last_partition_date:
            return min(
                datetime.strptime(lower_bound_date, date_format),
                datetime.strptime(last_partition_date, date_format),
            ).strftime(date_format)
        else:
            return datetime.strptime(lower_bound_date, date_format).strftime(
                date_format
            )
    return datetime.strptime(last_partition_date, date_format).strftime(date_format)


def build_single_partition_query(
    query: str,
    partition_column: str,
    lower_bound_date: Optional[str],
    last_partition_date: str,
    date_format: str,
    database_type: str,
    offset: Optional[int]
) -> dict:
    """
    Constrói uma query com filtro de partição para extração incremental.

    Adiciona WHERE clause na query base para extrair apenas registros
    posteriores à última partição existente.

    Args:
        query: Query SQL base.
        partition_column: Nome da coluna de data para filtro.
        lower_bound_date: Data mínima ou alias.
        last_partition_date: Data da última partição existente.
        date_format: Formato da data.
        database_type: Tipo do banco para sintaxe específica.
        offset: Offset para datas relativas.

    Returns:
        Dict com 'query', 'start_date' e 'end_date'.
    """
    last_date = get_last_date(
        lower_bound_date=lower_bound_date,
        date_format=date_format,
        last_partition_date=last_partition_date,
        offset=offset
    )
    aux_name = f"a{uuid4().hex}"[:8]

    log(
        f"Partitioned DETECTED: {partition_column}, returning a NEW QUERY with partitioned columns and filters"  # noqa
    )

    if database_type == "oracle":
        oracle_date_format = "YYYY-MM-DD" if date_format == "%Y-%m-%d" else date_format
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where {partition_column} >= TO_DATE('{last_date}', '{oracle_date_format}')
        """
    elif database_type == "mysql":
        query = f"""
        select * from ({query}) as subquery
        where DATE({partition_column}) >= '{last_date}'
        """
    elif database_type in ["postgres", "sql_server"]:
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where CONVERT(DATE, {partition_column}) >= '{last_date}'
        """
    else:
        raise ValueError(f"Unsupported database type: {database_type}")

    return {
        "query": query,
        "start_date": last_date,
        "end_date": last_date,
    }


def build_chunked_queries(
    query: str,
    partition_column: str,
    date_format: str,
    database_type: str,
    break_query_start: Optional[str],
    break_query_end: Optional[str],
    break_query_frequency: Optional[str],
    lower_bound_date: Optional[str],
    last_partition_date: str,
    offset: Optional[int]
) -> List[dict]:
    """
    Quebra query em múltiplos chunks temporais para processamento paralelo.

    Divide o intervalo de datas em períodos menores (dia, semana, mês, etc) para
    evitar timeouts e permitir paralelização da extração.

    Args:
        query: Query SQL base.
        partition_column: Coluna de data para filtro.
        date_format: Formato da data.
        database_type: Tipo do banco.
        break_query_start: Data inicial do período total.
        break_query_end: Data final do período total.
        break_query_frequency: Frequência dos chunks ('day', 'week', 'month', 'year',
            'bimester', 'trimester', 'quadrimester', 'semester').
        lower_bound_date: Data mínima ou alias.
        last_partition_date: Data da última partição.
        offset: Offset para datas relativas.

    Returns:
        Lista de dicts com 'query', 'start_date' e 'end_date' para cada chunk.
    """
    start_date_str = get_last_date(
        lower_bound_date=break_query_start,
        date_format=date_format,
        last_partition_date=None,
        offset=offset
    )
    end_date_str = get_last_date(
        lower_bound_date=break_query_end,
        date_format=date_format,
        last_partition_date=None,
        offset=offset
    )
    end_date = datetime.strptime(end_date_str, date_format)

    if break_query_end == "current_month":
        end_date = get_last_day_of_month(date=end_date)
        end_date_str = end_date.strftime(date_format)
    elif break_query_end == "current_year":
        end_date = get_last_day_of_year(year=end_date.year)
        end_date_str = end_date.strftime(date_format)

    log("Breaking query into multiple chunks based on frequency")
    log(f"    break_query_frequency: {break_query_frequency}")
    log(f"    break_query_start: {start_date_str}")
    log(f"    break_query_end: {end_date_str}")

    current_start = datetime.strptime(start_date_str, date_format)
    end_date = datetime.strptime(end_date_str, date_format)
    queries = []

    while current_start <= end_date:
        current_end = calculate_end_date(
            current_start=current_start,
            end_date=end_date,
            break_query_frequency=break_query_frequency,
        )
        queries.append(
            build_chunk_query(
                query=query,
                partition_column=partition_column,
                date_format=date_format,
                database_type=database_type,
                current_start=current_start,
                current_end=current_end,
            )
        )
        current_start = get_next_start_date(
            current_start=current_start, break_query_frequency=break_query_frequency
        )

    log(f"Total queries created: {len(queries)}")
    return queries


def calculate_end_date(
    current_start: datetime, end_date: datetime, break_query_frequency: Optional[str]
) -> datetime:
    """
    Calcula a data final de um chunk baseada na frequência especificada.

    Args:
        current_start: Data inicial do chunk.
        end_date: Data limite máxima.
        break_query_frequency: Frequência ('day', 'week', 'month', 'year', etc).

    Returns:
        Data final do chunk (min entre o calculado e end_date).

    Raises:
        ValueError: Se break_query_frequency for inválida.
    """
    if break_query_frequency.lower() == "month":
        return min(get_last_day_of_month(date=current_start), end_date)
    elif break_query_frequency.lower() == "year":
        return min(get_last_day_of_year(year=current_start.year), end_date)
    elif break_query_frequency.lower() == "day":
        return min(current_start, end_date)
    elif break_query_frequency.lower() == "week":
        return min(current_start + timedelta(days=6), end_date)
    elif break_query_frequency.lower() == "bimester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=2)),
            end_date,
        )
    elif break_query_frequency.lower() == "trimester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=3)),
            end_date,
        )
    elif break_query_frequency.lower() == "quadrimester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=4)),
            end_date,
        )
    elif break_query_frequency.lower() == "semester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=6)),
            end_date,
        )
    else:
        raise ValueError(
            f"Unsupported break_query_frequency: {break_query_frequency}. Use one of the following: year, month, day, week, bimester, trimester, quadrimester and semester"  # noqa
        )


def build_chunk_query(
    query: str,
    partition_column: str,
    date_format: str,
    database_type: str,
    current_start: datetime,
    current_end: datetime,
) -> dict:
    """
    Constrói query com filtro de intervalo de datas para um chunk específico.

    Args:
        query: Query SQL base.
        partition_column: Coluna de data para filtro.
        date_format: Formato da data.
        database_type: Tipo do banco para sintaxe específica.
        current_start: Data inicial do chunk.
        current_end: Data final do chunk.

    Returns:
        Dict com 'query', 'start_date' e 'end_date'.

    Raises:
        ValueError: Se database_type for inválido.
    """
    aux_name = f"a{uuid4().hex}"[:8]

    if database_type == "oracle":
        oracle_date_format: str = (
            "YYYY-MM-DD" if date_format == "%Y-%m-%d" else date_format
        )
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where {partition_column} >= TO_DATE('{current_start.strftime(date_format)}', '{oracle_date_format}')
            and {partition_column} <= TO_DATE('{current_end.strftime(date_format)}', '{oracle_date_format}')
        """
    elif database_type == "sql_server":
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where CONVERT(DATE, CAST({partition_column} AS VARCHAR)) >= '{current_start.strftime(date_format)}'
            and CONVERT(DATE, CAST({partition_column} AS VARCHAR)) <= '{current_end .strftime(date_format)}'
        """
    elif database_type == "mysql":
        query = f"""
        select * from ({query}) as subquery
        where DATE({partition_column}) >= '{current_start.strftime(date_format)}'
            and DATE({partition_column}) <= '{current_end .strftime(date_format)}'
        """
    elif database_type == "postgres":
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where CONVERT(DATE, {partition_column}) >= '{current_start.strftime(date_format)}'
            and CONVERT(DATE, {partition_column}) <= '{current_end .strftime(date_format)}'
        """
    else:
        raise ValueError(f"Unsupported database type: {database_type}")

    return {
        "query": query,
        "start_date": current_start.strftime(date_format),
        "end_date": current_end.strftime(date_format),
    }


def get_next_start_date(
    current_start: datetime, break_query_frequency: Optional[str]
) -> datetime:
    """
    Calcula a data inicial do próximo chunk baseada na frequência.

    Avança a data de início pelo intervalo especificado na frequência. Usado para
    gerar sequência de chunks temporais em processamento paralelo.

    Args:
        current_start: Data inicial do chunk atual.
        break_query_frequency: Frequência dos chunks - 'day', 'week', 'month', 'year',
            'bimester' (2 meses), 'trimester' (3 meses), 'quadrimester' (4 meses)
            ou 'semester' (6 meses).

    Returns:
        Data inicial do próximo chunk.

    Examples:
        >>> get_next_start_date(datetime(2024, 1, 1), 'month')
        datetime.datetime(2024, 2, 1, 0, 0)
        >>> get_next_start_date(datetime(2024, 1, 1), 'week')
        datetime.datetime(2024, 1, 8, 0, 0)
        >>> get_next_start_date(datetime(2024, 1, 1), 'trimester')
        datetime.datetime(2024, 4, 1, 0, 0)
    """
    if break_query_frequency.lower() == "month":
        return add_months(start_date=current_start, months=1)
    elif break_query_frequency.lower() == "year":
        return datetime(current_start.year + 1, 1, 1)
    elif break_query_frequency.lower() == "day":
        return current_start + timedelta(days=1)
    elif break_query_frequency.lower() == "week":
        return current_start + timedelta(days=7)
    elif break_query_frequency.lower() in [
        "bimester",
        "trimester",
        "quadrimester",
        "semester",
    ]:
        months_to_add = {
            "bimester": 2,
            "trimester": 3,
            "quadrimester": 4,
            "semester": 6,
        }
        return add_months(
            start_date=current_start,
            months=months_to_add[break_query_frequency.lower()],
        )
    return current_start


def get_last_day_of_month(date: datetime) -> datetime:
    """
    Retorna o último dia do mês da data fornecida.

    Calcula dinamicamente o último dia do mês, considerando meses com
    diferentes quantidades de dias e anos bissextos.

    Args:
        date: Data de referência.

    Returns:
        Datetime do último dia do mês às 00:00:00.

    Examples:
        >>> get_last_day_of_month(datetime(2024, 2, 15))
        datetime.datetime(2024, 2, 29, 0, 0)  # ano bissexto
        >>> get_last_day_of_month(datetime(2024, 4, 1))
        datetime.datetime(2024, 4, 30, 0, 0)
    """
    next_month = date.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def get_last_day_of_year(year: int) -> datetime:
    """
    Retorna o último dia do ano especificado (31 de dezembro).

    Args:
        year: Ano de referência (ex: 2024).

    Returns:
        Datetime de 31 de dezembro do ano especificado às 00:00:00.

    Examples:
        >>> get_last_day_of_year(2024)
        datetime.datetime(2024, 12, 31, 0, 0)
    """
    return datetime(year, 12, 31)


def add_months(start_date: datetime, months: int) -> datetime:
    """
    Adiciona meses a uma data mantendo o dia.

    Manipula corretamente overflow de ano ao adicionar meses. Mantém o mesmo
    dia do mês (exceto quando não existe no mês destino).

    Args:
        start_date: Data inicial.
        months: Número de meses a adicionar (pode ser negativo para subtrair).

    Returns:
        Nova data com os meses adicionados, mantendo o dia original.

    Examples:
        >>> add_months(datetime(2024, 1, 15), 2)
        datetime.datetime(2024, 3, 15, 0, 0)
        >>> add_months(datetime(2024, 11, 15), 3)
        datetime.datetime(2025, 2, 15, 0, 0)
        >>> add_months(datetime(2024, 6, 15), -2)
        datetime.datetime(2024, 4, 15, 0, 0)
    """
    new_month = start_date.month + months
    year_increment = (new_month - 1) // 12
    new_month = (new_month - 1) % 12 + 1
    new_year = start_date.year + year_increment
    return datetime(new_year, new_month, start_date.day)


# def dump_upload_batch(
#     database_type: str,
#     hostname: str,
#     port: int,
#     user: str,
#     password: str,
#     database: str,
#     queries: List[dict],
#     batch_size: int,
#     dataset_id: str,
#     table_id: str,
#     dump_mode: str,
#     charset: str = NOT_SET,
#     partition_columns: List[str] = [],
#     batch_data_type: str = "csv",
#     biglake_table: bool = True,
#     log_number_of_batches: int = 100,
#     retry_dump_upload_attempts: int = 2,
# ):
#     """
#     This task will dump and upload batches of data, sequentially.
#     """
#     # Log BD version
#     bd_version = bd.__version__
#     log(f"Using basedosdados@{bd_version}")

#     # Keep track of cleared stuff
#     prepath = f"data/{uuid4()}/"
#     cleared_partitions = set()
#     cleared_table = False

#     wait_seconds = 30
#     total_idx = 0
#     total_batchs_len = 0
#     queries_strings = [query["query"] for query in queries]
#     for n_query, query in enumerate(queries_strings):
#         attempts = retry_dump_upload_attempts
#         while attempts >= 0:
#             try:
#                 log(f"Attempt: { retry_dump_upload_attempts - attempts}")
#                 log(
#                     f"query {n_query+1} of {len(queries_strings)} |{ round(100 * (n_query+1) / len(queries_strings), 2)}"
#                 )

#                 db_object = database_get_db(
#                     database_type=database_type,
#                     hostname=hostname,
#                     port=port,
#                     user=user,
#                     password=password,
#                     database=database,
#                     charset=charset,
#                 )

#                 database_execute(
#                     database=db_object,
#                     query=query,
#                 )

#                 # Get data columns
#                 columns = db_object.get_columns()
#                 log(f"Got columns: {columns}")

#                 new_query_cols = build_query_new_columns(table_columns=columns)
#                 log(f"New query columns without accents: {new_query_cols}")

#                 prepath = Path(prepath)

#                 if not partition_columns or partition_columns[0] == "":
#                     partition_column = None
#                 else:
#                     partition_column = partition_columns[0]

#                 if not partition_column:
#                     log("NO partition column specified! Writing unique files")
#                 else:
#                     log(
#                         f"Partition column: {partition_column} FOUND!! Write to partitioned files"
#                     )

#                 # Now loop until we have no more data.
#                 batch = db_object.fetch_batch(batch_size)
#                 idx = 0
#                 batchs_len = 0
#                 while len(batch) > 0:
#                     prepath.mkdir(parents=True, exist_ok=True)
#                     # Log progress each 100 batches.
#                     log_mod(
#                         msg=f"Dumping batch {idx+1} with size {len(batch)}",
#                         index=idx,
#                         mod=log_number_of_batches,
#                     )
#                     batchs_len += len(batch)

#                     # Dump batch to file.
#                     dataframe = batch_to_dataframe(batch=batch, columns=columns)
#                     old_columns = dataframe.columns.tolist()
#                     dataframe.columns = remove_columns_accents(dataframe)
#                     new_columns_dict = dict(
#                         zip(old_columns, dataframe.columns.tolist())
#                     )
#                     dataframe = clean_dataframe(dataframe)
#                     saved_files = []
#                     if partition_column:
#                         dataframe, date_partition_columns = parse_date_columns(
#                             dataframe, new_columns_dict[partition_column]
#                         )
#                         partitions = date_partition_columns + [
#                             new_columns_dict[col] for col in partition_columns[1:]
#                         ]
#                         saved_files = to_partitions(
#                             data=dataframe,
#                             partition_columns=partitions,
#                             savepath=prepath,
#                             data_type=batch_data_type,
#                             suffix=f"{datetime.now().strftime('%Y%m%d-%H%M%S')}",
#                         )
#                     elif batch_data_type == "csv":
#                         fname = prepath / f"{uuid4()}.csv"
#                         dataframe_to_csv(dataframe, fname)
#                         saved_files = [fname]
#                     elif batch_data_type == "parquet":
#                         fname = prepath / f"{uuid4()}.parquet"
#                         dataframe_to_parquet(dataframe, fname)
#                         saved_files = [fname]
#                     else:
#                         raise ValueError(f"Unknown data type: {batch_data_type}")

#                     # Log progress each 100 batches.

#                     log_mod(
#                         msg=f"Batch generated {len(saved_files)} files. Will now upload.",
#                         index=idx,
#                         mod=log_number_of_batches,
#                     )

#                     # Upload files.
#                     tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
#                     table_staging = f"{tb.table_full_name['staging']}"
#                     st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
#                     storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
#                     storage_path_link = (
#                         f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
#                         f"/staging/{dataset_id}/{table_id}"
#                     )
#                     dataset_is_public = tb.client["bigquery_prod"].project == "datario"
#                     # If we have a partition column
#                     if partition_column:
#                         # Extract the partition from the filenames
#                         partitions = []
#                         for saved_file in saved_files:
#                             # Remove the prepath and filename. This is the partition.
#                             partition = str(saved_file).replace(str(prepath), "")
#                             partition = partition.replace(saved_file.name, "")
#                             # Strip slashes from beginning and end.
#                             partition = partition.strip("/")
#                             # Add to list.
#                             partitions.append(partition)
#                         # Remove duplicates.
#                         partitions = list(set(partitions))
#                         log_mod(
#                             msg=f"Got partitions: {partitions}",
#                             index=idx,
#                             mod=log_number_of_batches,
#                         )
#                         # Loop through partitions and delete files from GCS.
#                         blobs_to_delete = []
#                         for partition in partitions:
#                             if partition not in cleared_partitions:
#                                 blobs = list_blobs_with_prefix(
#                                     bucket_name=st.bucket_name,
#                                     prefix=f"staging/{dataset_id}/{table_id}/{partition}",
#                                     mode="staging",
#                                 )
#                                 blobs_to_delete.extend(blobs)
#                             cleared_partitions.add(partition)
#                         if blobs_to_delete:
#                             delete_blobs_list(
#                                 bucket_name=st.bucket_name, blobs=blobs_to_delete
#                             )
#                             log_mod(
#                                 msg=f"Deleted {len(blobs_to_delete)} blobs from GCS: {blobs_to_delete}",  # noqa
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )
#                     if dump_mode == "append":
#                         if tb.table_exists(mode="staging"):
#                             log_mod(
#                                 msg=(
#                                     "MODE APPEND: Table ALREADY EXISTS:"
#                                     + f"\n{table_staging}"
#                                     + f"\n{storage_path_link}"
#                                 ),
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )
#                         else:
#                             # the header is needed to create a table when dosen't exist
#                             log_mod(
#                                 msg="MODE APPEND: Table DOESN'T EXISTS\nStart to CREATE HEADER file",  # noqa
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )
#                             header_path = dump_header_to_file(data_path=saved_files[0])
#                             log_mod(
#                                 msg="MODE APPEND: Created HEADER file:\n"
#                                 f"{header_path}",
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )

# tb.create(
#     path=header_path,
#     if_storage_data_exists="replace",
#     if_table_exists="replace",
#     biglake_table=biglake_table,
#     dataset_is_public=dataset_is_public,
#     set_biglake_connection_permissions=False,
# )

#                             log_mod(
#                                 msg=(
#                                     "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
#                                     + f"{table_staging}\n"
#                                     + f"{storage_path_link}"
#                                 ),
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )

#                             if not cleared_table:
#                                 st.delete_table(
#                                     mode="staging",
#                                     bucket_name=st.bucket_name,
#                                     not_found_ok=True,
#                                 )
#                                 log_mod(
#                                     msg=(
#                                         "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"  # noqa
#                                         + f"{storage_path}\n"
#                                         + f"{storage_path_link}"
#                                     ),
#                                     index=idx,
#                                     mod=log_number_of_batches,
#                                 )
#                                 cleared_table = True
#                     elif dump_mode == "overwrite":
#                         if tb.table_exists(mode="staging") and not cleared_table:
#                             log_mod(
#                                 msg=(
#                                     "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
#                                     + f"{storage_path}\n"
#                                     + f"{storage_path_link}"
#                                 ),
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )
#                             st.delete_table(
#                                 mode="staging",
#                                 bucket_name=st.bucket_name,
#                                 not_found_ok=True,
#                             )
#                             log_mod(
#                                 msg=(
#                                     "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
#                                     + f"{storage_path}\n"
#                                     + f"{storage_path_link}"
#                                 ),
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )
#                             # delete only staging table and let DBT overwrite the prod table
#                             tb.delete(mode="staging")
#                             log_mod(
#                                 msg=(
#                                     "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
#                                     + f"{table_staging}\n"
#                                 ),
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )

#                         if not cleared_table:
#                             # the header is needed to create a table when dosen't exist
#                             # in overwrite mode the header is always created
#                             st.delete_table(
#                                 mode="staging",
#                                 bucket_name=st.bucket_name,
#                                 not_found_ok=True,
#                             )
#                             log_mod(
#                                 msg=(
#                                     "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
#                                     + f"{storage_path}\n"
#                                     + f"{storage_path_link}"
#                                 ),
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )

#                             log_mod(
#                                 msg="MODE OVERWRITE: Table DOSEN'T EXISTS\nStart to CREATE HEADER file",  # noqa
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )
#                             header_path = dump_header_to_file(data_path=saved_files[0])
#                             log_mod(
#                                 "MODE OVERWRITE: Created HEADER file:\n"
#                                 f"{header_path}",
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )

# tb.create(
#     path=header_path,
#     if_storage_data_exists="replace",
#     if_table_exists="replace",
#     biglake_table=biglake_table,
#     dataset_is_public=dataset_is_public,
#     set_biglake_connection_permissions=False,
# )

#                             log_mod(
#                                 msg=(
#                                     "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
#                                     + f"{table_staging}\n"
#                                     + f"{storage_path_link}"
#                                 ),
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )

#                             st.delete_table(
#                                 mode="staging",
#                                 bucket_name=st.bucket_name,
#                                 not_found_ok=True,
#                             )
#                             log_mod(
#                                 msg=(
#                                     "MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"  # noqa
#                                     + f"{storage_path}\n"
#                                     + f"{storage_path_link}"
#                                 ),
#                                 index=idx,
#                                 mod=log_number_of_batches,
#                             )
#                             cleared_table = True

#                     log_mod(
#                         msg="STARTING UPLOAD TO GCS",
#                         index=idx,
#                         mod=log_number_of_batches,
#                     )
#                     if tb.table_exists(mode="staging"):
#                         # Upload them all at once
#                         tb.append(filepath=prepath, if_exists="replace")
#                         log_mod(
#                             msg=f"STEP UPLOAD: Sucessfully uploaded batch {idx +1} file with size {len(batch)} to Storage",
#                             index=idx,
#                             mod=log_number_of_batches,
#                         )
#                         for saved_file in saved_files:
#                             # Delete the files
#                             saved_file.unlink()
#                     else:
#
#                         log_mod(
#                             msg="STEP UPLOAD: Table does not exist in STAGING, need to create first",  # noqa
#                             index=idx,
#                             mod=log_number_of_batches,
#                         )
#                     # Get next batch.
#                     batch = db_object.fetch_batch(batch_size)
#                     idx += 1

#                     # delete batch data from prepath
#                     shutil.rmtree(prepath)

#                 # end try
#                 attempts = -1

#             except Exception as e:
#                 if attempts == 0:
#                     log(f"last executed query: {query}")
#                     raise e
#                 else:
#                     log(
#                         f"Remaning Attempts: {attempts}. Retry in {wait_seconds}s",
#                         level="error",
#                     )
#                     log(f"executed query: {query}", level="error")
#                     log(e, level="error")
#                     # delete batch data from prepath
#                     shutil.rmtree(prepath)

#                     attempts -= 1
#                     time.sleep(wait_seconds)  # wait 30 secondds

#             # end back while

#         log(
#             msg=f"Successfully dumped {idx-1} batches, total of  {batchs_len} rows",  # noqa
#         )
#         # end of for queries
#         total_idx += idx
#         total_batchs_len += batchs_len

#     log(
#         msg=f"Successfully dumped {len(queries_strings)} queries, {total_idx} batches, total of {total_batchs_len} rows"  # noqa
#     )
