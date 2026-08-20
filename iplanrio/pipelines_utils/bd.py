# -*- coding: utf-8 -*-
"""Utilitários para integração com o Base dos Dados (BD+) e Google Cloud Storage.

Fornece funções para criação de tabelas, upload de dados para GCS, gerenciamento
de datasets staging/prod, listagem de blobs, manipulação de credenciais e
configurações do Base dos Dados.
"""
import base64
from pathlib import Path
from typing import Dict, List, Union

import basedosdados as bd
from basedosdados import Base
from google.cloud import storage
from google.cloud.storage.blob import Blob
from prefect import task

from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import dump_header_to_file


@task
def create_table_and_upload_to_gcs_task(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    biglake_table: bool = True,
    source_format: str = "csv",
    only_staging_dataset: bool = False,
) -> Union[str, Path]:
    return create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
        source_format=source_format,
        only_staging_dataset=only_staging_dataset,
    )


def _delete_prod_dataset(only_staging_dataset: bool, dataset_id: str):
    """Remove dataset de produção se only_staging_dataset for True.

    Args:
        only_staging_dataset: Se True, deleta o dataset de produção.
        dataset_id: ID do dataset a ser deletado.
    """
    ds = bd.Dataset(dataset_id=dataset_id)
    if only_staging_dataset and ds.exists(mode="prod"):
        try:
            ds.delete(mode="prod")
            log("Successfully deleted prod dataset")
        except Exception as e:
            log(f"Error while deleting prod dataset: {e}")


def create_table_and_upload_to_gcs(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    biglake_table: bool = True,
    source_format: str = "csv",
    only_staging_dataset: bool = False,
) -> Union[str, Path]:
    """Cria tabela no BigQuery e faz upload de dados para GCS usando BD+.

    Gerencia criação de tabelas em modo append ou overwrite, cria headers
    quando necessário, e faz upload dos dados para o Google Cloud Storage.

    Args:
        data_path: Caminho dos dados a serem enviados.
        dataset_id: ID do dataset no BigQuery.
        table_id: ID da tabela no BigQuery.
        dump_mode: Modo de dump ("append" ou "overwrite").
        biglake_table: Se True, cria tabela BigLake.
        source_format: Formato dos dados ("csv" ou "parquet").
        only_staging_dataset: Se True, trabalha apenas com staging (sem prod).

    Returns:
        Caminho dos dados enviados.
    """
    bd_version = bd.__version__
    log(f"USING BASEDOSDADOS {bd_version}")
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    log(f"Dataset:{dataset_id} Table:{table_id} ")
    table_staging = f"{tb.table_full_name['staging']}"
    log(f"table_staging: {table_staging}")

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    log(f"storage_path: {storage_path}")
    storage_path_link = (
        f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
        f"/staging/{dataset_id}/{table_id}"
    )
    log(f"storage_path_link: {storage_path_link}")

    # prod datasets is public if the project is datario. staging are private im both projects
    dataset_is_public = tb.client["bigquery_prod"].project == "datario"
    log(f"dataset_is_public: {dataset_is_public}")

    #####################################
    #
    # MANAGEMENT OF TABLE CREATION
    #
    #####################################
    log("STARTING TABLE CREATION MANAGEMENT")
    if dump_mode == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"MODE APPEND: Table ALREADY EXISTS:"
                f"\n{table_staging}"
                f"\n{storage_path_link}"
            )
        else:
            # the header is needed to create a table when doesn't exist
            log("MODE APPEND: Table DOESN'T EXISTS\nStart to CREATE HEADER file")
            header_path = dump_header_to_file(
                data_path=data_path, data_type=source_format
            )
            log("MODE APPEND: Created HEADER file:\n" f"{header_path}")

            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_exists="replace",
                biglake_table=biglake_table,
                dataset_is_public=dataset_is_public,
                source_format=source_format,
                set_biglake_connection_permissions=False,
            )

            log(
                "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                f"{table_staging}\n"
                f"{storage_path_link}"
            )

            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )
    elif dump_mode == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )
            tb.delete(mode="staging")
            log(
                "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                f"{table_staging}\n"
                # f"{tb.table_full_name['prod']}"
            )

        # the header is needed to create a table when doesn't exist
        # in overwrite mode the header is always created
        log("MODE OVERWRITE: Table DOESN'T EXISTS\nStart to CREATE HEADER file")
        header_path = dump_header_to_file(data_path=data_path, data_type=source_format)
        log("MODE OVERWRITE: Created HEADER file:\n" f"{header_path}")

        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
            biglake_table=biglake_table,
            dataset_is_public=dataset_is_public,
            source_format=source_format,
            set_biglake_connection_permissions=False,
        )

        log(
            "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
            f"{table_staging}\n"
            f"{storage_path_link}"
        )

        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        log(
            f"MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )

    if only_staging_dataset:
        _delete_prod_dataset(
            only_staging_dataset=only_staging_dataset, dataset_id=dataset_id
        )
    #####################################
    #
    # Uploads a bunch of files using BD+
    #
    #####################################

    log("STARTING UPLOAD TO GCS")
    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(filepath=data_path, if_exists="replace")

        log(
            f"STEP UPLOAD: Successfully uploaded {data_path} to Storage:\n"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )
    else:
        log("STEP UPLOAD: Table does not exist in STAGING, need to create first")

    return data_path


def get_storage_blobs(dataset_id: str, table_id: str, mode: str = "staging") -> list:
    """Obtém todos os blobs de uma tabela no GCS.

    Args:
        dataset_id: ID do dataset.
        table_id: ID da tabela.
        mode: Modo de acesso ("staging" ou "prod").

    Returns:
        Lista de blobs do Google Cloud Storage.
    """

    bd_storage = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    return list(
        bd_storage.client["storage_staging"]
        .bucket(bd_storage.bucket_name)
        .list_blobs(prefix=f"{mode}/{bd_storage.dataset_id}/{bd_storage.table_id}/")
    )


def get_project_id(mode: str = None) -> str:
    """Obtém o ID do projeto Google Cloud a partir das configurações.

    Args:
        mode: Modo do ambiente ("prod" ou "staging").

    Returns:
        ID do projeto Google Cloud.

    Raises:
        ValueError: Se mode não for "prod" ou "staging".
    """

    if mode not in ["prod", "staging"]:
        raise ValueError("Mode must be 'prod' or 'staging'")
    base = Base()
    return base.config["gcloud-projects"][mode]["name"]


def list_blobs_with_prefix(
    bucket_name: str, prefix: str, mode: str = "prod"
) -> List[Blob]:
    """Lista blobs no bucket que começam com o prefixo especificado.

    Útil para listar blobs em uma "pasta" específica.

    Args:
        bucket_name: Nome do bucket GCS.
        prefix: Prefixo para filtrar blobs (ex: "public/").
        mode: Modo de acesso ("prod" ou "staging").

    Returns:
        Lista de objetos Blob que correspondem ao prefixo.
    """

    credentials = get_bd_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    return list(blobs)


def secret_to_base64(secret_dict: Dict) -> str:
    """Converte dicionário para string Base64.

    Serializa o dicionário para JSON e codifica em Base64.

    Args:
        secret_dict: Dicionário a ser codificado.

    Returns:
        String codificada em Base64.
    """
    input_string = str(secret_dict).replace("'", '"')
    bytes_data = input_string.encode("utf-8")
    base64_data = base64.b64encode(bytes_data)
    base64_string = base64_data.decode("utf-8")
    return base64_string


def get_base64_bd_config(projec_id: str) -> str:
    """Gera configuração Base dos Dados em Base64 para um projeto.

    Cria arquivo de configuração TOML com credenciais staging/prod e
    retorna codificado em Base64.

    Args:
        projec_id: ID do projeto Google Cloud.

    Returns:
        String de configuração codificada em Base64.
    """

    string = f"""# What is the bucket that you are saving all the data? It should be
                # an unique name.
                bucket_name = "{projec_id}"

                [gcloud-projects]

                [gcloud-projects.staging]
                    credentials_path = "~/.basedosdados/credentials/staging.json"
                    name = "{projec_id}"

                [gcloud-projects.prod]
                    credentials_path = "~/.basedosdados/credentials/prod.json"
                    name = "{projec_id}"

                [api]
                url = "https://api.dados.rio/api/v1/graphql"
            """.replace(
        "                ", ""
    )

    string_bytes = string.encode("utf-8")

    encoded_string = base64.b64encode(string_bytes).decode("utf-8")

    return encoded_string


def base64_to_string(base64_string: str) -> str:
    """Decodifica string Base64 para string regular.

    Args:
        base64_string: String codificada em Base64.

    Returns:
        String decodificada.
    """
    base64_bytes = base64_string.encode("utf-8")
    message_bytes = base64.b64decode(base64_bytes)
    message = message_bytes.decode("utf-8")
    return message
