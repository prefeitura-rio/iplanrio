# -*- coding: utf-8 -*-
"""Utilitários para interação com Google Cloud Storage (GCS).

Fornece funções para gerenciamento de blobs, upload de arquivos, listagem
com prefixos, deleção em lote e extração de informações de particionamento
a partir de estruturas de diretórios.
"""
from typing import List

from google.cloud import storage
from google.cloud.storage.blob import Blob

from iplanrio.pipelines_utils.env import get_bd_credentials_from_env


def delete_blobs_list(bucket_name: str, blobs: List[Blob], mode: str = "prod") -> None:
    """Deleta todos os blobs especificados do bucket.

    Args:
        bucket_name: Nome do bucket GCS.
        blobs: Lista de blobs a serem deletados.
        mode: Modo de acesso ("prod" ou "staging").
    """
    storage_client = get_gcs_client(mode=mode)

    bucket = storage_client.bucket(bucket_name)
    bucket.delete_blobs(blobs)


def get_gcs_client(mode: str = "staging") -> storage.Client:
    """Obtém cliente GCS autenticado com credenciais do ambiente.

    Args:
        mode: Modo de acesso ("prod" ou "staging").

    Returns:
        Cliente Google Cloud Storage autenticado.
    """

    credentials = get_bd_credentials_from_env(mode=mode)
    return storage.Client(credentials=credentials)


def list_blobs_with_prefix(
    bucket_name: str, prefix: str, mode: str = "staging"
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
    storage_client = get_gcs_client(mode=mode)
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return list(blobs)


def parse_blobs_to_partition_dict(blobs: list) -> dict:
    """Extrai informações de particionamento dos caminhos dos blobs.

    Analisa caminhos no formato "partition=value" e agrupa por chave.

    Args:
        blobs: Lista de blobs a serem analisados.

    Returns:
        Dicionário mapeando chaves de partição para listas de valores.
    """

    partitions_dict = {}
    for blob in blobs:
        for folder in blob.name.split("/"):
            if "=" in folder:
                key = folder.split("=")[0]
                value = folder.split("=")[1]
                try:
                    partitions_dict[key].append(value)
                except KeyError:
                    partitions_dict[key] = [value]
    return partitions_dict


def parse_blobs_to_partition_list(blobs: List[Blob]) -> List[str]:
    """Extrai valores da partição 'data_particao' dos caminhos dos blobs.

    Args:
        blobs: Lista de blobs a serem analisados.

    Returns:
        Lista de valores encontrados para a partição 'data_particao'.
    """
    partitions = []
    for blob in blobs:
        for folder in blob.name.split("/"):
            if "=" in folder:
                key = folder.split("=")[0]
                value = folder.split("=")[1]
                if key == "data_particao":
                    partitions.append(value)
    return partitions


def upload_file_to_bucket(
    bucket_name: str, file_path: str, destination_blob_name: str, mode: str = None
) -> "Blob":
    """Faz upload de arquivo para o bucket GCS.

    Args:
        bucket_name: Nome do bucket de destino.
        file_path: Caminho local do arquivo a ser enviado.
        destination_blob_name: Nome do blob no bucket.
        mode: Modo de acesso ("prod" ou "staging").

    Returns:
        Objeto Blob representando o arquivo enviado.
    """

    storage_client = get_gcs_client(mode=mode)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    return blob
