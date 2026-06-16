# -*- coding: utf-8 -*-
"""Utilitários para manipulação e transformação de DataFrames.

Este módulo fornece funções para conversão, limpeza, particionamento e exportação
de dados em diferentes formatos (CSV, Parquet), com suporte a tabelas particionadas
no padrão Hive e tratamento de colunas.
"""
import re
from datetime import datetime
from os import walk
from os.path import join
from pathlib import Path
from typing import List, Tuple, Union
from uuid import uuid4

import numpy as np
import pandas as pd

from iplanrio.pipelines_utils.logging import log


def batch_to_dataframe(batch: List[List], columns: List[str]) -> pd.DataFrame:
    """Converte um lote de linhas em DataFrame.

    Args:
        batch: Lista de listas representando as linhas de dados.
        columns: Lista com os nomes das colunas.

    Returns:
        DataFrame construído a partir do batch e colunas fornecidos.
    """
    return pd.DataFrame(data=batch, columns=columns)


def build_query_new_columns(table_columns: List[str]) -> str:
    """Cria cláusulas SELECT com aliases de colunas sem acentos.

    Args:
        table_columns: Lista com os nomes originais das colunas.

    Returns:
        String com cláusulas "coluna_original AS coluna_sem_acento" separadas por quebra de linha.
    """
    new_cols = remove_columns_accents(pd.DataFrame(columns=table_columns))
    return "\n".join(
        [
            f"{old_col} AS {new_col},"
            for old_col, new_col in zip(table_columns, new_cols)
        ]
    )


def clean_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Limpa caracteres indesejados em colunas de texto do DataFrame.

    Remove caracteres nulos, substitui quebras de linha e converte "None" para NaN
    em todas as colunas do tipo object.

    Args:
        dataframe: DataFrame a ser limpo.

    Returns:
        DataFrame com colunas de texto limpas.

    Raises:
        Exception: Se houver erro ao processar alguma coluna, exibe informações
            detalhadas e repropaga a exceção.
    """
    for col in dataframe.columns.tolist():
        try:
            if dataframe[col].dtype == object:
                dataframe[col] = (
                    dataframe[col]
                    .astype(str)
                    .replace(
                        {"\x00": "", "None": np.nan, "\n": " ", "\r": " "}, regex=True
                    )
                )
        except Exception as exc:
            print(
                "Column: ",
                col,
                "\nData: ",
                dataframe[col].tolist(),
                "\n",
                exc,
            )
            print("dataframe:\n", dataframe.head(42))
            raise
    return dataframe


def dump_header_to_file(data_path: Union[str, Path], data_type: str = "csv"):
    """Extrai e salva apenas o cabeçalho de um arquivo de dados.

    Busca o primeiro arquivo do tipo especificado, lê apenas a primeira linha
    e salva em um diretório temporário, preservando a estrutura de particionamento.

    Args:
        data_path: Caminho do diretório ou arquivo de dados.
        data_type: Tipo do arquivo ("csv" ou "parquet").

    Returns:
        Caminho do diretório onde o arquivo de cabeçalho foi salvo.

    Raises:
        ValueError: Se data_type não for "csv" ou "parquet".
    """
    try:
        assert data_type in ["csv", "parquet"]
    except AssertionError as exc:
        raise ValueError(f"Invalid data type: {data_type}") from exc
    # Remove filename from path
    path = Path(data_path)
    if not path.is_dir():
        path = path.parent
    # Grab first `data_type` file found
    found: bool = False
    file: str = None
    for subdir, _, filenames in walk(str(path)):
        for fname in filenames:
            if fname.endswith(f".{data_type}"):
                file = join(subdir, fname)
                log(f"Found {data_type.upper()} file: {file}")
                found = True
                break
        if found:
            break

    save_header_path = f"data/{uuid4()}"
    # discover if it's a partitioned table
    if partition_folders := [folder for folder in file.split("/") if "=" in folder]:
        partition_path = "/".join(partition_folders)
        save_header_file_path = Path(
            f"{save_header_path}/{partition_path}/header.{data_type}"
        )
        log(f"Found partition path: {save_header_file_path}")

    else:
        save_header_file_path = Path(f"{save_header_path}/header.{data_type}")
        log(f"Do not found partition path: {save_header_file_path}")

    # Create directory if it doesn't exist
    save_header_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read just first row and write dataframe to file
    if data_type == "csv":
        dataframe = pd.read_csv(file, nrows=1)
        dataframe_to_csv(dataframe=dataframe, filepath=save_header_file_path)
    elif data_type == "parquet":
        dataframe = pd.read_parquet(file)[:1]
        dataframe_to_parquet(dataframe=dataframe, path=save_header_file_path)

    log(f"Wrote {data_type.upper()} header at {save_header_file_path}")

    return save_header_path


def final_column_treatment(column: str) -> str:
    """Aplica tratamento final no nome da coluna para garantir validade.

    Adiciona underscore antes do nome se for composto apenas por números,
    ou remove todos os caracteres não alfanuméricos exceto underscores.

    Args:
        column: Nome da coluna a ser tratado.

    Returns:
        Nome da coluna tratado.
    """
    try:
        int(column)
        return f"_{column}"
    except ValueError:
        non_alpha_removed = re.sub(r"[\W]+", "", column)
        return non_alpha_removed


def add_ingestion_timestamp(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a timestamp column indicating when the data was extracted.

    Args:
        dataframe: The DataFrame to add the timestamp to

    Returns:
        DataFrame with the _prefect_extracted_at column added
    """
    ingestion_col = "_prefect_extracted_at"

    if ingestion_col in dataframe.columns:
        raise ValueError(
            f"Column {ingestion_col} already exists, please review your model."
        )

    dataframe[ingestion_col] = datetime.now()

    return dataframe


def parse_date_columns(
    dataframe: pd.DataFrame, partition_date_column: str
) -> Tuple[pd.DataFrame, List[str]]:
    """Extrai colunas de particionamento temporal a partir de uma coluna de data.

    Cria as colunas ano_particao, mes_particao e data_particao a partir
    da coluna de data especificada.

    Args:
        dataframe: DataFrame contendo os dados.
        partition_date_column: Nome da coluna que contém as datas.

    Returns:
        Tupla contendo o DataFrame modificado e lista com os nomes das colunas criadas.

    Raises:
        ValueError: Se alguma das colunas de partição já existir no DataFrame.
    """
    ano_col = "ano_particao"
    mes_col = "mes_particao"
    data_col = "data_particao"
    cols = [ano_col, mes_col, data_col]
    for col in cols:
        if col in dataframe.columns:
            raise ValueError(f"Column {col} already exists, please review your model.")

    dataframe[partition_date_column] = dataframe[partition_date_column].astype(str)
    dataframe[data_col] = pd.to_datetime(
        dataframe[partition_date_column], errors="coerce"
    )

    dataframe[ano_col] = (
        dataframe[data_col]
        .dt.year.fillna(-1)
        .astype(int)
        .astype(str)
        .replace("-1", np.nan)
    )

    dataframe[mes_col] = (
        dataframe[data_col]
        .dt.month.fillna(-1)
        .astype(int)
        .astype(str)
        .replace("-1", np.nan)
    )

    dataframe[data_col] = dataframe[data_col].dt.date

    return dataframe, [ano_col, mes_col, data_col]


def remove_columns_accents(dataframe: pd.DataFrame) -> list:
    """Remove acentos e normaliza nomes das colunas do DataFrame.

    Converte para minúsculas, remove acentos, substitui espaços e caracteres
    especiais por underscores, e aplica tratamento final.

    Args:
        dataframe: DataFrame cujos nomes de colunas serão normalizados.

    Returns:
        Lista com os nomes das colunas normalizados.
    """
    columns = [str(column) for column in dataframe.columns]
    dataframe.columns = columns
    return list(
        dataframe.columns.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .map(lambda x: x.strip())
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("-", "_")
        .str.replace("\a", "_")
        .str.replace("\b", "_")
        .str.replace("\n", "_")
        .str.replace("\t", "_")
        .str.replace("\v", "_")
        .str.replace("\f", "_")
        .str.replace("\r", "_")
        .str.replace("(", "_")
        .str.replace(")", "_")
        .str.lower()
        .map(final_column_treatment)
    )


def to_json_dataframe(
    dataframe: "pd.DataFrame" = None,
    csv_path: Union[str, Path] = None,
    key_column: str = None,
    read_csv_kwargs: dict = None,
    save_to: Union[str, Path] = None,
) -> "pd.DataFrame":
    """Transforma DataFrame movendo colunas para formato JSON em coluna 'content'.

    Mantém a key_column e agrupa todas as outras colunas em um dicionário
    na coluna 'content'.

    Args:
        dataframe: DataFrame a ser transformado (opcional se csv_path fornecido).
        csv_path: Caminho para arquivo CSV (alternativa ao dataframe).
        key_column: Nome da coluna chave a ser preservada.
        read_csv_kwargs: Argumentos adicionais para pd.read_csv.
        save_to: Caminho para salvar o resultado em CSV.

    Returns:
        DataFrame com estrutura [key_column, content] ou apenas [content].

    Raises:
        ValueError: Se nem dataframe nem csv_path forem fornecidos.

    Example:
        Input: pd.DataFrame({"key": ["a", "b"], "col1": [1, 2], "col2": [3, 4]})
        Output: pd.DataFrame({"key": ["a", "b"], "content": [{"col1": 1, "col2": 3}, ...]})
    """
    if dataframe is None and not csv_path:
        raise ValueError("dataframe or dataframe_path is required")
    if csv_path:
        dataframe = pd.read_csv(csv_path, **read_csv_kwargs)
    if key_column:
        dataframe["content"] = dataframe.drop(columns=[key_column]).to_dict(
            orient="records"
        )
        dataframe = dataframe[["key", "content"]]
    else:
        dataframe["content"] = dataframe.to_dict(orient="records")
        dataframe = dataframe[["content"]]
    if save_to:
        dataframe.to_csv(save_to, index=False)
    return dataframe


# pylint: disable=R0913
def handle_dataframe_chunk(
    dataframe: pd.DataFrame,
    save_path: str,
    partition_columns: List[str],
    event_id: str,
    idx: int,
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
):
    """Processa e salva um chunk de DataFrame com particionamento opcional.

    Remove acentos das colunas, limpa dados, e salva em partições Hive ou
    arquivo único dependendo da configuração.

    Args:
        dataframe: DataFrame a ser processado.
        save_path: Caminho onde os dados serão salvos.
        partition_columns: Lista de colunas para particionamento.
        event_id: Identificador do evento para nomeação de arquivos.
        idx: Índice do chunk (usado para logging e nomeação).
        build_json_dataframe: Se True, converte dados para formato JSON.
        dataframe_key_column: Coluna chave para conversão JSON.
    """
    if not partition_columns or partition_columns[0] == "":
        partition_column = None
    else:
        partition_column = partition_columns[0]

    old_columns = dataframe.columns.tolist()
    dataframe.columns = remove_columns_accents(dataframe)
    new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
    if idx == 0:
        if partition_column:
            log(
                f"Partition column: {partition_column} FOUND!! Write to partitioned files"
            )

        else:
            log("NO partition column specified! Writing unique files")

        log(f"New columns without accents: {new_columns_dict}")

    dataframe = clean_dataframe(dataframe)

    if partition_column:
        dataframe, date_partition_columns = parse_date_columns(
            dataframe, new_columns_dict[partition_column]
        )

        partitions = date_partition_columns + [
            new_columns_dict[col] for col in partition_columns[1:]
        ]
        to_partitions(
            data=dataframe,
            partition_columns=partitions,
            savepath=save_path,
            data_type="csv",
            build_json_dataframe=build_json_dataframe,
            dataframe_key_column=dataframe_key_column,
        )
    else:
        dataframe_to_csv(
            dataframe=dataframe,
            filepath=Path(save_path) / f"{event_id}-{idx}.csv",
            build_json_dataframe=build_json_dataframe,
            dataframe_key_column=dataframe_key_column,
        )


# pylint: disable=R0913
def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: str,
    data_type: str = "csv",
    suffix: str = None,
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
) -> List[Path]:  # sourcery skip: raise-specific-error
    """Salva DataFrame em partições no formato Hive.

    Cria estrutura de diretórios no padrão partition=value e salva os dados
    particionados, removendo as colunas de partição dos arquivos.

    Args:
        data: DataFrame a ser particionado.
        partition_columns: Lista de colunas usadas para particionamento.
        savepath: Caminho base onde as partições serão salvas.
        data_type: Formato do arquivo ("csv" ou "parquet").
        suffix: Sufixo adicional para o nome do arquivo.
        build_json_dataframe: Se True, converte dados para formato JSON.
        dataframe_key_column: Coluna chave para conversão JSON.

    Returns:
        Lista de caminhos dos arquivos salvos.

    Raises:
        ValueError: Se data_type não for "csv" ou "parquet".
        BaseException: Se data não for um pandas DataFrame.

    Example:
        data = {"ano": [2020, 2021], "mes": [1, 2], "valor": [10, 20]}
        to_partitions(pd.DataFrame(data), ['ano', 'mes'], 'output/')
        # Cria: output/ano=2020/mes=1/data.csv e output/ano=2021/mes=2/data.csv
    """
    saved_files = []
    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns).reset_index(drop=True)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            if suffix is not None:
                file_filter_save_path = (
                    Path(filter_save_path) / f"data_{suffix}.{data_type}"
                )
            else:
                file_filter_save_path = Path(filter_save_path) / f"data.{data_type}"

            if build_json_dataframe:
                df_filter = to_json_dataframe(
                    df_filter, key_column=dataframe_key_column
                )

            if data_type == "csv":
                # append data to csv
                df_filter.to_csv(
                    file_filter_save_path,
                    index=False,
                    mode="a",
                    header=not file_filter_save_path.exists(),
                )
                saved_files.append(file_filter_save_path)
            elif data_type == "parquet":
                dataframe_to_parquet(dataframe=df_filter, path=file_filter_save_path)
                saved_files.append(file_filter_save_path)
            else:
                raise ValueError(f"Invalid data type: {data_type}")
    else:
        raise BaseException("Data need to be a pandas DataFrame")

    return saved_files


def dataframe_to_csv(
    dataframe: "pd.DataFrame",
    filepath: Union[str, Path],
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
) -> None:
    """Salva DataFrame em arquivo CSV.

    Cria diretórios necessários e salva o DataFrame em CSV com encoding UTF-8,
    com opção de conversão para formato JSON.

    Args:
        dataframe: DataFrame a ser salvo.
        filepath: Caminho completo do arquivo de destino.
        build_json_dataframe: Se True, converte para formato JSON antes de salvar.
        dataframe_key_column: Coluna chave para conversão JSON.
    """
    if build_json_dataframe:
        dataframe = to_json_dataframe(dataframe, key_column=dataframe_key_column)

    # Remove filename from path
    filepath = Path(filepath)
    # Create directory if it doesn't exist
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Write dataframe to CSV
    dataframe.to_csv(filepath, index=False, encoding="utf-8")


def dataframe_to_parquet(
    dataframe: pd.DataFrame,
    path: Union[str, Path],
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
):
    """Salva DataFrame em arquivo Parquet com merge incremental.

    Se o arquivo já existir, carrega e concatena com os novos dados antes de salvar.
    Usa engine PyArrow para escrita.

    Args:
        dataframe: DataFrame a ser salvo.
        path: Caminho completo do arquivo de destino.
        build_json_dataframe: Se True, converte para formato JSON antes de salvar.
        dataframe_key_column: Coluna chave para conversão JSON.

    Note:
        Adaptado de https://stackoverflow.com/a/70817689/9944075
    """
    # Code adapted from
    # https://stackoverflow.com/a/70817689/9944075

    if build_json_dataframe:
        dataframe = to_json_dataframe(dataframe, key_column=dataframe_key_column)

    # If the file already exists, we:
    # - Load it
    # - Merge the new dataframe with the existing one
    if Path(path).exists():
        # Load it
        original_df = pd.read_parquet(path)
        # Merge the new dataframe with the existing one
        dataframe = pd.concat([original_df, dataframe], sort=False)

    # Write dataframe to Parquet
    dataframe.to_parquet(path, engine="pyarrow")
