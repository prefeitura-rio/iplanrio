# -*- coding: utf-8 -*-
"""Utilitários para entrada/saída e manipulação de dados auxiliares.

Fornece funções para validação de datas, formatação de queries, carregamento de arquivos YAML,
extração de partições, validação de expressões cron e formatação de valores legíveis por humanos.
"""
import re
import textwrap
from datetime import datetime
from pathlib import Path
from typing import List, Union

import croniter
import ruamel.yaml as ryaml

from iplanrio.pipelines_utils.logging import log


def determine_whether_to_execute_or_not(
    cron_expression: str, datetime_now: datetime, datetime_last_execution: datetime
) -> bool:
    """Determina se uma expressão cron deve ser executada no momento atual.

    Calcula a próxima execução baseada na última execução e verifica se já
    é hora de executar novamente.

    Args:
        cron_expression: Expressão cron a ser validada.
        datetime_now: Data/hora atual.
        datetime_last_execution: Data/hora da última execução.

    Returns:
        True se a expressão cron deve ser disparada, False caso contrário.
    """
    cron_expression_iterator = croniter.croniter(
        cron_expression, datetime_last_execution
    )
    next_cron_expression_time = cron_expression_iterator.get_next(datetime)
    return next_cron_expression_time <= datetime_now


def extract_last_partition_date(partitions_dict: dict, date_format: str):
    """Extrai a data mais recente de um dicionário de partições.

    Itera sobre os valores do dicionário, valida quais são datas válidas
    no formato especificado e retorna a mais recente.

    Args:
        partitions_dict: Dicionário com partições e seus valores.
        date_format: Formato de data esperado (ex: "%Y-%m-%d").

    Returns:
        String com a data mais recente no formato especificado, ou None se
        nenhuma data válida for encontrada.
    """
    last_partition_date = None
    for partition, values in partitions_dict.items():
        new_values = [
            date
            for date in values
            if is_date(date_string=date, date_format=date_format)
        ]
        try:
            last_partition_date = datetime.strptime(
                max(new_values), date_format
            ).strftime(date_format)
            log(
                f"last partition from {partition} is in date format "
                f"{date_format}: {last_partition_date}"
            )
        except ValueError:
            log(
                f"partition {partition} is not a date or not in correct format {date_format}"
            )
    return last_partition_date


def get_root_path() -> Path:
    """Retorna o caminho raiz do projeto.

    Detecta automaticamente se está rodando em container Docker (via site-packages)
    e ajusta o caminho raiz para /app nesses casos.

    Returns:
        Path do diretório raiz do projeto.

    Raises:
        ImportError: Se o pacote iplanrio não for encontrado.
    """
    try:
        import iplanrio
    except ImportError as exc:
        raise ImportError("pipelines package not found") from exc
    root_path = Path(iplanrio.__file__).parent.parent
    # If the root path is site-packages, we're running in a Docker container. Thus, we
    # need to change the root path to /app
    if str(root_path).endswith("site-packages"):
        root_path = Path("/app")
    return root_path


def human_readable(
    value: Union[int, float],
    unit: str = "",
    unit_prefixes: List[str] = None,
    unit_divider: int = 1000,
    decimal_places: int = 2,
):
    """Formata um valor numérico de forma legível com prefixos de unidade.

    Converte valores grandes usando prefixos (k, M, G, T, etc.) para facilitar
    a leitura.

    Args:
        value: Valor numérico a ser formatado.
        unit: Unidade base (ex: "B" para bytes).
        unit_prefixes: Lista de prefixos a usar (padrão: ["", "k", "M", "G", ...]).
        unit_divider: Divisor entre prefixos (padrão: 1000).
        decimal_places: Número de casas decimais (padrão: 2).

    Returns:
        String formatada com valor e unidade (ex: "1.5MB").
    """
    if unit_prefixes is None:
        unit_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"]
    if value == 0:
        return f"{value}{unit}"
    unit_prefix = unit_prefixes[0]
    for prefix in unit_prefixes[1:]:
        if value < unit_divider:
            break
        unit_prefix = prefix
        value /= unit_divider
    return f"{value:.{decimal_places}f}{unit_prefix}{unit}"


def is_date(date_string: str, date_format: str = "%Y-%m-%d") -> Union[datetime, bool]:
    """Verifica se uma string é uma data válida no formato especificado.

    Args:
        date_string: String a ser validada como data.
        date_format: Formato de data esperado (padrão: "%Y-%m-%d").

    Returns:
        String da data formatada se válida, False caso contrário.
    """
    try:
        return datetime.strptime(date_string, date_format).strftime(date_format)
    except ValueError:
        return False


def query_to_line(query: str) -> str:
    """Converte query SQL multi-linha em string de linha única.

    Remove indentação e quebras de linha, juntando tudo em uma única linha.

    Args:
        query: Query SQL com múltiplas linhas.

    Returns:
        Query em linha única com espaços únicos.
    """
    query = textwrap.dedent(query)
    return " ".join([line.strip() for line in query.split("\n")])


def remove_tabs_from_query(query: str) -> str:
    """Remove tabs e espaços múltiplos de uma query SQL.

    Normaliza todos os espaços em branco para espaços únicos.

    Args:
        query: Query SQL a ser limpa.

    Returns:
        Query normalizada com espaços únicos.
    """
    query = query_to_line(query)
    return re.sub(r"\s+", " ", query).strip()


def untuple_clocks(clocks):
    """Extrai clocks de uma lista que pode conter tuplas.

    Se o elemento for tupla, extrai o primeiro elemento; caso contrário,
    mantém o elemento original.

    Args:
        clocks: Lista de clocks que podem estar em tuplas.

    Returns:
        Lista de clocks extraídos.
    """
    return [clock[0] if isinstance(clock, tuple) else clock for clock in clocks]


def load_ruamel():
    """Configura e retorna parser YAML ruamel com formatação padronizada.

    Define configurações de indentação e estilo para leitura/escrita de YAML.

    Returns:
        Instância configurada de ruamel.yaml.YAML.
    """
    ruamel = ryaml.YAML()
    ruamel.default_flow_style = False
    ruamel.top_level_colon_align = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    return ruamel


def load_yaml_file(filepath: str) -> dict:
    """Carrega arquivo YAML e retorna como dicionário.

    Args:
        filepath: Caminho do arquivo YAML.

    Returns:
        Dicionário com o conteúdo do arquivo YAML.
    """
    ruamel = load_ruamel()
    return ruamel.load((Path(filepath)).open(encoding="utf-8"))
