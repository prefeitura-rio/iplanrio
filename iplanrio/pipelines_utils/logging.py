# -*- coding: utf-8 -*-
"""Logging unificado para todos os pipelines do workspace.

Todo módulo (flow, task ou utilitário) deve obter seu logger via :func:`get_logger`
em vez de chamar diretamente ``logging.getLogger`` ou ``prefect.get_run_logger``.

## Como os logs chegam à UI do Prefect

O Prefect captura logs de loggers externos via a variável de ambiente
``PREFECT_LOGGING_EXTRA_LOGGERS`` (definida globalmente, no Infisical). Para cada
logger listado, o Prefect adiciona seus handlers (que enviam logs para a API).
Todo logger filho de um logger listado propaga automaticamente para cima.

``get_logger`` devolve um ``logging.Logger`` padrão, com ``propagate=True``, cujo
nome sempre reflete o caminho do módulo (ex: ``pipelines.rj_sec__pipe.flow``),
de modo que basta listar as raízes (``pipelines``, ``iplanrio``, ...) na variável.

## Nível

O nível vem de ``PREFECT_LOGGING_LEVEL``. Se a variável não existir (ou for
inválida), usa ``DEBUG``.

## Uso

Em qualquer módulo — flow, tasks ou utils::

    from iplanrio.pipelines_utils.logging import get_logger

    logger = get_logger(__name__)

    logger.info("Processando %d registros", count)
    logger.warning("Tentando novamente após erro transitório")
    logger.error("Falha no upload: %s", error)

O Prefect carrega ``flow.py`` a partir do caminho do arquivo, então ``__name__``
ali é só ``"flow"`` (ou ``"__main__"``). Nesses casos o nome é descoberto
automaticamente a partir do caminho do arquivo chamador
(``.../pipelines/rj_sec__pipe/flow.py`` -> ``pipelines.rj_sec__pipe.flow``).
"""

import logging
import sys
from logging import Logger
from pathlib import Path
from typing import Any, Optional

import prefect

LEVEL_ENV_VAR = "PREFECT_LOGGING_LEVEL"
DEFAULT_LEVEL = logging.DEBUG
PIPELINES_DIR = "pipelines"

_FALLBACK_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s - %(message)s"
_FALLBACK_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _level_from_env() -> int:
    """Lê o nível de log de ``PREFECT_LOGGING_LEVEL``; ``DEBUG`` se ausente/inválido.

    ``env`` importa este módulo, por isso é importado aqui dentro e não no topo.
    Consequência: ``get_logger`` não pode ser chamado em nível de módulo dentro de
    ``env.py`` (o módulo ainda estaria incompleto); chame-o dentro das funções.
    """
    from iplanrio.pipelines_utils.env import getenv_or_action

    value = str(getenv_or_action(LEVEL_ENV_VAR, default="", action="ignore"))
    return logging.getLevelNamesMapping().get(value.strip().upper(), DEFAULT_LEVEL)


def _name_from_file(filename: str) -> Optional[str]:
    """Converte o caminho de um arquivo em nome de módulo, a partir de ``pipelines/``.

    ``/app/pipelines/rj_sec__pipe/flow.py`` -> ``pipelines.rj_sec__pipe.flow``.
    Retorna ``None`` se o caminho não estiver dentro de um diretório ``pipelines``.
    """
    parts = Path(filename).with_suffix("").parts
    if PIPELINES_DIR not in parts:
        return None
    start = len(parts) - 1 - parts[::-1].index(PIPELINES_DIR)
    return ".".join(parts[start:])


def _resolve_name(name: Optional[str], caller_globals: dict[str, Any]) -> str:
    """Decide o nome do logger.

    Nomes com pacote (``pipelines.x.tasks``) são mantidos. Nomes soltos — ``None``,
    ``"__main__"`` ou o stem do arquivo carregado pelo Prefect (``"flow"``) — são
    substituídos pelo nome derivado do arquivo do chamador, quando possível.
    """
    if name and "." in name:
        return name
    derived = _name_from_file(caller_globals.get("__file__", ""))
    return derived or name or caller_globals.get("__name__", "root")


def get_logger(name: Optional[str] = None) -> Logger:
    """Retorna o logger do módulo chamador.

    Passe ``__name__``. Quando o módulo é carregado pelo Prefect como script
    (``__name__`` igual a ``"__main__"`` ou ``"flow"``), o nome correto é
    descoberto pelo caminho do arquivo; ``get_logger()`` sem argumento também
    funciona.

    O nível vem de ``PREFECT_LOGGING_LEVEL`` (padrão ``DEBUG``). Os handlers que
    enviam os logs à UI são instalados pelo Prefect via
    ``PREFECT_LOGGING_EXTRA_LOGGERS``.

    Fora de contexto Prefect (testes, scripts), quando nenhum logger da hierarquia
    tem handler, adiciona um :class:`logging.StreamHandler` com formato legível.

    :param name: Nome do módulo — normalmente ``__name__``.
    :returns: Uma instância de :class:`logging.Logger`.
    """
    logger = logging.getLogger(_resolve_name(name, sys._getframe(1).f_globals))
    logger.setLevel(_level_from_env())

    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(fmt=_FALLBACK_FORMAT, datefmt=_FALLBACK_DATE_FORMAT)
        )
        logger.addHandler(handler)

    return logger


def log(msg: Any, level: str = "info") -> None:
    """
    Logs a message to prefect's logger.
    """
    levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    blank_spaces = 4 * " "
    msg = blank_spaces + "----\n" + str(msg)
    msg = "\n".join([blank_spaces + line for line in msg.split("\n")]) + "\n\n"

    if level not in levels:
        raise ValueError(f"Invalid log level: {level}")
    logger = prefect.get_run_logger()
    logger.log(level=levels[level], msg=msg)


def log_mod(msg: Any, level: str = "info", index: int = 0, mod: int = 1):
    """
    Only logs a message if the index is a multiple of mod.
    """
    if index % mod == 0 or index == 0:
        log(msg=f"iteration {index}:\n {msg}", level=level)
