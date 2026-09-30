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

import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging import Logger
from pathlib import Path
from typing import Any, Optional

import prefect

LEVEL_ENV_VAR = "PREFECT_LOGGING_LEVEL"
DEFAULT_LEVEL = logging.DEBUG
PIPELINES_DIR = "pipelines"
ENVIRONMENT_ENV_VAR = "PREFECT_LOGGING_ENVIRONMENT"

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


# Atributos que todo LogRecord tem; o resto é "campo extra" (``extra={...}``).
_STANDARD_RECORD_ATTRS = frozenset(logging.makeLogRecord({}).__dict__) | {
    "message",
    "asctime",
    "taskName",
}


def _pipeline_from_logger_name(name: str) -> Optional[str]:
    """``pipelines.rj_sec__pipe.tasks`` -> ``rj_sec__pipe``."""
    parts = name.split(".")
    if parts[0] == PIPELINES_DIR and len(parts) > 1:
        return parts[1]
    return None


class RunContextFilter(logging.Filter):
    """Anexa ao record o ambiente, o pipeline e os dados do flow/task run.

    Do run: nomes, IDs, tentativa (``*_run_count``), ``deployment_id``,
    ``deployment_version`` (tag da imagem) e ``work_pool_name``. Valores ausentes
    (ex.: run sem deployment) não são anexados.

    O ambiente (``staging``/``prod``) vem de ``PREFECT_LOGGING_ENVIRONMENT``, definida
    no Infisical; sem a variável, o campo ``environment`` não é anexado.

    Funciona fora de contexto (não anexa nada do run e nunca levanta erro). Campos
    já presentes no record — como os dos loggers de run do Prefect — são mantidos.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        from prefect.context import FlowRunContext, TaskRunContext

        fields: dict[str, Any] = {}
        if environment := os.environ.get(ENVIRONMENT_ENV_VAR):
            fields["environment"] = environment
        if pipeline := _pipeline_from_logger_name(record.name):
            fields["pipeline"] = pipeline
        if (flow_ctx := FlowRunContext.get()) and flow_ctx.flow_run:
            flow_run = flow_ctx.flow_run
            fields["flow_name"] = flow_ctx.flow.name if flow_ctx.flow else None
            fields["flow_run_id"] = str(flow_run.id)
            fields["flow_run_name"] = flow_run.name
            fields["flow_run_count"] = flow_run.run_count
            fields["deployment_id"] = str(flow_run.deployment_id or "") or None
            fields["deployment_version"] = flow_run.deployment_version
            fields["work_pool_name"] = flow_run.work_pool_name
        if (task_ctx := TaskRunContext.get()) and task_ctx.task_run:
            fields["task_name"] = task_ctx.task.name if task_ctx.task else None
            fields["task_run_id"] = str(task_ctx.task_run.id)
            fields["task_run_name"] = task_ctx.task_run.name
            fields["task_run_count"] = task_ctx.task_run.run_count
        for key, value in fields.items():
            if value is not None:
                record.__dict__.setdefault(key, value)
        return True


# Início dos flow runs (segundos), para calcular a duração no log final.
_FLOW_RUN_START: dict[str, float] = {}

_TASK_LOGGER = "prefect.task_runs"
_FLOW_LOGGER = "prefect.flow_runs"


def _finish_fields(record: logging.LogRecord) -> dict[str, Any]:
    """Campos do log de término de um task/flow run (a mensagem do próprio Prefect).

    Transforma ``Finished in state Completed()`` em um evento resumo: ``event``
    (``task_finished``/``flow_finished``), ``state``, ``outcome`` e, para flows,
    ``duration_ms``.
    """
    if record.name not in (_TASK_LOGGER, _FLOW_LOGGER):
        return {}
    message = record.getMessage()
    is_task = record.name == _TASK_LOGGER
    run_id = str(getattr(record, "task_run_id" if is_task else "flow_run_id", ""))

    if not is_task and message.startswith("Beginning flow run"):
        _FLOW_RUN_START[run_id] = record.created
        return {}
    if not message.startswith("Finished in state"):
        return {}

    fields: dict[str, Any] = {
        "event": "task_finished" if is_task else "flow_finished",
        "state": message.removeprefix("Finished in state ").split("(")[0].strip(),
        "outcome": "success" if record.levelno < logging.ERROR else "error",
    }
    if (start := _FLOW_RUN_START.pop(run_id, None)) is not None and not is_task:
        fields["duration_ms"] = round((record.created - start) * 1000)
    return fields


class StructuredFormatter(logging.Formatter):
    """Formata o record como uma linha JSON com esquema fixo.

    Campos: ``timestamp`` (UTC, ISO 8601), ``severity_text``, ``body``, ``logger``,
    os campos de contexto anexados por :class:`RunContextFilter`, os campos extras
    e, se houver exceção, ``exc`` com ``type``, ``message`` e ``traceback``.

    O log de término de cada task/flow run do Prefect (``Finished in state ...``)
    vira um evento resumo automático: ``event``, ``state``, ``outcome`` e, nos
    flows, ``duration_ms``.

    Uso (Infisical)::

        PREFECT_LOGGING_FORMATTERS_JSON_CLASS=iplanrio.pipelines_utils.logging.StructuredFormatter
        PREFECT_LOGGING_HANDLERS_CONSOLE_FORMATTER=json

    Afeta só o console (stdout do pod); a UI do Prefect continua recebendo a mensagem.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "severity_text": record.levelname,
            "body": record.getMessage(),
            "logger": record.name,
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_ATTRS and key not in payload:
                payload[key] = value
        for key, value in _finish_fields(record).items():
            payload.setdefault(key, value)
        if record.exc_info and record.exc_info[0] is not None:
            exc_type, exc, _ = record.exc_info
            payload["exc"] = {
                "type": exc_type.__name__,
                "message": str(exc),
                "traceback": self.formatException(record.exc_info),
            }
        return json.dumps(payload, default=str, ensure_ascii=False)


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

    if not any(isinstance(f, RunContextFilter) for f in logger.filters):
        logger.addFilter(RunContextFilter())

    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(fmt=_FALLBACK_FORMAT, datefmt=_FALLBACK_DATE_FORMAT)
        )
        logger.addHandler(handler)

    return logger


_DEPRECATION_WARNED_FILES: set[str] = set()


def _warn_log_deprecated() -> None:
    """Avisa, uma vez por arquivo chamador, que ``log()`` está deprecated."""
    caller = sys._getframe(2)
    filename = caller.f_code.co_filename
    if filename in _DEPRECATION_WARNED_FILES:
        return
    _DEPRECATION_WARNED_FILES.add(filename)
    prefect.get_run_logger().warning(
        "DEPRECATED: `log()` de iplanrio.pipelines_utils.logging será removido. "
        "Migre para `logger = get_logger(__name__)` e use `logger.info(...)`. "
        "Chamado em %s:%d",
        filename,
        caller.f_lineno,
    )


def log(msg: Any, level: str = "info") -> None:
    """
    Logs a message to prefect's logger.

    .. deprecated::
        Use ``logger = get_logger(__name__)`` e ``logger.info(...)``.
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
    _warn_log_deprecated()
    logger = prefect.get_run_logger()
    logger.log(level=levels[level], msg=msg)


def log_mod(
    msg: Any,
    *,
    logger: Logger,
    level: str = "info",
    index: int = 0,
    mod: int = 1,
) -> None:
    """
    Só loga a mensagem se ``index`` for múltiplo de ``mod`` (ou for 0).

    :param msg: Mensagem a logar.
    :param logger: Logger do módulo chamador, obtido com :func:`get_logger`.
    :param level: Nível do log (``debug``, ``info``, ``warning``, ``error``, ``critical``).
    :param index: Índice da iteração atual.
    :param mod: Loga a cada ``mod`` iterações.
    """
    if index % mod == 0 or index == 0:
        logger.log(
            logging.getLevelNamesMapping()[level.upper()],
            "iteration %d:\n %s",
            index,
            msg,
        )
