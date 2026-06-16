# -*- coding: utf-8 -*-
"""Utilitários para logging integrado com Prefect.

Fornece funções para registrar mensagens nos diferentes níveis de log
(debug, info, warning, error, critical) com formatação padronizada e
suporte a logging condicional baseado em frequência.
"""
import logging
from typing import Any

import prefect


def log(msg: Any, level: str = "info") -> None:
    """Registra mensagem no logger do Prefect com formatação padronizada.

    Adiciona indentação e separadores visuais à mensagem antes de logar.

    Args:
        msg: Mensagem a ser logada (qualquer tipo, será convertido para string).
        level: Nível de log ("debug", "info", "warning", "error", "critical").

    Raises:
        ValueError: Se level não for um nível válido.
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
    """Registra mensagem apenas quando índice é múltiplo do módulo.

    Útil para logging em loops, evitando logs excessivos ao registrar
    apenas a cada N iterações.

    Args:
        msg: Mensagem a ser logada.
        level: Nível de log.
        index: Índice atual da iteração.
        mod: Módulo para determinar quando logar (loga se index % mod == 0).
    """
    if index % mod == 0 or index == 0:
        log(msg=f"iteration {index}:\n {msg}", level=level)
