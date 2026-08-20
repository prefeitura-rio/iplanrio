# -*- coding: utf-8 -*-
"""Classes abstratas e implementações para conexão com bancos de dados SQL.

Fornece interfaces unificadas para interação com diferentes bancos de dados:
SQL Server, MySQL, Oracle e PostgreSQL. Implementa operações comuns como
execução de queries, busca em lote e extração de metadados de colunas.
"""
from abc import ABC, abstractmethod
from typing import List

import psycopg2
import pymysql.cursors
import pyodbc


class Database(ABC):
    """Classe abstrata base para conexão com bancos de dados SQL.

    Define interface comum para operações em diferentes bancos de dados,
    incluindo conexão, execução de queries e busca de dados.
    """

    def __init__(
        self,
        hostname: str,
        port: int,
        user: str,
        password: str,
        database: str,
        **kwargs,
    ) -> None:
        """Inicializa a conexão com o banco de dados.

        Args:
            hostname: Nome ou IP do servidor do banco.
            port: Porta de conexão.
            user: Nome de usuário.
            password: Senha de acesso.
            database: Nome do banco de dados.
            **kwargs: Argumentos adicionais específicos do banco.
        """
        self._hostname = hostname
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._connection = self.connect()
        self._cursor = self.get_cursor()

    @abstractmethod
    def connect(self):
        """Estabelece conexão com o banco de dados.

        Returns:
            Objeto de conexão específico do banco.
        """

    @abstractmethod
    def get_cursor(self):
        """Retorna cursor para execução de queries.

        Returns:
            Objeto cursor específico do banco.
        """

    @abstractmethod
    def execute_query(self, query: str) -> None:
        """Executa query SQL no banco de dados.

        Args:
            query: Query SQL a ser executada.
        """

    @abstractmethod
    def get_columns(self) -> List[str]:
        """Retorna nomes das colunas do resultado da última query.

        Returns:
            Lista com nomes das colunas.
        """

    @abstractmethod
    def fetch_batch(self, batch_size: int) -> List[List]:
        """Busca um lote de linhas do resultado da query.

        Args:
            batch_size: Número de linhas a buscar.

        Returns:
            Lista de listas representando as linhas.
        """

    @abstractmethod
    def fetch_all(self) -> List[List]:
        """Busca todas as linhas do resultado da query.

        Returns:
            Lista de listas representando todas as linhas.
        """


class SqlServer(Database):
    """Implementação para Microsoft SQL Server.

    Usa ODBC Driver 17 para conexão com SQL Server.
    """

    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        port: int = 1433,
        **kwargs,
    ) -> None:
        """Inicializa conexão com SQL Server.

        Args:
            hostname: Nome ou IP do servidor SQL Server.
            user: Nome de usuário.
            password: Senha de acesso.
            database: Nome do banco de dados.
            port: Porta de conexão (padrão: 1433).
            **kwargs: Argumentos adicionais.
        """
        super().__init__(
            hostname,
            port,
            user,
            password,
            database,
        )

    def connect(self):
        """Estabelece conexão com SQL Server via ODBC.

        Returns:
            Objeto de conexão pyodbc.
        """
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self._hostname},{self._port};"
            f"DATABASE={self._database};"
            f"UID={self._user};"
            f"PWD={self._password};"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
        return pyodbc.connect(conn_str, timeout=300)

    def get_cursor(self):
        """
        Returns a cursor for the SQL Server.
        """
        return self._connection.cursor()

    def execute_query(self, query: str) -> None:
        """
        Execute query on the SQL Server.

        Args:
            query: The query to execute.
        """
        self._cursor.execute(query)

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the SQL Server.
        """
        return [column[0] for column in self._cursor.description]

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the SQL Server.
        """
        return [list(item) for item in self._cursor.fetchmany(batch_size)]

    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the SQL Server.
        """
        return [list(item) for item in self._cursor.fetchall()]


class MySql(Database):
    """Implementação para MySQL/MariaDB.

    Usa pymysql para conexão com MySQL.
    """

    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
        charset: str = None,
        **kwargs,
    ) -> None:
        """Inicializa conexão com MySQL.

        Args:
            hostname: Nome ou IP do servidor MySQL.
            user: Nome de usuário.
            password: Senha de acesso.
            database: Nome do banco de dados.
            port: Porta de conexão (padrão: 3306).
            charset: Charset do banco (padrão: utf8mb4).
            **kwargs: Argumentos adicionais.
        """
        port = port if isinstance(port, int) else int(port)
        self._charset = charset or "utf8mb4"
        super().__init__(
            hostname,
            port,
            user,
            password,
            database,
        )

    def connect(self):
        """
        Connect to the MySQL.
        """
        return pymysql.connect(
            host=self._hostname,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset=self._charset,
        )

    def get_cursor(self):
        """
        Returns a cursor for the MySQL.
        """
        return self._connection.cursor()

    def execute_query(self, query: str) -> None:
        """
        Execute query on the MySQL.

        Args:
            query: The query to execute.
        """
        self._cursor.execute(query)

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the MySQL.
        """
        return [column[0] for column in self._cursor.description]

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the MySQL.
        """
        return [list(item) for item in self._cursor.fetchmany(batch_size)]

    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the MySQL.
        """
        return [list(item) for item in self._cursor.fetchall()]


class Oracle(Database):
    """Implementação para Oracle Database.

    Usa oracledb (lazy import) para conexão com Oracle.
    """

    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        port: int = 1521,
        **kwargs,
    ) -> None:
        """Inicializa conexão com Oracle.

        Args:
            hostname: Nome ou IP do servidor Oracle.
            user: Nome de usuário.
            password: Senha de acesso.
            database: Nome do serviço/SID.
            port: Porta de conexão (padrão: 1521).
            **kwargs: Argumentos adicionais.
        """
        super().__init__(
            hostname,
            port,
            user,
            password,
            database,
        )

    def connect(self):
        """Estabelece conexão com Oracle Database.

        Usa lazy import do oracledb para evitar erros quando suporte
        Oracle não é necessário.

        Returns:
            Objeto de conexão oracledb.
        """
        import oracledb  # Lazy import - only loaded when Oracle is used

        return oracledb.connect(
            user=self._user,
            password=self._password,
            dsn=f"{self._hostname}:{self._port}/{self._database}",
        )

    def get_cursor(self):
        """
        Returns a cursor for the Oracle.
        """
        return self._connection.cursor()

    def execute_query(self, query: str) -> None:
        """
        Execute query on the Oracle.

        Args:
            query: The query to execute.
        """
        self._cursor.execute(query)

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the Oracle.
        """
        return [column[0] for column in self._cursor.description]

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the Oracle.
        """
        return [list(item) for item in self._cursor.fetchmany(batch_size)]

    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the Oracle.
        """
        return [list(item) for item in self._cursor.fetchall()]


class Postgres(Database):
    """Implementação para PostgreSQL.

    Usa psycopg2 para conexão com PostgreSQL.
    """

    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        port: int = 5432,
        **kwargs,
    ) -> None:
        """Inicializa conexão com PostgreSQL.

        Args:
            hostname: Nome ou IP do servidor PostgreSQL.
            user: Nome de usuário.
            password: Senha de acesso.
            database: Nome do banco de dados.
            port: Porta de conexão (padrão: 5432).
            **kwargs: Argumentos adicionais.
        """
        super().__init__(
            hostname,
            port,
            user,
            password,
            database,
        )

    def connect(self):
        """
        Connect to the PostgreSQL database.
        """
        return psycopg2.connect(
            host=self._hostname,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
        )

    def get_cursor(self):
        """
        Returns a cursor for the PostgreSQL database.
        """
        return self._connection.cursor()

    def execute_query(self, query: str) -> None:
        """
        Execute query on the PostgreSQL database.

        Args:
            query: The query to execute.
        """
        self._cursor.execute(query)

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the PostgreSQL database.
        """
        return [desc[0] for desc in self._cursor.description]

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the PostgreSQL database.
        """
        return self._cursor.fetchmany(batch_size)

    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the PostgreSQL database.
        """
        return self._cursor.fetchall()
