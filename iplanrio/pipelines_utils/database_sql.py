# -*- coding: utf-8 -*-
"""
Database definitions for SQL pipelines.
"""

from abc import ABC, abstractmethod
from typing import List

import cx_Oracle
import psycopg2
import pymongo
import pymysql.cursors
import pyodbc


class Database(ABC):
    """
    Database abstract class.
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
        """
        Initializes the database.

        Args:
            hostname: The hostname of the database.
            port: The port of the database.
            user: The username of the database.
            password: The password of the database.
            database: The database name.
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
        """
        Connect to the database.
        """

    @abstractmethod
    def get_cursor(self):
        """
        Returns a cursor for the database.
        """

    @abstractmethod
    def execute_query(self, query: str) -> None:
        """
        Execute query on the database.

        Args:
            query: The query to execute.
        """

    @abstractmethod
    def get_columns(self) -> List[str]:
        """
        Returns the column names of the database.
        """

    @abstractmethod
    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the database.
        """

    @abstractmethod
    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the database.
        """


class SqlServer(Database):
    """
    SQL Server database.
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
        """
        Initializes the SQL Server database.

        Args:
            hostname: The hostname of the SQL Server.
            port: The port of the SQL Server.
            user: The username of the SQL Server.
            password: The password of the SQL Server.
            database: The database name.
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
        Connect to the SQL Server.
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
    """
    MySQL database.
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
        """
        Initializes the MySQL database.

        Args:
            hostname: The hostname of the database.
            port: The port of the database.
            user: The username of the database.
            password: The password of the database.
            database: The database name.
            charset: The charset of the database. Default is utf8mb4.
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
    """
    Oracle Database
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
        """
        Initializes the Oracle database.

        Args:
            hostname: The hostname of the database.
            port: The port of the database.
            user: The username of the database.
            password: The password of the database.
            database: The database name.
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
        Connect to the Oracle.
        """
        return cx_Oracle.connect(
            f"{self._user}/{self._password}@"
            f"{self._hostname}:{self._port}/{self._database}"
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
    """
    PostgreSQL database.
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
        """
        Initializes the PostgreSQL database.

        Args:
            hostname: The hostname of the database.
            port: The port of the database.
            user: The username of the database.
            password: The password of the database.
            database: The database name.
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


class MongoDB(Database):
    """
    MongoDB database. Compatible with MongoDB 3.4+
    """

    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        port: int = 27017,
        **kwargs,
    ) -> None:
        """
        Initializes the MongoDB database.

        Args:
            hostname: The hostname of the database.
            port: The port of the database.
            user: The username of the database.
            password: The password of the database.
            database: The database name.
        """
        port = port if isinstance(port, int) else int(port)
        self._collection_name = None
        self._documents = []
        self._current_index = 0
        self._columns = []
        super().__init__(
            hostname,
            port,
            user,
            password,
            database,
        )

    def connect(self):
        """
        Connect to the MongoDB.
        """
        connection_string = (
            f"mongodb://{self._user}:{self._password}@"
            f"{self._hostname}:{self._port}/{self._database}"
        )
        client = pymongo.MongoClient(connection_string)
        return client[self._database]

    def get_cursor(self):
        """
        Returns a cursor for the MongoDB (the database object itself).
        """
        return self._connection

    def execute_query(self, query: str) -> None:
        """
        Execute query on the MongoDB.
        For MongoDB, the 'query' parameter should be the collection name. Example: 'FILES.files' or 'FILES.chunks'

        Args:
            query: The collection name to query (format: database.collection or just collection).
        """
        collection_name = query.strip()

        # If the query contains a dot, it's in format "database.collection"
        if "." in collection_name:
            collection_name = collection_name.split(".")[-1]

        self._collection_name = collection_name
        # Get the collection
        collection = self._cursor[collection_name]
        # Fetch all documents from the collection
        self._documents = list(collection.find())

        # Determine columns based on keys in documents
        if self._documents:
            all_keys = set()
            for doc in self._documents:
                all_keys.update(doc.keys())
            self._columns = sorted(list(all_keys))
        else:
            self._columns = []

        # Reset index for fetching
        self._current_index = 0

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the MongoDB collection.
        """
        return self._columns

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of documents from the MongoDB collection.
        Converts documents to lists based on column order.
        """
        batch = []
        end_index = min(self._current_index + batch_size, len(self._documents))

        for i in range(self._current_index, end_index):
            doc = self._documents[i]
            # Convert document to list based on column order
            row = []
            for col in self._columns:
                value = doc.get(col)
                # Convert ObjectId to string for compatibility
                if isinstance(value, pymongo.objectid.ObjectId):
                    value = str(value)
                # Convert Binary data to string representation
                elif isinstance(value, bytes):
                    value = value.hex()
                row.append(value)
            batch.append(row)

        self._current_index = end_index
        return batch

    def fetch_all(self) -> List[List]:
        """
        Fetches all remaining documents from the MongoDB collection.
        """
        return self.fetch_batch(len(self._documents) - self._current_index)
