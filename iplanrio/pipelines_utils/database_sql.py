# -*- coding: utf-8 -*-
"""
Database definitions for SQL pipelines.
"""

import base64
import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

import cx_Oracle
import psycopg2
import pymongo
import pymysql.cursors
import pyodbc
from bson import ObjectId


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
        auth_source: str = "admin",
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
            auth_source: The authentication source database. Defaults to "admin".
        """
        port = port if isinstance(port, int) else int(port)
        self._mongo_cursor = None
        self._columns = []
        self._auth_source = auth_source
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
            f"?authSource={self._auth_source}"
        )
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.server_info()
        return client[self._database]

    def get_cursor(self):
        """
        Returns a cursor for the MongoDB.
        """
        return self._connection

    def execute_query(self, query: str) -> None:
        """
        Execute query on the MongoDB.

        Args:
            query: The collection name to query. Optionally include a filter using pipe separator:
                   "COLLECTION" or "COLLECTION|{filter}"
                   Example: "FILES.chunks|{\"files_id\": \"xxx\"}"
        """
        query = query.strip()

        # Parse collection name and optional filter
        if "|" in query:
            collection_name, filter_str = query.split("|", 1)
            collection_name = collection_name.strip()
            filter_str = filter_str.strip()
            # Parse JSON filter
            import json

            mongo_filter = json.loads(filter_str)
            # Auto-convert 24-char hex strings to ObjectId for fields ending with _id
            for key, value in mongo_filter.items():
                if not key.endswith("_id"):
                    continue

                # Simple value: "files_id": "xxx"
                if isinstance(value, str) and len(value) == 24:
                    try:
                        mongo_filter[key] = ObjectId(value)
                    except Exception:
                        pass

                # Operator with array: "files_id": {"$in": ["xxx", "yyy"]}
                elif isinstance(value, dict):
                    for op_key, op_value in value.items():
                        if isinstance(op_value, list):
                            converted_list = []
                            for item in op_value:
                                if isinstance(item, str) and len(item) == 24:
                                    try:
                                        converted_list.append(ObjectId(item))
                                    except Exception:
                                        converted_list.append(item)
                                else:
                                    converted_list.append(item)
                            value[op_key] = converted_list
        else:
            collection_name = query
            mongo_filter = {}

        collection = self._cursor[collection_name]

        # Get sample docs with filter
        sample_docs = list(collection.find(mongo_filter).limit(100))
        if sample_docs:
            all_keys = set()
            for doc in sample_docs:
                all_keys.update(doc.keys())
            self._columns = sorted(list(all_keys))
        else:
            self._columns = []

        # Create cursor with filter
        self._mongo_cursor = collection.find(mongo_filter)

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the MongoDB collection.
        """
        return self._columns

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of documents from the MongoDB collection.
        """
        batch = []
        try:
            for _ in range(batch_size):
                doc = next(self._mongo_cursor)
                row = []
                for col in self._columns:
                    value = doc.get(col)
                    if isinstance(value, ObjectId):
                        value = str(value)
                    elif isinstance(value, bytes):
                        value = base64.b64encode(value).decode("utf-8")
                    elif isinstance(value, datetime):
                        value = value.isoformat()
                    elif isinstance(value, (dict, list)):
                        value = json.dumps(value, default=str)
                    elif value is not None and not isinstance(
                        value, (str, int, float, bool)
                    ):
                        # Convert any other non-basic type (Decimal128, Timestamp, etc.) to string
                        value = str(value)
                    row.append(value)
                batch.append(row)
        except StopIteration:
            pass
        return batch

    def fetch_all(self) -> List[List]:
        """
        Fetches all remaining documents from the MongoDB collection.
        """
        all_rows = []
        while True:
            batch = self.fetch_batch(1000)
            if not batch:
                break
            all_rows.extend(batch)
        return all_rows
