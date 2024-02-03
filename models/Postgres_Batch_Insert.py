"""
This module contains a class that allows you to batch insert data into a PostgreSQL database.
"""

import psycopg2
import atexit
from pandas import DataFrame


class Postgres_Batch_Insert:
    buffer = []
    query = None

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        batch_size: int,
    ):
        """
        Initialize the connection parameters for the database and establish a connection.

        Parameters:
            host (str): The host of the database.
            port (int): The port of the database.
            dbname (str): The name of the database.
            user (str): The username for the database connection.
            password (str): The password for the database connection.
            query (str): The query to be executed.
            batch_size (int): The batch size for processing data.

        Returns:
            None
        """
        self.host = host
        self.port = port
        self.batch_size = batch_size
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self.cursor = self.connection.cursor()

        # Register the close_connection function to be called when the program exits
        atexit.register(self.close_connection)

    def __del__(self):
        """
        Destructor method to close the connection.
        """
        self.close_connection()

    def _send_data(self):
        self.cursor.executemany(self.query, self.buffer)
        self.connection.commit()
        self.buffer = []

    def _q_build_condition(self, column_names) -> str:
        """
        Builds a condition string for a SQL query based on the given column names.

        Args:
            column_names (list[str]): A list of column names.

        Returns:
            str: The condition string for the SQL query.
        """
        return " AND ".join(f"t.{col} = exist_tbl.{col}" for col in column_names)

    def _q_build_values(self, data_str) -> str:
        """
        Builds and returns a string by applying a lambda function to each element of `data_str`.
        The lambda function converts each element into a string representation surrounded by
        parentheses and separated by commas. The resulting strings are then concatenated
        together with commas as separators.

        Parameters:
        - `data_str`: A string containing the data.

        Returns:
        - str: The concatenated string.
        """
        return data_str.apply(lambda x: "(" + ",".join(x) + ")").str.cat(sep=",")

    def verify_if_data_no_exist(
        self, data: DataFrame, column_names: list, table_name: str
    ) -> list:
        """
        Verify if the given data does not exist in the specified table.

        Args:
            data (DataFrame): The data to be verified.
            column_names (list): The names of the columns to be checked.
            table_name (str): The name of the table to check against.

        Returns:
            list: A list of keys that do not exist in the table.
        """
        # Convert column values to strings and join them with commas
        data_str = data[column_names].astype(str).apply(lambda x: ",".join(x), axis=1)

        # Get the list of keys that do not exist in the table
        query = f"SELECT t.* FROM (VALUES {self._q_build_values(data_str)}) AS t ({','.join(column_names)}) WHERE NOT EXIST (SELECT * FROM {table_name} exist_tbl WHERE {self._q_build_condition(column_names)})"
        self.cursor.execute(query)

        return self.cursor.fetchall()

    def set_insert_query(self, query: str) -> None:
        """
        Set the insert query for the object.

        Parameters:
            query (str): The insert query to be set.

        Returns:
            None
        """
        self.query = query

    def batch_insert(self, data: DataFrame) -> None:
        while len(self.buffer) + len(data) > self.batch_size:
            # Extend the buffer to accommodate the new data but not exceed the batch size
            diff = self.batch_size - len(self.buffer)
            self.buffer.extend(data[:diff])
            # Trim the new data to the remaining space
            self._send_data()
            data = data[diff:]

        self.buffer.extend(data)

    def close_connection(self) -> None:
        try:
            if len(self.buffer) > 0:
                self._send_data()
        finally:
            self.cursor.close()
            self.connection.close()
