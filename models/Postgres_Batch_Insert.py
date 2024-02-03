"""
This module contains a class that allows you to batch insert data into a PostgreSQL database.
"""

import os
import psycopg2
import atexit
from pandas import DataFrame


class Postgres_Batch_Insert:
    buffer = []
    query = None
    posible_date_columns = os.getenv("DATE_COLUMNS", "Date,CreatedAt").split(",")

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
    def __del__(self):
        """
        Destructor method to close the connection.
        """
        self.close_connection()

    def _send_data(self):
        self.buffer = [tuple(element.tolist()) for element in self.buffer]
        self.cursor.executemany(self.query, [tuple(element) for element in self.buffer])
        self.buffer = []

    def _q_build_condition(self, column_names) -> str:
        """
        Builds a condition string for a SQL query based on the given column names.

        Args:
            column_names (list[str]): A list of column names.

        Returns:
            str: The condition string for the SQL query.
        """
        return " AND ".join(
            (
                f"DATE(t.{col}) = exist_tbl.{col}"
                if col in self.posible_date_columns
                else f"t.{col} = exist_tbl.{col}"
            )
            for col in column_names
        )

    def _q_build_values(self, data_values) -> str:
        """
        get the data_strings and return a string with the structure (value), (value), ...
        Parameters:
        - `data_str`: A string containing the data.

        Returns:
        - str: The concatenated string.
        """

        return f"({'),('.join(data_values)})"

    def create_table_if_not_exist(
        self, table_name: str, column_names: list, data_types, cmpd_primary_key: list
    ) -> None:
        """
        Create a table if it does not exist.

        Args:
            table_name (str): The name of the table to be created.
            column_names (list): A list of column names.
            cmpd_primary_key (list): A list of the compouned primary key columns
        """
        # verify if table exist

        columns = [
            f"{column_names[i]} {data_types[i]} NOT NULL"
            for i in range(len(data_types))
        ]

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(columns)}, PRIMARY KEY ({','.join(cmpd_primary_key)}))"
        )
        # Commit the transaction
        self.cursor = self.connection.cursor()

        print(f"Table {table_name} created successfully.")

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
        data_values = (
            data[column_names].astype(str).apply(lambda x: ",".join(x), axis=1)
        )

        # Get the list of keys that do not exist in the table
        query = f"SELECT t.* FROM (VALUES {self._q_build_values(data_values)}) AS t ({','.join(column_names)}) WHERE NOT EXISTS (SELECT 1 FROM {table_name} exist_tbl WHERE {self._q_build_condition(column_names)})"
        self.query2 = query
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

    def batch_insert(self, df) -> None:
        data_list = list(df.to_records(index=False))
        while len(self.buffer) + len(data_list) > self.batch_size:
            # Extend the buffer to accommodate the new data but not exceed the batch size
            diff = self.batch_size - len(self.buffer)
            self.buffer.extend(data_list[:diff])
            # Trim the new data to the remaining space
            self._send_data()
            data_list = data_list[diff:]

        self.buffer.extend(data_list)

    def close_connection(self) -> None:
        try:
            if len(self.buffer) > 0:
                self._send_data()
        finally:
            self.cursor.close()
            self.connection.commit()
            self.connection.close()
