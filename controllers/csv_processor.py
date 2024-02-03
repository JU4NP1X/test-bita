import os
import pandas as pd
from tqdm import tqdm
from view.Command_Parser import Command_Interface
from models.Postgres_Batch_Insert import Postgres_Batch_Insert
from pandas.api.types import is_numeric_dtype


def csv_processor():
    command_parser = Command_Interface()
    command_parser.parse_command()
    string_columns = os.getenv("STRING_COLUMNS", "Product").split(",")
    date_columns = os.getenv("DATE_COLUMNS", "Date,CreatedAt").split(",")

    (path, host, dbname, tname, port, user, password, batch_size, cmpd_primary_key) = (
        command_parser.get_args_values()
    )

    total_rows = sum(1 for _ in open(path))  # Get the total number of rows in the file

    db = Postgres_Batch_Insert(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        batch_size=batch_size,
    )
    is_first = True
    # Create a progress bar using tqdm
    with tqdm(total=total_rows) as pbar:
        for df in pd.read_csv(path, chunksize=batch_size, sep=";"):
            if is_first:
                is_first = False
                columns = df.columns.tolist()
                #
                data_types = [
                    (
                        "VARCHAR"
                        if col in string_columns
                        else "DATE" if col in date_columns else "BIGINT"
                    )
                    for col in columns
                ]
                # create the table if not exist
                db.create_table_if_not_exist(
                    tname, columns, data_types, cmpd_primary_key
                )
                # create the query with the column names if first iteration
                db.set_insert_query(
                    f"INSERT INTO {tname} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in range(len(columns))])}) ON CONFLICT DO NOTHING"
                )

            db.batch_insert(df)
            pbar.update(len(df))
