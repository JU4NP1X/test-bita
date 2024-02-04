import os
import pandas as pd
from tqdm import tqdm
from view.Command_Parser import Command_Interface
from models.Postgres_Batch_Insert import Postgres_Batch_Insert
import math
import gc
import multiprocessing as mp
from config.config import config


def csv_processor():
    command_parser = Command_Interface()
    command_parser.parse_command()
    string_columns = config["string_columns"]
    date_columns = config["date_columns"]

    (
        path,
        host,
        dbname,
        tname,
        port,
        user,
        password,
        batch_size,
        cmpd_primary_key,
        threads,
    ) = command_parser.get_args_values()

    threads = threads if threads > 0 else 1

    df = pd.read_csv(path, nrows=1, sep=";")
    columns = df.columns.tolist()

    data_types = [
        (
            "VARCHAR"
            if col in string_columns
            else "DATE" if col in date_columns else "BIGINT"
        )
        for col in columns
    ]
    db = Postgres_Batch_Insert(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        batch_size=batch_size,
    )
    # create the table if not exist
    db.create_table_if_not_exist(tname, columns, data_types, cmpd_primary_key)

    db.close_connection()
    # create the query with the column names if first iteration
    insert_query = f"INSERT INTO {tname} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in range(len(columns))])}) ON CONFLICT DO NOTHING"

    total_rows = sum(1 for _ in open(path))  # Get the total number of rows in the file

    # spawn stop memory redundancy
    size_per_thread = math.ceil(total_rows / threads)
    argsList = [
        (
            i,
            path,
            host,
            dbname,
            port,
            user,
            password,
            size_per_thread,
            batch_size,
            insert_query,
        )
        for i in range(threads)
    ]

    # distribute the work among the threads only at the beginning (to avoid bug of memory leak of pool in mp.Pool)
    ctx = mp.get_context("spawn")
    with ctx.Pool(threads) as pool:
        for _ in pool.imap_unordered(thread_process, argsList, chunksize=1):
            pool.close()
            pool.join()
            gc.collect()


def thread_process(args):
    (
        i,
        path,
        host,
        dbname,
        port,
        user,
        password,
        size_per_thread,
        batch_size,
        insert_query,
    ) = args

    db = Postgres_Batch_Insert(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        batch_size=batch_size,
    )
    db.set_insert_query(insert_query)
    total_rows = sum(1 for _ in open(path))

    db_insertion(db, i, path, batch_size, size_per_thread, total_rows)


def db_insertion(db, i, path, batch_size, size_per_thread, total_rows):
    end_of_range = (
        (i + 1) * size_per_thread
        if (i + 1) * size_per_thread < total_rows
        else total_rows
    )
    start_of_range = i * size_per_thread
    for df in tqdm(
        pd.read_csv(
            path,
            skiprows=start_of_range,
            nrows=end_of_range - start_of_range,
            sep=";",
            chunksize=batch_size,
        ),
        desc=f"Thread {i} range [{start_of_range}-{end_of_range}]",
        unit=f"x{batch_size} rows",
        total=math.ceil((end_of_range - start_of_range) / batch_size),
    ):
        db.batch_insert(df)
        del df
        gc.collect()
