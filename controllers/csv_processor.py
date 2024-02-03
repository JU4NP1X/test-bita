import pandas as pd
from tqdm import tqdm
from view.Command_Parser import Command_Interface
from models.Postgres_Batch_Insert import Postgres_Batch_Insert


def csv_processor():
    command_parser = Command_Interface()
    command_parser.parse_command()

    (
        path,
        host,
        dbname,
        tname,
        port,
        user,
        password,
        batch_size,
    ) = command_parser.get_args_values()

    total_rows = sum(1 for _ in open(path))  # Get the total number of rows in the file

    db = Postgres_Batch_Insert(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        batch_size=batch_size,
    )

    db.set_insert_query(
        f"INSERT INTO {tname} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in range(len(columns))])})"
    )

    # Create a progress bar using tqdm
    with tqdm(total=total_rows) as pbar:
        for df in pd.read_csv(path, chunksize=batch_size):

            # create the query with the column names
            columns = df.columns.tolist()
            not_existing_ids = db.verify_if_data_no_exist(df, "id", tname)

            # remove existing data
            df = df[~df["id"].isin(not_existing_ids)]

            db.batch_insert(df.to_dict(orient="records"))
            pbar.update(len(df))
