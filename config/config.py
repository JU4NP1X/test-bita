import os

config = {
    "port": int(os.getenv("DEFAULT_PORT", 5432)),
    "host": str(os.getenv("DEFAULT_HOST", "localhost")),
    "dbname": str(os.getenv("DEFAULT_DBNAME", "postgres")),
    "tname": str(os.getenv("DEFAULT_TNAME", "test")),
    "user": str(os.getenv("DEFAULT_USER", "postgres")),
    "password": str(os.getenv("DEFAULT_PASSWORD", None)),
    "string_columns": os.getenv("STRING_COLUMNS", "Product").split(","),
    "date_columns": os.getenv("DATE_COLUMNS", "Date,CreatedAt").split(","),
    "batch_size": int(os.getenv("BATCH_SIZE", 1000)),
    "cmpd_primary_key": os.getenv("PRIMARY_KEY", "PointOfSale,Product,Date").split(","),
    "threads": int(os.getenv("THREADS", 10)),
}
