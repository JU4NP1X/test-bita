# Command Line CSV to Database Import Tool

This command line tool allows you to perform a massive insert from a CSV file to a database table. It provides various options to customize the import process. Below is a list of available options:

- **-h, --help:** Show the help message and exit.
- **-H HOST, --host HOST:** Specify the database host.
- **-dn DATABASENAME, --databasename DATABASENAME:** Specify the database name.
- **-tn TABLENAME, --tablename TABLENAME:** Specify the table name to be inserted.
- **-u USER, --user USER:** Specify the database user.
- **-p PORT, --port PORT:** Specify the database port.
- **-s SECURE, --secure SECURE:** Insert the database password during execution and not in the command line (HIGHLY RECOMMENDED IF YOU HAVE NOT SET IT IN ENVIRONMENT VARIABLES).
- **-P PASSWORD, --password PASSWORD:** Specify the database password (NOT RECOMMENDED, IT WILL BE STORED IN COMMAND LINE HISTORY, use instead --secure).
- **-k PRIMARYKEY, --primarykey PRIMARYKEY:** Specify the primary key (if it is a complex key, separate by comma).
- **-t THREADS, --threads THREADS:** Specify the number of threads in the data insertion process (use 0 to not use threads).
- **-b BATCH, --batch BATCH:** Specify the batch size.

To use this tool, simply run the command with the required arguments and any optional arguments you want to specify. For example:

Please note that some options, such as the database host and user, may have default values specified in the environment variables. You can override these defaults by providing the corresponding command line arguments.

Make sure to properly set the database password, either by using the --secure option to enter it during execution or by setting it in the environment variables. Avoid specifying the password in the command line directly, as it will be stored in the command line history.

If you have any questions or need further assistance, please refer to the help message (-h option) or consult the documentation for this tool.

## Installation process
- **Direct installation:**

    To add the required modules to your project, you can include them in your requirements.txt file. Open the requirements.txt file and add the following lines:

        python-dotenv
        psycopg2
        pandas
        tqdm

    Or just run:
    ```bash
    pip install -r requirements.txt
    ```
    

- **Using virtual enviroment (recommended):**
    
    Run the following commands:
    ```bash
    pip install virtualenv
    ```
    ```bash
    sudo apt install virtualenv
    ```
    ```bash
    virtualenv myenv
    ```
    Every time you want to use the virtual enviroment run the following command on the root of your venv:
    ```bash
    source myenv/bin/activate
    ```
    Now you can run the same process as the direct installation:
    ```bash
    pip install -r requirements.txt
    ```

## Warning
This code was only tested in python 3.10


## Considerations:
This approach of using Python for performing batch insertion from a CSV file to a database is less efficient than using the copy command directly in the database.

The main reason is that using the copy command in the database allows for faster and more efficient data insertion, as it is an optimized operation specifically designed for this purpose. However, in this case, the requirement is to not use the copy command and instead implement the solution in Python for its practicality and customization capabilities.

It is important to note that using _**threads**_ in the data insertion process (_**threads**_) is only recommended when dealing with a large amount of data. In situations where the amount of data is not substantial, using _**threads**_ may actually slow down the process instead of speeding it up. Therefore, it is necessary to evaluate the data quantity and determine if using threads is appropriate for each specific case.

Additionally, the batch size (_**batch**_) used in the insertion process is also an important factor to consider. A larger batch size can result in a faster process as fewer insertion operations are performed in the database. However, this also means that more memory is consumed as more data needs to be stored in memory before being inserted into the database. Therefore, it is necessary to adjust the batch size according to the amount of information present in the CSV file and the availability of resources in the system where the process is executed. Also, you need to consider the database string length limit when calculating the batch size.


## .env configuration:

The .env file contains the configuration parameters for the Python program that performs the insertion of the .csv file into the PostgreSQL database. Here's a breakdown of each parameter:
```markdown
DEFAULT_PORT: Specifies the port number of the PostgreSQL database. The default value is 5432.
DEFAULT_HOST: Specifies the host or IP address of the PostgreSQL database. The default value is localhost.
DEFAULT_DBNAME: Specifies the name of the target database in PostgreSQL. The default value is "test".
DEFAULT_TNAME: Specifies the name of the table in the target database where the data will be inserted. The default value is "test".
DEFAULT_USER: Specifies the username for accessing the PostgreSQL database. The default value is "postgres".
DEFAULT_PASSWORD: Specifies the password for the specified database user. Please make sure to update this value with the actual password for your PostgreSQL database.
BATCH_SIZE: Specifies the batch size for data insertion. The program will insert data in batches, and this parameter determines the number of rows per batch. The default value is 10000.
PRIMARY_KEY: Specifies the primary key columns for the target table in the database. If the primary key is composed of multiple columns, separate them with commas. For example, "PointOfSale,Product,Date".
DATE_COLUMNS: Specifies the columns in the .csv file that contain date values. This information is used for query building and type conversion. Separate the column names with commas. For example, "Date,DateCreated,DateUpdated".
STRING_COLUMNS: Specifies the columns in the .csv file that contain string values. This information is used for query building and type conversion. Separate the column names with commas. For example, "Product,PointOfSale".
Please make sure to update the values of the parameters according to your specific PostgreSQL database configuration.
```


I hope this explanation helps! Let me know if you have any further questions.