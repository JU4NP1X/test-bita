"""
This module contains a class that give an interface to the user to insert data from a .csv file to the database.
"""

import argparse
from dotenv import load_dotenv
import os
import getpass


load_dotenv()


class Command_Interface:
    port = os.getenv("DEFAULT_PORT", 5432)
    host = os.getenv("DEFAULT_HOST", "localhost")
    dbname = os.getenv("DEFAULT_DBNAME", "postgres")
    tname = os.getenv("DEFAULT_TNAME", "test")
    user = os.getenv("DEFAULT_USER", "postgres")
    password = os.getenv("DEFAULT_PASSWORD", None)
    use_secure_pass = False
    path = None
    batch_size = os.getenv("BATCH_SIZE", 1000)

    def __init__(self):
        """
        Initialize the class with an argument parser and add arguments for path, host, and port.

        Args:
            self: The object instance

        Returns:
            None
        """

        self.parser = argparse.ArgumentParser(
            description="This command allows you to insert data from a .csv file to the database"
        )
        self.parser.add_argument("path", help="Path to the .csv file")
        self.parser.add_argument("-H", "--host", help="Specify the database host")
        self.parser.add_argument(
            "-dn", "--databasename", help="Specify the database name"
        )
        self.parser.add_argument("-b", "--batch", help="Specify the batch size")
        self.parser.add_argument("-tn", "--tablename", help="Specify the table name")
        self.parser.add_argument("-u", "--user", help="Specify the database user")
        self.parser.add_argument("-p", "--port", help="Specify the database port")
        self.parser.add_argument(
            "-s",
            "--secure",
            help="Insert the database password during execution and not in command line (HIGHLY RECOMMENDED IF YOU NOT SET IT IN ENVIRONMENT VARIABLES)",
        )
        self.parser.add_argument(
            "-P",
            "--password",
            help="Specify the database password (NOT RECOMMENDED, IT WILL BE STORED IN COMMAND LINE HISTORY, use instead --secure)",
        )

    def parse_command(self):
        """
        Parse the command line arguments and set the 'host' and 'port' attributes if provided.
        """

        args = self.parser.parse_args()
        if args.path:
            self.path = args.path
            if args.host:
                self.host = args.host

            if args.port:
                self.port = int(args.port)

            if args.databasename:
                self.dbname = args.databasename

            if args.tablename:
                self.tname = args.tablename

            if args.user:
                self.user = args.user

            if args.secure:
                self.use_secure_pass = True

            if args.password:
                self.password = args.password
        else:
            print("No path provided. Use --help for more information")
        # Get the password as a secure string input with a prompt
        if self.use_secure_pass:
            self.password = getpass.getpass(prompt="Enter the database password: ")

    def get_args_values(self):
        """
        Get the values of the 'host' and 'port' attributes.

        Args:
            self: The object instance

        Returns:
            path: The value of the 'csv path' attribute
            host: The value of the 'host' of the database
            dbname: The value of the name of the database
            tname: The value of the table name of the database
            port: The value of the 'port' of the database
            user: The value of the 'user' of the database
            password: The value of the 'password' of the database
            batch_size: The value of the 'batch_size'


        """

        return (
            self.path,
            self.host,
            self.dbname,
            self.tname,
            self.port,
            self.user,
            self.password,
            self.batch_size,
        )