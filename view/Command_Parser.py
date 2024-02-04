"""
This module contains a class that give an interface to the user to insert data from a .csv file to the database.
"""

import argparse
import os
import getpass
from config.config import config


class Command_Interface:
    port: int = config["port"]
    host: str = config["host"]
    dbname: str = config["dbname"]
    tname: str = config["tname"]
    user: str = config["user"]
    password: str = config["password"]
    use_secure_pass: bool = False
    path: str = None
    batch_size: int = config["batch_size"]
    cmpd_primary_key: list = config["cmpd_primary_key"]
    threads: int = config["threads"]

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
        self.parser.add_argument(
            "-tn", "--tablename", help="Specify the table name to be inserted"
        )
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
        self.parser.add_argument(
            "-k",
            "--primarykey",
            help="Specify the primary key (if is complex key separate by comma)",
        )
        self.parser.add_argument(
            "-t",
            "--threads",
            help="Specify the number of threads in the data insertion process (use 0 to not use threads)",
        )

        self.parser.add_argument("-b", "--batch", help="Specify the batch size")

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

            if args.batch:
                self.batch_size = int(args.batch)

            # Get array of comma string
            if args.primarykey:
                self.cmpd_primary_key = args.primarykey.split(",")

            if args.threads:
                self.threads = int(args.threads)

        else:
            print("No path provided. Use --help for more information")
        # Get the password as a secure string input with a prompt
        if self.use_secure_pass:
            self.password = getpass.getpass(prompt="Enter the database password: ")

        # Exit if not password
        if not self.password:
            print(
                "No password provided in command line or .env. Use --help for more information"
            )
            exit()

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
            primary_key: The value of the 'primary_key'
            threads: The value of the 'threads' used in the insertion


        """
        # Print the type of data of the batch
        return (
            self.path,
            self.host,
            self.dbname,
            self.tname,
            self.port,
            self.user,
            self.password,
            self.batch_size,
            self.cmpd_primary_key,
            self.threads,
        )
