"""
This module is the main file that allows you to insert data from a .csv file to a PostgreSQL database.
"""
from controllers.csv_processor import csv_processor

if __name__ == "__main__":
    """
    Main function that orquest the execution of the command line.
    """
    csv_processor()
