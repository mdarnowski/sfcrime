import os

import pandas as pd


class DataLoader:
    """
    A singleton class used to load data from a CSV file into a DataFrame.

    This class implements the singleton pattern to ensure only one instance is created. It provides
    a static method to get the instance of the class and a method to load the data from the CSV file.

    :param file_path: relative path to the CSV file.
    """
    __instance = None
    __file_path = None

    @staticmethod
    def get_instance():
        """
        Get the singleton instance of this class.

        This static method provides access to the singleton instance of this class. If the instance
        has not been created yet, it creates the instance.

        :return: The singleton instance of this class.
        """
        if DataLoader.__instance is None:
            DataLoader()
        return DataLoader.__instance

    def __init__(self, file_path='../data/crime_sf.csv'):
        """
        Virtually private constructor.

        This constructor initializes the singleton instance of this class. It sets the file path to
        the CSV file and stores the instance in a class variable. If the instance has already been
        created, it raises an exception.

        :param file_path: relative path to the CSV file.
        :raises Exception: If the instance has already been created.
        """
        if DataLoader.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            # Find the directory this script is in
            script_directory = os.path.dirname(os.path.abspath(__file__))
            # Use it to build an absolute path to the data file
            DataLoader.__file_path = os.path.join(script_directory, file_path)
            DataLoader.__instance = self

    def load_data(self):
        """
        Load data from the CSV file into a DataFrame.

        This method reads the CSV file into a pandas DataFrame, replaces spaces with underscores and
        converts column names to lower case in the DataFrame, replaces null values with None in the
        DataFrame, and returns the DataFrame.

        :return: DataFrame containing the data from the CSV file.
        """
        df = pd.read_csv(DataLoader.__file_path)
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        return df.where(pd.notnull(df), None)
