import os

import pandas as pd


class DataLoader:
    __instance = None
    __file_path = None

    @staticmethod
    def get_instance():
        """ Static access method. """
        if DataLoader.__instance is None:
            DataLoader()
        return DataLoader.__instance

    def __init__(self, file_path='../data/crime_sf.csv'):
        """ Virtually private constructor. """
        if DataLoader.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            # Find the directory this script is in
            script_directory = os.path.dirname(os.path.abspath(__file__))
            # Use it to build an absolute path to the data file
            DataLoader.__file_path = os.path.join(script_directory, file_path)
            DataLoader.__instance = self

    def load_data(self):
        """ Load data from CSV file into DataFrame. """
        df = pd.read_csv(DataLoader.__file_path)
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        return df.where(pd.notnull(df), None)
