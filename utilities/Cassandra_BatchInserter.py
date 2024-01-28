import pandas as pd
import queue
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
import os

os.environ['CLUSTER_IPS'] = '127.0.0.1'
import corm
from model.Incidents_Cassandra import IncidentDetails


class Singleton(type):
    """A Singleton Metaclass to ensure only one instance of a class."""
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class CassandraBatchInserter(metaclass=Singleton):
    """Handles batch insertion into a Cassandra database."""

    def __init__(self):
        self.insert_queue = queue.Queue()
        self.thread_count = 5
        self.executor = ThreadPoolExecutor(max_workers=self.thread_count)
        self.init_threads()
        self.batches = []

    def init_threads(self):
        for _ in range(self.thread_count):
            self.executor.submit(self.insert_data_thread)

    def insert_data_thread(self):
        while True:
            batch = self.insert_queue.get()
            if batch is None:
                break
            corm.insert(batch)
            self.insert_queue.task_done()

    def prepare_data(self, filepath):
        csv_columns = [
            "Incident Datetime", "Incident Date", "Incident Time", "Incident Year",
            "Incident Day of Week", "Report Datetime", "Row ID", "Incident ID", "Incident Number",
            "CAD Number", "Report Type Code", "Report Type Description", "Filed Online", "Incident Code",
            "Incident Category", "Incident Subcategory", "Incident Description", "Resolution", "Intersection",
            "CNN", "Police District", "Analysis Neighborhood", "Supervisor District",
            "Supervisor District 2012", "Latitude", "Longitude", "Point", "Neighborhoods",
            "ESNCAG - Boundary File", "Central Market/Tenderloin Boundary Polygon - Updated",
            "Civic Center Harm Reduction Project Boundary", "HSOC Zones as of 2018-06-05",
            "Invest In Neighborhoods (IIN) Areas", "Current Supervisor Districts", "Current Police Districts"
        ]

        df = pd.read_csv(filepath, usecols=csv_columns)
        self.df = df
        self.preprocess_data(df)
        self.batches = self.create_batches(df)

    def preprocess_data(self, df):
        # Data preprocessing steps
        pass

    def create_batches(self, df, batch_size=100):
        for start in range(0, len(df), batch_size):
            yield df.iloc[start:start + batch_size]

    def insert_one_batch(self):
        try:
            batch_df = next(self.batches)
        except StopIteration:
            return False, 0

        insert_batch = []
        for _, row in batch_df.iterrows():
            incident = IncidentDetails(
                row['Incident Datetime'],
                row['Incident Year'],
                row['Incident Time'],
                row['Incident Day of Week'],
                row['Report Datetime'],
                row['Incident Category'],
                row['Incident Subcategory'],
                row['Incident Code'],
                row['Police District'],
                row['Analysis Neighborhood'],
                row['Incident Number'],
                row['Incident Description'],
                row['Latitude'],
                row['Longitude'],
                row['Resolution'],
            )
            insert_batch.append(incident)

        if insert_batch:
            self.insert_queue.put(insert_batch)

        return True, len(insert_batch)

    def run_insertion(self):
        self.prepare_data('../data/crime_sf.csv')
        success, count = self.insert_one_batch()
        while success:
            success, count = self.insert_one_batch()

        self.await_insertion_completion()

    def await_insertion_completion(self):
        self.insert_queue.join()
        for _ in range(self.thread_count):
            self.insert_queue.put(None)
        print("Data insertion completed.")


class InsertTask(metaclass=Singleton):
    """
    Manages the data insertion task for a Cassandra database.
    """

    def __init__(self):
        self.total_rows_added = 0
        self.inserter = CassandraBatchInserter()
        self.total_batches = 0
        self.progress = 0
        self.running = False

    def run(self, filepath):
        self.running = True
        self.inserter.prepare_data(filepath)

        # Now the inserter has the 'df' attribute after prepare_data is called
        self.total_batches = -(-len(self.inserter.df) // 100)  # Calculate the total number of batches
        current_batch = 0
        self.total_rows_added = 0
        self.progress = 0
        success, batch_rows_added = self.inserter.insert_one_batch()

        while success:
            current_batch += 1
            self.total_rows_added += batch_rows_added
            self.progress = (current_batch / self.total_batches) * 100
            success, batch_rows_added = self.inserter.insert_one_batch()

        self.inserter.await_insertion_completion()
        self.running = False
        print(f"Insertion completed. {self.total_rows_added} rows added.")

task = InsertTask()
task.run('../data/crime_sf.csv')