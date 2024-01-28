import time

import pandas as pd
import queue
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
import os
from utilities.DataLoader import DataLoader

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


def create_batches(df, batch_size=100):
    for start in range(0, len(df), batch_size):
        yield df.iloc[start:start + batch_size]


class CassandraBatchInserter(metaclass=Singleton):
    """Handles batch insertion into a Cassandra database."""

    def __init__(self):
        self.df = None
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

    def prepare_data(self):
        df = DataLoader.get_instance().load_data()
        datetime_cols = ['incident_datetime', 'report_datetime']
        df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime, errors='coerce')
        # Converting data types for numeric columns
        int_cols = ['incident_year', 'incident_number', 'cad_number', 'cnn']
        df[int_cols] = df[int_cols].apply(pd.to_numeric, downcast='integer', errors='coerce')
        # Float columns
        float_cols = ['latitude', 'longitude']
        df[float_cols] = df[float_cols].apply(pd.to_numeric, downcast='float', errors='coerce')
        self.df = df
        self.batches = create_batches(df)

    def insert_one_batch(self):
        try:
            batch_df = next(self.batches)
        except StopIteration:
            return False, 0

        insert_batch = []
        for _, row in batch_df.iterrows():
            incident = IncidentDetails(
                row['incident_datetime'],
                row['incident_year'],
                row['incident_datetime'],
                row['incident_day_of_week'],
                row['report_datetime'],
                row['incident_category'],
                row['incident_subcategory'],
                row['incident_code'],
                row['police_district'],
                row['analysis_neighborhood'],
                row['incident_number'],
                row['incident_description'],
                row['latitude'],
                row['longitude'],
                row['resolution'],
            )
            insert_batch.append(incident)

        if insert_batch:
            self.insert_queue.put(insert_batch)

        return True, len(insert_batch)

    def run_insertion(self):
        self.prepare_data()
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

    def run(self):
        self.running = True
        self.inserter.prepare_data()

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
            print(f"Batch {current_batch} inserted. {self.total_rows_added} rows added.")

        print(f"Insertion completed")
        self.running = False
        print(f"Insertion completed. {self.total_rows_added} rows added.")

