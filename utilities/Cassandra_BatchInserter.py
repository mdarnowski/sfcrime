import pandas as pd
import os
import threading
import queue
from uuid import uuid4
from utilities.DataLoader import DataLoader

os.environ['CLUSTER_IPS'] = '127.0.0.1'
import corm
from model.Incidents_Cassandra import IncidentDetails

# Database Configuration
corm.register_table(IncidentDetails)
corm.sync_schema()


class Cassandra_BatchInserter:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Cassandra_BatchInserter, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):  # To avoid re-initializing
            self.insert_queue = queue.Queue()
            self.thread_count = 5  # Number of threads for inserting data
            self.initialized = True
            self.init_threads()

    def init_threads(self):
        for _ in range(self.thread_count):
            thread = threading.Thread(target=self.insert_data_thread)
            thread.daemon = True
            thread.start()

    def insert_data_thread(self):
        while True:
            batch = self.insert_queue.get()
            if batch is None:
                break  # Stop the thread
            corm.insert(batch)
            self.insert_queue.task_done()

    def insertData(self):

        batch_size = 100
        insert_batch = []

        df = DataLoader.get_instance().load_data()
        df['incident_datetime'] = pd.to_datetime(df['incident_datetime'])
        df['report_datetime'] = pd.to_datetime(df['report_datetime'])
        df['incident_time'] = pd.to_datetime(df['incident_time'])
        df['incident_year'] = df['incident_year'].astype(int)
        df['incident_code'] = df['incident_code'].astype(int)
        df['incident_number'] = df['incident_number'].astype(int)
        df['latitude'] = df['latitude'].astype(float)
        df['longitude'] = df['longitude'].astype(float)
        df['incident_id'] = [uuid4() for _ in range(len(df))]

        for index, row in df.iterrows():
            incident = IncidentDetails(
                row['incident_datetime'],
                row['incident_year'],
                row['incident_time'],
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

            if len(insert_batch) >= batch_size:
                self.insert_queue.put(insert_batch)
                insert_batch = []

        if insert_batch:
            self.insert_queue.put(insert_batch)

        self.insert_queue.join()

        for _ in range(self.thread_count):
            self.insert_queue.put(None)

        print("Data insertion completed.")


# Usage
inserter = Cassandra_BatchInserter()
inserter.insertData()
