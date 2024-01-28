import numpy as np
import pandas as pd
import os
import threading
import queue
from uuid import uuid4

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

        df = pd.read_csv('../data/crime_sf.csv', usecols=csv_columns)
        df['Incident Datetime'] = pd.to_datetime(df['Incident Datetime'])
        df['Report Datetime'] = pd.to_datetime(df['Report Datetime'])
        df['Incident Time'] = pd.to_datetime(df['Incident Time'])
        df['Incident Year'] = df['Incident Year'].astype(int)
        df['Incident Code'] = df['Incident Code'].astype(int)
        df['Incident Number'] = df['Incident Number'].astype(int)
        df['Latitude'] = df['Latitude'].astype(float)
        df['Longitude'] = df['Longitude'].astype(float)
        df['Incident ID'] = [uuid4() for _ in range(len(df))]

        default_value = np.NAN
        df.fillna(default_value, inplace=True)

        batch_size = 100
        insert_batch = []

        for index, row in df.iterrows():
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
