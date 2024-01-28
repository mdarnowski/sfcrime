from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import datetime
import os

from cassandra.policies import DCAwareRoundRobinPolicy

from model.Incidents_Cassandra import IncidentDetails

os.environ['CLUSTER_IPS'] = '127.0.0.1'
import corm

class CassandraManager:
    """
    A singleton class to manage Cassandra database connections and operations.
    """

    __instance = None

    @staticmethod
    def get_instance():
        """
        Retrieve the singleton instance of CassandraDBManager.
        If an instance doesn't exist, it initializes a new one.

        :return: The singleton instance of CassandraDBManager.
        """
        if CassandraManager.__instance is None:
            CassandraManager()
        return CassandraManager.__instance

    def __init__(self):
        """
        Initialize the CassandraDBManager singleton instance.
        Set up database configurations and establish a database connection.
        """
        if CassandraManager.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            CassandraManager.__instance = self
            corm.register_table(IncidentDetails)
            corm.sync_schema()
            self.cluster = Cluster(['127.0.0.1']) # Replace with your cluster's IPs and authentication
            self.session = self.cluster.connect('sfcrime_keyspace')

    def drop_all_data(self):
        """
        Drops all tables in the specified keyspace. This action is irreversible.
        """
        # Fetching all table names in the keyspace
        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
        result = self.session.execute(query, ['sfcrime_keyspace'])

        # Dropping each table
        for row in result:
            drop_query = f"DROP TABLE IF EXISTS sfcrime_keyspace.{row.table_name}"
            try:
                self.session.execute(drop_query)
                print(f"Dropped table: {row.table_name}")
            except Exception as e:
                print(f"Error dropping table {row.table_name}: {str(e)}")

        print("All tables dropped.")

    def create_database(self):
        self.drop_all_data()
        corm.register_table(IncidentDetails)
        corm.sync_schema()

    def recreate_tables(self):
        self.drop_all_data()
        corm.register_table(IncidentDetails)
        corm.sync_schema()
    def fetch_category_counts(self):
        query = "SELECT incident_category FROM IncidentDetails"
        result = self.session.execute(query)
        df = pd.DataFrame(list(result), columns=['incident_category'])
        return df['incident_category'].value_counts().reset_index(name='num_of_incidents').rename(
            columns={'index': 'incident_category'})

    def fetch_category_resolution_counts(self):
        query = "SELECT incident_category, resolution FROM IncidentDetails"
        result = self.session.execute(query)
        df = pd.DataFrame(list(result), columns=['incident_category', 'resolution'])
        return df.groupby(['incident_category', 'resolution']).size().reset_index(name='num_of_incidents')

    def fetch_most_frequent_crimes(self, past_days=365):
        past_date = datetime.datetime.now() - datetime.timedelta(days=past_days)
        # Add "ALLOW FILTERING" at the end of the query
        query = "SELECT incident_category FROM IncidentDetails WHERE incident_datetime >= %s ALLOW FILTERING"
        result = self.session.execute(query, [past_date])
        df = pd.DataFrame(list(result), columns=['incident_category'])
        return df['incident_category'].value_counts().reset_index(name='num_of_incidents').rename(
            columns={'index': 'incident_category'})

    def fetch_crime_hotspots(self):
        print("Fetching crime hotspots...")
        query = "SELECT latitude, longitude FROM IncidentDetails"
        result = self.session.execute(query)
        df = pd.DataFrame(list(result), columns=['latitude', 'longitude'])
        return df.groupby(['latitude', 'longitude']).size().reset_index(name='num_of_incidents')

    def fetch_crime_trends(self):
        # Fetch only the necessary columns
        query = "SELECT incident_day_of_week, incident_category, incident_time FROM IncidentDetails"
        result = self.session.execute(query)
        df = pd.DataFrame(list(result), columns=['incident_day_of_week', 'incident_category', 'incident_time'])

        # Convert 'incident_time' to just the time part if it's a datetime object
        df['incident_time'] = df['incident_time'].dt.time

        # Group by 'incident_day_of_week', 'incident_category', and 'incident_time', and count occurrences
        trends_df = df.groupby(['incident_day_of_week', 'incident_category', 'incident_time']).size().reset_index(
            name='num_of_incidents')

        # Sort the results by 'incident_time' and 'incident_day_of_week'
        sorted_df = trends_df.sort_values(by=['incident_time', 'incident_day_of_week'])

        return sorted_df

    def fetch_district_crimes(self):
        query = "SELECT police_district, incident_category FROM IncidentDetails"
        result = self.session.execute(query)
        df = pd.DataFrame(list(result), columns=['police_district', 'incident_category'])
        return df.groupby(['police_district', 'incident_category']).size().reset_index(name='num_of_incidents')

    def fetch_incident_details(self):
        query = "SELECT incident_description FROM IncidentDetails"
        result = self.session.execute(query)
        df = pd.DataFrame(list(result), columns=['incident_description'])
        return df['incident_description'].value_counts().reset_index(name='num_of_incidents').rename(
            columns={'index': 'incident_description'})
