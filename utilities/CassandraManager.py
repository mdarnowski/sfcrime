from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import datetime


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
            self.cluster = Cluster(['127.0.0.1'])  # Replace with your cluster's IPs and authentication
            self.session = self.cluster.connect('sfcrime_keyspace')

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
        query = "SELECT incident_day_of_week, incident_category FROM IncidentDetails"
        result = self.session.execute(query)
        df = pd.DataFrame(list(result), columns=['incident_day_of_week', 'incident_category'])
        return df.groupby(['incident_day_of_week', 'incident_category']).size().reset_index(name='num_of_incidents')

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
