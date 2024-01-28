from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import datetime


def fetch_category_counts(session):
    query = "SELECT incident_category FROM IncidentDetails"
    result = session.execute(query)
    df = pd.DataFrame(list(result), columns=['incident_category'])
    return df['incident_category'].value_counts().reset_index(name='num_of_incidents').rename(
        columns={'index': 'incident_category'})


def fetch_category_resolution_counts(session):
    query = "SELECT incident_category, resolution FROM IncidentDetails"
    result = session.execute(query)
    df = pd.DataFrame(list(result), columns=['incident_category', 'resolution'])
    return df.groupby(['incident_category', 'resolution']).size().reset_index(name='num_of_incidents')


def fetch_most_frequent_crimes(session, past_days=365):
    past_date = datetime.datetime.now() - datetime.timedelta(days=past_days)
    query = "SELECT incident_category, incident_datetime FROM IncidentDetails WHERE incident_datetime >= %s"
    result = session.execute(query, [past_date])
    df = pd.DataFrame(list(result), columns=['incident_category', 'incident_datetime'])
    return df['incident_category'].value_counts().reset_index(name='num_of_incidents').rename(
        columns={'index': 'incident_category'})


def fetch_crime_hotspots(session):
    query = "SELECT latitude, longitude FROM IncidentDetails"
    result = session.execute(query)
    df = pd.DataFrame(list(result), columns=['latitude', 'longitude'])
    return df.groupby(['latitude', 'longitude']).size().reset_index(name='num_of_incidents')


def fetch_crime_trends(session):
    query = "SELECT incident_day_of_week, incident_category FROM IncidentDetails"
    result = session.execute(query)
    df = pd.DataFrame(list(result), columns=['incident_day_of_week', 'incident_category'])
    return df.groupby(['incident_day_of_week', 'incident_category']).size().reset_index(name='num_of_incidents')


def fetch_district_crimes(session):
    query = "SELECT police_district, incident_category FROM IncidentDetails"
    result = session.execute(query)
    df = pd.DataFrame(list(result), columns=['police_district', 'incident_category'])
    return df.groupby(['police_district', 'incident_category']).size().reset_index(name='num_of_incidents')


def fetch_incident_details(session):
    query = "SELECT incident_description FROM IncidentDetails"
    result = session.execute(query)
    df = pd.DataFrame(list(result), columns=['incident_description'])
    return df['incident_description'].value_counts().reset_index(name='num_of_incidents').rename(
        columns={'index': 'incident_description'})


cluster = Cluster(['127.0.0.1'])  # Replace with your cluster's IPs and authentication
s = cluster.connect('sfcrime_keyspace')

print(fetch_incident_details(s))
