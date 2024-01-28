import numpy as np
import pandas as pd
from datetime import datetime
from uuid import uuid4
import os
os.environ['CLUSTER_IPS'] = '127.0.0.1'
import corm
from model.incidents_cas import IncidentDetails  # Replace with the actual file name

# Database Configuration

# Register and create the table
corm.register_table(IncidentDetails)
corm.sync_schema()

# Read and transform the data
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
df['Incident Date'] = pd.to_datetime(df['Incident Date']).dt.date
df['Incident Time'] = pd.to_datetime(df['Incident Time'], format='%H:%M').dt.time
df['Incident Year'] = df['Incident Year'].astype(int)
df['Incident Code'] = df['Incident Code'].astype(int)
df['Incident Number'] = df['Incident Number'].astype(int)
df['Latitude'] = df['Latitude'].astype(float)
df['Longitude'] = df['Longitude'].astype(float)
df['Incident ID'] = [uuid4() for _ in range(len(df))]  # Generate UUIDs


# Fill missing values in all columns with appropriate values
# You can replace 'your_default_value' with the value you want to use for missing data
default_value = np.NAN
df.fillna(default_value, inplace=True)

i = 0
list = []
# Insert data into Cassandra
for index, row in df.iterrows():
    print("Inserting row", i)
    print(row)
    i += 1
    incident = IncidentDetails(
        row['Incident Datetime'],
        row['Incident Year'],
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

    list.append(incident)

corm.insert(list)


print("Data insertion completed.")
