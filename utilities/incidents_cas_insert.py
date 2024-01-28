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

batch_size = 100  # You can adjust the batch size as needed
insert_batch = []
i = 0
for index, row in df.iterrows():
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

    insert_batch.append(incident)

    if len(insert_batch) >= batch_size:
        i += 1
        # Insert the batch into Cassandra
        print(i * 100)
        corm.insert(insert_batch)
        insert_batch = []

# Insert any remaining rows
if insert_batch:
    corm.insert(insert_batch)

print("Data insertion completed.")
