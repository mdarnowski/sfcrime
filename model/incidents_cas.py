import os

# Set environment variable for Cassandra cluster IPs
os.environ['CLUSTER_IPS'] = '127.0.0.1'  # or your Docker host IP

import corm
from corm.models import CORMBase
from datetime import datetime
import uuid

# Define the keyspace name
keyspace_name = 'sfcrime_keyspace'

# Create a CQL script to create the keyspace
create_keyspace_cql = f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};
"""

# Use cqlsh to execute the script and create the keyspace
os.system(
    f'docker run --rm --network cassandra -e CQLSH_HOST=cassandra -e CQLSH_PORT=9042 nuvo/docker-cqlsh -e "{create_keyspace_cql}"')


# Define the table structure with UUID type for incident_id
class IncidentDetails(CORMBase):
    __keyspace__ = keyspace_name
    incident_id: uuid.UUID
    incident_datetime: datetime
    incident_date: datetime
    incident_time: datetime
    incident_year: int
    incident_day_of_week: str
    report_datetime: datetime
    incident_category: str
    incident_subcategory: str
    incident_code: int
    police_district: str
    analysis_neighborhood: str
    incident_number: int
    incident_description: str
    latitude: float
    longitude: float
    resolution: str


# Register and create the table
corm.register_table(IncidentDetails)
corm.sync_schema()
