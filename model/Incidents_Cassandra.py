import os

os.environ['CLUSTER_IPS'] = '127.0.0.1'
import corm
from corm.models import CORMBase
from datetime import datetime, time
import uuid

# Define the keyspace name
keyspace_name = 'sfcrime_keyspace'


# Define the table structure with UUID type for incident_id
class IncidentDetails(CORMBase):
    __keyspace__ = keyspace_name
    incident_datetime: datetime
    incident_year: int
    incident_time: datetime
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
