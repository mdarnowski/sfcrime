import corm
from corm.models import CORMBase
from datetime import datetime
from uuid import uuid4

class IncidentDetails(CORMBase):
    __keyspace__ = 'your_keyspace'
    incident_id: uuid4
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
