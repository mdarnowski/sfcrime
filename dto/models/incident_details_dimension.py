from sqlalchemy import Column, Integer, String, INT, TEXT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class IncidentDetailsDimension(Base):
    __tablename__ = 'incidents_details_dimension'

    incident_details_key = Column(Integer, primary_key=True, autoincrement=True)
    incident_number = Column(INT, nullable=False)
    incident_description = Column(TEXT)
    incident_code = Column(Integer, nullable=False)
