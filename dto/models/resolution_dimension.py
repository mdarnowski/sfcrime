from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ResolutionDimension(Base):
    __tablename__ = 'resolution_dimension'

    resolution_key = Column(Integer, primary_key=True, autoincrement=True)
    incident_category = Column(String(255), nullable=False)
