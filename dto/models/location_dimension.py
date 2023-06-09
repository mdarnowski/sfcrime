from sqlalchemy import Column, Integer, String, FLOAT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class LocationDimension(Base):
    __tablename__ = 'location_dimension'

    location_key = Column(Integer, primary_key=True, autoincrement=True)
    latitude = Column(FLOAT)
    longitude = Column(FLOAT)

