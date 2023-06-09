from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DistrictDimension(Base):
    __tablename__ = 'district_dimension'

    district_key = Column(Integer, primary_key=True, autoincrement=True)
    police_district = Column(String(255))
    analysis_Neighborhood = Column(String(255))

