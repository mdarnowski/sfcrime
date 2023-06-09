from sqlalchemy import Column, Integer, String, TIMESTAMP, DATE, TIME
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DateDimension(Base):
    """Date Dimension Model"""
    __tablename__ = 'date_dimension'

    date_key = Column(Integer, primary_key=True, autoincrement=True)
    incident_datetime = Column(TIMESTAMP, nullable=False)
    incident_date = Column(DATE, nullable=False)
    incident_time = Column(TIME, nullable=False)
    incident_year = Column(Integer, nullable=False)
    incident_day_of_week = Column(String(255), nullable=False)
    report_datetime = Column(TIMESTAMP, nullable=False)
