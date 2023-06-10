from sqlalchemy.orm import sessionmaker
import sqlalchemy
from sqlalchemy import Column, Integer, String, TIMESTAMP, DATE, TIME, FLOAT, ForeignKey, INT, TEXT

Base = sqlalchemy.orm.declarative_base()


def get_base():
    return Base


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


class CategoryDimension(Base):
    __tablename__ = 'category_dimension'

    category_key = Column(Integer, primary_key=True, autoincrement=True)
    incident_category = Column(String(255))
    incident_subcategory = Column(String(255))
    incident_code = Column(Integer, nullable=False)


class DistrictDimension(Base):
    __tablename__ = 'district_dimension'

    district_key = Column(Integer, primary_key=True, autoincrement=True)
    police_district = Column(String(255))
    analysis_neighborhood = Column(String(255))


class IncidentDetailsDimension(Base):
    __tablename__ = 'incident_details_dimension'

    incident_details_key = Column(Integer, primary_key=True, autoincrement=True)
    incident_number = Column(INT, nullable=False)
    incident_description = Column(TEXT)


class LocationDimension(Base):
    __tablename__ = 'location_dimension'

    location_key = Column(Integer, primary_key=True, autoincrement=True)
    latitude = Column(FLOAT)
    longitude = Column(FLOAT)


class ResolutionDimension(Base):
    __tablename__ = 'resolution_dimension'

    resolution_key = Column(Integer, primary_key=True, autoincrement=True)
    resolution = Column(String(255), nullable=False)


class Incidents(Base):
    __tablename__ = 'incidents'

    incident_id = Column(sqlalchemy.types.BIGINT, primary_key=True, autoincrement=True)
    date_key = Column(Integer, ForeignKey('date_dimension.date_key'))
    category_key = Column(Integer, ForeignKey('category_dimension.category_key'))
    district_key = Column(Integer, ForeignKey('district_dimension.district_key'))
    resolution_key = Column(Integer, ForeignKey('resolution_dimension.resolution_key'))
    location_key = Column(Integer, ForeignKey('location_dimension.location_key'))
    incident_details_key = Column(Integer, ForeignKey('incident_details_dimension.incident_details_key'))
