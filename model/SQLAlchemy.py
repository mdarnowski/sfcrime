from sqlalchemy.orm import declarative_base, DeclarativeMeta
from sqlalchemy import Column, Integer, String, TIMESTAMP, DATE, TIME, FLOAT, ForeignKey, INT, TEXT
from abc import abstractmethod, ABCMeta

Base = declarative_base()


def get_base():
    return Base


class ABCWithSQLAlchemy(ABCMeta, DeclarativeMeta):
    pass


class Dimension(Base, metaclass=ABCWithSQLAlchemy):
    __abstract__ = True
    key = Column(Integer, primary_key=True, autoincrement=True)

    @classmethod
    @abstractmethod
    def get_columns(cls):
        pass


class DateDimension(Dimension):
    __tablename__ = 'date_dimension'

    incident_datetime = Column(TIMESTAMP, nullable=False)
    incident_date = Column(DATE, nullable=False)
    incident_time = Column(TIME, nullable=False)
    incident_year = Column(Integer, nullable=False)
    incident_day_of_week = Column(String(255), nullable=False)
    report_datetime = Column(TIMESTAMP, nullable=False)

    @classmethod
    def get_columns(cls):
        return ['incident_datetime', 'incident_date', 'incident_time',
                'incident_year', 'incident_day_of_week', 'report_datetime']


class CategoryDimension(Dimension):
    __tablename__ = 'category_dimension'

    incident_category = Column(String(255))
    incident_subcategory = Column(String(255))
    incident_code = Column(Integer, nullable=False)

    @classmethod
    def get_columns(cls):
        return ['incident_category', 'incident_subcategory', 'incident_code']


class DistrictDimension(Dimension):
    __tablename__ = 'district_dimension'

    police_district = Column(String(255))
    analysis_neighborhood = Column(String(255))

    @classmethod
    def get_columns(cls):
        return ['police_district', 'analysis_neighborhood']


class IncidentDetailsDimension(Dimension):
    __tablename__ = 'incident_details_dimension'

    incident_number = Column(INT, nullable=False)
    incident_description = Column(TEXT)

    @classmethod
    def get_columns(cls):
        return ['incident_number', 'incident_description']


class LocationDimension(Dimension):
    __tablename__ = 'location_dimension'

    latitude = Column(FLOAT)
    longitude = Column(FLOAT)

    @classmethod
    def get_columns(cls):
        return ['latitude', 'longitude']


class ResolutionDimension(Dimension):
    __tablename__ = 'resolution_dimension'

    resolution = Column(String(255), nullable=False)

    @classmethod
    def get_columns(cls):
        return ['resolution']


class Incidents(Base):
    __tablename__ = 'incidents'

    incident_id = Column(Integer, primary_key=True, autoincrement=True)
    date_key = Column(Integer, ForeignKey('date_dimension.key'))
    category_key = Column(Integer, ForeignKey('category_dimension.key'))
    district_key = Column(Integer, ForeignKey('district_dimension.key'))
    resolution_key = Column(Integer, ForeignKey('resolution_dimension.key'))
    location_key = Column(Integer, ForeignKey('location_dimension.key'))
    incident_details_key = Column(Integer, ForeignKey('incident_details_dimension.key'))
