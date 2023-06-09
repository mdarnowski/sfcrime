
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy
from sqlalchemy import Column, Integer, String, TIMESTAMP, DATE, TIME, FLOAT, ForeignKey, INT, TEXT


Base = sqlalchemy.orm.declarative_base()


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
    analysis_Neighborhood = Column(String(255))


class IncidentDetailsDimension(Base):
    __tablename__ = 'incidents_details_dimension'

    incident_details_key = Column(Integer, primary_key=True, autoincrement=True)
    incident_number = Column(INT, nullable=False)
    incident_description = Column(TEXT)
    incident_code = Column(Integer, nullable=False)


class LocationDimension(Base):
    __tablename__ = 'location_dimension'

    location_key = Column(Integer, primary_key=True, autoincrement=True)
    latitude = Column(FLOAT)
    longitude = Column(FLOAT)


class ResolutionDimension(Base):
    __tablename__ = 'resolution_dimension'

    resolution_key = Column(Integer, primary_key=True, autoincrement=True)
    incident_category = Column(String(255), nullable=False)


class Incidents(Base):
    __tablename__ = 'incidents'

    Incident_ID = Column(Integer, primary_key=True, autoincrement=True)
    Date_Key = Column(Integer, ForeignKey('date_dimension.date_key'))
    Category_Key = Column(Integer, ForeignKey('category_dimension.category_key'))
    District_Key = Column(Integer, ForeignKey('district_dimension.district_key'))
    Resolution_Key = Column(Integer, ForeignKey('resolution_dimension.resolution_key'))
    Location_Key = Column(Integer, ForeignKey('location_dimension.location_key'))
    Incident_Details_Key = Column(Integer, ForeignKey('incidents_details_dimension.incident_details_key'))


# import other table classes here...

# The declarative_base from one of the imported files can be reused


# Creating engine and tables
engine = create_engine('postgresql://postgres:sa@localhost/crime_data_sf')

Base.metadata.create_all(engine)
# Create a session
Session = sessionmaker(bind=engine)
session = Session()
date_dimension_instance = ResolutionDimension(
    incident_category='LARCENY/THEFT',
)
session.add(date_dimension_instance)

session.commit()
