from sqlalchemy import create_engine, Column, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()


class Incidents(Base):
    __tablename__ = 'Incidents'

    Incident_ID = Column(Integer, primary_key=True, autoincrement=True)
    Date_Key = Column(Integer, ForeignKey('Date_Dimension.Date_Key'))
    Category_Key = Column(Integer, ForeignKey('Category_Dimension.Category_Key'))
    District_Key = Column(Integer, ForeignKey('District_Dimension.District_Key'))
    Resolution_Key = Column(Integer, ForeignKey('Resolution_Dimension.Resolution_Key'))
    Location_Key = Column(Integer, ForeignKey('Location_Dimension.Location_Key'))
    Incident_Details_Key = Column(Integer, ForeignKey('Incident_Details_Dimension.Incident_Details_Key'))