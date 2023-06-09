from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CategoryDimension(Base):
    __tablename__ = 'category_dimension'

    category_key = Column(Integer, primary_key=True, autoincrement=True)
    incident_category = Column(String(255))
    incident_subcategory = Column(String(255))
    incident_code = Column(Integer, nullable=False)
