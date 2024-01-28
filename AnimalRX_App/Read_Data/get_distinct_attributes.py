from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import JSONB
from typing import Optional, Union, List, Dict
import json


class Age(BaseModel):
    max: Optional[float] = None
    min: Optional[float] = None
    qualifier: Optional[str] = None
    unit: Optional[str] = None


class Breed(BaseModel):
    is_crossbred: Optional[Union[str, int, float, List, Dict]] = None
    breed_component: Optional[Union[str, int, float, List, Dict]] = None

    class Config:
        arbitrary_types_allowed = True  # Allow unexpected types


class Weight(BaseModel):
    max: Optional[float] = None
    min: Optional[float] = None
    qualifier: Optional[str] = None
    unit: Optional[str] = None


class Animal(BaseModel):
    age: Optional[Age] = None
    breed: Optional[Breed] = None
    female_animal_physiological_status: Optional[str] = None
    gender: Optional[str] = None
    reproductive_status: Optional[str] = None
    species: Optional[str] = None
    weight: Optional[Weight] = None


Base = declarative_base()
engine = create_engine(
    "postgresql://clinicaltrials:clinicaltrials@localhost:5432/postgres"
)
Session = sessionmaker(bind=engine)
session = Session()
# Define your table with a JSONB column


class Product(Base):
    __tablename__ = "animalevent"
    unique_aer_id_number = Column(String, primary_key=True)
    animal = Column(JSONB)


def get_distinct_values(attribute, subattribute=None):
    products = session.query(Product).all()
    distinct_values = set()

    for product in products:
        animal_data = (
            json.loads(product.animal)
            if isinstance(product.animal, str)
            else product.animal
        )

        # Extract the value of the given attribute
        value = animal_data.get(attribute)

        if subattribute and isinstance(value, dict):
            # Extract subattribute if specified and the attribute is a dictionary
            value = value.get(subattribute)

        # Handle different types of values (single value, list, dictionary)
        if isinstance(value, list):
            distinct_values.update([item for item in value if item is not None])
        elif isinstance(value, dict):
            distinct_values.update(value.keys())
        elif value is not None:
            distinct_values.add(value)

    return list(distinct_values)


# Example usage
# print(get_distinct_values("breed", "breed_component"))
# print(get_distinct_values("species"))
# print(get_distinct_values("gender"))
# print(get_distinct_values("breed"))
