from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic import BaseModel, ValidationError
from sqlalchemy.dialects.postgresql import JSONB
from typing import Optional, Union, List, Dict
import json
import pandas as pd


# Your Pydantic models (as before)
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


# Query the database


# Deserialize into Pydantic models
def find_similar_breeds(breed_query):
    products = session.query(Product).all()
    similar_breeds = []

    for product in products:
        try:
            animal_data = (
                json.loads(product.animal)
                if isinstance(product.animal, str)
                else product.animal
            )
            pydantic_product = Animal.model_validate(animal_data)
        except ValidationError as e:
            print(f"Validation error for product {product.unique_aer_id_number}: {e}")
            continue  # Skip to the next product if validation fails

        # Check if breed is None
        if pydantic_product.breed is None:
            continue

        # Check the type of breed_component and perform the search
        breed_component = pydantic_product.breed.breed_component
        if isinstance(breed_component, dict):
            breed_name = breed_component.get("name", "")
        elif isinstance(breed_component, str):
            breed_name = breed_component
        else:
            breed_name = ""

        if breed_query.lower() in breed_name.lower():
            similar_breeds.append(pydantic_product.model_dump())

    df = pd.json_normalize(similar_breeds, sep="_")
    return df


# Example usage
breed_query = "Doberman Pinscher"
df_similar_breeds = find_similar_breeds(breed_query)
print(df_similar_breeds.head())
