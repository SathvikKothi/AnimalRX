from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic import BaseModel, ValidationError
from sqlalchemy.dialects.postgresql import JSONB
from typing import Optional, Union, List, Dict
import json
import pandas as pd


# Your Pydantic models (as before)


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

# products = session.query(Product).all()

# Deserialize into Pydantic models

# pydantic_products = []
# for product in products:
# animal_data = json.loads(product.animal) if isinstance(product.animal, str) else product.animal
# pydantic_product = Animal(**animal_data)
# pydantic_products.append(pydantic_product)
# Existing imports and class definitions (Animal, Breed, etc.)

# Ensure pandas is installed: pip install pandas


def get_animals_by_breed(breed_input):
    # Query the database for animals with the specified breed
    products = session.query(Product).all()
    matched_animals = []

    for product in products:
        try:
            animal_data = (
                json.loads(product.animal)
                if isinstance(product.animal, str)
                else product.animal
            )
            animal = Animal.model_validate(animal_data)
        except ValidationError as e:
            print(f"Validation error for product {product.unique_aer_id_number}: {e}")
            continue  # Skip to the next product if validation fails

        # Check if breed is None
        if animal.breed is None:
            continue

        # Check if the breed matches the input
        if (
            animal.breed
            and animal.breed.breed_component
            and breed_input in animal.breed.breed_component
        ):
            matched_animals.append(animal)

    # Convert the matched animals into a DataFrame
    df = pd.DataFrame([animal.model_dump() for animal in matched_animals])
    return df


def count_reactions_by_species(session):
    reaction_counts = {}
    products = session.query(Product).all()
    for product in products:
        try:
            animal_data = (
                json.loads(product.animal)
                if isinstance(product.animal, str)
                else product.animal
            )
            species = animal_data.get("species")
            if species:
                reaction_counts[species] = reaction_counts.get(species, 0) + 1
        except ValidationError:
            continue
    return reaction_counts


# Example usage
# breed_df = get_animals_by_breed("Doberman Pinscher")
# print(breed_df.head())
