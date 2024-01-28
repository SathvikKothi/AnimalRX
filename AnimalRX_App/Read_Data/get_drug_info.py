from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic import BaseModel, ValidationError
from sqlalchemy.dialects.postgresql import JSONB
from typing import Optional, Union, List, Dict
import json
import pandas as pd


# Pydantic models for nested data
class Dose(BaseModel):
    numerator: Optional[str] = None
    numerator_unit: Optional[str] = None
    denominator: Optional[str] = None
    denominator_unit: Optional[str] = None


class ActiveIngredient(BaseModel):
    name: Optional[str] = None
    dose: Optional[Dose] = None


class Manufacturer(BaseModel):
    name: Optional[str] = None
    registration_number: Optional[str] = None


class Drug(BaseModel):
    manufacturing_date: Optional[str] = None
    lot_number: Optional[str] = None
    lot_expiration: Optional[str] = None
    product_ndc: Optional[str] = None
    brand_name: Optional[str] = None
    dosage_form: Optional[str] = None
    manufacturer: Optional[Manufacturer] = None
    number_of_defective_items: Optional[str] = None
    number_of_items_returned: Optional[str] = None
    atc_vet_code: Optional[str] = None
    active_ingredients: Optional[List[ActiveIngredient]] = None


# Database connection and model setup
Base = declarative_base()
engine = create_engine(
    "postgresql://clinicaltrials:clinicaltrials@localhost:5432/postgres"
)
Session = sessionmaker(bind=engine)
session = Session()


# Define the SQLAlchemy model
class Product(Base):
    __tablename__ = "animalevent"
    unique_aer_id_number = Column(String, primary_key=True)
    drug = Column(JSONB)


def summarize_drug_usage(session):
    drug_usage = {}
    products = session.query(Product).all()
    for product in products:
        try:
            drug_data = (
                json.loads(product.drug)
                if isinstance(product.drug, str)
                else product.drug
            )
            for drug in drug_data:
                drug_name = drug.get("brand_name")
                if drug_name:
                    drug_usage[drug_name] = drug_usage.get(drug_name, 0) + 1
        except ValidationError:
            continue
    return drug_usage


def get_drug_info():
    # Query the database for products
    products = session.query(Product).all()

    # Process each product, handling potential validation errors and data types
    drug_data_list = []
    for product in products:
        try:
            drug_data = (
                json.loads(product.drug)
                if isinstance(product.drug, str)
                else product.drug
            )

            # Ensure drug_data is a list before validation
            if not isinstance(drug_data, list):
                drug_data = [drug_data]  # Wrap in a list if it's a single dictionary

            # Validate each item in the list using the Drug model
            validated_drug_data = [Drug(**item) for item in drug_data]  # Corrected line
            drug_data_list.extend(validated_drug_data)

        except ValidationError as e:
            print(f"Validation error for product {product.unique_aer_id_number}: {e}")
            continue

    # Create the DataFrame, converting Drug objects back to dictionaries
    df = pd.json_normalize(
        [drug.model_dump() for drug in drug_data_list],  # Conversion step
        record_path="active_ingredients",
        meta=[
            "manufacturing_date",
            "lot_number",
            "lot_expiration",
            "product_ndc",
            "brand_name",
            "dosage_form",
            "manufacturer",
            "number_of_defective_items",
            "number_of_items_returned",
            "atc_vet_code",
        ],
    )

    # Close the database session
    session.close()
    # Print the DataFrame
    # print(df.head())
    return df


# Call the function to retrieve and process drug information
# summarize_drug_usage('Famphur')
