import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic import BaseModel, ValidationError
from typing import Optional, List, Dict, Union
from sqlalchemy.dialects.postgresql import JSONB
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


class HealthAssessmentPriorToExposure(BaseModel):
    assessed_by: Optional[str] = None
    condition: Optional[str] = None


class ReactionDetail(BaseModel):
    veddra_version: Optional[str] = None
    veddra_term_code: Optional[str] = None
    veddra_term_name: Optional[str] = None
    number_of_animals_affected: Optional[str] = None
    accuracy: Optional[str] = None


class Receiver(BaseModel):
    organization: Optional[str] = None
    street_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None


class OutcomeDetail(BaseModel):
    medical_status: Optional[str] = None
    number_of_animals_affected: Optional[str] = None


class Age(BaseModel):
    max: Optional[float] = None
    min: Optional[float] = None
    qualifier: Optional[str] = None
    unit: Optional[str] = None


class Age(BaseModel):
    max: Optional[float]
    min: Optional[float]
    qualifier: Optional[str]
    unit: Optional[str]


class Breed(BaseModel):
    is_crossbred: Optional[Union[str, int, float, List, Dict]]
    breed_component: Optional[Union[str, int, float, List, Dict]]

    class Config:
        arbitrary_types_allowed = True  # Allow unexpected types


class Weight(BaseModel):
    max: Optional[float]
    min: Optional[float]
    qualifier: Optional[str]
    unit: Optional[str]


class Animal(BaseModel):
    age: Optional[Age]
    breed: Optional[Breed]
    female_animal_physiological_status: Optional[str]
    gender: Optional[str]
    reproductive_status: Optional[str]
    species: Optional[str]
    weight: Optional[Weight]


class Result(BaseModel):
    health_assessment_prior_to_exposure: Optional[HealthAssessmentPriorToExposure]
    onset_date: Optional[str]
    reaction: Optional[List[ReactionDetail]]
    receiver: Optional[Receiver]
    unique_aer_id_number: Optional[str]
    report_id: Optional[str]
    original_receive_date: Optional[str]
    primary_reporter: Optional[str]
    type_of_information: Optional[str]
    drug: Optional[List[Drug]]
    treated_for_ae: Optional[str]
    number_of_animals_affected: Optional[str]
    number_of_animals_treated: Optional[str]
    secondary_reporter: Optional[str]
    animal: Optional[Animal]
    serious_ae: Optional[str]
    outcome: Optional[List[OutcomeDetail]]
    time_between_exposure_and_onset: Optional[str]


class AnimalEvent(BaseModel):
    results: List[Result]


# Database connection and model setup
Base = declarative_base()
engine = create_engine(
    "postgresql://clinicaltrials:clinicaltrials@localhost:5432/postgres"
)
Session = sessionmaker(bind=engine)
session = Session()


class Product(Base):
    __tablename__ = "animalevent"
    unique_aer_id_number = Column(String, primary_key=True)
    drug = Column(JSONB)
    reaction = Column(JSONB)
    health_assessment_prior_to_exposure = Column(JSONB)


def extract_drugs_to_dataframe():
    products = session.query(Product).all()
    all_drugs = []

    for product in products:
        drugs_data = json.loads(product.drug)
        unique_aer_id = product.unique_aer_id_number
        for drug_dict in drugs_data:
            try:
                drug_obj = Drug.model_validate(drug_dict)
                drug_info = drug_obj.model_dump()

                # Include unique_aer_id_number
                drug_info["unique_aer_id_number"] = unique_aer_id

                # Flatten active_ingredients
                for ai in drug_obj.active_ingredients or []:
                    ai_dict = ai.model_dump()
                    for key, value in ai_dict.items():
                        drug_info[f"active_ingredient_{key}"] = value

                # Flatten manufacturer
                if drug_obj.manufacturer:
                    for key, value in drug_obj.manufacturer.model_dump().items():
                        drug_info[f"manufacturer_{key}"] = value

                all_drugs.append(drug_info)
            except ValidationError as e:
                print(f"Validation error for drug data: {drug_dict}, error: {e}")
    return pd.DataFrame(all_drugs)


def extract_health_assessment_to_dataframe():
    products = session.query(Product).all()
    all_assessments = []

    for product in products:
        if (
            product.health_assessment_prior_to_exposure
        ):  # Check if assessment data exists
            # Load the assessment data as JSON
            assessment_data = json.loads(product.health_assessment_prior_to_exposure)
            unique_aer_id = product.unique_aer_id_number

            # Check if assessment_data is a dictionary and process it
            if isinstance(assessment_data, dict):
                try:
                    assessment_obj = HealthAssessmentPriorToExposure.model_validate(
                        assessment_data
                    )
                    assessment_info = assessment_obj.model_dump()
                    assessment_info["unique_aer_id_number"] = unique_aer_id
                    all_assessments.append(assessment_info)
                except ValidationError as e:
                    print(
                        f"Validation error for assessment data: {assessment_data}, error: {e}"
                    )

    return pd.DataFrame(all_assessments)


def most_common_reactions(session, top_n=5):
    reaction_freq = {}
    products = session.query(Product).all()
    for product in products:
        try:
            reaction_data = (
                json.loads(product.reaction)
                if isinstance(product.reaction, str)
                else product.reaction
            )
            for reaction in reaction_data:
                reaction_name = reaction.get("veddra_term_name")
                if reaction_name:
                    reaction_freq[reaction_name] = (
                        reaction_freq.get(reaction_name, 0) + 1
                    )
        except ValidationError:
            continue
    sorted_reactions = sorted(reaction_freq.items(), key=lambda x: x[1], reverse=True)
    return sorted_reactions[:top_n]


# Example usage
# drug_df = extract_drugs_to_dataframe()
# print(drug_df.head())

# assessment_df = extract_health_assessment_to_dataframe()
# print(assessment_df.head())
