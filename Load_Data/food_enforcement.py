import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
from typing import Optional, List, Dict


# Pydantic model
class Result(BaseModel):
    address_1: Optional[str]
    address_2: Optional[str]
    center_classification_date: Optional[str]
    city: Optional[str]
    classification: Optional[str]
    code_info: Optional[str]
    country: Optional[str]
    distribution_pattern: Optional[str]
    event_id: Optional[str]
    initial_firm_notification: Optional[str]
    more_code_info: Optional[str]
    openfda: Optional[Dict]
    product_code: Optional[str]
    product_description: Optional[str]
    product_quantity: Optional[str]
    product_type: Optional[str]
    reason_for_recall: Optional[str]
    recall_initiation_date: Optional[str]
    recall_number: Optional[str]
    recalling_firm: Optional[str]
    report_date: Optional[str]
    state: Optional[str]
    status: Optional[str]
    termination_date: Optional[str]
    voluntary_mandated: Optional[str]


class FoodEnforcement(BaseModel):
    results: List[Result]


# SQLAlchemy ORM class
Base = declarative_base()


class FoodEnforcementDB(Base):
    __tablename__ = "food_enforcement"
    address_1 = Column(String)
    address_2 = Column(String)
    center_classification_date = Column(String)
    city = Column(String)
    classification = Column(String)
    code_info = Column(String)
    country = Column(String)
    distribution_pattern = Column(String)
    event_id = Column(String)
    initial_firm_notification = Column(String)
    more_code_info = Column(String)
    openfda = Column(JSONB)  # Use JSONB for dictionary type
    product_code = Column(String)
    product_description = Column(String)
    product_quantity = Column(String)
    product_type = Column(String)
    reason_for_recall = Column(String)
    recall_initiation_date = Column(String)
    recall_number = Column(String, primary_key=True)
    recalling_firm = Column(String)
    report_date = Column(String)
    state = Column(String)
    status = Column(String)
    termination_date = Column(String)
    voluntary_mandated = Column(String)


# Connect to the database
engine = create_engine(
    "postgresql://clinicaltrials:clinicaltrials@localhost:5432/postgres"
)
Session = sessionmaker(bind=engine)
session = Session()

# Load JSON data
with open(
    "/Users/ramanakothi/langchain/learn_lang2/food/enforcement/food-enforcement-0001-of-0001.json"
) as reader:
    data = json.load(reader)
model = FoodEnforcement(results=data["results"])  # Access nested results

# Persist data using SQLAlchemy ORM
for result in model.results:
    record = FoodEnforcementDB(
        address_1=result.address_1,
        address_2=result.address_2,
        center_classification_date=result.center_classification_date,
        city=result.city,
        classification=result.classification,
        code_info=result.code_info,
        country=result.country,
        distribution_pattern=result.distribution_pattern,
        event_id=result.event_id,
        initial_firm_notification=result.initial_firm_notification,
        more_code_info=result.more_code_info,
        openfda=result.openfda,
        product_code=result.product_code,
        product_description=result.product_description,
        product_quantity=result.product_quantity,
        product_type=result.product_type,
        reason_for_recall=result.reason_for_recall,
        recall_initiation_date=result.recall_initiation_date,
        recall_number=result.recall_number,
        recalling_firm=result.recalling_firm,
        report_date=result.report_date,
        state=result.state,
        status=result.status,
        termination_date=result.termination_date,
        voluntary_mandated=result.voluntary_mandated,
    )
    session.add(record)

session.commit()
session.close()
