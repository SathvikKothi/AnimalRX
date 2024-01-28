import json
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Table,
    String,
    Text,
    MetaData,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel, HttpUrl, constr, ConstrainedInt, Extra
from sqlalchemy.dialects.postgresql import JSONB
from typing import Optional, List, Dict


class Consumer(BaseModel):
    age: Optional[str] = None
    age_unit: Optional[str] = None
    gender: Optional[str] = None


class Product(BaseModel):
    industry_code: Optional[str] = None
    industry_name: Optional[str] = None
    name_brand: Optional[str] = None
    role: Optional[str] = None


class Outcome(BaseModel):
    outcome: str


class Reaction(BaseModel):
    reaction: str


class Result(BaseModel):
    report_number: str = None
    date_created: Optional[str] = None
    date_started: Optional[str] = None
    consumer: Consumer = None
    outcomes: Optional[List[str]]
    reactions: Optional[List[str]]
    products: List[Product] = None

    class Config:
        allow_population_by_field_name = True
        extra = Extra.forbid


class OpenFDAResultsModel(BaseModel):
    results: list[Result]


# SQLAlchemy models
Base = declarative_base()


class ResultORM(Base):
    __tablename__ = "results2"
    report_number = Column(Text, primary_key=True)
    date_created = Column(Text)
    date_started = Column(Text)
    consumer_age = Column(Text)
    consumer_age_unit = Column(Text)
    consumer_gender = Column(Text)
    outcomes = Column(JSONB)
    reactions = Column(JSONB)
    products = Column(JSONB)


# Connect to the database
engine = create_engine(
    "postgresql://clinicaltrials:clinicaltrials@localhost:5432/postgres"
)
Session = sessionmaker(bind=engine)
session = Session()

# Load JSON data (using ijson or other methods)
with open(
    "/Users/ramanakothi/langchain/learn_lang2/food/event/food-event-0001-of-0001.json"
) as reader:
    data = json.load(reader)
    model = OpenFDAResultsModel(results=data["results"])  # Access nested results

# Persist data using SQLAlchemy ORM
for result in model.results:
    outcomes_data = [{"outcome": o} for o in result.outcomes]  # Wrap strings in dicts
    reactions_data = [
        {"reaction": r} for r in result.reactions
    ]  # Wrap strings in dicts
    products_data = [product.dict() for product in result.products]

    openfda_data = ResultORM(
        report_number=result.report_number,
        date_created=result.date_created,
        date_started=result.date_started,
        consumer_age=result.consumer.age,
        consumer_age_unit=result.consumer.age_unit,
        consumer_gender=result.consumer.gender,
        outcomes=json.dumps(outcomes_data),
        reactions=json.dumps(reactions_data),
        products=json.dumps(products_data),
    )
    session.add(openfda_data)

session.commit()
session.close()
