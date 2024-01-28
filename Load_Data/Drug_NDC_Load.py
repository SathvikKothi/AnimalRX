def drug_ndc_func(json_path, conn_string):
    from pydantic import BaseModel, Field, ValidationError
    from typing import Optional, List, Dict, Union
    import json
    from sqlalchemy import create_engine, Column, String, JSON
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.orm import sessionmaker, declarative_base
    from enum import Enum

    class DeaSchedule(str, Enum):
        CI = "CI"
        CII = "CII"
        CIII = "CIII"
        CIV = "CIV"
        CV = "CV"

    class ActiveIngredient(BaseModel):
        name: Optional[Union[str, int, float, bool, List, Dict]] = None
        strength: Optional[Union[str, int, float, bool, List, Dict]] = None

    class Packaging(BaseModel):
        package_ndc: Optional[Union[str, int, float, bool, List, Dict]] = None
        description: Optional[Union[str, int, float, bool, List, Dict]] = None
        marketing_start_date: Optional[Union[str, int, float, bool, List, Dict]] = None
        marketing_end_date: Optional[Union[str, int, float, bool, List, Dict]] = None
        sample: Optional[bool] = None

    class OpenFDA(BaseModel):
        is_original_packager: Optional[Union[str, int, float, bool, List, Dict]] = None
        manufacturer_name: Optional[Union[str, int, float, bool, List, Dict]] = None
        nui: Optional[Union[str, int, float, bool, List, Dict]] = None
        pharm_class_cs: Optional[Union[str, int, float, bool, List, Dict]] = None
        pharm_class_epc: Optional[Union[str, int, float, bool, List, Dict]] = None
        pharm_class_moa: Optional[Union[str, int, float, bool, List, Dict]] = None
        pharm_class_pe: Optional[Union[str, int, float, bool, List, Dict]] = None
        rxcui: Optional[Union[str, int, float, bool, List, Dict]] = None
        spl_set_id: Optional[Union[str, int, float, bool, List, Dict]] = None
        unii: Optional[Union[str, int, float, bool, List, Dict]] = None
        upc: Optional[Union[str, int, float, bool, List, Dict]] = None

    class DrugNDC(BaseModel):
        product_id: Optional[Union[str, int, float, bool, List, Dict]] = None
        product_ndc: Optional[Union[str, int, float, bool, List, Dict]] = None
        spl_id: Optional[Union[str, int, float, bool, List, Dict]] = None
        product_type: Optional[Union[str, int, float, bool, List, Dict]] = None
        finished: Optional[Union[str, int, float, bool, List, Dict]] = None
        brand_name: Optional[Union[str, int, float, bool, List, Dict]] = None
        brand_name_base: Optional[Union[str, int, float, bool, List, Dict]] = None
        brand_name_suffix: Optional[Union[str, int, float, bool, List, Dict]] = None
        generic_name: Optional[Union[str, int, float, bool, List, Dict]] = None
        dosage_form: Optional[Union[str, int, float, bool, List, Dict]] = None
        route: Optional[Union[str, int, float, bool, List, Dict]] = None
        marketing_start_date: Optional[Union[str, int, float, bool, List, Dict]] = None
        marketing_end_date: Optional[Union[str, int, float, bool, List, Dict]] = None
        marketing_category: Optional[Union[str, int, float, bool, List, Dict]] = None
        application_number: Optional[Union[str, int, float, bool, List, Dict]] = None
        pharm_class: Optional[Union[str, int, float, bool, List, Dict]] = None
        listing_expiration_date: Optional[
            Union[str, int, float, bool, List, Dict]
        ] = None
        dea_schedule: Optional[DeaSchedule] = None
        active_ingredients: List[ActiveIngredient] = []
        packaging: List[Packaging] = []
        openfda: Optional[OpenFDA] = None

    class DrugNDCModel(BaseModel):
        results: List[DrugNDC]

    # Load the JSON data
    with open(json_path, "r") as file:
        data = json.load(file)

    # SQLAlchemy ORM class
    Base = declarative_base()

    class DrugNDC_BASE(Base):
        __tablename__ = "drug_ndc"
        product_id = Column(String, primary_key=True)
        product_ndc = Column(String)
        spl_id = Column(String)
        product_type = Column(String)
        finished = Column(String)
        brand_name = Column(String)
        brand_name_base = Column(String)
        brand_name_suffix = Column(String)
        generic_name = Column(String)
        dosage_form = Column(String)
        route = Column(String)
        marketing_start_date = Column(String)
        marketing_end_date = Column(String)
        marketing_category = Column(String)
        application_number = Column(String)
        pharm_class = Column(String)
        listing_expiration_date = Column(String)
        dea_schedule = Column(JSONB)
        active_ingredients = Column(JSONB)
        packaging = Column(JSONB)
        openfda = Column(JSONB)

    # Connect to the database
    engine = create_engine(conn_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Process each record in the data
    for result in data["results"]:
        try:
            # Parse the data using the DrugNDC model
            drug_ndc_record = DrugNDC.model_validate(result)

            # Create a new DrugNDC_BASE record
            record = DrugNDC_BASE(
                product_id=drug_ndc_record.product_id,
                product_ndc=drug_ndc_record.product_ndc,
                spl_id=drug_ndc_record.spl_id,
                product_type=drug_ndc_record.product_type,
                finished=drug_ndc_record.finished,
                brand_name=drug_ndc_record.brand_name,
                brand_name_base=drug_ndc_record.brand_name_base,
                brand_name_suffix=drug_ndc_record.brand_name_suffix,
                generic_name=drug_ndc_record.generic_name,
                dosage_form=drug_ndc_record.dosage_form,
                route=drug_ndc_record.route,
                marketing_start_date=drug_ndc_record.marketing_start_date,
                marketing_end_date=drug_ndc_record.marketing_end_date,
                marketing_category=drug_ndc_record.marketing_category,
                application_number=drug_ndc_record.application_number,
                pharm_class=drug_ndc_record.pharm_class,
                dea_schedule=drug_ndc_record.dea_schedule,
                listing_expiration_date=drug_ndc_record.listing_expiration_date,
                active_ingredients=(
                    [
                        ingredient.model_dump()
                        for ingredient in drug_ndc_record.active_ingredients
                    ]
                ),
                packaging=(
                    [package.model_dump() for package in drug_ndc_record.packaging]
                ),
                openfda=(
                    drug_ndc_record.openfda.model_dump()
                    if drug_ndc_record.openfda
                    else {}
                ),
            )

            # Add the record to the session and commit
            session.add(record)
            session.commit()

        except ValidationError as e:
            print(f"Error parsing record: {e}")

    # Close the session
    session.close()


class DrugNDCClass:
    def create_table(self, conn_string):
        import psycopg2

        # Connect to Postgres
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        # Drop existing table if it exists
        cur.execute("DROP TABLE IF EXISTS drug_NDC")

        # Create the table
        cur.execute(
            """CREATE TABLE drug_NDC (
                product_id TEXT,
                product_ndc TEXT,
                spl_id TEXT,
                product_type TEXT,
                finished TEXT,
                brand_name TEXT,
                brand_name_base TEXT,
                brand_name_suffix TEXT,
                generic_name TEXT,
                dosage_form TEXT,
                route TEXT,
                marketing_start_date TEXT,
                marketing_end_date TEXT,
                marketing_category TEXT,
                application_number TEXT,
                pharm_class TEXT,
                dea_schedule TEXT,
                listing_expiration_date TEXT,
                active_ingredients JSONB,
                packaging JSONB,
                openfda JSONB
            )"""
        )

        # Commit and close the connection
        conn.commit()
        cur.close()
        conn.close()

    def load_full_data(self, dir_path, conn_string):
        import os
        import glob
        import zipfile
        from tqdm import tqdm

        # Change to the specified directory
        os.chdir(dir_path)

        # Find all zip files in the specified path
        zip_file_list = glob.glob("./openfda/drug/ndc/*.json.zip")

        # Unzip each file found
        for zip_file in zip_file_list:
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                zip_ref.extractall(os.path.dirname(zip_file))

        # Find all json files in the specified path after unzipping
        file_list = glob.glob("./openfda/drug/ndc/*.json")
        print(file_list)

        # Process each JSON file
        for file_path in file_list:
            print(file_path)
            try:
                drug_ndc_func(file_path, conn_string)
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")


load_process = DrugNDCClass()
dir_path = "./AnimalRx/Data/"
json_path = (
    "./AnimalRx/Data/openfda/drug/ndc/drug-ndc-0001-of-0001.json"
)
conn_string = "postgresql://clinicaltrials:clinicaltrials@localhost:5432/postgres"
load_process.create_table(conn_string)
load_process.load_full_data(dir_path, conn_string)
