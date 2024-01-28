from pydantic import BaseModel, ValidationError, Field
from typing import Optional, List, Dict, Union
import json
from sqlalchemy import create_engine, Column, String, Integer, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from pydantic import BaseModel
import uuid


class OpenFDAModel(BaseModel):
    application_number: Optional[Union[str, int, float, bool, List, Dict]] = None
    brand_name: Optional[Union[str, int, float, bool, List, Dict]] = None
    generic_name: Optional[Union[str, int, float, bool, List, Dict]] = None
    manufacturer_name: Optional[Union[str, int, float, bool, List, Dict]] = None
    product_ndc: Optional[Union[str, int, float, bool, List, Dict]] = None
    product_type: Optional[Union[str, int, float, bool, List, Dict]] = None
    route: Optional[Union[str, int, float, bool, List, Dict]] = None
    substance_name: Optional[Union[str, int, float, bool, List, Dict]] = None
    rxcui: Optional[Union[str, int, float, bool, List, Dict]] = None
    spl_id: Optional[Union[str, int, float, bool, List, Dict]] = None
    spl_set_id: Optional[Union[str, int, float, bool, List, Dict]] = None
    package_ndc: Optional[Union[str, int, float, bool, List, Dict]] = None
    nui: Optional[Union[str, int, float, bool, List, Dict]] = None
    pharm_class_epc: Optional[Union[str, int, float, bool, List, Dict]] = None
    pharm_class_moa: Optional[Union[str, int, float, bool, List, Dict]] = None
    unii: Optional[Union[str, int, float, bool, List, Dict]] = None


class DrugModel(BaseModel):
    drugcharacterization: Optional[Union[str, int, float, bool, List, Dict]] = None
    medicinalproduct: Optional[Union[str, int, float, bool, List, Dict]] = None
    drugauthorizationnumb: Optional[Union[str, int, float, bool, List, Dict]] = None
    drugdosagetext: Optional[Union[str, int, float, bool, List, Dict]] = None
    drugadministrationroute: Optional[Union[str, int, float, bool, List, Dict]] = None
    drugindication: Optional[Union[str, int, float, bool, List, Dict]] = None
    drugstartdateformat: Optional[Union[str, int, float, bool, List, Dict]] = None
    drugstartdate: Optional[Union[str, int, float, bool, List, Dict]] = None
    openfda: Optional[Union[str, int, float, bool, List, Dict]] = None


class ReactionModel(BaseModel):
    reactionmeddrapt: Optional[Union[str, int, float, bool, List, Dict]] = None


class PatientModel(BaseModel):
    patientsex: Optional[Union[str, int, float, bool, List, Dict]] = None
    reaction: Optional[List[ReactionModel]] = None
    drug: Optional[List[DrugModel]] = None


class PrimarySourceModel(BaseModel):
    reportercountry: Optional[Union[str, int, float, bool, List, Dict]] = None
    qualification: Optional[Union[str, int, float, bool, List, Dict]] = None


class SenderModel(BaseModel):
    senderorganization: Optional[Union[str, int, float, bool, List, Dict]] = None


class ReceiverModel(BaseModel):
    receiverorganization: Optional[Union[str, int, float, bool, List, Dict]] = None


class ResultModel(BaseModel):
    safetyreportid: Optional[Union[str, int, float, bool, List, Dict]] = None
    transmissiondateformat: Optional[Union[str, int, float, bool, List, Dict]] = None
    transmissiondate: Optional[Union[str, int, float, bool, List, Dict]] = None
    serious: Optional[Union[str, int, float, bool, List, Dict]] = None
    seriousnesshospitalization: Optional[
        Union[str, int, float, bool, List, Dict]
    ] = None
    seriousnessother: Optional[Union[str, int, float, bool, List, Dict]] = None
    receivedateformat: Optional[Union[str, int, float, bool, List, Dict]] = None
    receivedate: Optional[Union[str, int, float, bool, List, Dict]] = None
    receiptdateformat: Optional[Union[str, int, float, bool, List, Dict]] = None
    receiptdate: Optional[Union[str, int, float, bool, List, Dict]] = None
    fulfillexpeditecriteria: Optional[Union[str, int, float, bool, List, Dict]] = None
    companynumb: Optional[Union[str, int, float, bool, List, Dict]] = None
    primarysource: Optional[PrimarySourceModel] = None
    sender: Optional[SenderModel] = None
    receiver: Optional[ReceiverModel] = None
    patient: Optional[PatientModel] = None


class DrugEventReportModel(BaseModel):
    results: List[ResultModel]


# SQLAlchemy ORM class
Base = declarative_base()


class DrugLabelORM(Base):
    __tablename__ = "drug_events"
    id = Column(String, primary_key=True)
    safetyreportid = Column(String)
    transmissiondateformat = Column(String)
    transmissiondate = Column(String)
    serious = Column(String)
    seriousnesshospitalization = Column(String)
    seriousnessother = Column(String)
    receivedateformat = Column(String)
    receivedate = Column(String)
    receiptdateformat = Column(String)
    receiptdate = Column(String)
    fulfillexpeditecriteria = Column(String)
    companynumb = Column(String)
    primarysource = Column(JSONB)
    sender = Column(JSONB)
    receiver = Column(JSONB)
    patient = Column(JSONB)


# Class to handle drug event data loading
class DrugEventClass:
    def load_full_data(self, dir_path, conn_string, table_name, recreate_flag="N"):
        import os
        import glob
        import zipfile
        from tqdm import tqdm

        os.chdir(dir_path)
        zip_file_list = glob.glob("./openfda/drug/event/*/*.json.zip")

        for zip_file in tqdm(zip_file_list):
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                zip_ref.extractall(os.path.dirname(zip_file))
                file_path = zip_file.replace(".zip", "")

                if recreate_flag == "Y":
                    drug_event_func(None, conn_string, recreate_flag, table_name)
                    recreate_flag = "N"

                try:
                    drug_event_func(file_path, conn_string, recreate_flag, table_name)
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error in file {file_path}: {e}")
                    continue


def generate_unique_id():
    return str(uuid.uuid4())


def drug_event_func(json_path, conn_string, recreate_flag, table_name):
    engine = create_engine(conn_string)
    if recreate_flag == "Y":
        with engine.begin() as conn:
            my_query = text(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(my_query)
        Base.metadata.create_all(engine)

    if json_path is not None:
        Session = sessionmaker(bind=engine)
        session = Session()
        with open(json_path, "r") as file:
            json_data = json.load(file)

        results = json_data.get("results")
        if not results:
            print("No 'results' key found in JSON data")
            return

        try:
            for record in results:
                try:
                    drug_event_data = ResultModel.model_validate(record)
                    drug_event_dict = drug_event_data.model_dump()
                    # Generate a unique ID if not present
                    drug_event_dict[
                        "id"
                    ] = (
                        generate_unique_id()
                    )  # Replace generate_unique_id() with your ID generation logic
                    drug_event_orm = DrugLabelORM(**drug_event_dict)
                    session.add(drug_event_orm)
                except ValidationError as e:
                    print(f"Error parsing JSON data: {e}")
                    session.rollback()

            session.commit()
        finally:
            session.close()


table_name = "drug_events"
load_process = DrugEventClass()
dir_path = "/Users/ramanakothi/AnimalRx/Data"
conn_string = "postgresql://clinicaltrials:clinicaltrials@localhost:5432/postgres"
load_process.load_full_data(dir_path, conn_string, table_name, recreate_flag="Y")
