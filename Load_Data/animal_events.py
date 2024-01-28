def animal_event_func(json_path, conn_string):
    from pydantic import BaseModel, Field, ValidationError
    from typing import Union
    from typing import Optional, List, Dict
    import json
    from sqlalchemy import create_engine, Column, Integer, String, Boolean
    from sqlalchemy.dialects.postgresql import JSONB

    # from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from pydantic import BaseModel
    from sqlalchemy.orm import declarative_base

    # Define Pydantic models based on the provided JSON structure
    class Dose(BaseModel):
        numerator: Optional[Union[str, int, float, List, Dict]] = None
        numerator_unit: Optional[Union[str, int, float, List, Dict]] = None
        denominator: Optional[Union[str, int, float, List, Dict]] = None
        denominator_unit: Optional[Union[str, int, float, List, Dict]] = None

    class ActiveIngredient(BaseModel):
        name: Optional[Union[str, int, float, List, Dict]] = None
        dose: Optional[Union[str, int, float, List, Dict]] = None

    class Manufacturer(BaseModel):
        name: Optional[Union[str, int, float, List, Dict]] = None
        registration_number: Optional[Union[str, int, float, List, Dict]] = None

    class Drug(BaseModel):
        manufacturing_date: Optional[Union[str, int, float, List, Dict]] = None
        lot_number: Optional[Union[str, int, float, List, Dict]] = None
        lot_expiration: Optional[Union[str, int, float, List, Dict]] = None
        product_ndc: Optional[Union[str, int, float, List, Dict]] = None
        brand_name: Optional[Union[str, int, float, List, Dict]] = None
        dosage_form: Optional[Union[str, int, float, List, Dict]] = None
        manufacturer: Optional[Manufacturer] = None
        number_of_defective_items: Optional[Union[str, int, float, List, Dict]] = None
        number_of_items_returned: Optional[Union[str, int, float, List, Dict]] = None
        atc_vet_code: Optional[Union[str, int, float, List, Dict]] = None
        active_ingredients: Optional[List[ActiveIngredient]] = None

    class HealthAssessmentPriorToExposure(BaseModel):
        assessed_by: Optional[Union[str, int, float, List, Dict]] = None
        condition: Optional[Union[str, int, float, List, Dict]] = None

    class ReactionDetail(BaseModel):
        veddra_version: Optional[Union[str, int, float, List, Dict]] = None
        veddra_term_code: Optional[Union[str, int, float, List, Dict]] = None
        veddra_term_name: Optional[Union[str, int, float, List, Dict]] = None
        number_of_animals_affected: Optional[Union[str, int, float, List, Dict]] = None
        accuracy: Optional[Union[str, int, float, List, Dict]] = None

    class Receiver(BaseModel):
        organization: Optional[Union[str, int, float, List, Dict]] = None
        street_address: Optional[Union[str, int, float, List, Dict]] = None
        city: Optional[Union[str, int, float, List, Dict]] = None
        state: Optional[Union[str, int, float, List, Dict]] = None
        postal_code: Optional[Union[str, int, float, List, Dict]] = None
        country: Optional[Union[str, int, float, List, Dict]] = None

    class OutcomeDetail(BaseModel):
        medical_status: Optional[Union[str, int, float, List, Dict]] = None
        number_of_animals_affected: Optional[Union[str, int, float, List, Dict]] = None

    class Age(BaseModel):
        max: Optional[Union[str, int, float, List, Dict]] = None
        min: Optional[Union[str, int, float, List, Dict]] = None
        qualifier: Optional[Union[str, int, float, List, Dict]] = None
        unit: Optional[Union[str, int, float, List, Dict]] = None

    class Breed(BaseModel):
        is_crossbred: Optional[Union[str, int, float, List, Dict]] = None
        breed_component: Optional[Union[str, int, float, List, Dict]] = None

        class Config:
            arbitrary_types_allowed = True  # Allow unexpected types

    class Weight(BaseModel):
        max: Optional[Union[str, int, float, List, Dict]] = None
        min: Optional[Union[str, int, float, List, Dict]] = None
        qualifier: Optional[Union[str, int, float, List, Dict]] = None
        unit: Optional[Union[str, int, float, List, Dict]] = None

    class Animal(BaseModel):
        age: Optional[Age] = None
        breed: Optional[Breed] = None
        female_animal_physiological_status: Optional[
            Union[str, int, float, List, Dict]
        ] = None
        gender: Optional[Union[str, int, float, List, Dict]] = None
        reproductive_status: Optional[str] = None
        species: Optional[Union[str, int, float, List, Dict]] = None
        weight: Optional[Weight] = None

    class Duration(BaseModel):
        unit: Optional[Union[str, int, float, List, Dict]] = None
        value: Optional[Union[str, int, float, List, Dict]] = None

    class Result(BaseModel):
        health_assessment_prior_to_exposure: Optional[HealthAssessmentPriorToExposure]
        onset_date: Optional[Union[str, int, float, List, Dict]] = None
        duration: Optional[Duration] = None
        reaction: Optional[List[ReactionDetail]] = None
        receiver: Optional[Receiver] = None
        unique_aer_id_number: Optional[Union[str, int, float, List, Dict]] = None
        report_id: Optional[Union[str, int, float, List, Dict]] = None
        original_receive_date: Optional[Union[str, int, float, List, Dict]] = None
        primary_reporter: Optional[Union[str, int, float, List, Dict]] = None
        type_of_information: Optional[Union[str, int, float, List, Dict]] = None
        drug: Optional[List[Drug]] = None
        treated_for_ae: Optional[Union[str, int, float, List, Dict]] = None
        number_of_animals_affected: Optional[Union[str, int, float, List, Dict]] = None
        number_of_animals_treated: Optional[Union[str, int, float, List, Dict]] = None
        secondary_reporter: Optional[Union[str, int, float, List, Dict]] = None
        animal: Optional[Animal] = None
        serious_ae: Optional[Union[str, int, float, List, Dict]] = None
        outcome: Optional[List[OutcomeDetail]] = None
        time_between_exposure_and_onset: Optional[
            Union[str, int, float, List, Dict]
        ] = None

    class AnimalEvent(BaseModel):
        results: List[Result]

    # Load the JSON data
    with open(json_path, "r") as file:
        data = json.load(file)

    for result in data["results"]:
        animal = result.get("animal")
        if animal and animal.get("breed"):
            breed_component = animal["breed"].get("breed_component")
            is_crossbred = animal["breed"].get("is_crossbred")
            if breed_component is not None:
                animal["breed"]["breed_component"] = str(breed_component)
            if is_crossbred is not None:
                animal["breed"]["is_crossbred"] = str(is_crossbred)
        for i in [
            result.get("unique_aer_id_number"),
            result.get("number_of_animals_affected"),
            result.get("number_of_animals_treated"),
            result.get("onset_date"),
            result.get("original_receive_date"),
            result.get("primary_reporter"),
            result.get("report_id"),
            result.get("secondary_reporter"),
            result.get("serious_ae"),
            result.get("time_between_exposure_and_onset"),
            result.get("treated_for_ae"),
            result.get("type_of_information"),
        ]:
            if i is not None:
                i = str(i)

    # Enhanced parsing with validation error handling
    try:
        animal_event = AnimalEvent.model_validate(data)
    except ValidationError as e:
        print("Error parsing JSON data:")
        for err in e.errors():
            print(err)

    # SQLAlchemy ORM class
    Base = declarative_base()

    class AnimalEventDB(Base):
        __tablename__ = "animalevent"
        unique_aer_id_number = Column(String, primary_key=True)
        animal = Column(JSONB)
        drug = Column(JSONB)
        duration = Column(JSONB)
        health_assessment_prior_to_exposure = Column(JSONB)
        number_of_animals_affected = Column(String)
        number_of_animals_treated = Column(String)
        onset_date = Column(String)
        original_receive_date = Column(String)
        outcome = Column(JSONB)
        primary_reporter = Column(String)
        reaction = Column(JSONB)
        receiver = Column(JSONB)
        report_id = Column(String)
        secondary_reporter = Column(String)
        serious_ae = Column(String)
        time_between_exposure_and_onset = Column(String)
        treated_for_ae = Column(String)
        type_of_information = Column(String)

    # Connect to the database
    engine = create_engine(conn_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Load JSON data
    with open(json_path) as reader:
        data = json.load(reader)
    model = AnimalEvent(results=data["results"])  # Access nested results

    # Persist data using SQLAlchemy ORM
    for result in model.results:
        health_assessment_prior_to_exposure_data = (
            result.health_assessment_prior_to_exposure.model_dump()
            if result.health_assessment_prior_to_exposure
            else {}
        )
        animal_data = result.animal.model_dump() if result.animal else {}
        reaction_data = (
            [r.model_dump() for r in result.reaction] if result.reaction else []
        )
        outcome_data = (
            [o.model_dump() for o in result.outcome] if result.outcome else []
        )
        drug_data = [d.model_dump() for d in result.drug] if result.drug else []
        duration_data = result.duration.model_dump() if result.duration else {}
        receiver_data = result.receiver.model_dump() if result.receiver else {}

        record = AnimalEventDB(
            animal=animal_data,
            drug=drug_data,
            duration=duration_data,
            health_assessment_prior_to_exposure=health_assessment_prior_to_exposure_data,
            number_of_animals_affected=result.number_of_animals_affected,
            number_of_animals_treated=result.number_of_animals_treated,
            onset_date=result.onset_date,
            original_receive_date=result.original_receive_date,
            outcome=outcome_data,
            primary_reporter=result.primary_reporter,
            reaction=reaction_data,
            receiver=receiver_data,
            report_id=result.report_id,
            secondary_reporter=result.secondary_reporter,
            serious_ae=result.serious_ae,
            time_between_exposure_and_onset=result.time_between_exposure_and_onset,
            treated_for_ae=result.treated_for_ae,
            type_of_information=result.type_of_information,
            unique_aer_id_number=result.unique_aer_id_number,
        )
        session.add(record)

    session.commit()
    session.close()


class AnimalEventClass:
    def create_table(self, conn_string):
        import psycopg2

        # Connect to Postgres
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        # Create single table
        cur.execute(
            """
            drop table if exists AnimalEvent
            """
        )

        # Create single table
        cur.execute(
            """create table AnimalEvent(
                unique_aer_id_number text,
                number_of_animals_affected text,
                number_of_animals_treated text,
                onset_date text,
                original_receive_date text,
                primary_reporter text,
                report_id text,
                secondary_reporter text,
                serious_ae text,
                time_between_exposure_and_onset text,
                treated_for_ae  text,
                type_of_information  text,
                animal jsonb,
                health_assessment_prior_to_exposure jsonb,
                drug jsonb,
                duration jsonb,
                reaction jsonb,
                receiver jsonb,
                outcome jsonb
                )
                """
        )
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
        zip_file_list = glob.glob("./openfda/animalandveterinary/event/*/*.json.zip")

        # Unzip each file found
        for zip_file in zip_file_list:
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                # Extract all the contents into the same directory as the zip file
                zip_ref.extractall(os.path.dirname(zip_file))

        # Find all json files in the specified path after unzipping
        file_list = glob.glob("./openfda/animalandveterinary/event/*/*.json")

        # Print the paths of the json files
        for file_path in tqdm(file_list):
            try:
                animal_event_func(file_path, conn_string)
                print(file_path)
            except:
                print(f" there is an error in this file {file_path}")
                continue


load_process = AnimalEventClass()
dir_path = "./langchain/learn_lang2/"
json_path = "./langchain/learn_lang2/openfda/animalandveterinary/event/2023q2/animalandveterinary-event-0001-of-0001.json"
conn_string = "postgresql://clinicaltrials:clinicaltrials@localhost:5432/postgres"
load_process.create_table(conn_string)
# animal_event_func(json_path,conn_string)
load_process.load_full_data(dir_path, conn_string)
