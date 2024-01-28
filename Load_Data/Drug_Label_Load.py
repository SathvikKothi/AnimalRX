from pydantic import BaseModel, ValidationError, Field
from typing import Optional, List, Dict, Union
import json
from sqlalchemy import create_engine, Column, String, JSON, Integer, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import JSONB


class DrugAttributeItem(BaseModel):
    format: Optional[Union[str, int, float, bool, List, Dict]] = None
    is_exact: Optional[Union[str, int, float, bool, List, Dict]] = None
    possible_values: Optional[Union[str, int, float, bool, List, Dict]] = None
    type: Optional[Union[str, int, float, bool, List, Dict]] = None


class DrugAttribute(BaseModel):
    area: Optional[Union[str, int, float, bool, List, Dict]] = None
    items: Optional[List[DrugAttributeItem]] = None
    type: Optional[Union[str, int, float, bool, List, Dict]] = None


class OpenFDA(BaseModel):
    application_number: Optional[Union[str, int, float, bool, List, Dict]] = None
    brand_name: Optional[Union[str, int, float, bool, List, Dict]] = None
    generic_name: Optional[Union[str, int, float, bool, List, Dict]] = None
    manufacturer_name: Optional[Union[str, int, float, bool, List, Dict]] = None
    product_ndc: Optional[Union[str, int, float, bool, List, Dict]] = None
    product_type: Optional[Union[str, int, float, bool, List, Dict]] = None
    route: Optional[Union[str, int, float, bool, List, Dict]] = None
    substance_name: Optional[Union[str, int, float, bool, List, Dict]] = None
    spl_id: Optional[Union[str, int, float, bool, List, Dict]] = None
    spl_set_id: Optional[Union[str, int, float, bool, List, Dict]] = None
    package_ndc: Optional[Union[str, int, float, bool, List, Dict]] = None
    original_packager_product_ndc: Optional[
        Union[str, int, float, bool, List, Dict]
    ] = None
    upc: Optional[Union[str, int, float, bool, List, Dict]] = None
    nui: Optional[Union[str, int, float, bool, List, Dict]] = None
    pharm_class_epc: Optional[Union[str, int, float, bool, List, Dict]] = None
    pharm_class_cs: Optional[Union[str, int, float, bool, List, Dict]] = None
    unii: Optional[Union[str, int, float, bool, List, Dict]] = None


class DrugLabel(BaseModel):
    id: str
    abuse: Optional[Union[str, List[str]]] = Field(default_factory=list)
    abuse_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    accessories: Optional[Union[str, List[str]]] = Field(default_factory=list)
    accessories_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    active_ingredient: Optional[Union[str, List[str]]] = Field(default_factory=list)
    active_ingredient_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    adverse_reactions: Optional[Union[str, List[str]]] = Field(default_factory=list)
    adverse_reactions_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    alarms: Optional[Union[str, List[str]]] = Field(default_factory=list)
    alarms_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    animal_pharmacology_and_or_toxicology: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    animal_pharmacology_and_or_toxicology_table: Optional[
        Union[str, List[str]]
    ] = Field(default_factory=list)
    ask_doctor: Optional[Union[str, List[str]]] = Field(default_factory=list)
    ask_doctor_or_pharmacist: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    ask_doctor_or_pharmacist_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    ask_doctor_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    assembly_or_installation_instructions: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    assembly_or_installation_instructions_table: Optional[
        Union[str, List[str]]
    ] = Field(default_factory=list)
    boxed_warning: Optional[Union[str, List[str]]] = Field(default_factory=list)
    boxed_warning_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    calibration_instructions: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    calibration_instructions_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    carcinogenesis_and_mutagenesis_and_impairment_of_fertility: Optional[
        Union[str, List[str]]
    ] = Field(default_factory=list)
    carcinogenesis_and_mutagenesis_and_impairment_of_fertility_table: Optional[
        Union[str, List[str]]
    ] = Field(default_factory=list)
    cleaning: Optional[Union[str, List[str]]] = Field(default_factory=list)
    cleaning_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    clinical_pharmacology: Optional[Union[str, List[str]]] = Field(default_factory=list)
    clinical_pharmacology_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    clinical_studies: Optional[Union[str, List[str]]] = Field(default_factory=list)
    clinical_studies_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    components: Optional[Union[str, List[str]]] = Field(default_factory=list)
    components_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    contraindications: Optional[Union[str, List[str]]] = Field(default_factory=list)
    contraindications_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    controlled_substance: Optional[Union[str, List[str]]] = Field(default_factory=list)
    controlled_substance_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    description: Optional[Union[str, List[str]]] = Field(default_factory=list)
    description_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    dosage_and_administration: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    dosage_and_administration_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    dosage_forms_and_strengths: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    dosage_forms_and_strengths_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    drug_abuse_and_dependence: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    drug_abuse_and_dependence_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    drug_and_or_laboratory_test_interactions: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    drug_and_or_laboratory_test_interactions_table: Optional[
        Union[str, List[str]]
    ] = Field(default_factory=list)
    drug_interactions: Optional[Union[str, List[str]]] = Field(default_factory=list)
    drug_interactions_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    effective_time: Optional[Union[str]] = Field(default_factory=str)
    general_precautions: Optional[Union[str, List[str]]] = Field(default_factory=list)
    general_precautions_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    geriatric_use: Optional[Union[str, List[str]]] = Field(default_factory=list)
    geriatric_use_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    how_supplied: Optional[Union[str, List[str]]] = Field(default_factory=list)
    how_supplied_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    indications_and_usage: Optional[Union[str, List[str]]] = Field(default_factory=list)
    indications_and_usage_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    information_for_owners_or_caregivers: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    information_for_owners_or_caregivers_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    information_for_patients: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    information_for_patients_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    instructions_for_use: Optional[Union[str, List[str]]] = Field(default_factory=list)
    instructions_for_use_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    intended_use_of_the_device: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    intended_use_of_the_device_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    labeler: Optional[Union[str, List[str]]] = Field(default_factory=list)
    labeler_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    laboratory_tests: Optional[Union[str, List[str]]] = Field(default_factory=list)
    laboratory_tests_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    mechanism_of_action: Optional[Union[str, List[str]]] = Field(default_factory=list)
    mechanism_of_action_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    microbiology: Optional[Union[str, List[str]]] = Field(default_factory=list)
    microbiology_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    nonclinical_toxicology: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    nonclinical_toxicology_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    nursing_mothers: Optional[Union[str, List[str]]] = Field(default_factory=list)
    nursing_mothers_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    overdosage: Optional[Union[str, List[str]]] = Field(default_factory=list)
    overdosage_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    package_label_principal_display_panel: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    package_label_principal_display_panel_table: Optional[
        Union[str, List[str]]
    ] = Field(default_factory=list)
    pediatric_use: Optional[Union[str, List[str]]] = Field(default_factory=list)
    pediatric_use_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    pharmacodynamics: Optional[Union[str, List[str]]] = Field(default_factory=list)
    pharmacodynamics_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    pharmacogenomics: Optional[Union[str, List[str]]] = Field(default_factory=list)
    pharmacogenomics_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    pharmacokinetics: Optional[Union[str, List[str]]] = Field(default_factory=list)
    pharmacokinetics_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    precautions: Optional[Union[str, List[str]]] = Field(default_factory=list)
    precautions_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    pregnancy: Optional[Union[str, List[str]]] = Field(default_factory=list)
    pregnancy_or_reproductive_potential: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    pregnancy_or_reproductive_potential_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    pregnancy_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    product_label: Optional[Union[str, List[str]]] = Field(default_factory=list)
    product_label_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    purpose: Optional[Union[str, List[str]]] = Field(default_factory=list)
    purpose_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    questions: Optional[Union[str, List[str]]] = Field(default_factory=list)
    questions_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    recent_major_changes: Optional[Union[str, List[str]]] = Field(default_factory=list)
    recent_major_changes_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    references: Optional[Union[str, List[str]]] = Field(default_factory=list)
    references_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    risks: Optional[Union[str, List[str]]] = Field(default_factory=list)
    risks_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    safe_handling_warning: Optional[Union[str, List[str]]] = Field(default_factory=list)
    safe_handling_warning_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    set_id: Optional[Union[str]] = Field(default_factory=str)
    spl_indexing_data_elements: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    spl_indexing_data_elements_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    spl_medguide: Optional[Union[str, List[str]]] = Field(default_factory=list)
    spl_medguide_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    spl_patient_package_insert: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    spl_patient_package_insert_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    spl_product_data_elements: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    spl_product_data_elements_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    spl_unclassified_section: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    spl_unclassified_section_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    statement_of_identity: Optional[Union[str, List[str]]] = Field(default_factory=list)
    statement_of_identity_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    stop_use: Optional[Union[str, List[str]]] = Field(default_factory=list)
    stop_use_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    storage_and_handling: Optional[Union[str, List[str]]] = Field(default_factory=list)
    storage_and_handling_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    summary_of_safety_and_effectiveness: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    summary_of_safety_and_effectiveness_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    teratogenic_effects: Optional[Union[str, List[str]]] = Field(default_factory=list)
    teratogenic_effects_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    troubleshooting: Optional[Union[str, List[str]]] = Field(default_factory=list)
    troubleshooting_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    use_in_specific_populations: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    use_in_specific_populations_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    user_safety_warnings: Optional[Union[str, List[str]]] = Field(default_factory=list)
    user_safety_warnings_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    version: Optional[Union[str]] = Field(default_factory=str)
    warnings: Optional[Union[str, List[str]]] = Field(default_factory=list)
    warnings_and_cautions: Optional[Union[str, List[str]]] = Field(default_factory=list)
    warnings_and_cautions_table: Optional[Union[str, List[str]]] = Field(
        default_factory=list
    )
    warnings_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    when_using: Optional[Union[str, List[str]]] = Field(default_factory=list)
    when_using_table: Optional[Union[str, List[str]]] = Field(default_factory=list)
    openfda: Optional[OpenFDA] = None


# SQLAlchemy ORM class
Base = declarative_base()


class DrugLabelORM(Base):
    __tablename__ = "drug_label"
    id = Column(String, primary_key=True)
    abuse = Column(JSONB)
    abuse_table = Column(JSONB)
    accessories = Column(JSONB)
    accessories_table = Column(JSONB)
    openfda = Column(JSONB)
    active_ingredient = Column(JSONB)
    active_ingredient_table = Column(JSONB)
    adverse_reactions = Column(JSONB)
    adverse_reactions_table = Column(JSONB)
    alarms = Column(JSONB)
    alarms_table = Column(JSONB)
    animal_pharmacology_and_or_toxicology = Column(JSONB)
    animal_pharmacology_and_or_toxicology_table = Column(JSONB)
    ask_doctor = Column(JSONB)
    ask_doctor_or_pharmacist = Column(JSONB)
    ask_doctor_or_pharmacist_table = Column(JSONB)
    ask_doctor_table = Column(JSONB)
    assembly_or_installation_instructions = Column(JSONB)
    assembly_or_installation_instructions_table = Column(JSONB)
    boxed_warning = Column(JSONB)
    boxed_warning_table = Column(JSONB)
    calibration_instructions = Column(JSONB)
    calibration_instructions_table = Column(JSONB)
    carcinogenesis_and_mutagenesis_and_impairment_of_fertility = Column(JSONB)
    carcinogenesis_and_mutagenesis_and_impairment_of_fertility_table = Column(JSONB)
    cleaning = Column(JSONB)
    cleaning_table = Column(JSONB)
    clinical_pharmacology = Column(JSONB)
    clinical_pharmacology_table = Column(JSONB)
    clinical_studies = Column(JSONB)
    clinical_studies_table = Column(JSONB)
    components = Column(JSONB)
    components_table = Column(JSONB)
    contraindications = Column(JSONB)
    contraindications_table = Column(JSONB)
    controlled_substance = Column(JSONB)
    controlled_substance_table = Column(JSONB)
    description = Column(JSONB)
    description_table = Column(JSONB)
    dosage_and_administration = Column(JSONB)
    dosage_and_administration_table = Column(JSONB)
    dosage_forms_and_strengths = Column(JSONB)
    dosage_forms_and_strengths_table = Column(JSONB)
    drug_abuse_and_dependence = Column(JSONB)
    drug_abuse_and_dependence_table = Column(JSONB)
    drug_and_or_laboratory_test_interactions = Column(JSONB)
    drug_and_or_laboratory_test_interactions_table = Column(JSONB)
    drug_interactions = Column(JSONB)
    drug_interactions_table = Column(JSONB)
    effective_time = Column(String)
    general_precautions = Column(JSONB)
    general_precautions_table = Column(JSONB)
    geriatric_use = Column(JSONB)
    geriatric_use_table = Column(JSONB)
    how_supplied = Column(JSONB)
    how_supplied_table = Column(JSONB)
    indications_and_usage = Column(JSONB)
    indications_and_usage_table = Column(JSONB)
    information_for_owners_or_caregivers = Column(JSONB)
    information_for_owners_or_caregivers_table = Column(JSONB)
    information_for_patients = Column(JSONB)
    information_for_patients_table = Column(JSONB)
    instructions_for_use = Column(JSONB)
    instructions_for_use_table = Column(JSONB)
    intended_use_of_the_device = Column(JSONB)
    intended_use_of_the_device_table = Column(JSONB)
    labeler = Column(JSONB)
    labeler_table = Column(JSONB)
    laboratory_tests = Column(JSONB)
    laboratory_tests_table = Column(JSONB)
    mechanism_of_action = Column(JSONB)
    mechanism_of_action_table = Column(JSONB)
    microbiology = Column(JSONB)
    microbiology_table = Column(JSONB)
    nonclinical_toxicology = Column(JSONB)
    nonclinical_toxicology_table = Column(JSONB)
    nursing_mothers = Column(JSONB)
    nursing_mothers_table = Column(JSONB)
    overdosage = Column(JSONB)
    overdosage_table = Column(JSONB)
    package_label_principal_display_panel = Column(JSONB)
    package_label_principal_display_panel_table = Column(JSONB)
    pediatric_use = Column(JSONB)
    pediatric_use_table = Column(JSONB)
    pharmacodynamics = Column(JSONB)
    pharmacodynamics_table = Column(JSONB)
    pharmacogenomics = Column(JSONB)
    pharmacogenomics_table = Column(JSONB)
    pharmacokinetics = Column(JSONB)
    pharmacokinetics_table = Column(JSONB)
    precautions = Column(JSONB)
    precautions_table = Column(JSONB)
    pregnancy = Column(JSONB)
    pregnancy_or_reproductive_potential = Column(JSONB)
    pregnancy_or_reproductive_potential_table = Column(JSONB)
    pregnancy_table = Column(JSONB)
    product_label = Column(JSONB)
    product_label_table = Column(JSONB)
    purpose = Column(JSONB)
    purpose_table = Column(JSONB)
    questions = Column(JSONB)
    questions_table = Column(JSONB)
    recent_major_changes = Column(JSONB)
    recent_major_changes_table = Column(JSONB)
    references = Column(JSONB)
    references_table = Column(JSONB)
    risks = Column(JSONB)
    risks_table = Column(JSONB)
    safe_handling_warning = Column(JSONB)
    safe_handling_warning_table = Column(JSONB)
    set_id = Column(String)
    spl_indexing_data_elements = Column(JSONB)
    spl_indexing_data_elements_table = Column(JSONB)
    spl_medguide = Column(JSONB)
    spl_medguide_table = Column(JSONB)
    spl_patient_package_insert = Column(JSONB)
    spl_patient_package_insert_table = Column(JSONB)
    spl_product_data_elements = Column(JSONB)
    spl_product_data_elements_table = Column(JSONB)
    spl_unclassified_section = Column(JSONB)
    spl_unclassified_section_table = Column(JSONB)
    statement_of_identity = Column(JSONB)
    statement_of_identity_table = Column(JSONB)
    stop_use = Column(JSONB)
    stop_use_table = Column(JSONB)
    storage_and_handling = Column(JSONB)
    storage_and_handling_table = Column(JSONB)
    summary_of_safety_and_effectiveness = Column(JSONB)
    summary_of_safety_and_effectiveness_table = Column(JSONB)
    teratogenic_effects = Column(JSONB)
    teratogenic_effects_table = Column(JSONB)
    troubleshooting = Column(JSONB)
    troubleshooting_table = Column(JSONB)
    use_in_specific_populations = Column(JSONB)
    use_in_specific_populations_table = Column(JSONB)
    user_safety_warnings = Column(JSONB)
    user_safety_warnings_table = Column(JSONB)
    version = Column(String)
    warnings = Column(JSONB)
    warnings_and_cautions = Column(JSONB)
    warnings_and_cautions_table = Column(JSONB)
    warnings_table = Column(JSONB)
    when_using = Column(JSONB)
    when_using_table = Column(JSONB)
    openfda = Column(JSONB)


class DrugLabelClass:
    def load_full_data(self, dir_path, conn_string, table_name, recreate_flag="N"):
        import os
        import glob
        import zipfile
        from tqdm import tqdm

        os.chdir(dir_path)
        zip_file_list = glob.glob("./openfda/drug/label/*.json.zip")

        for zip_file in tqdm(zip_file_list):
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                zip_ref.extractall(os.path.dirname(zip_file))
                file_path = zip_file.replace(".zip", "")

                if recreate_flag == "Y":
                    drug_label_func(None, conn_string, recreate_flag, table_name)
                    recreate_flag = "N"

                try:
                    drug_label_func(file_path, conn_string, recreate_flag, table_name)
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error in file {file_path}: {e}")
                    continue


def drug_label_func(json_path, conn_string, recreate_flag, table_name):
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
                    drug_label_data = DrugLabel.model_validate(record)
                    drug_label_dict = drug_label_data.model_dump()
                    drug_label_orm = DrugLabelORM(**drug_label_dict)
                    session.add(drug_label_orm)
                except ValidationError as e:
                    print(f"Error parsing JSON data: {e}")
                    session.rollback()

            session.commit()
        finally:
            session.close()


table_name = "drug_label"
load_process = DrugLabelClass()
dir_path = "/Users/ramanakothi/AnimalRx/Data"
conn_string = "postgresql://clinicaltrials:clinicaltrials@localhost:5432/postgres"
load_process.load_full_data(dir_path, conn_string, table_name, recreate_flag="Y")
