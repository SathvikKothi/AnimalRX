import pandas as pd
import wikipediaapi
import pandas as pd
import streamlit as st
import random
import glob
import os, re
import pandas as pd
#from Read_data.functions_used import *
from Read_data.functions_used_local import *
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Date
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns

conn_string = "postgres://tsdbadmin:YIZ3c-8i-90iJm@q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com:37746/tsdb?sslmode=require"


def get_distinct_drug_indicaiton():
    import psycopg2
    # conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)

    query = """
    select distinct active_ingredient_name,veddra_term_name from Active_ing_veddra_term_name
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    active_ingredient_name = df["active_ingredient_name"].tolist()
    veddra_term_name = df["veddra_term_name"].tolist()
    return active_ingredient_name,veddra_term_name

def get_distinct_breeds(species):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    query = f"""
        select breed_component from species_breed_info 
        where include_flag ='Y' and  upper(species) = '{species.upper()}'
     """
    df = pd.read_sql_query(query, conn)
    conn.close()
    bread_component = df["breed_component"].tolist()
    return bread_component


def get_distinct_species():
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    query = f"""
         select distinct species from species_breed_info 
        where include_flag ='Y'
        """
    df = pd.read_sql_query(query, conn)
    conn.close()
    species = df["species"].tolist()
    return species


# Function to fetch information from Wikipedia
def fetch_wikipedia_info(selected_species, animal_name):
    user_agent = "AnimalInfoExplorer/1.0 (ramana@zendata.co)"
    wiki_api = wikipediaapi.Wikipedia(
        language="en",
        user_agent=user_agent,
        extract_format=wikipediaapi.ExtractFormat.WIKI,
    )
    page = wiki_api.page(animal_name)
    if page.exists():
        return page.summary
    else:
        try:
            page = wiki_api.page(selected_species + animal_name)
            if page.exists():
                return page.summary
            else:
                page = wiki_api.page(selected_species)
                if page.exists():
                    return page.summary
                else:
                    return ""
        except:
            return ""


#################
def get_unique_drugs(species, breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # Use parameterized queries instead of string formatting for safety and correctness
    query = f"""
    select drug,cnt frequency from drugs_by_breed_species where breed =  '{breed}'
    and species = '{species}'
    order by cnt desc
    """
    #print(f"Query get_most_common_drugs  {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


##########
def get_most_common_drugs(species, breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # Use parameterized queries instead of string formatting for safety and correctness
    query = f"""
        select active_ingredient_name,cnt frequency from drugs_by_breed_species where breed =  '{breed}'
        and species = '{species}'
        order by cnt desc
        """
    #print(f"Query get_most_common_drugs  {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def get_animals_by_species(species, breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves the number of animals affected by species."""
    query = f"""
    select  breed as species, round(cnt) as count from species_breed_cnts
        where upper(species) = '{species}' 
        and upper(breed) = '{breed}'
        """
    #print(  f"Query with no data for Details on animals by species with severity  {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def get_reactions_by_severity(species, breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves the number of reactions by severity."""
    query = f"""
        SELECT reaction->>'veddra_term_name' as reaction, COUNT(*) as count 
        FROM animal_events ae
        WHERE  TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') AND TO_DATE('{end_date}', 'YYYY-MM-DD')
        and ae.reaction IS NOT NULL 
        and ae.original_receive_date IS NOT NULL 
        AND ae.onset_date IS NOT NULL
        AND length(ae.original_receive_date)=8 
        AND length(ae.onset_date)=8
        AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%' 
        and length(trim(upper(ae.animal ->> 'species'))) > 1 
        and ae.animal -> 'breed' ->> 'breed_component' is not null
        and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
        and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
        AND upper(ae.animal ->> 'species') = '{species}' 
        and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{breed}'
        GROUP BY reaction
        """
    #print(f"Query Details on reactions_over_time with severity  {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Get methods for insights (updated for correct date handling)
def get_reactions_over_time(selected_species, selected_breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves the number of reactions over time, handling date conversion."""
    query = f"""
        SELECT date_trunc('month',TO_DATE(onset_date, 'YYYYMMDD')) as month, COUNT(*) as count
        FROM animal_events ae
        where ae.onset_date is not null
        and TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') AND TO_DATE('{end_date}', 'YYYY-MM-DD')
        and ae.original_receive_date IS NOT NULL 
        AND length(ae.original_receive_date)=8 
        AND length(ae.onset_date)=8
        AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%' 
        and length(trim(upper(ae.animal ->> 'species'))) > 1 
        and ae.animal -> 'breed' ->> 'breed_component' is not null
        and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
        and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
        AND upper(ae.animal ->> 'species') = '{selected_species}'
        and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
        GROUP BY month
        ORDER BY month
        """
    #print(f"Query Details on reactions_over_time information  {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    df["month"] = pd.to_datetime(df["month"])
    return df


def get_reactions_over_time(selected_species, selected_breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves reactions over time, efficiently handling dates and filtering."""
    query = f"""
        SELECT TO_DATE(onset_date, 'YYYYMMDD') AS month,
               COUNT(*) AS count
        FROM animal_events ae
        WHERE  TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') AND TO_DATE('{end_date}', 'YYYY-MM-DD')
        and ae.original_receive_date IS NOT NULL 
        AND ae.onset_date IS NOT NULL
        AND length(ae.original_receive_date)=8 
        AND length(ae.onset_date)=8
        AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%' 
        and length(trim(upper(ae.animal ->> 'species'))) > 1 
        and ae.animal -> 'breed' ->> 'breed_component' is not null
        and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
        and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
        AND upper(ae.animal ->> 'species') = '{selected_species}'
        and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
        GROUP BY month
        ORDER BY month
        """
    #print(f"Query for Details on reactions_over_time information  {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


##--------------------------------------------------------------------------------
## Detail Drug information with filter by species, animal and time
##----------------------------------------------------------------------------------
def get_detail_drug_info(selected_species, selected_breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves reactions over time, efficiently handling dates and filtering."""
    query = f"""
        SELECT
        t.unique_aer_id_number,
        t.number_of_animals_affected,
        t.number_of_animals_treated,
        TO_DATE(t.onset_date, 'YYYYMMDD') as onset_date,
        TO_DATE(t.original_receive_date, 'YYYYMMDD') as original_receive_date,
        t.drug_details->>'brand_name' AS brand_name,
        t.drug_details->>'lot_number' AS lot_number,
        t.drug_details->>'dosage_form' AS dosage_form,
        t.drug_details->>'product_ndc' AS product_ndc,
        t.drug_details->>'atc_vet_code' AS atc_vet_code,
        t.drug_details->'manufacturer'->>'name' AS manufacturer_name,
        t.drug_details->'manufacturer'->>'registration_number' AS manufacturer_registration_number,
        ai.active_ingredient_names
    FROM (
        SELECT
            unique_aer_id_number,
            number_of_animals_affected,
            number_of_animals_treated,
            onset_date,
            original_receive_date,
            jsonb_array_elements(drug) AS drug_details
        FROM animal_events ae
        WHERE  TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
                AND TO_DATE('{end_date}', 'YYYY-MM-DD')
                and ae.original_receive_date IS NOT NULL
                AND ae.onset_date IS NOT NULL
                AND length(ae.original_receive_date)=8
                AND length(ae.onset_date)=8
                AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
                and length(trim(upper(ae.animal ->> 'species'))) > 1
                and ae.animal -> 'breed' ->> 'breed_component' is not null
                and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
                and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
                AND upper(ae.animal ->> 'species') = '{selected_species}'
                and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
    ) t
    CROSS JOIN LATERAL (
        SELECT
            CASE
                WHEN jsonb_typeof(t.drug_details->'active_ingredients') = 'array' THEN
                    string_agg(ai_element->>'name', ', ')
                ELSE
                    NULL
            END AS active_ingredient_names
        FROM
            jsonb_array_elements(
                CASE
                    WHEN jsonb_typeof(t.drug_details->'active_ingredients') = 'array' THEN
                        t.drug_details->'active_ingredients'
                    ELSE
                        '[]'::jsonb
                END
            ) AS ai_element
    ) ai
    GROUP BY
        t.unique_aer_id_number,
        t.number_of_animals_affected,
        t.number_of_animals_treated,
        t.onset_date,
        t.original_receive_date,
        t.drug_details,
        ai.active_ingredient_names
        """
    #print(f"Query for Detail Drug information query {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


##--------------------------------------------------------------------------------
## Detail Animal information with filter by species, animal and time
##----------------------------------------------------------------------------------
def get_detail_animal_info(selected_species, selected_breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves reactions over time, efficiently handling dates and filtering."""
    query = f"""
        SELECT
    ae.unique_aer_id_number,
    ae.number_of_animals_affected,
    ae.number_of_animals_treated,
    ae.animal -> 'age' ->> 'max' AS age_max,
    ae.animal -> 'age' ->> 'min' AS age_min,
    ae.animal -> 'age' ->> 'unit' AS age_unit,
    ae.animal -> 'age' ->> 'qualifier' AS age_qualifier,
    ae.animal -> 'breed' ->> 'is_crossbred' AS breed_is_crossbred,
    ae.animal -> 'breed' ->> 'breed_component' AS breed_component,
    ae.animal ->> 'gender' AS gender,
    ae.animal -> 'weight' ->> 'max' AS weight_max,
    ae.animal -> 'weight' ->> 'min' AS weight_min,
    ae.animal -> 'weight' ->> 'unit' AS weight_unit,
    ae.animal -> 'weight' ->> 'qualifier' AS weight_qualifier,
    ae.animal ->> 'species' AS species,
    ae.animal ->> 'reproductive_status' AS reproductive_status,
    ae.animal ->> 'female_animal_physiological_status' AS female_animal_physiological_status
FROM
    animal_events ae
WHERE
    ae.original_receive_date IS NOT NULL
    and TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
    AND TO_DATE('{end_date}', 'YYYY-MM-DD')
    AND ae.onset_date IS NOT NULL
    AND length(ae.original_receive_date)=8
    AND length(ae.onset_date)=8
    AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
    and length(trim(upper(ae.animal ->> 'species'))) > 1
   	and ae.animal -> 'breed' ->> 'breed_component' is not null
    and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
    AND upper(ae.animal ->> 'species') = '{selected_species}'
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
        """
    #print(f"Query for Detail Animal information  {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


##--------------------------------------------------------------------------------
## Detail Reaction information with filter by species, animal and time
##----------------------------------------------------------------------------------
def get_detail_reaction_info(selected_species, selected_breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves reactions over time, efficiently handling dates and filtering."""
    query = f"""
    SELECT
    ae.unique_aer_id_number,
    --ae.number_of_animals_affected,
    ae.number_of_animals_treated,
    ae.Reaction -> 'veddra_version' AS veddra_version,
    ae.Reaction -> 'veddra_term_code' AS veddra_term_code,
    ae.Reaction -> 'veddra_term_name' AS veddra_term_name,
    ae.Reaction -> 'number_of_animals_affected' AS number_of_animals_affected,
    ae.Reaction -> 'accuracy' AS accuracy
    FROM
    animal_events ae
    WHERE
    ae.original_receive_date IS NOT NULL
    and TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
    AND TO_DATE('{end_date}', 'YYYY-MM-DD')
    AND ae.onset_date IS NOT NULL
    AND length(ae.original_receive_date)=8
    AND length(ae.onset_date)=8
    AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
    and length(trim(upper(ae.animal ->> 'species'))) > 1
   	and ae.animal -> 'breed' ->> 'breed_component' is not null
    and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
    AND upper(ae.animal ->> 'species') = '{selected_species}'
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
        """
    #print(f"Query for Detail Reaction information {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


##--------------------------------------------------------------------------------
## Detail Health_Assessment_Prior_To_Exposure information with filter by species, animal and time
##----------------------------------------------------------------------------------
def get_detail_Health_Assessment_Prior_To_Exposure_info(
    selected_species, selected_breed, start_date, end_date
):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    query = f"""
    SELECT
    ae.unique_aer_id_number,
    ae.number_of_animals_affected,
    ae.number_of_animals_treated,
    ae.Health_Assessment_Prior_To_Exposure -> 'assessed_by' AS assessed_by,
    ae.Health_Assessment_Prior_To_Exposure -> 'condition' AS condition
    FROM
    animal_events ae
    WHERE
    ae.original_receive_date IS NOT NULL
    and TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
    AND TO_DATE('{end_date}', 'YYYY-MM-DD')
    AND ae.onset_date IS NOT NULL
    AND length(ae.original_receive_date)=8
    AND length(ae.onset_date)=8
    AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
    and length(trim(upper(ae.animal ->> 'species'))) > 1
   	and ae.animal -> 'breed' ->> 'breed_component' is not null
    and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
    AND upper(ae.animal ->> 'species') = '{selected_species}'
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
    """
    #print(f"Query for Detail Health_Assessment_Prior_To_Exposure information {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

##--------------------------------------------------------------------------------
## Detail Receiver information with filter by species, animal and time
##----------------------------------------------------------------------------------
def get_detail_receiver_info(selected_species, selected_breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves reactions over time, efficiently handling dates and filtering."""
    query = f"""
    SELECT
    ae.unique_aer_id_number,
    ae.number_of_animals_affected,
    ae.number_of_animals_treated,
    ae.Receiver -> 'organization' AS organization,
    ae.Receiver -> 'street_address' AS street_address,
	ae.Receiver -> 'city' AS city,
    ae.Receiver -> 'state' AS state,
	ae.Receiver -> 'postal_code' AS postal_code,
    ae.Receiver -> 'country' AS country
    FROM
    animal_events ae
    WHERE
    ae.original_receive_date IS NOT NULL
    and TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
    AND TO_DATE('{end_date}', 'YYYY-MM-DD')
    AND ae.onset_date IS NOT NULL
    AND length(ae.original_receive_date)=8
    AND length(ae.onset_date)=8
    AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
    and length(trim(upper(ae.animal ->> 'species'))) > 1
   	and ae.animal -> 'breed' ->> 'breed_component' is not null
    and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
    AND upper(ae.animal ->> 'species') = '{selected_species}'
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
    """
    #print(f"Query with no data for Detail Receiver information query {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

##--------------------------------------------------------------------------------
## Detail Outcome information with filter by species, animal and time
##----------------------------------------------------------------------------------
def get_detail_outcome_info(selected_species, selected_breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves reactions over time, efficiently handling dates and filtering."""
    query = f"""
    SELECT
    ae.unique_aer_id_number,
    --ae.number_of_animals_affected,
    ae.number_of_animals_treated,
    ae.Outcome -> 'medical_status' AS medical_status,
    ae.Outcome -> 'number_of_animals_affected' AS number_of_animals_affected
    FROM
    animal_events ae
    WHERE
    ae.original_receive_date IS NOT NULL
    and TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
    AND TO_DATE('{end_date}', 'YYYY-MM-DD')
    AND ae.onset_date IS NOT NULL
    AND length(ae.original_receive_date)=8
    AND length(ae.onset_date)=8
    AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
    and length(trim(upper(ae.animal ->> 'species'))) > 1
   	and ae.animal -> 'breed' ->> 'breed_component' is not null
    and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
    AND upper(ae.animal ->> 'species') = '{selected_species}'
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
    """
    #print(f"Query with no data for Detail Outcome information query {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df



##--------------------------------------------------------------------------------
## Detail Duration information with filter by species, animal and time
##----------------------------------------------------------------------------------
def get_detail_duration_info(selected_species, selected_breed, start_date, end_date):
    import psycopg2

    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves reactions over time, efficiently handling dates and filtering."""
    query = f"""
    SELECT
    ae.unique_aer_id_number,
    ae.number_of_animals_affected,
    ae.number_of_animals_treated,
    ae.duration -> 'unit' AS unit, 
	ae.duration -> 'value' AS value
    FROM animal_events ae
    Where ae.original_receive_date IS NOT NULL
    and TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
    AND TO_DATE('{end_date}', 'YYYY-MM-DD')
    AND ae.onset_date IS NOT NULL
    AND length(ae.original_receive_date)=8
    AND length(ae.onset_date)=8
    AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
    and length(trim(upper(ae.animal ->> 'species'))) > 1
   	and ae.animal -> 'breed' ->> 'breed_component' is not null
    and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
    and (ae.duration -> 'unit' ) is not null
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
    AND upper(ae.animal ->> 'species') = '{selected_species}'
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
    """
    #print(f"Query with no data for Detail Outcome information query {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

##--------------------------------------------------------------------------------
## Detail not nested information with filter by species, animal and time
##----------------------------------------------------------------------------------
def get_detail_info(selected_species, selected_breed, start_date, end_date):
    import psycopg2
    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves reactions over time, efficiently handling dates and filtering."""
    query = f"""
   select unique_aer_id_number,report_id, type_of_information,
number_of_animals_affected,number_of_animals_treated, onset_date, 
original_receive_date,primary_reporter,secondary_reporter,serious_ae,time_between_exposure_and_onset, treated_for_ae
from animal_events ae
    Where ae.original_receive_date IS NOT NULL
    and TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
    AND TO_DATE('{end_date}', 'YYYY-MM-DD')
    AND ae.onset_date IS NOT NULL
    AND length(ae.original_receive_date)=8
    AND length(ae.onset_date)=8
    AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
    and length(trim(upper(ae.animal ->> 'species'))) > 1
   	and ae.animal -> 'breed' ->> 'breed_component' is not null
    and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
    AND upper(ae.animal ->> 'species') = '{selected_species}'
    and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
    """
    #print(f"Query with no data for Detail Outcome information query {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def get_detail_reaction_info2(selected_species, selected_breed, start_date, end_date):
    import psycopg2
    #conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn_string = "dbname='tsdb' user='tsdbadmin' password='YIZ3c-8i-90iJm' host='q3xnhhwewy.yck8myadj4.tsdb.cloud.timescale.com' port='37746' sslmode='require'"
    conn = psycopg2.connect(conn_string)
    # """Retrieves reactions over time, efficiently handling dates and filtering."""
    query = f"""
       SELECT 
        ae.unique_aer_id_number, 
        ae.number_of_animals_affected, 
        ae.number_of_animals_treated,
        STRING_AGG(distinct(reaction_details.accuracy), ', ') AS accuracy,
        STRING_AGG(distinct(reaction_details.veddra_version), ', ') AS veddra_version,
        STRING_AGG(distinct(reaction_details.veddra_term_code), ', ') AS veddra_term_code,
        STRING_AGG(distinct(reaction_details.veddra_term_name), ', ') AS veddra_term_name
        --, STRING_AGG(COALESCE(CAST(reaction_details.number_of_animals_affected AS TEXT), ''), ', ') AS number_of_animals_affected
    FROM 
        animal_events ae
        CROSS JOIN LATERAL jsonb_array_elements(ae.reaction) AS reaction_element
        CROSS JOIN LATERAL jsonb_to_record(reaction_element) AS reaction_details(
            accuracy TEXT, 
            veddra_version TEXT, 
            veddra_term_code TEXT, 
            veddra_term_name TEXT, 
            number_of_animals_affected INTEGER
        )
    WHERE ae.original_receive_date IS NOT NULL
        and TO_DATE(ae.onset_date, 'YYYYMMDD') BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
        AND TO_DATE('{end_date}', 'YYYY-MM-DD')
        AND ae.onset_date IS NOT NULL
        AND length(ae.original_receive_date)=8
        AND length(ae.onset_date)=8
        AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
        and length(trim(upper(ae.animal ->> 'species'))) > 1
        and ae.animal -> 'breed' ->> 'breed_component' is not null
        and length(trim(ae.animal -> 'breed' ->> 'breed_component' )) > 1
        and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) NOT LIKE 'UNKNOWN'
        AND upper(ae.animal ->> 'species') = '{selected_species}'
        and upper(trim(ae.animal -> 'breed' ->> 'breed_component' )) = '{selected_breed}'
    GROUP BY 
        ae.unique_aer_id_number, 
        ae.number_of_animals_affected, 
        ae.number_of_animals_treated,
        ae.onset_date,
        ae.original_receive_date
        """
    #print(f"Query with no data for Detail Outcome information query {query}")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def read_description_file(species, breed):
    """Reads the description from a text file if it exists."""
    description_path = os.path.join("./Images", species.lower(), breed.lower().replace(' ',''), "description.txt")
    if os.path.isfile(description_path):
        with open(description_path, 'r') as file:
            return file.read()
    return None
# Function to fetch data from the database

def select_from_db_as_dataframe(breed, species, connection):
    # SQL query
    select_query = """SELECT * FROM expanded_animal_events WHERE upper(animal_breed_component) = %s  AND upper(animal_species) = %s"""
    df = pd.read_sql_query(select_query, connection, params=(breed, species))
    return df

def read_drug_description_file(species, drug):
    """Reads the description from a text file if it exists."""
    drug_updated=re.sub(r'\W+', '', drug)
    description_path = os.path.join("./Drugs", species.lower(), drug_updated.lower().replace(' ','').replace('\n',''), "description.txt")
    print(description_path)
    if os.path.isfile(description_path):
        with open(description_path, 'r') as file:
            return file.read()
    return None

def format_description(description):
    """Formats the breed description for better presentation."""
    # Split the description into lines
    lines = description.split('\n')

    formatted_lines = ["<div style='text-align: justify;'>"]  # Start with justified text for better word wrap

    for line in lines[3:]:  # Start from the second line
        if len(line.strip()) > 10:
            # Format as a bullet point if it looks like a listed item
            if line.strip()[0].isdigit():
                line_content = line[line.find('.') + 1:].strip()
                formatted_lines.append(f"<li>{line_content}</li>")
            else:
                formatted_lines.append(f"<p>{line}</p>")

    formatted_lines.append("</div>")

    # Join all the formatted lines
    return ''.join(formatted_lines)


def plot_yearly_trend(df):
    st.subheader('Yearly Trend of Animal Events')
    df['onset_year'] = df['onset_date'].dt.year
    yearly_trend = df.groupby('onset_year').size()
    if not yearly_trend.empty:
        fig, ax = plt.subplots()
        yearly_trend.plot(kind='bar', ax=ax)
        ax.set_xlabel('Year')
        ax.set_ylabel('Number of Events')
        st.pyplot(fig)
    else:
        st.write("No yearly trend data available.")

def plot_species_distribution(df):
    st.subheader('Distribution of Events by Animal Species')
    species_distribution = df['animal_species'].value_counts()
    if not species_distribution.empty:
        fig, ax = plt.subplots()
        species_distribution.plot(kind='bar', ax=ax)
        ax.set_xlabel('Species')
        ax.set_ylabel('Number of Events')
        st.pyplot(fig)
    else:
        st.write("No species distribution data available.")


def plot_common_reactions(df):
    reaction_distribution = df['reaction_veddra_term_name'].value_counts().head(20)
    fig, ax = plt.subplots()
    reaction_distribution.plot(kind='bar', ax=ax)
    ax.set_title('Top 20 Reactions in Animal Events')
    ax.set_xlabel('Reactions')
    ax.set_ylabel('Number of Occurrences')
    st.pyplot(fig)

def plot_active_ingredients(df):
    ingredient_distribution = df['active_ingredient_name'].value_counts().head(20)
    fig, ax = plt.subplots()
    ingredient_distribution.plot(kind='bar', ax=ax)
    ax.set_title('Top 20 Active Ingredients in Animal Events')
    ax.set_xlabel('Active Ingredients')
    ax.set_ylabel('Number of Events')
    st.pyplot(fig)

def plot_country_distribution(df):
    df['country_code'] = df['unique_aer_id_number'].apply(lambda x: x.split('-')[0])
    country_distribution = df['country_code'].value_counts()
    fig, ax = plt.subplots()
    country_distribution.plot(kind='bar', ax=ax)
    ax.set_title('Distribution of Animal Events by Country')
    ax.set_xlabel('Country Code')
    ax.set_ylabel('Number of Events')
    st.pyplot(fig)

def plot_drug_reaction(df):
    top_drugs = df['drug_brand_name'].value_counts().head(5).index
    drug_reaction_df = df[df['drug_brand_name'].isin(top_drugs)]
    drug_reaction_freq = drug_reaction_df.groupby(['drug_brand_name', 'reaction_veddra_term_name']).size().reset_index(name='count')
    for drug in top_drugs:
        fig, ax = plt.subplots()
        subset = drug_reaction_freq[drug_reaction_freq['drug_brand_name'] == drug]
        sns.barplot(x='count', y='reaction_veddra_term_name', data=subset.sort_values('count', ascending=False).head(10), ax=ax)
        ax.set_title(f'Top 10 Reactions for {drug}')
        ax.set_xlabel('Count')
        ax.set_ylabel('Reactions')
        st.pyplot(fig)


