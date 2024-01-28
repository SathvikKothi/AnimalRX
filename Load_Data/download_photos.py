import requests
import pandas as pd
import psycopg2

conn_string = (
    "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
)
conn = psycopg2.connect(conn_string)
import os


# images_folder_path = "Images/dog/"


def get_distinct_species():
    query = """
        WITH CleanedData AS (
        SELECT 
            ae.animal ->> 'species' AS species,
            unnest(string_to_array(regexp_replace(ae.animal -> 'breed' ->> 'breed_component', '[\[/\]""]', '', 'g'), ',')) AS breed_component
        FROM 
            Animal_Events ae
        WHERE 
            ae.original_receive_date IS NOT NULL 
            AND ae.onset_date IS NOT NULL
            AND length(ae.original_receive_date) = 8 
            AND length(ae.onset_date) = 8 
            AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
            AND upper(ae.animal ->> 'species') != 'DOG'

    )
    SELECT DISTINCT
        Substr(trim(breed_component),1,50) AS breed_component,
        replace(Substr(trim(breed_component),1,50),' ','') AS breed_component_name,
        species
    FROM 
        CleanedData
    WHERE 
        breed_component <> '' -- Optionally filter out empty strings
     """
    df = pd.read_sql_query(query, conn)
    # print(df.head())
    # print(species)
    return df


# download_path='../Streamlit/Images/'
# download_path='../Streamlit/Images/dog'
# client_id = "-tGvsUOa_Zx93qlxTjrqTAydsaurCltBu3TgNQGpUTs"  # Replace with your Unsplash access key
def download_images_for_species_unsplash(
    species, download_path, client_id, num_images=2
):
    base_url = "https://api.unsplash.com/search/photos"
    query = f"{species}".replace(" ", "+")
    params = {"query": query, "client_id": client_id, "per_page": num_images}

    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Raises an error for bad status codes
    data = response.json()

    # Create image folder if it doesn't exist
    image_folder = os.path.join(download_path, species.lower().replace(" ", ""))
    os.makedirs(image_folder, exist_ok=True)

    for i, photo in enumerate(data["results"]):
        image_url = photo["urls"]["regular"]
        try:
            img_response = requests.get(image_url)
            img_response.raise_for_status()

            # Save image
            with open(os.path.join(image_folder, f"{query}_{i}.jpg"), "wb") as file:
                file.write(img_response.content)
        except requests.RequestException as e:
            print(f"Failed to download image from Unsplash: {e}")


def get_distinct_breeds(species):
    query = f"""
        WITH CleanedData AS (
        SELECT 
            ae.animal ->> 'species' AS species,
            unnest(string_to_array(regexp_replace(ae.animal -> 'breed' ->> 'breed_component', '[\[/\]""]', '', 'g'), ',')) AS breed_component
        FROM 
            Animal_Events ae
        WHERE 
            ae.original_receive_date IS NOT NULL 
            AND ae.onset_date IS NOT NULL
            AND length(ae.original_receive_date) = 8 
            AND length(ae.onset_date) = 8 
            AND upper(ae.animal ->> 'species') NOT LIKE 'OTHER%'
            and upper(ae.animal ->> 'species') = upper('{species}')
    )
    SELECT DISTINCT
        Substr(trim(breed_component),1,50) AS breed_component,
        replace(Substr(trim(breed_component),1,50),' ','') AS breed_component_name,
        species
    FROM 
        CleanedData
    WHERE 
        breed_component <> '' -- Optionally filter out empty strings
     """
    df = pd.read_sql_query(query, conn)
    # print(df.head())
    # print(species)
    return df


# Example usage

species_breed = get_distinct_species()
species_list = species_breed["species"].tolist()

# for i in species_list:
#    download_images_for_species_unsplash(i, client_id)

from tqdm import tqdm
import time

for species in species_list:
    print(species)
    breeds = get_distinct_breeds(species)
    breeds_list = breeds["breed_component"].tolist()
    download_path = f"../AnimalRx_App/Images/{species}/"
    client_id = "-tGvsUOa_Zx93qlxTjrqTAydsaurCltBu3TgNQGpUTs"  # Replace with your Unsplash access key
    for i in tqdm(breeds_list):
        try:
            download_images_for_species_unsplash(i, download_path, client_id)
        except:
            time.sleep(120)
            print("Error with ", i)
            pass
