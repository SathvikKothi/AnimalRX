import requests
import pandas as pd
import psycopg2
import os
from tqdm import tqdm
import time
import openai
from langchain.llms import OpenAI

# Initialize Langchain with OpenAI

import os
import openai
from openai import OpenAI

from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv())  # read local .env file
openai.api_key = os.environ["OPENAI_API_KEY"]

openai_client = OpenAI()
llm = OpenAI()
client_id = "-tGvsUOa_Zx93qlxTjrqTAydsaurCltBu3TgNQGpUTs"  # Replace with your Unsplash access key

conn_string = (
    "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
)
conn = psycopg2.connect(conn_string)

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
client_id = "-tGvsUOa_Zx93qlxTjrqTAydsaurCltBu3TgNQGpUTs"  # Replace with your Unsplash access key


def download_images_for_species_unsplash(
    species, breed, download_path, client_id, num_images=2
):
    base_url = "https://api.unsplash.com/search/photos"
    query = f"{species} of {breed}".replace(" ", "+")
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


def get_distinct_drug_list_species(species):
    query = f"""
            select distinct active_ingredient_name,species from Active_ing_veddra_term_name
            where upper(species) = upper('{species}')
            """
    df = pd.read_sql_query(query, conn)
    # print(df.head())
    # print(species)
    return df


def get_description_for_species_or_breed(species,breed):
    try:
        from langchain.prompts import (
            ChatPromptTemplate,
            FewShotChatMessagePromptTemplate,
        )
        import pprint
        from langchain_community.llms import OpenAI

        examples = [
            {
                "input": "Now, I will provide similar information for the dog(species) breed: Labrador Retriever(Breed). If breed is not found provide generic information for species",
                "output": """Here is an example for the dog Labrador Retriever breed:
                Description 
                The American Pit Bull Terrier is a medium-sized, short-haired dog, of a solid build, whose early ancestors came from England. the American Pit Bull is described to be medium-sized and has a short coat and smooth well-defined muscle structure, and its eyes are to be round to almond-shaped, and its ears are to be small to medium in length, typically half prick or rose in carriage. The tail is prescribed to be slightly thick and tapering to a point.Twelve countries in Europe, as well as Australia, Canada, some parts of the United States, Ecuador, Malaysia, New Zealand, Puerto Rico, Singapore, and Venezuela, have enacted some form of breed-specific legislation on pit bull–type dogs, including American Pit Bull Terriers, ranging from outright bans to restrictions and conditions on ownership. Several states in Australia place restrictions on the breed, including mandatory sterilization. Pit Bull Terriers are banned in the United Kingdom, in the Canadian province of Ontario, and in many locations in the United States.
                1. Average lifespan: 10-12 years
                2. Health issues: Prone to hip dysplasia and obesity
                3. Exercise needs: High, requires daily exercise
                4. Prey drive: Moderate
                5. Child and animal friendly: Yes, very friendly
                6. Trainability: High, easy to train
                7. Grooming needs: Moderate, regular brushing required
                8. Dietary requirements: Balanced diet, prone to overeating
                9. Apartment living: Adaptable, but needs space for exercise
                10. tendencies: Moderate, barks to alert
                11. Protective instinct: Moderate, good watchdog
                12. Separation anxiety: Can suffer if left alone for too long
                13. Wandering tendencies: Low, usually stays close to home
                14. Time spent alone: Can handle moderate alone time
                15. Good with strangers: Yes, generally friendly""",
            },
        ]

        # Format each individual example as a sequence of tuples.
        example_prompt = ChatPromptTemplate.from_messages(
            [
                ("human", "{input}"),
                ("ai", "{output}"),
            ]
        )
        few_shot_prompt = FewShotChatMessagePromptTemplate(
            example_prompt=example_prompt,
            examples=examples,
        )
        # Use the few_shot_prompt.format() in another message, not directly as a ChatPromptTemplate message.
        final_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "As a veterinarian and animal science expert, I will provide detailed information about different species breeds. For this breed, I will cover aspects such as physical traits, average lifespan, health issues, exercise needs, temperament, grooming needs, dietary requirements, adaptability to living environments, behavior traits, and interactions with humans and other animals.",
                ),
                (
                    "human",
                    few_shot_prompt.format(),
                ),  # Insert the formatted few-shot examples here.
                ("human", "{species} of {breed}"),
            ]
        )
        chain = final_prompt | OpenAI(temperature=0.0)
        output = chain.invoke({"breed": breed, "species": species})
        print(output)
        return output
    except Exception as e:
        print(f"Error in generating description for {breed}: {e}")
        return None

def file_exists(path, filename="description.txt"):
    """Check if a file exists in the given path."""
    return os.path.isfile(os.path.join(path, filename))

def images_exist(path, image_count=1):
    """Check if the specified number of image files exist in the given path."""
    image_files = [f for f in os.listdir(path) if f.endswith('.jpg')]
    return len(image_files) >= image_count

def save_description(description, path):
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "description.txt"), "w") as file:
        file.write(description)


def get_description_for_drugs(species,drug):
    try:
        from langchain.prompts import (
            ChatPromptTemplate,
            FewShotChatMessagePromptTemplate,
        )
        import pprint
        from langchain_community.llms import OpenAI

        examples = [
            {
                "input": "Now, I will provide similar information for a {drug} for (species) in the fixed json format as stated, if more than one drug listed provide the same for each : If drug is not found provide generic information for species",
                "output": """Metronidazole Suspension for a dog.
                1. Purpose of the Medication: Metronidazole is an antibiotic and antiprotozoal medication. It's often prescribed for dogs to treat a variety of conditions, including certain infections (such as giardia), inflammatory bowel disease, and diarrhea caused by intestinal parasites or bacteria.
                2. Correct Dosage: The dosage of Metronidazole for dogs typically depends on the dog's weight and the condition being treated. A vet must determine the exact dosage.
                3. Method of Administration: Metronidazole Suspension is given orally. It can be administered directly into the mouth or mixed with a small amount of food.
                4. Potential Side Effects: Common side effects in dogs might include nausea, diarrhea, loss of appetite, or lethargy. In rare cases, neurological side effects like seizures or unsteadiness can occur, especially with high doses or prolonged treatment.
                5. Duration of Treatment: The length of treatment varies based on the condition being treated. It's important to complete the full course as prescribed, even if the symptoms seem to improve.
                6. Specific Time of Day/Frequency: This depends on the veterinarian's prescription. Metronidazole is typically given once or twice daily.
                7. With Food or on an Empty Stomach: Metronidazole can be given with food to reduce stomach upset.
                8. Interactions with Other Medications: Inform your vet about any other medications or supplements your dog is taking. Metronidazole can interact with various medications, including certain anticoagulants and anti-seizure drugs.
                9. Handling Missed Doses: If a dose is missed, give it as soon as you remember, but if it's almost time for the next dose, skip the missed dose and resume the regular schedule. Do not double up on doses.
                10. Storage: Store Metronidazole Suspension at room temperature away from moisture and heat. Keep it out of reach of children and pets.""",
            },
        ]

        # Format each individual example as a sequence of tuples.
        example_prompt = ChatPromptTemplate.from_messages(
            [
                ("human", "{input}"),
                ("ai", "{output}"),
            ]
        )
        few_shot_prompt = FewShotChatMessagePromptTemplate(
            example_prompt=example_prompt,
            examples=examples,
        )
        # Use the few_shot_prompt.format() in another message, not directly as a ChatPromptTemplate message.
        final_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "As a veterinarian and animal drug expert, I will provide detailed information about different drugs for animal species. if you cannot find info say i dont know",
                ),
                (
                    "human",
                    few_shot_prompt.format(),
                ),  # Insert the formatted few-shot examples here.
                ("human", "{species} of {drug}"),
            ]
        )
        chain = final_prompt | OpenAI(temperature=0.0)
        output = chain.invoke({"drug": drug, "species": species})
        print(output)
        return output
    except Exception as e:
        print(f"Error in generating description for {drug}: {e}")
        return None

# Example usage
"""
species_breed = get_distinct_species()
species_list = species_breed["species"].str.lower().unique().tolist()
for species in species_list:
    species_description = get_description_for_species_or_breed(species,species)
    image_path = f"../AnimalRx_App/Images/{species}/"
    if not file_exists(image_path):
        save_description(species_description, image_path)
    else:
        print(f"No description generated for species: {species}")

    breeds = get_distinct_breeds(species)
    breeds_list = breeds["breed_component"].str.lower().unique().tolist()

    for breed in tqdm(breeds_list):
        breed_path = os.path.join(image_path, breed.replace(' ', ''))
        # Check if description file already exists for the breed
        if not file_exists(breed_path):
            try:
                breed_description = get_description_for_species_or_breed(species, breed)
                if breed_description:
                    save_description(breed_description, breed_path)
                else:
                    print(f"No description generated for breed: {breed}")
            except Exception as e:
                print(f"Error with {breed}: {e}")
                continue
        if not images_exist(breed_path):
            try:
                download_images_for_species_unsplash(species,breed, image_path, client_id,num_images=1)
            except Exception as e:
                print(f"Error with {breed}: {e}")
                time.sleep(60)
                continue
"""
species_list = ['cat','dog']
for species in species_list:
    #species_description = get_distinct_drug_list_species(species)
    image_path = f"../AnimalRx_App/Drugs/{species}/"
    #if not file_exists(image_path):
    #    save_description(species_description, image_path)
    #else:
    #    print(f"No description generated for species: {species}")

    drugs = get_distinct_drug_list_species(species)
    drugs_list = drugs["active_ingredient_name"].str.lower().unique().tolist()

    for drug in tqdm(drugs_list):
        drug_path = os.path.join(image_path, drug.replace(' ', ''))
        # Check if description file already exists for the breed
        if not file_exists(drug_path):
            try:
                breed_description = get_description_for_drugs(species, drug)
                if breed_description:
                    save_description(breed_description, drug_path)
                else:
                    print(f"No description generated for breed: {drug}")
            except Exception as e:
                print(f"Error with {drug}: {e}")
                continue
