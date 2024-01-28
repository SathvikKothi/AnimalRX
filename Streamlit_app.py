import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
import random
import glob
import os
import plotly.express as px

# Assuming functions from Read_data.functions_used_local are imported correctly
from Read_data.functions_used_local import *


def main():
    st.set_page_config(page_title="Animal Event Insights", layout="wide")
    st.title("Animal Event Insights")
    st.markdown("Explore comprehensive insights into animal events, drugs, reactions, and more.")

    # Sidebar for filters
    with st.sidebar:
        st.header("Filters")
        species_list = get_distinct_species()
        selected_species = st.selectbox('Select Species', species_list)
        breed_list = get_distinct_breeds(selected_species) if selected_species else []
        selected_breed = st.selectbox('Select Breed', breed_list)
        start_date = st.date_input(
                "Start Date",
                min_value=pd.to_datetime("2020-01-01"),
                value=pd.to_datetime("2023-01-01")
            )
        end_date = st.date_input(
                "End Date",
                min_value=pd.to_datetime("2023-12-01"),
                value=pd.to_datetime("now")
            )
        selected_side_effect = ''
        if selected_side_effect:
            drug_list = get_drug_by_indication(selected_species, selected_breed, selected_side_effect)
            selected_drug = st.selectbox("Select a Drug", drug_list)
        else:
            drug_list = get_drug_by_indication(selected_species, selected_breed)
            selected_drug = st.selectbox("Select a Drug", drug_list)
        if selected_drug != '':
            side_effect_list = get_indicaiton_by_drug(selected_species, selected_breed, selected_drug)
            selected_side_effect = st.selectbox("Select a Side effect", side_effect_list)
        else:
            side_effect_list = get_indicaiton_by_drug(selected_species, selected_breed)
            selected_side_effect = st.selectbox("Select a Side effect", side_effect_list)

            # Dropdown for Active Ingredient and Veddra Term

    # Main content area
    if selected_species and selected_breed:
        # Slider for image size
        #image_size = st.slider("Select Image Size", 100, 700, 500)
        display_image_and_description(selected_species, selected_breed)
        try:
            if selected_side_effect is not None and selected_drug is None:
                df = get_distinct_drug_ind(species=selected_species, breed=selected_breed, active_ingredient_name='',
                                           veddra_term_name=selected_side_effect)
            elif selected_side_effect is None and selected_drug is not None:
                df = get_distinct_drug_ind(species=selected_species, breed=selected_breed,
                                           active_ingredient_name=selected_drug, veddra_term_name='')
            elif selected_side_effect is not None and selected_drug is not None:
                df = get_distinct_drug_ind(species=selected_species, breed=selected_breed,
                                           active_ingredient_name=selected_drug, veddra_term_name=selected_side_effect)
            else:
                df = get_distinct_drug_ind(species=selected_species, breed=selected_breed, active_ingredient_name='',
                                           veddra_term_name='')
            # veddra_term_list, active_ingredient_list = get_distinct_drug_indicaiton()
            # print(df, selected_species, selected_breed)
            if not df.empty:
                df.fillna('', inplace=True)
                st.subheader("Most common side effects and drug ")
                st.dataframe(df)
        except Exception as e:
            st.error(f"Error with get_distinct_drug_ind: {e}")
            pass  # Skip block if there's an error
        # Dropdown for Active Ingredient and Veddra Term
        try:
            description = read_drug_description_file(selected_species, selected_drug)
            if description:
                st.subheader("Drug Description ")
                formatted_description = format_description(description)
                st.markdown(formatted_description, unsafe_allow_html=True)
            else:
                # info = fetch_wikipedia_info(selected_species, selected_drug)
                info = None
                if info:
                    st.subheader("Drug Description from Wikipedia")
                    st.write(info)
        except Exception as e:
            #st.error(f"Error with fetch_wikipedia_info: {e}")
            pass  # Skip block if there's an error
        process_data_and_display_charts(selected_species, selected_breed)

    # Chat interface for queries
    st.header("Query Animal Events")
    user_input = st.text_input("Enter your query:")
    if user_input:
        display_query_results(user_input)


def display_image_and_description(species, breed):
    image_files = glob.glob(os.path.join("./Images", species.lower(), breed.lower().replace(' ', ''), "*.jpg"))
    if not image_files:
        image_files = glob.glob(os.path.join("./Images", species.lower(), "*.jpg"))

    if image_files:
        image_path = random.choice(image_files)
        st.image(image_path, caption=breed, width=500)

    try:
        description = read_description_file(species, breed)
        if description:
            formatted_description = format_description(description)
            st.markdown(formatted_description, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error with description: {e}")

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


def process_data_and_display_charts(species, breed):
    conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
    conn = psycopg2.connect(conn_string)
    animal_events_df = select_from_db_as_dataframe(breed, species, conn)
    conn.close()

    if not animal_events_df.empty:
        animal_events_df['onset_date'] = pd.to_datetime(animal_events_df['onset_date'], errors='coerce')
        animal_events_df['original_receive_date'] = pd.to_datetime(animal_events_df['original_receive_date'],
                                                                   errors='coerce')
        # Plotting charts
        plot_yearly_trend(animal_events_df)
        plot_species_distribution(animal_events_df)
        plot_common_reactions(animal_events_df)
        plot_active_ingredients(animal_events_df)
        plot_country_distribution(animal_events_df)
        plot_drug_reaction(animal_events_df)

    else:
        st.error("No data available for the selected species and breed.")

if __name__ == "__main__":
    main()
