import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2

def select_from_db_as_dataframe(breed, species, connection):
    # Prepare SQL query to select records from the database.
    select_query = """
    SELECT * FROM expanded_animal_events
    WHERE animal_breed_component = %s  AND animal_species = %s 
    """

    df = pd.read_sql_query(select_query, connection, params=(breed, species))
    return df


# Connect to your database
conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
conn = psycopg2.connect(conn_string)

# Example usage
breed = "Labrador"
species = "Dog"
animal_events_df = select_from_db_as_dataframe('Pug','Dog', conn)
print(animal_events_df)

#file_path = 'animal_events.csv'  # Update with the correct file path
#animal_events_df = pd.read_csv(file_path)

# Convert date columns to datetime
animal_events_df['onset_date'] = pd.to_datetime(animal_events_df['onset_date'], errors='coerce')
animal_events_df['original_receive_date'] = pd.to_datetime(animal_events_df['original_receive_date'], errors='coerce')

# Common Reactions Analysis
reaction_distribution = animal_events_df['reaction_veddra_term_name'].value_counts().head(20)
plt.figure(figsize=(12, 6))
reaction_distribution.plot(kind='bar')
plt.title('Top 20 Reactions in Animal Events')
plt.xlabel('Reactions')
plt.ylabel('Number of Occurrences')
plt.show()

# Active Ingredients Analysis
ingredient_distribution = animal_events_df['active_ingredient_name'].value_counts().head(20)
plt.figure(figsize=(12, 6))
ingredient_distribution.plot(kind='bar')
plt.title('Top 20 Active Ingredients in Animal Events')
plt.xlabel('Active Ingredients')
plt.ylabel('Number of Events')
plt.show()

# 2. Country Analysis from Unique ID
# Assuming the first few characters before the first dash represent the country code
animal_events_df['country_code'] = animal_events_df['unique_aer_id_number'].apply(lambda x: x.split('-')[0])
country_distribution = animal_events_df['country_code'].value_counts()
plt.figure(figsize=(12, 6))
country_distribution.plot(kind='bar')
plt.title('Distribution of Animal Events by Country')
plt.xlabel('Country Code')
plt.ylabel('Number of Events')
plt.show()

# 4. Drug-Reaction Analysis
top_drugs = animal_events_df['drug_brand_name'].value_counts().head(5).index
drug_reaction_df = animal_events_df[animal_events_df['drug_brand_name'].isin(top_drugs)]
drug_reaction_freq = drug_reaction_df.groupby(['drug_brand_name', 'reaction_veddra_term_name']).size().reset_index(name='count')
for drug in top_drugs:
    plt.figure(figsize=(12, 6))
    subset = drug_reaction_freq[drug_reaction_freq['drug_brand_name'] == drug]
    sns.barplot(x='count', y='reaction_veddra_term_name', data=subset.sort_values('count', ascending=False).head(10))
    plt.title(f'Top 10 Reactions for {drug}')
    plt.xlabel('Count')
    plt.ylabel('Reactions')
    plt.show()

