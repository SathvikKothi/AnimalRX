import psycopg2
import pandas as pd

# Database connection parameters
conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
conn = psycopg2.connect(conn_string)

# SQL Query
sql_query = """
SELECT * FROM expanded_animal_events
WHERE animal_breed_component = 'Pug' AND animal_species = 'Dog'
"""

# Reading data into a DataFrame
try:
    df = pd.read_sql(sql_query, conn)
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
# Writing DataFrame to CSV
df.to_csv("animal_events.csv", index=False)
