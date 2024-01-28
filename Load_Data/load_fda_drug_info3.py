import re
import psycopg2

def parse_records(text):
    # Define a regular expression pattern to extract the relevant fields
    pattern = r"(ANADA \d{3}-\d{3}|NADA \d{3}-\d{3})Proprietary Name:(.*?)Labeling Component:(.*?)\n"

    # Parse records using regular expression
    matches = re.finditer(pattern, text, re.DOTALL)
    parsed_records = []
    for match in matches:
        application = match.group(1).strip() or None
        proprietary_name = match.group(2).strip() or None
        labeling_component = match.group(3).strip() or None

        parsed_records.append({
            "Application": application,
            "Proprietary_Name": proprietary_name,
            "Labeling_Component": labeling_component
        })

    return parsed_records


# Function to insert records into the PostgreSQL table
def create_table_if_not_exists(connection, drop):
    cursor = connection.cursor()
    if drop=='Y':
        drop_table= """ drop table if exists drug_brand_label_info"""
        cursor.execute(drop_table)

    create_table = """
    CREATE TABLE if not exists drug_brand_label_info (
    Application VARCHAR(100),
    Proprietary_Name VARCHAR(255),
    Labeling_Component VARCHAR(255))
    """
    cursor.execute(create_table)
    cursor.close()

def insert_into_db(records, connection):
    # Prepare SQL query to insert a record into the database.
    insert_query = """
    INSERT INTO drug_brand_label_info (Application, Proprietary_Name, Labeling_Component)
    VALUES (%s, %s, %s)
    """

    cursor = connection.cursor()

    for record in records:
        values = (
            record['Application'],
            record['Proprietary_Name'],
            record['Labeling_Component']
        )
        cursor.execute(insert_query, values)

    connection.commit()
    cursor.close()

# Connect to your database
conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
conn = psycopg2.connect(conn_string)


# Read sample text from file
file = open('../Data/OtherDownloads/Manual_download_Drug_info3.txt', 'r')
sample_text = file.read()
file.close()

# Parse the sample text
parsed_records = parse_records(sample_text)
# Re-run the parsing with the updated logic
# Insert records into the database
create_table_if_not_exists(conn,drop='Y')
insert_into_db(parsed_records, conn)

# Close the database connection
conn.close()
