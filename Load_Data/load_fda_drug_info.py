import re
import psycopg2

def parse_records(text):
    # Split the records using "Back to top" as a delimiter
    records = text.split("Back to top")

    # Update the regular expression pattern to extract the full Drug_code and handle the variability in the last column
    pattern = r"([A-Z]+ \d{3}-\d{3}) Approval Type:(.*?)Date of Approval:(.*?)" \
              r"\nIngredient\(s\):(.*?)\nSponsor:(.*?)" \
              r"\n(?:Effect of Supplement|Indications For Use):(.*)"

    # Parse each record
    parsed_records = []
    for record in records:
        match = re.search(pattern, record, re.DOTALL)
        if match:
            parsed_records.append({
                "Application": match.group(1),
                "Approval_Type": match.group(2).strip(),
                "Date_of_Approval": match.group(3).strip(),
                "Ingredients": match.group(4).strip(),
                "Sponsor": match.group(5).strip(),
                "Effect_or_Indication": match.group(6).strip(),
            })

    return parsed_records

# Function to insert records into the PostgreSQL table
def create_table_if_not_exists(connection, drop):
    cursor = connection.cursor()
    if drop=='Y':
        drop_table= """ drop table if exists drug_approval_info """
        cursor.execute(drop_table)

    create_table = """
    CREATE TABLE if not exists drug_approval_info (
    Application VARCHAR(100),
    Approval_Type VARCHAR(255),
    Date_of_Approval DATE,
    Ingredients TEXT,
    Sponsor VARCHAR(255),
    Effect_or_Indication TEXT)
    """
    cursor.execute(create_table)
    cursor.close()

def insert_into_db(records, connection):
    # Prepare SQL query to insert a record into the database.
    insert_query = """
    INSERT INTO drug_approval_info (Application, Approval_Type, Date_of_Approval, Ingredients, Sponsor, Effect_or_Indication)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    cursor = connection.cursor()

    for record in records:
        values = (
            record['Application'],
            record['Approval_Type'],
            record['Date_of_Approval'],
            record['Ingredients'],
            record['Sponsor'],
            record['Effect_or_Indication']
        )
        cursor.execute(insert_query, values)

    connection.commit()
    cursor.close()

# Connect to your database
conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
conn = psycopg2.connect(conn_string)


# Read sample text from file
file = open('../Data/OtherDownloads/Manual_download_Drug_info.txt', 'r')
sample_text = file.read()
file.close()

# Parse the sample text
parsed_records = parse_records(sample_text)

# Insert records into the database
create_table_if_not_exists(conn,drop='Y')
insert_into_db(parsed_records, conn)

# Close the database connection
conn.close()
