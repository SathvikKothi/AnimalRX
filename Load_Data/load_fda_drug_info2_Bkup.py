import re
import psycopg2

# Read sample text from file
def parse_records(text):
    # Define a regular expression pattern to extract the relevant fields
    pattern = r"EA FONSI Proprietary Name:(.*?)Application:(.*?)" \
              r"Approval Type:(.*?)Date of Approval:(.*?)" \
              r"(?:Indication for Use|Effect of Supplement):(.*?)?(?=\nEA FONSI|\nBack to top|$)"

    # Parse records using regular expression
    matches = re.finditer(pattern, text, re.DOTALL)
    parsed_records = []
    for match in matches:
        # Extract data and set to None if empty
        proprietary_name = match.group(1).strip() or None
        application = match.group(2).strip() or None
        approval_type = match.group(3).strip() or None
        date_of_approval = match.group(4).strip() or None
        indication_or_effect = match.group(5).strip() if match.group(5) else None

        parsed_records.append({
            "Process_type": "EA FONSI",
            "Proprietary_Name": proprietary_name,
            "Application": application,
            "Approval_type": approval_type,
            "Date_of_Approval": date_of_approval,
            "Effect_or_Indication": indication_or_effect
        })

    return parsed_records


# Function to insert records into the PostgreSQL table
def create_table_if_not_exists(connection, drop):
    cursor = connection.cursor()
    if drop=='Y':
        drop_table= """ drop table if exists drug_brand_approval_info"""
        cursor.execute(drop_table)

    create_table = """
    CREATE TABLE if not exists drug_brand_approval_info (
    Process_type VARCHAR(100),
    Proprietary_Name VARCHAR(255),
    Application VARCHAR(100),
    Approval_type VARCHAR(255),
    Date_of_Approval DATE,
    Effect_or_Indication TEXT,
    EA_Document_link text,
    FONSI_Document_link text,
    )
    """
    cursor.execute(create_table)
    cursor.close()

def insert_into_db(records, connection):
    # Prepare SQL query to insert a record into the database.
    insert_query = """
    INSERT INTO drug_brand_approval_info (Process_type, Proprietary_Name, Application, Approval_type, Date_of_Approval, Effect_or_Indication)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    cursor = connection.cursor()

    for record in records:
        values = (
            record['Process_type'],
            record['Proprietary_Name'],
            record['Application'],
            record['Approval_type'],
            record['Date_of_Approval'],
            record['Effect_or_Indication']
        )
        cursor.execute(insert_query, values)

    connection.commit()
    cursor.close()

# Connect to your database
conn_string = "host='localhost' dbname='postgres' user='clinicaltrials' password='clinicaltrials'"
conn = psycopg2.connect(conn_string)


# Read sample text from file
file = open('../Data/OtherDownloads/Manual_download_Drug_info2.txt', 'r')
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

