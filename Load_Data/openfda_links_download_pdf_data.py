import os
import re
import psycopg2
import requests

# Database connection parameters
db_host = "localhost"
db_database = "postgres"
db_user = "clinicaltrials"
db_password = "clinicaltrials"

# Connect to your postgres DB
conn = psycopg2.connect(host=db_host, dbname=db_database, user=db_user, password=db_password)
cur = conn.cursor()

# Query to select data from the table
query = """
SELECT proprietary_name, application, ea_document_link, fonsi_document_link
FROM drug_brand_approval_info;
"""

try:
    # Execute the query
    cur.execute(query)

    # Fetch all the rows
    rows = cur.fetchall()

    for row in rows:
        proprietary_name, application, ea_document_link, fonsi_document_link = row

        # Sanitize application for file name
        sanitized_application = re.sub(r'[^a-zA-Z0-9]', '', application)

        # Download and save each document
        for link_type, link in zip(['ea', 'fonsi'], [ea_document_link, fonsi_document_link]):
            if link:
                response = requests.get(link)
                if response.status_code == 200:
                    file_name = f"{proprietary_name}_{sanitized_application}_{os.path.basename(link)}"
                    if not file_name.lower().endswith('.pdf'):
                        file_name += '.pdf'
                    file_path = os.path.join(f'../Data/OtherDownloads/{link_type}_data_download/', file_name)

                    with open(file_path, 'wb') as file:
                        file.write(response.content)
                        print(f"Downloaded and saved {file_path}")

                    # Update the database with the new file path
                    update_query = f"""
                    UPDATE drug_brand_approval_info
                    SET {link_type}_document_path = %s
                    WHERE proprietary_name = %s AND application = %s;
                    """
                    cur.execute(update_query, (file_path, proprietary_name, application))

    # Commit the transaction
    conn.commit()
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the cursor and connection
    cur.close()
    conn.close()
