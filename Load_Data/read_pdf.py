import tabula
import pandas as pd

# URL of the PDF file
# pdf_url = 'https://www.whocc.no/filearchive/publications/2023_atcvet_guidelines_web.pdf'

# Extract tables from the PDF
# We specify pages="12" to extract tables only from page 12
tables = tabula.read_pdf(
    "../Data/2023_atcvet_guidelines_web.pdf",
    pages="12-13",
    multiple_tables=True,
    stream=True,
)

# Assuming the first table on the page is the one we need
# This might need adjustment if there are multiple tables on the page
df = tables[0]

# Clean up the DataFrame if necessary
# This might involve renaming columns, handling missing values, etc.

print(df)
