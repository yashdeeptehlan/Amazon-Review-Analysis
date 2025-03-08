import pandas as pd
import urllib
from sqlalchemy import create_engine

# 1. Read the local TSV with error handling for bad lines
file_path = "/Users/user/Documents/BigDataProject/tsv_files/amazon_reviews_analysis.tsv/amazon_reviews_analysis.tsv"
print(f"Reading TSV file from {file_path}...")

df = pd.read_csv(
    file_path,
    sep="\t",            # Use tab as the separator for TSV
    on_bad_lines="skip", # or "warn" if you want to see warnings
    engine="python"      # The Python engine allows skipping malformed lines
)

print("DataFrame shape:", df.shape)
# print("All columns:", df.columns.tolist())
# print("Number of columns:", len(df.columns))
# print(df.head(5).T)

# 2. Define SQL Server connection details (Docker)
server = "localhost,1433"
database = "AmazonReviews"
username = "sa"
password = "Strong@Pass123"  # Update as needed

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    f"Connection Timeout=60;"
)
connection_string = f"mssql+pyodbc:///?odbc_connect={params}"

print("Connecting to SQL Server in Docker...")
engine = create_engine(connection_string)

# 3. Upload Data to SQL Server
# Make sure you have already created or altered the table in Azure Data Studio 
# to match your DataFrame columns, or use 'replace' to let Pandas create it automatically.
table_name = "AmazonReviewsAnalysis"
print(f"Inserting data into table '{table_name}'...")

# 'if_exists="append"' will insert rows into an existing table
# 'if_exists="replace"' will drop any existing table of that name and create a new one
df.to_sql(table_name, engine, if_exists="append", index=False, chunksize=1000)

print("Data successfully uploaded to SQL Server!")
