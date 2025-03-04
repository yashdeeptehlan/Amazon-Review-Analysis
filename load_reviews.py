import pandas as pd
from sqlalchemy import create_engine
import urllib

# Step 1: Load TSV file into a Pandas DataFrame
file_path = "/Users/user/Downloads/amazon_reviews_us_Electronics_v1_00.tsv"
print("Loading data from TSV file...")
df = pd.read_csv(file_path, sep='\t', low_memory=False, on_bad_lines='skip')
print("Data loaded. Number of records:", len(df))

# Step 2: Define SQL Server connection details
server = "localhost,1433"  # Use comma to separate host and port
database = "AmazonReviews"
username = "sa"
password = "Strong@Pass123"

# Construct connection string using URL encoding and an increased timeout (120 seconds)
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    f"Connection Timeout=120;"
)
connection_string = f"mssql+pyodbc:///?odbc_connect={params}"

print("Connecting to SQL Server with connection string:")
print(connection_string)

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Step 3: Insert data into the ElectronicsReviews table
print("Inserting data into SQL Server table 'ElectronicsReviews'...")
df.to_sql("ElectronicsReviews", engine, if_exists="append", index=False, chunksize=1000)
print("Data successfully loaded into SQL Server!")
