import pandas as pd
import urllib
from sqlalchemy import create_engine

# File paths for the three datasets
file_elec = "/Users/user/Documents/BigDataProject/tsv_files/amazon_reviews_us_Electronics_v1_00.tsv"
file_books = "/Users/user/Documents/BigDataProject/tsv_files/amazon_reviews_us_Books_v1_02.tsv"
file_watches = "/Users/user/Documents/BigDataProject/tsv_files/amazon_reviews_us_Watches_v1_00.tsv"

# Step 1: Load each TSV file into a DataFrame, skipping bad lines
print("Loading Electronics data...")
df_elec = pd.read_csv(file_elec, sep="\t", low_memory=False, on_bad_lines='skip')
print("Loading Books data...")
df_books = pd.read_csv(file_books, sep="\t", low_memory=False, on_bad_lines='skip')
print("Loading Watches data...")
df_watches = pd.read_csv(file_watches, sep="\t", low_memory=False, on_bad_lines='skip')

# Optionally, add a 'category' column if it doesn't already exist
df_elec["category"] = "Electronics"
df_books["category"] = "Books"
df_watches["category"] = "Watches"

# Step 2: Merge the three DataFrames
print("Merging datasets...")
df_merged = pd.concat([df_elec, df_books, df_watches], ignore_index=True)
print("Merged data shape:", df_merged.shape)

# Optionally, save the merged file to disk for record keeping
merged_file = "/Users/user/Documents/BigDataProject/tsv_files/amazon_reviews_merged.tsv"
df_merged.to_csv(merged_file, sep="\t", index=False)
print("Merged file saved to", merged_file)

# # Step 3: SQL Server connection details
# server = "localhost,1433"   # Using comma to separate host and port
# database = "AmazonReviews"
# username = "sa"
# password = "Strong@Pass123"   # Update if necessary

# # Construct connection string using URL encoding, with increased timeout
# params = urllib.parse.quote_plus(
#     f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#     f"SERVER={server};"
#     f"DATABASE={database};"
#     f"UID={username};"
#     f"PWD={password};"
#     f"Connection Timeout=120;"
# )
# connection_string = f"mssql+pyodbc:///?odbc_connect={params}"

# print("Connecting to SQL Server with connection string:")
# print(connection_string)

# # Step 4: Create the SQLAlchemy engine
# engine = create_engine(connection_string)

# # Step 5: Insert the merged DataFrame into a new table 'AmazonReviews_Merged'
# print("Inserting merged data into SQL Server table 'AmazonReviews_Merged'...")
# df_merged.to_sql("AmazonReviews_Merged", engine, if_exists="replace", index=False, chunksize=1000)
# print("Merged data successfully loaded into SQL Server!")
