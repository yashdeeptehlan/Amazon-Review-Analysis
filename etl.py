from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AmazonReviewsETL") \
    .getOrCreate()

# --- Step 1: Extract ---
# Load the merged TSV file into a DataFrame
merged_file_path = "/Users/user/Downloads/amazon_reviews_merged.tsv"
df = spark.read.csv(merged_file_path, sep="\t", header=True, inferSchema=True)
print("Total records loaded:", df.count())

# --- Step 2: Transform ---
# Remove rows with missing critical values: review_id, review_date, star_rating, and review_body.
df_clean = df.filter(
    (col("review_id").isNotNull()) & (col("review_id") != "") &
    (col("review_date").isNotNull()) & (col("review_date") != "") &
    (col("star_rating").isNotNull()) &
    (col("review_body").isNotNull()) & (col("review_body") != "")
)

# Remove duplicate records based on review_id
df_clean = df_clean.dropDuplicates(["review_id"])

# Convert review_date to proper Date type (assuming 'yyyy-MM-dd' format)
df_clean = df_clean.withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))

# Create a new column 'sentiment' based on star_rating:
# Ratings >= 4: Positive; 3: Neutral; Ratings < 3: Negative.
df_clean = df_clean.withColumn("sentiment", 
    when(col("star_rating") >= 4, lit("Positive"))
    .when(col("star_rating") == 3, lit("Neutral"))
    .otherwise(lit("Negative"))
)

# Optional: Show schema and a sample of records for verification
df_clean.printSchema()
df_clean.show(10, truncate=False)

# --- Step 3: Load ---
# Write the cleaned DataFrame to a new TSV file
output_path = "/Users/user/Downloads/amazon_reviews_cleaned.tsv"
df_clean.write.csv(output_path, sep="\t", header=True, mode="overwrite")

print("ETL process completed: Cleaned data written to", output_path)

# Stop the Spark session
spark.stop()
