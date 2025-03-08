from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, coalesce, lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AmazonReviewsETL_CrossSelling") \
    .getOrCreate()

# --- Step 1: Extract ---
# Load the merged TSV file into a DataFrame
merged_file_path = "/Users/user/Downloads/amazon_reviews_merged.tsv"
df = spark.read.csv(merged_file_path, sep="\t", header=True, inferSchema=True)
print("Total records loaded:", df.count())

# --- Step 2: Transform ---
# 2.1: Filter out rows with missing critical fields
df_clean = df.filter(
    (col("review_id").isNotNull()) & (col("review_id") != "") &
    (col("review_date").isNotNull()) & (col("review_date") != "") &
    (col("star_rating").isNotNull()) &
    (col("review_body").isNotNull()) & (col("review_body") != "")
)

# 2.2: Remove duplicate records based on review_id
df_clean = df_clean.dropDuplicates(["review_id"])

# 2.3: Convert review_date to proper Date type (assuming 'yyyy-MM-dd' format)
df_clean = df_clean.withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))

# 2.4: Aggregate reviews per customer by category
df_customer_counts = df_clean.groupBy("customer_id", "category") \
    .agg(count("review_id").alias("review_count"))

# 2.5: Pivot the aggregated data so each category becomes a column
df_customer_pivot = df_customer_counts.groupBy("customer_id") \
    .pivot("category", ["Electronics", "Books", "Watches"]) \
    .sum("review_count")

# 2.6: Replace nulls with 0 for each category column so we can do numerical comparisons
df_customer_pivot = (df_customer_pivot
    .withColumn("Electronics", coalesce(col("Electronics"), lit(0)))
    .withColumn("Books", coalesce(col("Books"), lit(0)))
    .withColumn("Watches", coalesce(col("Watches"), lit(0)))
)

# 2.7: Derive the Cross-Sell DataFrame
df_crosssell = df_customer_pivot.filter(
    ((col("Electronics") > 0).cast("int") +
     (col("Books") > 0).cast("int") +
     (col("Watches") > 0).cast("int")) >= 2
)

print("Pivoted Customer Data:")
df_customer_pivot.printSchema()
df_customer_pivot.show(10, truncate=False)

print("Cross-Sell Customer Data (at least 2 categories):")
df_crosssell.printSchema()
df_crosssell.show(10, truncate=False)

# --- Step 3: Load ---
# Write the cross-sell (customer overlap) DataFrame to a new TSV file
output_path = "/Users/user/Documents/BigDataProject/tsv_files/amazon_reviews_analysis"
df_crosssell.write.csv(output_path, sep="\t", header=True, mode="overwrite")
print("ETL process completed: Customer cross-sell data written to", output_path)

# Stop the Spark session
spark.stop()