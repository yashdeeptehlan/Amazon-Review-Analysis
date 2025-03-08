from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, to_date, lit, count, avg, sum as spark_sum
)

spark = SparkSession.builder \
    .appName("AmazonReviewsETL_CrossSellingEnhanced") \
    .getOrCreate()

merged_file_path = "/Users/user/Documents/BigDataProject/tsv_files/amazon_reviews_merged.tsv"
df = spark.read.csv(merged_file_path, sep="\t", header=True, inferSchema=True)
print("Total records loaded:", df.count())


df_clean = df.filter(
    (col("review_id").isNotNull()) & (col("review_id") != "") &
    (col("review_date").isNotNull()) & (col("review_date") != "") &
    (col("star_rating").isNotNull()) &
    (col("review_body").isNotNull()) & (col("review_body") != "")
).dropDuplicates(["review_id"])

df_clean = df_clean.withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))

df_clean = df_clean.withColumn(
    "sentiment",
    when(col("star_rating") >= 4, lit("Positive"))
    .when(col("star_rating") == 3, lit("Neutral"))
    .otherwise(lit("Negative"))
)

df_customer_cat = df_clean.groupBy("customer_id", "category").agg(
    count("review_id").alias("review_count"),
    avg("star_rating").alias("avg_rating")
)

pivot_review_count = df_customer_cat.groupBy("customer_id") \
    .pivot("category", ["Electronics", "Books", "Watches"]) \
    .sum("review_count").na.fill(0)
for cat in ["Electronics", "Books", "Watches"]:
    pivot_review_count = pivot_review_count.withColumnRenamed(cat, f"{cat.lower()}_review_count")

pivot_avg_rating = df_customer_cat.groupBy("customer_id") \
    .pivot("category", ["Electronics", "Books", "Watches"]) \
    .avg("avg_rating").na.fill(0)
for cat in ["Electronics", "Books", "Watches"]:
    pivot_avg_rating = pivot_avg_rating.withColumnRenamed(cat, f"{cat.lower()}_avg_rating")

df_overall = df_customer_cat.groupBy("customer_id").agg(
    spark_sum(col("review_count")).alias("total_reviews"),
    (spark_sum(col("avg_rating") * col("review_count")) / spark_sum(col("review_count"))).alias("weighted_avg_rating")
)

df_category_count = df_customer_cat.groupBy("customer_id").agg(
    count("category").alias("total_categories_reviewed")
)

df_final = df_overall.join(df_category_count, on="customer_id", how="left") \
    .join(pivot_review_count, on="customer_id", how="left") \
    .join(pivot_avg_rating, on="customer_id", how="left")

df_final = df_final.withColumn("overall_sentiment",
    when(col("weighted_avg_rating") >= 4, lit("Positive"))
    .when(col("weighted_avg_rating") == 3, lit("Neutral"))
    .otherwise(lit("Negative"))
)

df_final = df_final.withColumn("cross_sell_score", col("weighted_avg_rating") * col("total_categories_reviewed"))

df_final.printSchema()
df_final.show(10, truncate=False)

output_path = "/Users/user/Documents/BigDataProject/tsv_files/amazon_reviews_analysis.tsv"
df_final.write.csv(output_path, sep="\t", header=True, mode="overwrite")
print("ETL process completed: Enriched data written to", output_path)

spark.stop()
