from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

spark = SparkSession.builder \
    .appName("Use Case 10 - Most Profitable Group") \
    .getOrCreate()

group_path = "s3a://healthcare-insurance-data-platform/input-data/group.csv"

df = spark.read.csv(group_path, header=True, inferSchema=True)

print("===== GROUP SCHEMA =====")
df.printSchema()

print("===== SAMPLE DATA =====")
df.show(5, truncate=False)

# Find maximum premium_written
max_value = df.agg(
    spark_max("premium_written").alias("max_premium")
).collect()[0]["max_premium"]

print(f"Maximum premium_written: {max_value}")

# Filter group(s) with maximum premium_written
result_df = df.filter(col("premium_written") == max_value) \
    .select("Grp_Id", "Grp_Name", "Grp_Type", "premium_written")

print("===== FINAL RESULT: MOST PROFITABLE GROUP =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_10_most_profitable_group"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()