from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max

spark = SparkSession.builder \
    .appName("Use Case 03 - Group With Maximum Subgroups") \
    .getOrCreate()

input_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_group_subgroup"

df = spark.read.csv(input_path, header=True, inferSchema=True)

print("===== GROUP_SUBGROUP SCHEMA =====")
df.printSchema()

print("===== SAMPLE DATA =====")
df.show(5, truncate=False)

# Count subgroups per group
grouped_df = df.groupBy("Grp_Id") \
    .agg(count("SubGrp_ID").alias("total_subgroups"))

print("===== ALL GROUP COUNTS =====")
grouped_df.orderBy(col("total_subgroups").desc()).show(truncate=False)

# Find maximum subgroup count
max_count = grouped_df.agg(
    spark_max("total_subgroups").alias("max_subgroups")
).collect()[0]["max_subgroups"]

print(f"Maximum subgroup count: {max_count}")

# Filter only groups with maximum subgroup count
result_df = grouped_df.filter(col("total_subgroups") == max_count)

print("===== FINAL RESULT: GROUP(S) WITH MAXIMUM SUBGROUPS =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_03_group_max_subgroups"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()