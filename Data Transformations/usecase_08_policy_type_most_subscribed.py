from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max

spark = SparkSession.builder \
    .appName("Use Case 08 - Policy Type Most Subscribed") \
    .getOrCreate()

subscriber_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_subscriber"
group_subgroup_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_group_subgroup"
group_path = "s3a://healthcare-insurance-data-platform/input-data/group.csv"

subscriber_df = spark.read.csv(subscriber_path, header=True, inferSchema=True).alias("s")
group_subgroup_df = spark.read.csv(group_subgroup_path, header=True, inferSchema=True).alias("gs")
group_df = spark.read.csv(group_path, header=True, inferSchema=True).alias("g")

# Remove NA subgroup values
subscriber_df = subscriber_df.filter(
    (col("s.Subgrp_id").isNotNull()) & (col("s.Subgrp_id") != "NA")
)

# Join subscriber -> group_subgroup
join1 = subscriber_df.join(
    group_subgroup_df,
    col("s.Subgrp_id") == col("gs.SubGrp_ID"),
    "inner"
)

# Join with group
final_df = join1.join(
    group_df,
    col("gs.Grp_Id") == col("g.Grp_Id"),
    "inner"
)

print("===== JOINED DATA SAMPLE =====")
final_df.select(
    col("s.Subgrp_id").alias("Subgrp_id"),
    col("gs.Grp_Id").alias("Grp_Id"),
    col("g.Grp_Type").alias("Grp_Type")
).show(5, truncate=False)

# Count subscriptions by policy type
grouped_df = final_df.groupBy(col("g.Grp_Type")) \
    .agg(count("*").alias("total_subscriptions"))

print("===== ALL POLICY TYPE COUNTS =====")
grouped_df.show(truncate=False)

max_count = grouped_df.agg(
    spark_max("total_subscriptions").alias("max_subscriptions")
).collect()[0]["max_subscriptions"]

result_df = grouped_df.filter(col("total_subscriptions") == max_count)

print("===== FINAL RESULT: MOST SUBSCRIBED POLICY TYPE =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_08_policy_type_most_subscribed"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()