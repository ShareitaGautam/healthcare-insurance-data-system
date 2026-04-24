from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder \
    .appName("Use Case 09 - Average Monthly Premium") \
    .getOrCreate()

subscriber_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_subscriber"
group_subgroup_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_group_subgroup"
group_path = "s3a://healthcare-insurance-data-platform/input-data/group.csv"

subscriber_df = spark.read.csv(subscriber_path, header=True, inferSchema=True).alias("s")
group_subgroup_df = spark.read.csv(group_subgroup_path, header=True, inferSchema=True).alias("gs")
group_df = spark.read.csv(group_path, header=True, inferSchema=True).alias("g")

# Keep only subscribers with subgroup
subscriber_df = subscriber_df.filter(
    (col("s.Subgrp_id").isNotNull()) & (col("s.Subgrp_id") != "NA")
)

# Join subscriber -> group_subgroup
join1 = subscriber_df.join(
    group_subgroup_df,
    col("s.Subgrp_id") == col("gs.SubGrp_ID"),
    "inner"
)

# Join -> group
final_df = join1.join(
    group_df,
    col("gs.Grp_Id") == col("g.Grp_Id"),
    "inner"
)

print("===== JOINED DATA SAMPLE =====")
final_df.select(
    col("s.Subgrp_id").alias("Subgrp_id"),
    col("gs.Grp_Id").alias("Grp_Id"),
    col("g.premium_written").alias("premium_written")
).show(5, truncate=False)

# Average monthly premium
result_df = final_df.agg(
    avg(col("g.premium_written")).alias("average_monthly_premium")
)

print("===== FINAL RESULT: AVERAGE MONTHLY PREMIUM =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_09_avg_monthly_premium"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()