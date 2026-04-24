from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when

spark = SparkSession.builder \
    .appName("Clean Group Subgroup Data") \
    .getOrCreate()

input_path = "s3a://healthcare-insurance-data-platform/input-data/grpsubgrp.csv"

df = spark.read.csv(input_path, header=True, inferSchema=True)

print("===== ORIGINAL SCHEMA =====")
df.printSchema()

print("===== ORIGINAL DATA =====")
df.show(5, truncate=False)

# -----------------------------
# 1. Check duplicates
# -----------------------------
print("Total rows before dropDuplicates:", df.count())
df = df.dropDuplicates()
print("Total rows after dropDuplicates:", df.count())

# -----------------------------
# 2. Trim string columns
# -----------------------------
for column_name, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(column_name, trim(col(column_name)))

# -----------------------------
# 3. Convert empty/NaN to NULL
# -----------------------------
for column_name, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(
            column_name,
            when((col(column_name) == "") | (col(column_name) == "NaN"), None)
            .otherwise(col(column_name))
        )

# -----------------------------
# 4. Count NULL values
# -----------------------------
print("===== NULL COUNTS BEFORE =====")
for c in df.columns:
    print(c, df.filter(col(c).isNull()).count())

# -----------------------------
# 5. Replace NULL with 'NA'
# -----------------------------
string_columns = [name for name, dtype in df.dtypes if dtype == "string"]
df = df.fillna("NA", subset=string_columns)

print("===== CLEANED DATA =====")
df.show(10, truncate=False)

print("===== NULL COUNTS AFTER =====")
for c in df.columns:
    print(c, df.filter(col(c).isNull()).count())

# -----------------------------
# 6. Save cleaned data
# -----------------------------
output_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_group_subgroup"

df.write.mode("overwrite").csv(output_path, header=True)

print(f"Cleaned group_subgroup data written to: {output_path}")

spark.stop()