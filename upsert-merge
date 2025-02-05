from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# Initialize SparkSession with Delta Lake
spark = SparkSession.builder \
    .appName("UpsertExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Sample existing data (Target Table)
data_existing = [(1, "Tushar", 25), (2, "Rahul", 30)]
columns = ["id", "name", "age"]
df_existing = spark.createDataFrame(data_existing, columns)

# Save as Delta Table
delta_path = "/mnt/data/delta_table"
df_existing.write.format("delta").mode("overwrite").save(delta_path)

# New incoming data (some updates, some new records)
data_updates = [(1, "Tushar Annam", 26), (3, "Amit", 28)]
df_updates = spark.createDataFrame(data_updates, columns)

# Load Delta Table
delta_table = DeltaTable.forPath(spark, delta_path)

# Perform Merge (Upsert)
delta_table.alias("target").merge(
    df_updates.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(set={"name": "source.name", "age": "source.age"}) \
 .whenNotMatchedInsert(values={"id": "source.id", "name": "source.name", "age": "source.age"}) \
 .execute()

# Show Final Data
spark.read.format("delta").load(delta_path).show()
