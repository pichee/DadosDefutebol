from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.sql.functions import explode , col

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()
Tables = spark.read.option("multiline","true").json("../Raw/Times/TimesBrasileiros.json")
#Tables.printSchema()

df_flat = Tables.withColumn("teams_explode",explode(col('teams')))

df_times = df_flat.select(
    col("teams_explode.id").alias("id"),
    col("teams_explode.name").alias("name"),
    col("teams_explode.shortName").alias("shortName"),
    col("teams_explode.tla").alias("tla"),
    col("teams_explode.crest").alias("crest"),
    col("teams_explode.address").alias("address"),
    col("teams_explode.website").alias("website"),
    col("teams_explode.founded").alias("founded"),
    col("teams_explode.clubColors").alias("clubColors"),
    col("teams_explode.venue").alias("venue")
)

df_times.show(truncate=False)
df_times.write.mode("overwrite").parquet("../Truested/Times/times.parquet")



