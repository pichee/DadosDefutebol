from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.sql.functions import explode , col,when

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()
partidas2024 = spark.read.option("multiline","true").json("../Raw/Partidas/partidas2024.json")
partidas2025 = spark.read.option("multiline","true").json("../Raw/Partidas/partidas2025.json")

df_flat = partidas2024.withColumn("partidas_explode",explode(col('matches')))
df_flat2 = partidas2025.withColumn("partidas_explode",explode(col('matches')))

df_partidas2024 = df_flat.select(
    col("partidas_explode.id").alias("id_partida"), 
    col("partidas_explode.awayTeam.id").alias("id_time_visitante"),
    col("partidas_explode.homeTeam.id").alias("id_time_casa"),        
    col("partidas_explode.utcDate").alias("data"),
    col("partidas_explode.homeTeam.name").alias("time_casa"),
    col("partidas_explode.awayTeam.name").alias("time_visitante"),
    col("partidas_explode.score.fullTime.home").alias("gols_casa"),
    col("partidas_explode.score.fullTime.away").alias("gols_visitante"),
)

df_partidas2025 = df_flat2.select(
    col("partidas_explode.id").alias("id_partida"), 
    col("partidas_explode.awayTeam.id").alias("id_time_visitante"),
    col("partidas_explode.homeTeam.id").alias("id_time_casa"),        
    col("partidas_explode.utcDate").alias("data"),
    col("partidas_explode.homeTeam.name").alias("time_casa"),
    col("partidas_explode.awayTeam.name").alias("time_visitante"),
    col("partidas_explode.score.fullTime.home").alias("gols_casa"),
    col("partidas_explode.score.fullTime.away").alias("gols_visitante"),
)
df_partidas = df_partidas2024.union(df_partidas2025)

df_partidas = df_partidas.withColumn("Vencedor", when(col('gols_casa') > col('gols_visitante'),col('id_time_casa')).otherwise(col('id_time_visitante')))
df_partidas.write.mode("overwrite").option("header", True).csv("../Truested/Partidas/")