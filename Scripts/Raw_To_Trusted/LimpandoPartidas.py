from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, when, lit
from datetime import datetime

today = datetime.today()
year = today.year
month = today.month
day = today.day

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()


partidas2024 = spark.read.option("multiline","true").json("/usr/src/app/Raw/Partidas/year=2024")
partidas2025 = spark.read.option("multiline","true").json("/usr/src/app/Raw/Partidas/year=2025")


df_flat2024 = partidas2024.withColumn("partidas_explode", explode(col("matches")))
df_flat2025 = partidas2025.withColumn("partidas_explode", explode(col("matches")))

df_partidas2024 = df_flat2024.select(
    col("partidas_explode.id").alias("id_partida"),
    col("partidas_explode.awayTeam.id").alias("id_time_visitante"),
    col("partidas_explode.homeTeam.id").alias("id_time_casa"),
    col("partidas_explode.utcDate").alias("data"),
    col("partidas_explode.homeTeam.name").alias("time_casa"),
    col("partidas_explode.awayTeam.name").alias("time_visitante"),
    col("partidas_explode.score.fullTime.home").alias("gols_casa"),
    col("partidas_explode.score.fullTime.away").alias("gols_visitante"),
).withColumn("ano", lit(2024))

df_partidas2025 = df_flat2025.select(
    col("partidas_explode.id").alias("id_partida"),
    col("partidas_explode.awayTeam.id").alias("id_time_visitante"),
    col("partidas_explode.homeTeam.id").alias("id_time_casa"),
    col("partidas_explode.utcDate").alias("data"),
    col("partidas_explode.homeTeam.name").alias("time_casa"),
    col("partidas_explode.awayTeam.name").alias("time_visitante"),
    col("partidas_explode.score.fullTime.home").alias("gols_casa"),
    col("partidas_explode.score.fullTime.away").alias("gols_visitante"),
).withColumn("ano", lit(2025))

df_partidas = df_partidas2024.union(df_partidas2025)

df_partidas = df_partidas.withColumn("Vencedor", when(col("gols_casa") > col("gols_visitante"), col("id_time_casa")).when(col("gols_visitante") > col("gols_casa"), col("id_time_visitante")).otherwise(0))


df_partidas.write.mode("overwrite").option("header", True).csv(f"/usr/src/app/Trusted/Partidas/year={year}/month={month}/day={day}/")
