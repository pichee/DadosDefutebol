from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.sql.functions import explode,col,when
from pyspark.sql.functions import lit
from datetime import datetime

today = datetime.today()
day = today.day
month = today.month
year = today.year

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()


estatisticas2024 = spark.read.option("multiline", "true").json("/usr/src/app/Raw/Estatisticas/year=2024")
estatisticas2025 = spark.read.option("multiline", "true").json("/usr/src/app/Raw/Estatisticas/year=2025")


estatisticas = estatisticas2024.withColumn("jogadores_explode",explode(col('scorers')))

estatisticas2024 = estatisticas.select(
    col("jogadores_explode.player.id").alias("id_jogador"),
    col("jogadores_explode.player.name").alias("nome_jogador"),
    col("jogadores_explode.team.id").alias("id_time"),
    col("jogadores_explode.team.name").alias("time"),
    col("jogadores_explode.goals").alias("gols"),
    col("jogadores_explode.assists").alias("assistencias"),
    col("jogadores_explode.playedMatches").alias("partidas"),
    col("jogadores_explode.penalties").alias("penaltis")
)
estatisticas2024 = estatisticas2024.withColumn("ano",lit(2024))
estatisticas = estatisticas2025.withColumn("jogadores_explode",explode(col('scorers')))

estatisticas2025 = estatisticas.select(
    col("jogadores_explode.player.id").alias("id_jogador"),
    col("jogadores_explode.player.name").alias("nome_jogador"),
    col("jogadores_explode.team.id").alias("id_time"),
    col("jogadores_explode.team.name").alias("time"),
    col("jogadores_explode.goals").alias("gols"),
    col("jogadores_explode.assists").alias("assistencias"),
    col("jogadores_explode.playedMatches").alias("partidas"),
    col("jogadores_explode.penalties").alias("penaltis")
)
estatisticas2025 = estatisticas2025.withColumn("ano",lit(2025))
df_todasEstatisticas = estatisticas2024.union(estatisticas2025)

df_todasEstatisticas = df_todasEstatisticas.fillna(0)

df_todasEstatisticas.write.mode("overwrite").option("header", True).csv(f"/usr/src/app/Trusted/Estatisticas/year={year}/month={month}/day={day}/")