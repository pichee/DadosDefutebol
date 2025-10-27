from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum as spark_sum, trim
from datetime import datetime

today = datetime.today()
year = today.year
month = today.month
day = today.day


spark = SparkSession.builder.master("local[*]").appName("Classificacao").getOrCreate()


df_partidas = spark.read.option("header", True).csv("/usr/src/app/Trusted/Partidas/")

df_partidas = df_partidas.withColumn("gols_casa", col("gols_casa").cast("int")) \
                         .withColumn("gols_visitante", col("gols_visitante").cast("int"))


df_partidas = df_partidas.where(

    col("gols_casa").isNotNull() &
    col("gols_visitante").isNotNull()
)


df_casa = df_partidas.select(
    col("id_time_casa").alias("team_id"),
    trim(col("time_casa")).alias("team_name"),
    col("gols_casa").alias("gols_marcados"),
    col("gols_visitante").alias("gols_sofridos"),
    col("ano")
).withColumn("vitoria", when(col("gols_marcados") > col("gols_sofridos"), 1).otherwise(0)) \
 .withColumn("empate", when(col("gols_marcados") == col("gols_sofridos"), 1).otherwise(0)) \
 .withColumn("derrota", when(col("gols_marcados") < col("gols_sofridos"), 1).otherwise(0)) \
 .withColumn("jogos_disputados", lit(1))


df_visitante = df_partidas.select(
    col("id_time_visitante").alias("team_id"),
    trim(col("time_visitante")).alias("team_name"),
    col("gols_visitante").alias("gols_marcados"),
    col("gols_casa").alias("gols_sofridos"),
    col("ano")
).withColumn("vitoria", when(col("gols_marcados") > col("gols_sofridos"), 1).otherwise(0)) \
 .withColumn("empate", when(col("gols_marcados") == col("gols_sofridos"), 1).otherwise(0)) \
 .withColumn("derrota", when(col("gols_marcados") < col("gols_sofridos"), 1).otherwise(0)) \
 .withColumn("jogos_disputados", lit(1))


df_todos_jogos = df_casa.union(df_visitante)


df_classificacao = df_todos_jogos.groupBy("team_id", "team_name", "ano").agg(
    spark_sum("jogos_disputados").alias("jogos_disputados"),
    spark_sum("vitoria").alias("vitorias"),
    spark_sum("empate").alias("empates"),
    spark_sum("derrota").alias("derrotas"),
    spark_sum("gols_marcados").alias("gols_marcados"),
    spark_sum("gols_sofridos").alias("gols_sofridos")
)

df_classificacao = df_classificacao.withColumn("saldo_gols", col("gols_marcados") - col("gols_sofridos")) \
                                   .withColumn("pontos", col("vitorias")*3 + col("empates"))
df_classificacao.orderBy(
    col("pontos").desc(),
    col("saldo_gols").desc(),
    col("gols_marcados").desc()
).show(truncate=False)

df_classificacao.write.mode("overwrite").option("header",True).csv(f"/usr/src/app/Refined/Classificacao/year={year}/month={month}/day={day}/")
