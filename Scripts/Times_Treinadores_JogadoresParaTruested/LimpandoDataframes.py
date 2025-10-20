from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.sql.functions import explode , col

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()
Tables = spark.read.option("multiline","true").json("../Raw/Times/TimesBrasileiros.json")


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

df_times.write.mode("overwrite").parquet("../Truested/Times/times.parquet")

df_treinador = df_flat.select(
    col("teams_explode.id").alias("team_id"),
    col("teams_explode.coach.id").alias("coach_id"),
    col("teams_explode.name").alias("team_name"),
    col("teams_explode.coach.name").alias("coach_name"),
    col("teams_explode.coach.dateOfBirth").alias("coach_birth"),
    col("teams_explode.coach.nationality").alias("coach_nationality"),
    col("teams_explode.coach.contract.start").alias("coach_contract_start"),
    col("teams_explode.coach.contract.until").alias("coach_contract_until")
)

df_treinador.write.mode("overwrite").option("header", True).csv("../Truested/Treinador/")

df_squad = df_flat.withColumn("squad_explode", explode(col('teams_explode.squad')))

df_players = df_squad.select(
    col("squad_explode.id").alias("player_id"),
    col("teams_explode.id").alias("team_id"),
    col("teams_explode.name").alias("team_name"),
    col("squad_explode.name").alias("player_name"),
    col("squad_explode.dateOfBirth").alias("player_birth"),
    col("squad_explode.nationality").alias("player_nationality"),
    col("squad_explode.position").alias("player_position")
)
df_players.write.mode("overwrite").option("header", True).csv("../Truested/Jogadores/")






