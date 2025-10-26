from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.sql.functions import explode , col,when,current_date, months_between, floor
from datetime import datetime

today = datetime.today()
year = today.year
month = today.month
day = today.day

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()

df_jogador = spark.read.option("header", True).csv("/usr/src/app/Trusted/Jogadores/")

df_jogador = df_jogador.drop('team_name','player_nationality','year','month','day')
df_jogador = df_jogador.withColumn("Idade",floor(months_between(current_date(),col('player_birth'))/12))

df_jogador = df_jogador.withColumnRenamed('player_id','ID_Jogador')
df_jogador = df_jogador.withColumnRenamed('team_id','ID_Time')
df_jogador = df_jogador.withColumnRenamed('player_name','Nome')
df_jogador = df_jogador.withColumnRenamed('player_birth','Data_Nasc')
df_jogador = df_jogador.withColumnRenamed('player_position','Posicao')

df_jogador.write.mode("overwrite").option("header",True).csv(f"/usr/src/app/Refined/Jogador/year={year}/month={month}/day={day}/")
