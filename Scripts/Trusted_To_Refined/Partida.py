from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.sql.functions import explode , col,when,current_date, months_between, floor
from datetime import datetime

today = datetime.today()
year = today.year
month = today.month
day = today.day

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()

df_partida = spark.read.option("header", True).csv("/usr/src/app/Trusted/Partidas/")

df_partida = df_partida.drop('time_casa','time_visitante','year','month','day')
df_partida = df_partida.withColumnRenamed('data','ID_Tempo')
df_partida.write.mode("overwrite").option("header",True).csv(f"/usr/src/app/Refined/Partida/year={year}/month={month}/day={day}/")

