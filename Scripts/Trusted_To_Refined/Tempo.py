from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.sql.functions import explode , col,when,current_date, months_between, floor,dayofmonth,year,month
from datetime import datetime

today = datetime.today()
ano = today.year
mes = today.month
day = today.day

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()

df_tempo = spark.read.option("header", True).csv("/usr/src/app/Trusted/Partidas/")

df_tempo = df_tempo.select('data')
df_tempo = df_tempo.withColumn("ano", year(col("data"))) 
df_tempo = df_tempo.withColumn("mes", month(col("data"))) 
df_tempo = df_tempo.withColumn("dia", dayofmonth(col("data"))) 
df_tempo = df_tempo.withColumnRenamed('data','ID_Tempo')
df_tempo = df_tempo.dropDuplicates()


df_tempo.write.mode("overwrite").option("header",True).csv(f"/usr/src/app/Refined/Tempo/year={ano}/month={mes}/day={day}/")

