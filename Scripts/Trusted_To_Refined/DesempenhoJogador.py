from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.sql.functions import explode , col,when,current_date, months_between, floor,round
from datetime import datetime

today = datetime.today()
year = today.year
month = today.month
day = today.day

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()

df_desempenho = spark.read.option("header", True).csv("/usr/src/app/Trusted/Estatisticas/")

df_desempenho = df_desempenho.drop('nome_jogador','time','year','month','day')


df_desempenho = df_desempenho.withColumn('GolsEsperadosPorJogo',round(col('gols')/col('partidas'),2))


cols = df_desempenho.columns

cols.remove("ano")
cols.insert(2, "ano")

df_desempenho = df_desempenho.select(cols)

df_desempenho.write.mode("overwrite").option("header",True).csv(f"/usr/src/app/Refined/DesempenhoJogador/year={year}/month={month}/day={day}/")
