from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext
from pyspark.sql.functions import explode , col,when,current_date, months_between, floor
from datetime import datetime

today = datetime.today()
year = today.year
month = today.month
day = today.day

spark = SparkSession.builder.master("local[*]").appName("CreateTables").getOrCreate()

df_time = spark.read.option("header", True).csv("/usr/src/app/Trusted/Times/")

df_time = df_time.select('id','shortName','tla','crest')
df_time = df_time.withColumnRenamed('id','ID_time')
df_time = df_time.withColumnRenamed('shortName','Nome')
df_time = df_time.withColumnRenamed("tla","Sigla")
df_time = df_time.withColumnRenamed("crest",'Imagem_Url')

df_time.write.mode("overwrite").option("header",True).csv(f"/usr/src/app/Refined/Time/year={year}/month={month}/day={day}/")

