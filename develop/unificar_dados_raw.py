import findspark
import pandas
import os,json


findspark.init()

from pyspark.sql import SparkSession


local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv(variables['DATALAKE_RAW'] +"avenue/",header=True,sep=";")
df = df.toPandas()

df.to_csv(variables['DATALAKE_WORK'] + "dados_avenue.csv",index=False,sep=',')





