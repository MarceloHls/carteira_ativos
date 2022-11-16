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


def unificando_dados_avenue():
    spark = SparkSession \
        .builder \
        .appName("Unificando avenue") \
        .getOrCreate()
        # .config("spark.some.config.option", "some-value") \
       

    # df = spark.read.csv(variables['DATALAKE_RAW'] +"avenue/",header=True,sep=";")
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","/home/marcelohenriqueleite35/airflow_teste/path_service_account/tech-visa-jobs-7bd5336ca330.json")
    
    df = spark.read.csv('gs://datalake_carteira/raw',header=True,sep=";")
    df = df.toPandas()
    print(df)

    df.to_csv(variables['DATALAKE_WORK'] + "dados_avenue.csv",index=False,sep=',')





unificando_dados_avenue()