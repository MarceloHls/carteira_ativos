from google.cloud import storage
import pandas as pd
import os,json,sys

sys.path.append('/home/marcelohenriqueleite35/carteira_ativos/jobs')

from utils.df_distribuido import create_df

local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)


def criando_tabela_carteria():
    df_clear = pd.read_csv(variables['DATALAKE_WORK'] + "tb_carteira_clear.csv")
    df_clear['Tipo']='Fii'
    df_avenue = pd.read_csv(variables['DATALAKE_WORK'] + "tb_carteira_avenue.csv")
    df_avenue['Tipo']='Acao'
    df = pd.concat([df_clear,df_avenue])
    print(df)
    df.to_csv(variables['DATALAKE_TRUSTED'] + "TB_CARTEIRA.csv",index=False,sep=',')

criando_tabela_carteria()

