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



def unificando():
    pref = 'raw/cei/posicao'
    # columns = ['Data','Hora','Liquidação','Descrição','Valor (U$)','Saldo da conta (U$)']
    df_avenue = create_df(bucket_name='datalake_carteira',pref=pref)
    print(df_avenue)
    # df_avenue.to_csv(variables['DATALAKE_WORK'] + "dados_avenue.csv",index=False,sep=',')
            
unificando()
