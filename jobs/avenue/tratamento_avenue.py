import pandas as pd
import sys,os,json

sys.path.append('/home/marcelohenriqueleite35/carteira_ativos/jobs/avenue')

from tranformacao_descricao import criando_campo_tratamento
from criando_datasets import criando_datasets


local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)



def tratamento_avenue():

    path =variables['DATALAKE_WORK'] + 'dados_avenue.csv'
    df = pd.read_csv(path)


    
    df = criando_campo_tratamento(df)

    # Removendo dados nulo
    filter = df['Coluna Tratamento']!='Nulo'
    df = df[filter]


    
    datasets = criando_datasets(df)


    for name_table,data_table in datasets:
        data_table.to_csv(variables['DATALAKE_WORK'] + f'{name_table}.csv',index=None)







