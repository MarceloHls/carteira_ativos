import pandas as pd
import os,json


local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)



def __create_valor_atual_investido(df_carteira:pd.DataFrame):
    df_carteira['VALOR_ATUAL_INVESTIDO'] = 0
    for i in range(len(df_carteira)):
        quantidade = df_carteira.iloc[i,2]
        valor_atual = df_carteira.iloc[i,4]
        df_carteira.iloc[i,5] = round(quantidade*valor_atual,2)
    return df_carteira.sort_values('SIMBOLO')

def __create_currently_posicao(df_carteira:pd.DataFrame):
    total_value = df_carteira.VALOR_ATUAL_INVESTIDO.sum()
    df_carteira['POSICAO_ATUAL'] =  round(df_carteira.VALOR_ATUAL_INVESTIDO / total_value * 100,2)
    return df_carteira

def add_value():
    """
    Passo 4
    """
    df_carteira = pd.read_csv(variables['DATALAKE_WORK'] + 'carteira_fase_03.csv')
    df_carteira = __create_valor_atual_investido(df_carteira)
    df_carteira = __create_currently_posicao(df_carteira)
    df_carteira.to_csv(variables['DATALAKE_WORK'] + 'carteira_fase_04' + '.csv',index=None)
