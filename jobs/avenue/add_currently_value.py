import pandas as pd
import os,json


local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)



def __create_valor_atual_investido(df_carteira:pd.DataFrame):
    df_carteira['Valor_atual'] = 0
    for i in range(len(df_carteira)):
        print(df_carteira)
        quantidade = df_carteira.iloc[i,2]
        valor_atual = df_carteira.iloc[i,4]
        df_carteira.iloc[i,6] = round(quantidade*valor_atual,2)
    return df_carteira.sort_values('Simbolo')

def __create_currently_posicao(df_carteira:pd.DataFrame):
    total_value = df_carteira.Valor_atual.sum()
    df_carteira['Posicao'] =  round(df_carteira.Valor_atual / total_value * 100,2)
    return df_carteira

def add_value():
    """
    Passo 4
    """
    df_carteira = pd.read_csv(variables['DATALAKE_WORK'] + 'carteira_part_02' + '.csv')
    df_carteira = __create_valor_atual_investido(df_carteira)
    # df_carteira = __create_currently_posicao(df_carteira)
    df_carteira.to_csv(variables['DATALAKE_TRUSTED'] + 'TB_CARTEIRA' + '.csv', index=None)
