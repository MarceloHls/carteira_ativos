import pandas as pd
import os,json

local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)


def __add_profit(df_carteira:pd.DataFrame):
    df_carteira['LUCRO_VALOR'] =round(df_carteira.VALOR_ATUAL_INVESTIDO - (df_carteira.QUANTIDADE * df_carteira.VALOR_MEDIO_PAGO),2)
    df_carteira['LUCRO'] =round(df_carteira.LUCRO_VALOR /( df_carteira.QUANTIDADE * df_carteira.VALOR_MEDIO_PAGO) * 100,2)
    return df_carteira

def add_profit  ():
    """
    Passo 6
    """
    df_carteira = pd.read_csv(variables['DATALAKE_WORK'] + 'carteira_fase_05.csv')
    df_carteira = __add_profit(df_carteira)
    df_carteira.to_csv(variables['DATALAKE_TRUSTED'] + 'TB_CARTEIRA' + '.csv',index=None)


