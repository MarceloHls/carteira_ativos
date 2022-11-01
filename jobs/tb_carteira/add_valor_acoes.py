import pandas as pd
import sys
import os,json
sys.path.append('/home/marcelohenriqueleite35/carteira_ativos/jobs/scraping')

from scraping_value_stock import get_value

local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)

def get_values(symbols:pd.Series):
    list_values = {
        'SIMBOLO':[],
        'VALOR_ATUAL':[]
    }
    for symbol in symbols[0]:
        # try:
        print(f'Buscando valor atual acão {symbol}')
        value = get_value(symbol)

        # except:
        #     value = 0
        list_values['SIMBOLO'].append(symbol)
        list_values['VALOR_ATUAL'].append(value)
    return pd.DataFrame.from_dict(list_values)


def add_valor_acao():
    """
    Passo 3 
    """
    df_carteira = pd.read_csv(variables['DATALAKE_WORK'] + 'carteira_fase_02' + '.csv')
    df_valor_atual = get_values([df_carteira.SIMBOLO])
    df_carteira = pd.merge(df_carteira,df_valor_atual,how='inner',on=['SIMBOLO'])
    df_carteira.to_csv(variables['DATALAKE_WORK'] + 'carteira_fase_03' + '.csv', index=None)

if __name__ =='__main__':
    add_valor_acao()

