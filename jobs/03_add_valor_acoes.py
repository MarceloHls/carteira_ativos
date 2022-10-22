import pandas as pd
import yfinance as yf  
from datetime import datetime
from scraping_value_stock import get_value

DATALAKE_WORK = 'C:/Users/mhsilva/Documents/Workspace/acoes/data/work/'

def get_values(symbols:pd.Series):
    list_values = {
        'SIMBOLO':[],
        'VALOR_ATUAL':[]
    }
    for symbol in symbols[0]:
        try:
            value = get_value(symbol)

        except:
            value = 0
        list_values['SIMBOLO'].append(symbol)
        list_values['VALOR_ATUAL'].append(value)
    return pd.DataFrame.from_dict(list_values)


def add_valor_acao():
    df_carteira = pd.read_csv(DATALAKE_WORK + 'carteira_fase_02' + '.csv')
    df_valor_atual = get_values([df_carteira.SIMBOLO])
    df_carteira = pd.merge(df_carteira,df_valor_atual,how='inner',on=['SIMBOLO'])
    df_carteira.to_csv(DATALAKE_WORK + 'carteira' + '.csv', index=None)

if __name__ =='__main__':
    add_valor_acao()

