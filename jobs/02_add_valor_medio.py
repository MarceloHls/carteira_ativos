import pandas as pd


DATALAKE_RAW = 'C:/Users/mhsilva/Documents/Workspace/acoes/data/raw/'
DATALAKE_WORK = 'C:/Users/mhsilva/Documents/Workspace/acoes/data/work/'


def add_valor_medio():
    df_price_med = pd.read_csv(DATALAKE_RAW + 'price_med' + '.csv')
    df_carteira_wrk = pd.read_csv(DATALAKE_WORK + 'carteira_fase_01' + '.csv')
    df_carteira = pd.merge(df_price_med,df_carteira_wrk,how='inner',on=['SYMBOL','DESCRIPTION'])
    df_carteira['VALOR_MEDIO'] = 1 / df_carteira.QUANTITY * df_carteira.PRICE_MED
    df_carteira= df_carteira[['DESCRIPTION','SYMBOL','QUANTITY','VALOR_MEDIO']]
    df_carteira.columns = ['EMPRESA','SIMBOLO','QUANTIDADE','VALOR_MEDIO_PAGO']
    df_carteira.to_csv(DATALAKE_WORK + 'carteira_fase_02' + '.csv',index=None)


if __name__ =='__main__':
    add_valor_medio()