import pandas as pd


DATALAKE_RAW = 'data/raw/'
DATALAKE_WORK = 'data/work/'
DATALAKE_TRUSTED = 'data/trusted/'


def __add_profit(df_carteira:pd.DataFrame):
    df_carteira['LUCRO_VALOR'] =round(df_carteira.VALOR_ATUAL_INVESTIDO - (df_carteira.QUANTIDADE * df_carteira.VALOR_MEDIO_PAGO),2)
    df_carteira['LUCRO'] =round(df_carteira.LUCRO_VALOR /( df_carteira.QUANTIDADE * df_carteira.VALOR_MEDIO_PAGO) * 100,2)
    return df_carteira

def add_profit  ():
    """
    Passo 6
    """
    df_carteira = pd.read_csv(DATALAKE_WORK + 'carteira_fase_05.csv')
    df_carteira = __add_profit(df_carteira)
    df_carteira.to_csv(DATALAKE_TRUSTED + 'TB_CARTEIRA' + '.csv',index=None)


