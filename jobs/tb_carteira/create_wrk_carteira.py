import pandas as pd



DATALAKE_RAW = 'data/raw/'
DATALAKE_WORK = 'data/work/'


def create_carteira():
    """
    Passo 1 
    """
    df_cart = pd.read_csv(DATALAKE_RAW + 'data_row' + '.csv',sep=' ')
    df_cart = df_cart[['DESCRIPTION','SYMBOL','QUANTITY']]
    df_cart.to_csv(DATALAKE_WORK + 'carteira_fase_01' + '.csv',index=None)


if __name__ =='__main__':
    create_carteira()
