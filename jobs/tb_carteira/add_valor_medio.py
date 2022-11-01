import pandas as pd
import os,json

local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)

def _calcu_value_med(df:pd.DataFrame):
    value_med = []
    for i in range(len(df)):
        quantity = df.iloc[i,3]
        price_med = df.iloc[i,2]
        absluty_quantity = float(str(quantity).split('.')[0])
        fractioned_quantity = float(f'0.{str(quantity).split(".")[1]}')
        temp_value_med = price_med if absluty_quantity!= 0 else 1 / fractioned_quantity * price_med
        value_med.append(temp_value_med)
    return pd.Series(value_med)






def add_valor_medio():
    """
    Passo 2
    """
    df_price_med = pd.read_csv(variables['DATALAKE_RAW'] + 'price_med' + '.csv')
    df_carteira_wrk = pd.read_csv(variables['DATALAKE_WORK'] + 'carteira_fase_01' + '.csv')


    df_carteira = pd.merge(df_price_med,df_carteira_wrk,how='inner',on=['SYMBOL','DESCRIPTION'])
    
    df_carteira['VALOR_MEDIO'] = _calcu_value_med(df_carteira)
    print(df_carteira)
    df_carteira= df_carteira[['DESCRIPTION','SYMBOL','QUANTITY','VALOR_MEDIO']]

    df_carteira.columns = ['EMPRESA','SIMBOLO','QUANTIDADE','VALOR_MEDIO_PAGO']
    df_carteira.to_csv(variables['DATALAKE_WORK'] + 'carteira_fase_02' + '.csv',index=None)


if __name__ =='__main__':
    add_valor_medio()