import pandas as pd
import os,json


local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)


def create_carteira():
    """
    Passo 1 
    """
    df_cart = pd.read_csv(variables['DATALAKE_RAW']+ 'data_row' + '.csv',sep=' ')
    df_cart = df_cart[['DESCRIPTION','SYMBOL','QUANTITY']]
    df_cart.to_csv(variables['DATALAKE_WORK'] + 'carteira_fase_01' + '.csv',index=None)


if __name__ =='__main__':
    create_carteira()
