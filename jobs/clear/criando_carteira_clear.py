import pandas as pd
import os,json,sys
from datetime import datetime

sys.path.append('/home/marcelohenriqueleite35/carteira_ativos/jobs')

from utils.df_distribuido import create_df
from scraping.scraping_value_stock import get_value_fii

local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)

colunas = [ 
    "Data",
    "Simbolo", #
    "Posicao", #
    "Valor_Medio",
    "Valor_Adquirido",
    "Cotacao",
    "Valor_atual"
]

def add_valor_fii(df):
    df['Cotacao'] = df['Simbolo'].apply(lambda x: float(get_value_fii(x)))
    return df

def add_valor_atual(df):
    df['Valor_atual'] = df['Posicao'] * df['Cotacao']
    return df
def add_valor_medio(df):
    df_movimentacao_compra = pd.read_csv(variables['DATALAKE_TRUSTED'] + "compra_cei.csv")
    df_movimentacao_compra = df_movimentacao_compra[['Simbolo','Quantidade','Valor da Operação']]
    df_movimentacao_compra = df_movimentacao_compra.groupby(['Simbolo']).sum()
    df_movimentacao_compra['Valor Medio'] = df_movimentacao_compra['Valor da Operação'] / df_movimentacao_compra['Quantidade']
   
    df_movimentacao_compra = df_movimentacao_compra[['Valor Medio']]
    df = pd.merge(df,df_movimentacao_compra,on='Simbolo')
    return df

def add_valor_aplicado(df):
    df['Valor Adquirido']= df['Posicao'] * df['Valor Medio']
    return df


def criando_carteira_clear():

    df_clear = create_df('datalake_carteira',pref='raw/cei/posicao',drop_first_line=False)
    
    colunas_usadas = ['Código de Negociação','Quantidade']
    colunas = ['Simbolo','Posicao']
    df_clear = df_clear[colunas_usadas]
    df_clear.columns = colunas
    
    df_clear = df_clear.drop_duplicates()
    
    df_clear['Data'] = datetime(
        datetime.now().year,
        datetime.now().month,
        1
        )

    df_clear = add_valor_fii(df_clear)
    df_clear = add_valor_atual(df_clear)
    df_clear = add_valor_medio(df_clear)
    df_clear = add_valor_aplicado(df_clear)

    colunas = ['Data','Simbolo','Posicao','Valor Medio','Valor Adquirido','Cotacao','Valor_atual']
    df_clear = df_clear[colunas]
    print(df_clear)
    df_clear.to_csv(variables['DATALAKE_WORK'] + "tb_carteira_clear.csv",index=False,sep=',')
            
criando_carteira_clear()



