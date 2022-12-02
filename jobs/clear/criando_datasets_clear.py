import pandas
import os,json,sys

sys.path.append('/home/marcelohenriqueleite35/carteira_ativos/jobs')

from utils.df_distribuido import create_df

local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)

def criando_datasets_clear():

    df_clear = create_df('datalake_carteira',pref='raw/cei/historico',drop_first_line=False)

    df_clear['Simbolo']=df_clear['Produto'].apply(lambda x: x.split('-')[0].strip())
    

    df_movimentacao_compra = df_clear[
        (df_clear['Entrada/Saída']=='Credito') & 
        (df_clear['Movimentação']=='Transferência - Liquidação')]
    df_movimentacao_compra['Tipo']='Compra'
    


    df_dividendos =  df_clear[
        (df_clear['Entrada/Saída']=='Credito') & 
        (df_clear['Movimentação']=='Rendimento')]
    df_dividendos['Tipo']='Dividendo'


    df_movimentacao_compra = df_movimentacao_compra[['Tipo','Data','Simbolo','Quantidade','Preço unitário', 'Valor da Operação']]
    df_dividendos = df_dividendos[['Tipo','Data','Simbolo','Quantidade','Preço unitário', 'Valor da Operação']]
    
    
    df_movimentacao_compra.to_csv(variables['DATALAKE_TRUSTED'] + "compra_cei.csv",index=False,sep=',')
    df_dividendos.to_csv(variables['DATALAKE_TRUSTED'] + "dividendos_cei.csv",index=False,sep=',')
    
criando_datasets_clear()