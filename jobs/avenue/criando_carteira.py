import pandas as pd
import sys,os,json
from datetime import datetime


local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)


def create_df_comeco_investimento(df_historic,simbolo):
    df_historic['Ano']= df_historic['Ano'].astype(int)

    df = df_historic[(df_historic['Simbolo']==simbolo)]
    df.index = list(range(0,len(df)))
    index_inicial = df[df['Posicao']==0].last_valid_index()

    a= 1 
    data = df_historic[(df_historic['Simbolo']==simbolo)].iloc[index_inicial,6]

    df_calcu = df_historic[
        (df_historic['Simbolo']==simbolo) &
        (df_historic['Data']>= data) 
        ]
    
    return df_calcu

def calculo_valor_medio(df_historic:pd.DataFrame)->pd.DataFrame:
    divisoes_acoes = {
    'AMZN':[
        (20,'2022-05-27')
        ],
    'GOOGL':[
        (20,'2022-07-18')
        ],
    'TSLA':[
        (5,'2020-08-31'),
        (3,'2022-08-25')],

    }

    dict_values = {}
    for i in range(len(df_historic)):

       
        simbolo = df_historic.iloc[i,2]


        if simbolo not in dict_values.keys():
            dict_values[simbolo] = 0

        if simbolo in divisoes_acoes.keys():
            continue
  
        df_calcu = create_df_comeco_investimento(df_historic,simbolo)
        valor = df_calcu[
                    (df_calcu['Simbolo']==simbolo) & 
                    (df_calcu['Valor Movimentado (Mes) (US)']> 0) 
                    ]['Valor Movimentado (Mes) (US)'].sum()

        quantidade = df_calcu[
                    (df_calcu['Simbolo']==simbolo) & 
                    (df_calcu['Valor Movimentado (Mes) (US)']> 0) 
                    ]['Quantidade Movimentada (Mes)'].sum()

          
        valor_medio = valor / quantidade
        dict_values[simbolo] = valor_medio
    
    for simbolo_split in divisoes_acoes.keys():
        data_anterior = '2000-01-01'
        valor_medio = 0
        vezes = 0

        for multi,data in divisoes_acoes[simbolo_split]:
            # print(divisoes_acoes[simbolo_split])
            df_calcu = create_df_comeco_investimento(df_historic,simbolo_split)
            df_calcu = df_historic
            
            valor = df_calcu[
                (df_calcu['Simbolo']==simbolo_split) & 
                (df_calcu['Valor Movimentado (Mes) (US)']> 0) & 
                (df_calcu['Data']<= data) &
                (df_calcu['Data']> data_anterior)
                ]['Valor Movimentado (Mes) (US)'].sum()
                
            quantidade = df_calcu[
                (df_calcu['Simbolo']==simbolo_split) & 
                (df_calcu['Valor Movimentado (Mes) (US)']> 0) & 
                (df_calcu['Data']<= data) &
                (df_calcu['Data']> data_anterior) 
                ]['Quantidade Movimentada (Mes)'].sum()
            # print(quantidade)

            if valor!=0:
                valor_medio_parcial = round((valor / quantidade) / multi,2)
                data_anterior = data
                valor_medio=valor_medio + valor_medio_parcial
                vezes+=1

        dict_values[simbolo_split] = valor_medio / vezes

    df_values = {
    'Simbolo':[],
    'Valor Medio':[]
    }

    for simbolo in dict_values.keys():
        df_values['Simbolo'].append(simbolo)
        df_values['Valor Medio'].append(round(dict_values[simbolo],2))

    df_values = pd.DataFrame.from_dict(df_values).sort_values('Simbolo')
    return df_values




def create_carteira():
    df_historic = pd.read_csv(variables['DATALAKE_TRUSTED'] + 'historico.csv')
    df_valor_medio = calculo_valor_medio(df_historic)
    
    data = datetime.now().replace(day=1).strftime('%Y-%m-%d')
    df_carteira = df_historic[df_historic['Data']==data].sort_values(['Simbolo'])
    df_carteira = pd.merge(df_carteira,df_valor_medio,how='left')
    df_carteira['Valor Adquirido'] = round(df_carteira['Posicao'] * df_carteira['Valor Medio'],2)
    df_carteira = df_carteira[['Data','Simbolo','Posicao','Valor Medio','Valor Adquirido']]
    print(df_carteira)
    df_carteira.to_csv(variables['DATALAKE_WORK'] + 'carteira_part_01' + '.csv',index=None)

if __name__ =='__main__':
    create_carteira()