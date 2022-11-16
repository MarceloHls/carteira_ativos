import pandas as pd
import sys,os,json
from datetime import datetime


local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)



from datetime import datetime
def tranformando_date(row:str):
    try:
        date = row.split('/')
        year = int(date[2])
        month = int(date[1])
        day = int(date[0])

        return datetime(year,month,day)
    except:
        return row





def tratando_meta(row:str):
    if row=='FB':
        return 'META'
    return row





# Criando dicinario com dados historicos

def create_dict_historic(df:pd.DataFrame) -> dict:
    dict_historic = {

    }

    for i in range(len(df)):
        tipo = df.iloc[i,0]
        simbolo = df.iloc[i,1]
        quantidade = df.iloc[i,2]
        data = df.iloc[i,4]
        valor = df.iloc[i,7] 

        year = str(data.year)
        month = str(data.month)

        if year not in dict_historic.keys():
            dict_historic[year] = {}

        if month not in dict_historic[year].keys():
            dict_historic[year][month] = {}
        
        if simbolo not in dict_historic[year][month].keys():
            dict_historic[year][month][simbolo] = {}
            dict_historic[year][month][simbolo]['quantidade'] = 0.0
            dict_historic[year][month][simbolo]['valor atual'] = 0.0

        if tipo=='Compra':
            dict_historic[year][month][simbolo]['quantidade'] = dict_historic[year][month][simbolo]['quantidade'] + quantidade
            dict_historic[year][month][simbolo]['valor atual'] = dict_historic[year][month][simbolo]['valor atual'] + valor * - 1 
        else:
            dict_historic[year][month][simbolo]['quantidade'] = dict_historic[year][month][simbolo]['quantidade'] - quantidade
            dict_historic[year][month][simbolo]['valor atual'] = dict_historic[year][month][simbolo]['valor atual'] - valor 

    return dict_historic



# Tranformando dicionario historio em dataframe

def create_dataframe_historic(dict_historic:dict)-> pd.DataFrame:
    df_historic = {}


    colunms = ['Ano','Mes','Simbolo','Quantidade','Posicao']

    for colunm in colunms:
        df_historic[colunm] = []

    for year in dict_historic.keys():
        values = {}
        values['Ano'] = year
        for month in dict_historic[year].keys():
            values['Mes'] = month
            for simbolo in dict_historic[year][month].keys():
                values['Simbolo'] = simbolo
                values['Quantidade'] = dict_historic[year][month][simbolo]['quantidade']
                values['Posicao'] = dict_historic[year][month][simbolo]['valor atual']
                for colunm in colunms:
                    df_historic[colunm].append(values[colunm])
    return pd.DataFrame.from_dict(df_historic)

# Criando dataframe com datas historicas
def create_df_date(df_historic:pd.DataFrame) -> pd.DataFrame:
    df_date = {
        'Ano':[],
        'Mes':[]
    }
    for year in df_historic['Ano'].unique():
        date_year = datetime.now().year
        date_month = datetime.now().month
        for month in range(1,13):
            if year==date_year and month> date_month:
                pass
            else:
                df_date['Ano'].append(str(year))
                df_date['Mes'].append(str(month))


    return pd.DataFrame.from_dict(df_date)


def unifcando_df_hitoric_df_dates(df_historico,df_date) -> list[pd.DataFrame]:

    divisoes_acoes = [
    ('AMZN',19,'2022-05-27'),
    ('GOOGL',20,'2022-07-18'),
    ('TSLA',5,'2020-08-31'),
    ('TSLA',3,'2022-08-25')
    ]

    dfs = []

    for simbolo in df_historico['Simbolo'].unique():
        df_right = df_historico[df_historico['Simbolo']==simbolo]
        df_merge = df_date.merge(df_right,how='left',on=['Ano','Mes'])
        dfs.append(df_merge)



    for df_stock in dfs:
        simbolo = df_stock['Simbolo'].unique()[1]
        df_stock['Quantidade somanada'] = 0

        for i in range(len(df_stock)):
            quantidade = df_stock.iloc[i,3]
            year = df_stock.iloc[i,0]
            month = int(df_stock.iloc[i,1])
            
            if str(quantidade)=='nan':
                df_stock.iloc[i,2]=simbolo  # Simbolo
                df_stock.iloc[i,3]=0        # Quantidade Adquirada/Subtraida Mes
                df_stock.iloc[i,4]=0        # Quantidade somana

            if i!=0:   
                df_stock.iloc[i,5]= round(float(df_stock.iloc[i-1,5]) +  float(df_stock.iloc[i,3]),10)

            # Aplicando os desdobramentos das acoes   
            for simbolo_split,multi,date in divisoes_acoes:
                split_year = date.split('-')[0]
                split_month = int(date.split('-')[1])
                
                if simbolo_split==simbolo and year==split_year and month==split_month:
                    df_stock.iloc[i,5]= df_stock.iloc[i,5] * multi
                    df_stock.iloc[i,3]= df_stock.iloc[i,5] - df_stock.iloc[i - 1,5]
           

    return dfs


def unificando_dfs(dfs):
    df_historic = dfs[0]
    for i in range(1,len(dfs)):
        df_historic = pd.concat([df_historic,dfs[i]])

    df_historic.columns = ['Ano','Mes','Simbolo','Quantidade Movimentada (Mes)','Valor Movimentado (Mes) (US)','Posicao']
    df_historic['Mes']= df_historic['Mes'].astype(int)
    return df_historic.sort_values(['Ano','Mes'])

def criando_campo_data(df):
    df['Data'] = datetime.now()
    for i in range(len(df)):
            year = int(df.iloc[i,0])
            month = int(df.iloc[i,1])
            df.iloc[i,6] = datetime(year,month,1)
    return df

def create_historic():


    path_compra = variables['DATALAKE_WORK'] + 'Compra.csv'
    path_venda =variables['DATALAKE_WORK'] + 'Venda.csv'
    
    df_compra=pd.read_csv(path_compra)
    df_venda=pd.read_csv(path_venda)
    

    df = pd.concat([df_compra,df_venda])


    df['Data'] = df['Data'].apply(tranformando_date)
    df['Simbolo'] = df['Simbolo'].apply(tratando_meta)

    df = df.sort_values('Data')


    dict_historic = create_dict_historic(df)
    df_historic = create_dataframe_historic(dict_historic)
    df_date = create_df_date(df_historic)
    dfs = unifcando_df_hitoric_df_dates(df_historic,df_date)
    df_historic = unificando_dfs(dfs)
    df_historic = criando_campo_data(df_historic)

    df_historic.to_csv(variables['DATALAKE_TRUSTED'] + 'historico.csv',index=False)

if __name__ =='__main__':
    create_historic()