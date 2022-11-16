from google.cloud import storage
import pandas as pd
import os,json



local = os.environ['PWD']

with open(f'{local}/config/lake.json','r') as file:
    variables = json.load(file)

for variable in variables.keys():
    variables[variable] =  variables[variable].replace('local',local)


def create_file_avenue(bucket_name='datalake_carteira',pref='raw/avenue_dados'):

    storage_client = storage.Client()

    blobs = storage_client.list_blobs(bucket_name)
    df_avenue = ''
    for blob in blobs:
        if pref in blob.name:
            if blob.size==0:
                continue
            else:
                data = blob.download_as_text()
                df = pd.DataFrame([x.split(';') for x in data.split('\n')])
                columns = ['Data','Hora','Liquidação','Descrição','Valor (U$)','Saldo da conta (U$)']
                df.columns = columns
                if len(df_avenue) ==0:
                    df_avenue = df
                else:
                    df_avenue = pd.concat([df_avenue,df])

    df_avenue = df_avenue.drop_duplicates()
    df_avenue = df_avenue.drop([0])
    print(df_avenue)
    df_avenue.to_csv(variables['DATALAKE_WORK'] + "dados_avenue.csv",index=False,sep=',')
            

