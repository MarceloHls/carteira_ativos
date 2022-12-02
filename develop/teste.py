from google.cloud import storage
import pandas as pd
import os
import io

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/marcelohenriqueleite35/airflow_teste/path_service_account/tech-visa-jobs-7bd5336ca330.json'


def create_df(bucket_name,pref,columns=[],drop_first_line = True):

    storage_client = storage.Client()

    blobs = storage_client.list_blobs(bucket_name)
    df_uni = ''
    for blob in blobs:
        if pref in blob.name:
            if blob.size==0:
                continue
            else:
                try:
                    data = blob.download_as_text()
                    df = pd.DataFrame([x.split(';') for x in data.split('\n')])
                except:
                    data = blob.download_as_bytes()
                    data = io.BytesIO(data)
                    df = pd.read_excel(data)

                
                if len(columns) > 0:
                    df.columns = columns
                if len(df_uni) ==0:
                    df_uni = df
                else:
                    df_uni = pd.concat([df_uni,df])

    df_uni = df_uni.drop_duplicates()
    if drop_first_line:
        df_uni = df_uni.drop([0])       
    df_uni = df_uni.dropna()         
    print(df_uni)

    return df_uni

# create_df('datalake_carteira',pref='raw/cei/posicao',drop_first_line=False)

