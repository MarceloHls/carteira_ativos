import pandas as pd



def criando_datasets(df:pd.DataFrame):
    df_columns = list(df.columns)
    dict_datasets = {

    }
    for i in range(len(df)):
        # index do dataset que serao usadas nos novos dataset
        # 'Valor (U$)','Data', 'Hora', 'Liquidação', 'Saldo da conta (U$)'

        index_colunas = [0,1,2,4,5]
        valores_tratados = df.iloc[i,6]
        tipo = df.iloc[i,6]['Tipo']
        
        
        # Criando chave no dicionario conforme o tipo
        if tipo not in dict_datasets.keys():
            dict_datasets[tipo] = {}

        
        # Criando dentro do dict tipo, as chaves que vem do campo dos dados tratados
        for key in valores_tratados.keys():
            if key not in dict_datasets[tipo].keys():
                dict_datasets[tipo][key] = []
            dict_datasets[tipo][key].append(valores_tratados[key])

        # Obtendo algumas colunas do dataset original para ser usado no novo dataset
        for index in index_colunas:
            nome_coluna = df_columns[index]
            if nome_coluna not in dict_datasets[tipo].keys():
                dict_datasets[tipo][nome_coluna] = []
            dict_datasets[tipo][nome_coluna].append(df.iloc[i,index])

    
    dfs = [] 

    for key in dict_datasets.keys():
        df = pd.DataFrame.from_dict(dict_datasets[key])
        dfs.append((key,df))
        # break

    return dfs
