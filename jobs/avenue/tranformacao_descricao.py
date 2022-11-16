import pandas as pd


valores_invalidos = ['ACCOUNT CONVERSION CASH']
valores_substituidos = ['tax, 30% withheld','.']


def _tratamento_row(row:str,valores_invalidos,valores_replace):
    
    for valor in valores_invalidos:
        if valor in row:
            return None

    # Retenção Impostos sobre Dividendos MSFT. MSFT tax, 30% withheld
    
    for value in valores_replace:
        row = row.replace(value,'')
    
    return row




def _tranformacao_generica(row:str,tipo:str,valores_invalidos=valores_invalidos,valores_replace=valores_substituidos):

    
    

    try:
        itens = row.split(' ')
    except:
        return 'Nulo'

    if tipo=='Câmbio':
        # Câmbio Instantâneo : R$2082.09
        #['Câmbio', 'Instantâneo', ':', 'R$2082.09']

        tipo = " ".join(itens[:2])
        valor = itens[3]

        # ('Câmbio Instantâneo', 'R$2082.09')
        return {"Tipo":tipo,"Valor (BRL)":valor}
        
    if tipo=='Remessa':
        row = _tratamento_row(row,valores_invalidos,valores_replace)
        tipo = f" ".join(itens[:3])
        valor = itens[4]
        return {"Tipo":tipo,"Valor (BRL)":valor}
    
    if tipo=='Retenção':
        
        # Retenção Impostos sobre Dividendos MSFT. MSFT tax, 30% withheld
        # ['Retenção', 'Impostos', 'sobre', 'Dividendos', 'MSFT.', 'MICROSOFT', 'CORP']
        tipo = ' '.join(itens[:4]).strip()
        simbolo = itens[4].strip()
        empresa = ' '.join(itens[5:]).strip()

        # ('Retenção Impostos sobre Dividendos', 'MSFT', 'MSFT')
        return {"Tipo":tipo,"Simbolo":simbolo,"Empresa":empresa}

    if tipo=='Taxa' or tipo=='US':
        return {"Tipo":tipo}

    if tipo=='Corretagem':
        tipo = itens[0].strip()
        simbolo = itens[1].strip()
        return {"Tipo":tipo,"Simbolo":simbolo}
        
    if tipo=='Compra' or tipo=='Venda':
        tipo = itens[0].strip()
        quantidade = itens[2].strip()
        simbolo = itens[3].strip()
        valor =  ''.join(itens[5:]).strip()
        return {"Tipo":tipo,"Simbolo":simbolo,"Quantidade":quantidade,"Cotação":valor}
    
    if tipo=='Dividendos':
        row = _tratamento_row(row,valores_invalidos,valores_replace)
        tipo = itens[0].strip()
        simbolo = itens[2].strip()
        return {"Tipo":tipo,"Simbolo":simbolo}
        
    if tipo=='Estorno' or tipo=='AJUSTE':
        tipo = itens[0].strip()
        obsevacao = ' '.join(itens[1:]).strip()
        return {"Tipo":tipo,"Observação":obsevacao}

    

def criando_campo_tratamento(df:pd.DataFrame,valores_invalidos=valores_invalidos,valores_replace=valores_substituidos):
    df['Coluna Tratamento'] = df['Descrição'].apply(lambda x: _tranformacao_generica(x,x.split(' ')[0],valores_invalidos,valores_replace))
    return df
