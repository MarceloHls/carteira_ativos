import pandas as pd
import yfinance as yf  
from datetime import datetime

def get_values(symbols:pd.Series):
    end_date = str(datetime.now()).split(' ')[0]
    start_date = end_date[:-2]+str(int(end_date[-2:])-1)
    list_values = {
        'SIMBOLO':[],
        'VALOR_ATUAL':[]
    }
    for symbol in symbols[0]:
        try:
            data = yf.download(symbol,start=start_date,end=end_date)
            value = round(data.iloc[0,4],2)
        except:
            value = 0
        list_values['SIMBOLO'].append(symbol)
        list_values['VALOR_ATUAL'].append(value)
    return pd.DataFrame.from_dict(list_values)