from bs4 import BeautifulSoup
import requests

def get_value(symbol:str):

    html = requests.get(f'https://seekingalpha.com/symbol/{symbol.upper()}')
    soup = BeautifulSoup(html.content,features="lxml")
    mydiv = soup.find(attrs={"data-test-id" : "symbol-price"})
    return float(mydiv.get_text().replace('$',''))

def get_value_fii(symbol:str):
    html = requests.get(f'https://www.fundsexplorer.com.br/funds/{symbol.lower()}')
    soup = BeautifulSoup(html.content,features="lxml")
    mydiv = soup.find("div", {"class": "quotations-detail"})
    value = mydiv.find_all('span')[1].get_text()
    return value.replace(',','.')
if __name__ =='__main__':
    print(get_value_fii('knri11'))