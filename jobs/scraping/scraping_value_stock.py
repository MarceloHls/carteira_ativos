from bs4 import BeautifulSoup
import requests

def get_value(symbol:str):

    html = requests.get(f'https://seekingalpha.com/symbol/{symbol.upper()}')
    soup = BeautifulSoup(html.content,features="lxml")
    mydiv = soup.find(attrs={"data-test-id" : "symbol-price"})
    return float(mydiv.get_text().replace('$',''))


# if __name__ =='__main__':
#     print(get_value('AMZN'))