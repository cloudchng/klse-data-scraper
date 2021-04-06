#import required modules
from bs4 import BeautifulSoup
import ast
import pandas as pd
import re
import requests
from datetime import datetime
from sqlalchemy.engine import create_engine
import concurrent.futures
import threading
import time
from fake_useragent import UserAgent

database_dir = "stock.db"
table = 'klse'


def get_stock_price(ticker):
    # pass a ticker name to i3investor website url
    url = "https://klse.i3investor.com/servlets/stk/chart/{}.jsp". format(ticker)
    # get response from the site and extract the price data
    ua = UserAgent()
    userAgent = ua.random
    response = requests.get(url, headers={'User-Agent':f'{userAgent}'})
    soup = BeautifulSoup(response.content, "html.parser")
    script = soup.find_all('script')
    data_tag = script[20].contents[0]
    chart_data = ast.literal_eval(re.findall('\[(.*)\]', data_tag.split(';')[0])[0])
    # tabulate the price data into a dataframe
    chart_df = pd.DataFrame(list(chart_data), columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    # convert timestamp into readable date
    chart_df['Date'] = chart_df['Date'].apply(lambda x: \
      datetime.utcfromtimestamp(int(x)/1000).strftime('%Y-%m-%d'))
    return chart_df

def add_EMA(price, day):
    return price.ewm(span=day).mean()

def get_stock_list():
    # this is the website we're going to scrape from
    url = "https://www.malaysiastock.biz/Stock-Screener.aspx"
    response = requests.get(url, headers={'User-Agent':'test'})
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find(id = "MainContent2_tbAllStock")
    # return the result in a list
    return [stock.getText() for stock in table.find_all('a')]

# function to check for EMA crossing
def check_EMA_crossing(df):
    # condition 1: EMA18 is higher than EMA50 at the last trading day
    cond_1 = df.iloc[-1]['EMA18'] > df.iloc[-1]['EMA50']
    # condition 2: EMA18 is lower than EMA50 the previous day
    cond_2 = df.iloc[-2]['EMA18'] < df.iloc[-2]['EMA50']
    # condition 3: to filter out stocks with less than 50 candles
    cond_3 = len(df.index) > 50
    # will return True if all 3 conditions are met
    return (cond_1 and cond_2 and cond_3)

def updateSqlDB(database_dir, table_name, data_df, index=['Date','Stock']):
    engine = create_engine('sqlite:///' + database_dir, echo=False,connect_args={'check_same_thread': False})
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    data_df.loc[:, 'created_at'] = dt_string
    data_df.loc[:, 'modified_at'] = dt_string
    data_df = data_df.set_index(index)

    data_df.to_sql('my_tmp', con=engine, if_exists='replace', index=True)
    conn = engine.connect()
    trans = conn.begin()
    try:
        # delete those rows that we are going to "upsert"
        engine.execute(f"""delete from {table_name} where Date IN (select Date from my_tmp) and Stock IN (Select Stock from my_tmp)""")
        trans.commit()

        # insert changed rows
        data_df.to_sql(table_name, engine, if_exists='append', index=True)
    except:
        trans.rollback()
        raise

    engine.execute('DROP TABLE my_tmp;')
    print(table_name, "updated at: ", dt_string)

def download_date(each_stock):
    if each_stock:
        # Step 1: get stock price for each stock
        price_chart_df = get_stock_price(each_stock)
        price_chart_df['Stock'] = each_stock
        # Step 2: add technical indicators (in this case EMA)
        price_chart_df['EMA18'] = add_EMA(price_chart_df['Close'], 18)
        price_chart_df['EMA50'] = add_EMA(price_chart_df['Close'], 50)
        price_chart_df['EMA100'] = add_EMA(price_chart_df['Close'], 100)
        # if all 3 conditions are met, add stock into screened list
        if check_EMA_crossing(price_chart_df):
            price_chart_df['Ema'] = 1
        else :
            price_chart_df['Ema'] = 0

        print("Updating: ", each_stock)
        updateSqlDB(database_dir, table, price_chart_df)


# main program
if __name__ == '__main__':
    thread_count = 5
    # a list to store the screened results
    screened_list = []
    # get the full stock list
    stock_list = get_stock_list()
    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
        executor.map(download_date, stock_list)

