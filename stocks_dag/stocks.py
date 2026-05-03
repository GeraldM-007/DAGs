from massive import RESTClient
from airflow.sdk import task, dag
from datetime import datetime, timedelta
import http.client
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
import os
from dotenv import load_dotenv

#load the dotenv file
load_dotenv()

#import the API KEY stored in the dot env file
API_KEY = os.getenv('API_KEY')

#defining the dag
@dag(
    dag_id = 'stocks_prices_dag',
    start_date = datetime(2026, 04, 23),
    schedule = timedelta(minutes=5),
    catchup = False
)

def stocks_prices_dag():
    @task()
    def extract_stocks_prices():
        
        #Massive is the server, we are the client (telling massive to send data to us)
        client = RESTClient("{API_KEY}")
        
        #creating a list from to store the diff stocks we want to get prices of
        symbols = ['AAPL', 'GOOGL', 'TSLA', 'NFLX', 'AMZN']
        
        #create an empty list to store extracted stock price data
        returned_stock_data = []
        
        #massive free api does not allow pulling stock data of the current date. So set date to the previous day
        yesterday_date = datetime.today() - timedelta(days=1)
        
        #defining the date to be used for getting data and formating it ot the appropriate format
        date = yesterday_date.strftime('%Y-%m-%d')
        
        #using a for loop to loop through the list of stocks
        for stock in symbols:
            #using try and except block so that we can be able to catch errors and prevent the code from crushing
            try:
                #the python request we are sending to massive to get the data
                request = client.get_daily_open_close_agg(
                    stock,
                    date,
                    adjusted="true",
                )
                
                #storing the data we are receiving from massive to the empty list returned_stock_data
                returned_stock_data.append({
                    'symbol': stock, #symbol defines out type of stock. We are passing the stocks in the symbols list
                    'open': request.open,
                    'high': request.high,
                    'low': request.low,
                    'close': request.close,
                    'volume': request.volume                    
                })
            
            #output to be returned after an error occurs
            except Exception as e:
                print(f"Error fetching stock prices: {e}")
        
        return returned_stock_data
    
    @task()
    def transform_stocks_prices(raw_stocks_prices):
        
        #create a dataframe from the entire dictionary as the raw_stocks_prices are a dictionary
        stocks_df = pd.DataFrame(raw_stocks_prices)
        
        #convert to a dictionary because xcom does not pass data frames
        stocks_dict = stocks_df.to_dict(orient="records")
        
        return stocks_dict
    
    @task()
    def load_stocks_prices(cleaned_stocks_prices):
        
        #reconstruct the dataframe
        df = pd.DataFrame(cleaned_stocks_prices)
        
        #connect to the database
        engine = create_engine('postgresql+psycopg2://postgres:12345@localhost:5432/postgres') 
        
        with engine.begin() as conn:
            
            #load by creating a table and appending into it in the database
            df.to_sql(name = 'stocks_prices', schema = 'assignment', con=engine, if_exists = 'append', index = False)
        
    #Define task dependencies
    raw_stocks_prices = extract_stocks_prices()
    cleaned_stocks_prices = transform_stocks_prices(raw_stocks_prices)
    load_stocks_prices(cleaned_stocks_prices)

#using an object dag to call the function stocks_prices_dag() that runs executes the whole ETL     
dag = stocks_prices_dag()
        
        
        
        
        
        
        
        
        
        
        
        
    