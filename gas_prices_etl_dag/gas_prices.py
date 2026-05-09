from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
import http.client
import pandas as pd
from sqlalchemy import create_engine, text
from io import StringIO
import json
from dotenv import load_dotenv

#load the dotenv file
load_dotenv()

#get the API_KEY from the .env file
API_KEY = os.getenv(API_KEY)

#create a function extract the gas prices using the API
def extract_gasprices():
    #using http client to connect to the api
    conn = http.client.HTTPSConnection("api.collectapi.com")
    
    #pass the headers including the api key authentication/authorization
    headers = {
        'content-type': "application/json",
        'authorization': "{API_KEY}"
        }
    
    #use a http get request to send a request to the api
    conn.request("GET", "/gasPrice/stateUsaPrice?state=CA", headers=headers)
    
    #get the server response after sending the http request
    response = conn.getresponse()
    
    #using .read to extract the response body from the server response(the returned data is in bytes format)
    data = response.read()
    
    #converts the bytes data into a python string
    decoded_data = data.decode("utf-8")
    
    #sends the python string data back to whoever called the function
    return decoded_data

#create a function to transform extracted gas prices data
def transform_gasprices():
    
    #using an object raw_gas_prices to call the function extract_gasprices()
    raw_gas_prices = extract_gasprices()
    
    #using json.loads() to convert the python string into a python dictionary
    parsed_data = json.loads(raw_gas_prices)
    
    #accessing nested data inside the python dictionary and storing it in a variable cities data
    cities_data = parsed_data['result']['cities']
    
    #converts the python dictionary into a data frame
    cities_df = pd.DataFrame(cities_data)
    
    #renaming the column name to cities
    cities_df = cities_df.rename(columns={ 'name':'cities'})
    
    #removing the lowername column from the data frame
    cities_df = cities_df.drop(['lowername'], axis=1)
    
    #convert the cleaned/transformed data frame back to json
    json_data = cities_df.to_json(orient='records')
    
    #sends the json data back to the caller of the function
    return json_data

#create a function to load the transformed data into a database
def load_gasprices():
    
    #using an object json_data to call the function transform_gasprices()
    json_data = transform_gasprices()
    
    #convert the returned json data into data frame using pandas
    df = pd.read_json(StringIO(json_data))
    
    #getting the stored credentials from the .env file
    USER = os.getenv('USER')
    HOST = os.getenv('HOST')
    PASSWORD = os.getenv('PASSWORD')
    PORT = os.getenv('PORT')
    DB_NAME = os.getenv('DATABASE')
    
    #using sqlalchemy to create a connection to the database using defined creds
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
    
    '''with creates a context manager meaning a connection is opened, a transaction is opened automatically
     when everything succeeds a commit automatically happens and if it fails a roll back happpens and then 
     the connection session is closed
     '''
    with engine.begin() as conn:
        #load the dataframe into the database by apppending it into a table 'california_gas_prices' 
        df.to_sql('california_gas_prices', conn, if_exists='append', index=False)

#defining the dag name, starting time/date and interval time between runs
with DAG(
    dag_id='california_gas_prices_dag',
    start_date=datetime(2026, 4, 22),
    schedule =timedelta(minutes=10),
    catchup=False
) as dag:
    #dag task one: extraction
    extract = PythonOperator(
        task_id='extracting',
        python_callable=extract_gasprices
    )
    #dag task two: transformation
    transform = PythonOperator(
        task_id='transforming',
        python_callable=transform_gasprices
    )
    #dag task three: loading to a database
    load = PythonOperator(
        task_id='loading',
        python_callable=load_gasprices
    )
    #defining the task dependencies
    extract >> transform >> load
