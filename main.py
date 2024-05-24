import flask
import functions_framework
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from toolz import pipe
from datetime import datetime
from typing import Dict
from google.cloud import storage

@functions_framework.http
def main(request: flask.Request) -> str:
    """
    Entry point for the web scraping Google Cloud Function.

    This function orchestrates the web scraping process for Old School RuneScape gold prices,
    saves the data to a Google Cloud Storage bucket, and returns a summary message.

    Parameters
    ----------
    request : flask.Request
        The HTTP request object that triggered this function.

    Returns
    -------
    str
        A summary message detailing the names of the DataFrames and their respective date ranges.
    """
    data = webscrapper()
    save_to_bucket(data)
    
    message = "Webscraping Old School RuneScape Gold Prices\n"
    message += "="*40 + "\n"
    for timeframe, df in data.items():
        start_date = df.index.min().strftime('%Y-%m-%d %H:%M:%S')
        end_date = df.index.max().strftime('%Y-%m-%d %H:%M:%S')
        message += f"{timeframe}: From {start_date} to {end_date}\n"
    message += "="*40
    
    return message


def webscrapper() -> Dict[str, pd.DataFrame]:
    """
    Scrape gold prices from Old School RuneScape and return the data as DataFrames.

    This function scrapes the gold prices in dollars from the Old School RuneScape (OSRS) 
    gold price history page. The prices are categorized into different time frames: 
    Alltime, 90 Day, 30 Day, 7 Day, and 1 Day. The function returns a dictionary where 
    each key is a time frame (as a string) and the value is a pandas DataFrame containing 
    the date and price information.

    Returns
    -------
    dict of pandas.DataFrame
        A dictionary with the following structure:
        {
            'Alltime': DataFrame containing date and price,
            '90 Day': DataFrame containing date and price,
            '30 Day': DataFrame containing date and price,
            '7 Day': DataFrame containing date and price,
            '1 Day': DataFrame containing date and price
        }
        Each DataFrame has the following columns:
        - date: datetime64[ns]
            The date of the price entry.
        - price: float
            The price of gold in dollars.

    Examples
    --------
    >>> data = webscrapper()
    >>> data['90 Day'].head()
                        price
    date                      
    2024-02-23 15:30:00  0.229
    2024-02-23 21:00:00  0.227
    2024-02-24 01:30:00  0.222
    2024-02-24 07:00:00  0.216
    2024-02-24 08:15:00  0.210
    """
    
    url = 'https://osrsgoldprices.com/#osrs_gold_price_history'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, features='html.parser')
    raw_series = soup.find_all('script', attrs={'type':'text/javascript'})
    
    y_regex = re.compile(r'(?<=\"data\"\:\[).*?(?=\]\,\"yAxis\")')
    y = pipe(
        raw_series,
        lambda x: [y_regex.findall(str(axis.string)).pop() for axis in x],
        lambda x: [y.split(',') for y in x],
        lambda x: [map(float, y) for y in x],
        lambda x: [list(y) for y in x]
    )
    
    dates_regex = re.compile(r'(?<=\"categories\"\:\[).*?(?=\]\,\"title\")')
    dates = pipe(
        raw_series,
        lambda x: [dates_regex.findall(str(axis.string)).pop() for axis in x],
        lambda x: [y.split(',') for y in x],
        lambda x: [map(lambda z: z.replace('\\', ''), y) for y in x],
        lambda x: [map(lambda z: z.replace('"', ''), y) for y in x],
        lambda x: [map(lambda z: datetime.strptime(z, "%d/%m/%Y %I:%M %p"), y) for y in x],
        lambda x: [list(y) for y in x]
    )
    
    title_regex= re.compile(r'(?<=title\:\s\").*?(?=\"\,)')
    titles = pipe(
        raw_series,
        lambda x: [title_regex.findall(str(axis.string)) for axis in x],
    )
    
    
    data = {}
    for i in range(len(raw_series)):
        data = data | {
            titles[i][0]: pd.DataFrame({
                'date': dates[i],
                'price': y[i],
            }).set_index('date')
        }
    return data


def save_to_bucket(data: Dict[str, pd.DataFrame]) -> None:
    """
    Save the data to a Google Cloud Storage bucket as CSV files.

    Parameters
    ----------
    data : dict of pandas.DataFrame
        The data to save, with each key corresponding to a CSV file name.
    """
    # Nombre del bucket de Google Cloud Storage
    bucket_name = 'main_webscraping'
    
    # Inicializar el cliente de almacenamiento
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    
    # Guardar cada DataFrame en el bucket
    for timeframe, df in data.items():
        blob = bucket.blob(f'{timeframe}.csv')
        blob.upload_from_string(df.to_csv(), 'text/csv')