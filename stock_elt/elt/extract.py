from pandas import json_normalize 
import requests
import os
import json 
import logging
from pandas import DataFrame


def extract_trades(stock_ticker: str, date_range: "list(dict)") -> DataFrame:

    """ 

    Get trades for multiple date ranges using the ALPACA API 


    Parameters 
    ----------
    ticker: str 
        Ticker e.g. AAPL 


    date_range: A list of date ranges 
        e.g. [{'start_date': '01/01/2022', 'end_date': '02/01/2022'}, {}, ...]




    Returns
    -------
    response_data: DataFrame 
        A json normalised dataframe

    """ 

    api_key_id = os.environ.get("api_key_id")
    api_secret_key = os.environ.get("api_secret_key")
    base_url = f"https://data.alpaca.markets/v2/stocks/{stock_ticker}/trades"



    response_data = []

    for generated_date in date_range:

        start_time = generated_date.get("start_date")
        end_time = generated_date.get("end_date")

        params = {"start": start_time, "end": end_time}


        # Authentication
        headers = {
            "APCA-API-KEY-ID" : api_key_id,
            "APCA-API-SECRET-KEY": api_secret_key
        }

        response = requests.get(base_url, headers=headers, params=params)

        if response.json().get("trades") is not None:
            response_data.extend(response.json().get("trades"))



    if len(response_data) == 0:
        raise Exception("Empty data returned")
        return


    return json_normalize(response_data, max_level=0)


        

