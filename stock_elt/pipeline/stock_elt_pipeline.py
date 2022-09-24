from stock_elt.elt.extract import extract_trades
from stock_elt.elt.load import load_trades 
from stock_elt.elt.transform import transform_trades 
from pandas import read_csv
import logging



def main():

    """ Run elt pipeline """ 


    # configure logging 
    logging.basicConfig(format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)


    # retrieve the data for tesla
    date_range = [{"start_date": '2020-01-01', "end_date": "2020-01-02"}]

    try:

        logger.info("Extraction started")
        tesla_df = extract_trades('tsla', date_range=date_range)

        # read the exchange codes 
        exchange_codes_df = read_csv("data/exchange_codes.csv")

    except BaseException as err:
        logging.error(err)
        return False

    else:
        logger.info("Extraction complete")




    # load raw data and exchange codes 
    # raw stock trade data is stored in public.raw_trades
    # exchange code data is stored in public.tbl_exchange_codes

    try:
        logger.info("Load started")
        load_trades(tesla_df, exchange_codes_df)
    except BaseException as err:
        logging.error(err)
        return False
    else:
        logger.info("Load complete")



    try:
        logger.info("Transformation started")

        # run the models for transformation 
        transform_trades()

    except BaseException as err:
        logging.error(err)
        return False

    else: 
        logger.info("Transformation Complete")


    return True



if __name__ == "__main__":

    if main():
        print("Stock trades ELT pipeline run successfully")



