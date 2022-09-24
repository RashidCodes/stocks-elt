from database.postgres import create_pg_engine
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import MetaData, Float
from sqlalchemy.dialects import postgresql
from pandas import DataFrame



def load_trades(extracted_trades_df: DataFrame, exchange_codes_df: DataFrame) -> None:

    """ 
    Load trades using the Alpaca API. The raw trade data is stored in public.raw_trade and the exchange codes are stored in public.tbl_exchange_codes.


    Parameters
    ----------
    extracted_trades: DataFrame 
        Trades from Alpaca 


    exchange_codes: DataFrame 
        Data frame of exchange codes for enrichment 



    Returns
    -------
    None 

    """

    engine = create_pg_engine()

    # create the meta object
    meta = MetaData()

    target_table_name = "raw_trades"
    exchange_table_name = "tbl_exchange_codes"

    # drop the table if it already exists
    engine.execute(f"drop table if exists public.{target_table_name}")
    engine.execute(f"drop table if exists public.{exchange_table_name}")



    # specify the trade data table schema
    raw_trades = Table(
        target_table_name, meta,
        Column("i", String, primary_key=True),
        Column("t", String, primary_key=True),
        Column("x", String, primary_key=True),
        Column("p", Float),
        Column("s", Integer),
        Column("c", String),
        Column("z", String)
    )

    # specify exchange codes table schema 
    exchange_codes_table = Table(
        exchange_table_name, meta, 
        Column("exchange_code", String, primary_key=True),
        Column("exchange_name", String)
    )


    # creates table if it does not exist 
    meta.create_all(engine) 


    # upsert data to trade table 
    insert_statement = postgresql.insert(raw_trades).values(extracted_trades_df.to_dict(orient='records'))

    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=["i", "t", "x"],
        set_={c.key: c for c in insert_statement.excluded if c.key not in ['i', 't', 'x']}
    )

    # upsert data to exchange codes table 
    insert_exchange_statement = postgresql.insert(exchange_codes_table).values(exchange_codes_df.to_dict(orient='records'))

    exchange_upsert_statement = insert_exchange_statement.on_conflict_do_update(
        index_elements=["exchange_code"],
        set_={c.key: c for c in insert_exchange_statement.excluded if c.key not in ['exchange_code']}
    )

    # Always upsert
    engine.execute(upsert_statement)
    engine.execute(exchange_upsert_statement)


