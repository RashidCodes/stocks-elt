# Synopsis 

Extract trades on a stock using the Alpaca API, persist the raw data, transform the raw data by (```models/staging_trades.sql```), and serve the results (```models/serving_trades.sql```). The pipeline can be found in ```stock_elt/pipelines/stock_elt_pipeline```.

<br/>

# Why ELT?

- Cheaper Storage
- Access to raw data
- Faster time to insights

<br/>

# ELT

<img src="data_layers copy.png" />
