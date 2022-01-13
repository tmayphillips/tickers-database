#%%
from dotenv import load_dotenv
import pandas as pd
import os
import psycopg2
import matplotlib
from sqlalchemy import create_engine
from tqdm import tqdm_notebook


#%%
# Create a database connection
# db_password = 'pgAvery'
# db_password = os.environ.get('DB_PASSWORD')  # Set to your own password
# engine = create_engine('postgresql://postgres:{}@localhost/stockdata'.format(db_password))
# load_dotenv()

db_password = '8e2e84a5507c45cf3c54802e97948abd2ebb73ad29453ec3558f64cbe02469e7'
engine = create_engine('postgresql://vujwyosxlhitrw:{}@ec2-3-228-236-221.compute-1.amazonaws.com/djvd7ebilq70q'.format(db_password))

# Set some variables of where the CSV files are containing the pricing and ticker information
bars_path = '5min-data'
tickers_path = 'tickers'


#%%
# Create a SQL table directly from a dataframe
def create_prices_table(symbol):

    # Import the bar csv file into a dataframe
    df = pd.read_csv('{}/{}.csv'.format(bars_path, symbol))

    # Some formatting
    df = df[['date', 'volume', 'open', 'close', 'high', 'low', 'symbol']]
    df['date'] = pd.to_datetime(df['date'])
    df = df.fillna(0)
    df['updated'] = pd.to_datetime('now')

    # Write the data into the database, this is so fucking cool
    #df.to_sql('daily_prices', engine, if_exists='replace', index=False)
    df.to_sql('min5_prices', engine, if_exists='replace', index=False)

    # Create a primary key on the table
    query = """ALTER TABLE min5_prices 
                ADD PRIMARY KEY (symbol, date);"""
    engine.execute(query)
    
    return 'Daily prices table created'

create_prices_table('AAPL')


#%%
# This function will build out the sql insert statement to upsert (update/insert) new rows into the existing pricing table
def import_bar_file(symbol):
    path = bars_path + '/{}.csv'.format(symbol)
    df = pd.read_csv(path, index_col=[0], parse_dates=[0])
    
    # First part of the insert statement
    insert_init = """INSERT INTO min5_prices
                    (date, volume, open, close, high, low, symbol)
                    VALUES
                """
                
    # Add values for all days to the insert statement
    vals = ",".join(["""('{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(
                     date,
                     row.volume,
                     row.open,
                     row.close,
                     row.high,
                     row.low,
                     symbol,
                     ) for date, row in df.iterrows()])
    
    # Handle duplicate values - Avoiding errors if you've already got some data in your table
    insert_end = """ ON CONFLICT (symbol, date) DO UPDATE 
                SET
                volume = EXCLUDED.volume,
                open = EXCLUDED.open,
                close = EXCLUDED.close,
                high = EXCLUDED.high,
                low = EXCLUDED.low
                """

    # Put together the query string
    query = insert_init + vals + insert_end
    
    # Fire insert statement
    engine.execute(query)

# This function will loop through all the files in the directory and process the CSV files
def process_symbols():
    # symbols = [s[:-4] for s in os.listdir(bars_path)]

    symbols = ['AAPL','TSLA', 'NVDA', 'JPM', 'BAC']
    # symbols = ['NBR', 'GOOG', 'AXP', 'COF', 'WFC']
    # symbols = ['MSFT', 'FB', 'AMZN', 'GS', 'MS']
    # symbols = ['V', 'GME', 'NFLX', 'KO', 'JNJ']
    # symbols = ['CRM', 'PYPL', 'XOM', 'HD', 'DIS']
    # symbols = ['INTC', 'COP', 'CVX', 'SBUX', 'OXY']
    # symbols = ['WMT', 'MPC', 'SLB', 'PSX', 'VLO']

    for symbol in tqdm_notebook(symbols, desc='Importing...'):
        import_bar_file(symbol)

    return 'Process symbols complete'        


# Load bars into the database
process_symbols()


#%%
def process_tickers():
    # Read in the tickers file from a csv
    df = pd.read_csv('{}/ticker-list.csv'.format(tickers_path))

    # Formatting
    df = df[['ticker', 'name', 'phone_number', 'address', 'city', 'state', 'postal_code', 'homepage_url', 'sic_description', 'description']]
    # df.rename(columns={'ticker': 'symbol', 'name': 'symbol_name', 'primaryExch': 'primary_exch'}, inplace=True)
    df['updated'] = pd.to_datetime('now')

    # Run this once to create the table
    df.to_sql('tickers', engine, if_exists='replace', index=False)
    
    # Add a primary key to the symbol
    query = """ALTER TABLE tickers
                ADD PRIMARY KEY (ticker);"""
    engine.execute(query)
    
    return 'Tickers table created'
                
# Load tickers into the database    
process_tickers()


# %%
# Read in the PostgreSQL table into a dataframe
prices_df = pd.read_sql('daily_prices', engine, index_col=['symbol', 'date'])

# Show results of df
prices_df


# %%
#I can also pass in a sql query
prices_df2 = pd.read_sql_query('select * from daily_prices', engine, index_col=['symbol', 'date'])

# Plot the results
prices_df2.loc[['TSLA']]['close_adj'].plot()