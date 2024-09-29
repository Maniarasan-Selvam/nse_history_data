
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date

default_start_date = '2000-01-01'
end_date = str(date.today())

symbol = 'INFY'

def get_stock_historical_data(symbol, start_date, end_date):
    if start_date is None:
        start_date = default_start_date

    # Fetch historical data
    stock_data = yf.download(symbol + '.NS', start_date, end_date)

    # Add the date index as column
    stock_data_df = stock_data.reset_index()

    stock_data_df.columns = ['date', 'open', 'high', 'low', 'close', 'adj', 'volume']

    # Remove the 'adj' column
    stock_data_df_fin = stock_data_df.drop(columns=['adj'])

    # Insert 'symbol' column
    stock_data_df_fin.insert(1, 'symbol', symbol)

    # Rename columns to match target table
    stock_data_df_fin = stock_data_df_fin.rename(columns={
        'date': 'stock_date',
        'symbol': 'stock_id',
        'open': 'open_price',
        'high': 'high_price',
        'low': 'low_price',
        'close': 'close_price',
        'volume': 'stock_volume'
    })

    # Convert 'stock_date' to datetime.date
    stock_data_df_fin['stock_date'] = pd.to_datetime(stock_data_df_fin['stock_date']).dt.date

    # Add creation and update dates
    stock_data_df_fin['create_date'] = pd.to_datetime('now')

    return stock_data_df_fin


def insert_dly_hist_postgres(stock_hist_data):
    # PostgreSQL database credentials
    db_username = 'postgres'
    db_password = 'mani'
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'postgres'

    # Create connection string
    connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)

    # Specify the columns to insert and update
    columns_to_insert = ['stock_date', 'stock_id', 'open_price', 'high_price', 'low_price', 'close_price',
                         'stock_volume', 'create_date']
    columns_to_update = ['open_price', 'high_price', 'low_price', 'close_price', 'stock_volume', 'update_date']

    # Prepare the SQL statement with named placeholders
    insert_stmt = text(f"""
        INSERT INTO public.ind_stock_dly_price ({', '.join(columns_to_insert)})
        VALUES ({', '.join([f':{col}' for col in columns_to_insert])})
        ON CONFLICT (stock_date, stock_id) DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        stock_volume = EXCLUDED.stock_volume,
        update_date = NOW()
       WHERE 
            ind_stock_dly_price.open_price <> EXCLUDED.open_price OR
            ind_stock_dly_price.high_price <> EXCLUDED.high_price OR
            ind_stock_dly_price.low_price <> EXCLUDED.low_price OR
            ind_stock_dly_price.close_price <> EXCLUDED.close_price OR
            ind_stock_dly_price.stock_volume <> EXCLUDED.stock_volume;
    """)

    # Create a database connection
    with engine.connect() as connection:
        # Convert DataFrame rows to list of dictionaries
        data_to_insert = stock_hist_data[columns_to_insert].to_dict(orient='records')

        # Execute the insertion with upsert logic
        result = connection.execute(insert_stmt, data_to_insert)
        connection.commit()

    engine.dispose()

    print(f"{result.rowcount} rows inserted or updated")

    return result


stock_hist_data = get_stock_historical_data(symbol, None, end_date)

insert_dly_hist_postgres(stock_hist_data)
