from sqlalchemy import create_engine, text
import pandas as pd
from nselib import capital_market

def load_stock_list():
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

    # Assuming `capital_market.equity_list()` returns a DataFrame
    stock_list_data = capital_market.equity_list()

    # Rename columns for consistency
    stock_list_data.columns = ['stock_id', 'stock_name', 'series', 'date_of_listing', 'face_value']

    # Add creation and update dates
    stock_list_data['create_date'] = pd.to_datetime('now')

    # Specify the columns to insert and update
    columns_to_insert = ['stock_id', 'stock_name', 'series', 'date_of_listing', 'face_value', 'create_date']
    columns_to_update = ['stock_name', 'series', 'date_of_listing', 'face_value', 'update_date']

    # Prepare the SQL statement with named placeholders
    insert_stmt = text(f"""
        INSERT INTO public.dim_stock_list ({', '.join(columns_to_insert)})
        VALUES ({', '.join([f':{col}' for col in columns_to_insert])})
        ON CONFLICT (stock_id) DO UPDATE SET
        stock_name = EXCLUDED.stock_name,
        series = EXCLUDED.series,
        date_of_listing = EXCLUDED.date_of_listing,
        face_value = EXCLUDED.face_value,
        update_date = NOW()
        WHERE 
            public.dim_stock_list.stock_name <> EXCLUDED.stock_name OR
            public.dim_stock_list.series <> EXCLUDED.series OR
            public.dim_stock_list.date_of_listing <> EXCLUDED.date_of_listing OR
            public.dim_stock_list.face_value <> EXCLUDED.face_value;
    """)

    # Create a database connection
    with engine.connect() as connection:
        # Convert DataFrame rows to list of dictionaries
        data_to_insert = stock_list_data[columns_to_insert].to_dict(orient='records')

        # Execute the insertion with upsert logic
        result = connection.execute(insert_stmt, data_to_insert)
        connection.commit()

    engine.dispose()

    print(f"{result.rowcount} rows inserted or updated")

    return result

if __name__ == "__main__":
    load_stock_list()