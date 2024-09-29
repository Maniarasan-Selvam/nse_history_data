import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

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

sector_dict = {'BANK': ['bank-private', 'bank-public'],
               'AUTO': ['automobile-lcvs-hvcs', 'automobile-passenger-cars' ,'automobile-auto-truck-manufacturers'
                        ,'automobile-2-3-wheelers'],
               'PHARMA' : ['pharmaceuticals-drugs'],
               'IT' : ['software' ,'it-services-consulting'],
               'CHEMICAL' : ['speciality-chemicals' ,'chemicals' ,'diversified-chemicals'],
               'FINANCE' : ['Finance-Investment', 'Finance - Stock Broking' ,'finance-nbfc' ,'finance-housing'
                           ,'finance-term-lending'],
               'FMCG' : ['consumer-food', 'household-personal-products' ],
               'BATTERY' : ['Batteries'],
               'CEMENT' : ['cement'],
               'STEEL' : ['iron-steel'],
               'GOLD' : ['diamond-jewellery'],
               'DIVERSIFIED' : ['Diversified']
               }

for key, value_list in sector_dict.items():
    print(f"Key: {key}")
    for i in value_list:
        url =f"https://www.moneycontrol.com/markets/industry-analysis/{i}/"
        response = requests.get(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all spans with the specific class you want to target
        span_class = 'Cards_sectors_name__yyDH3'  # Replace with the actual class name
        spans = soup.find_all('span', class_=span_class)
        data = []
        for span in spans:
            cell = span.get_text(strip=True)
            data.append(cell)
            insert_stmt = text("""
            INSERT INTO public.dim_sector_stock_raw (sector_id, sub_sector, stock_name)
            VALUES (:sector_id, :sub_sector, :stock_name) """)

            with engine.connect() as connection:
                try:
                    connection.execute(insert_stmt, {
                        'sector_id': key,
                        'sub_sector': i,
                        'stock_name': cell
                    })
                    connection.commit()
                except Exception as e:
                    print(f"Failed to insert {cell}: {e}")
