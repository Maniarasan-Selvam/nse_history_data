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

# Define the bank_id (you need to set this to the correct value)
bank_id = 1  # Example value; replace with actual bank_id

# URL of the webpage
url = "https://www.moneycontrol.com/financials/hdfcbank/balance-sheetVI/HDF01#HDF01"
bank_id ='HDFCBANK'
bs_type = 'STANDALONE'

# Send a GET request to the website
response = requests.get(url)
html_content = response.content

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, "html.parser")

# Find the specific table with the balance sheet data
table = soup.find("table", {"class": "mctable1"})

# Extract data from the table
data = []
for row in table.find_all("tr"):
    cells = [cell.text.strip() for cell in row.find_all("td")]
    data.append(cells)

# Filter data to remove unwanted rows
data_list = [sublist for sublist in data if sublist[0] != '' and not all(x == '' for x in sublist[1:])]

# Prepare data for insertion
values = data_list[1:]


# Function to clean numeric strings
def clean_numeric(value):
    if value:
        return value.replace(",", "")
    return None

# Insert data into the database
for i in range(len(values)):
    if len(values[i]) >= 6:  # Ensure there are enough columns to insert
        insert_stmt = text("""
            INSERT INTO public.banks_bs_yearly 
            (bank_id, bs_type, bs_metrics, year_mar24, year_mar23, year_mar22, year_mar21, year_mar20)
            VALUES (:bank_id, :bs_type, :bs_metrics, :year_mar24, :year_mar23, :year_mar22, :year_mar21, :year_mar20)
        """)
        with engine.connect() as connection:
            try:
                connection.execute(insert_stmt, {
                    'bank_id': bank_id,
                    'bs_type': bs_type,
                    'bs_metrics': values[i][0],
                    'year_mar24': clean_numeric(values[i][1]),
                    'year_mar23': clean_numeric(values[i][2]),
                    'year_mar22': clean_numeric(values[i][3]),
                    'year_mar21': clean_numeric(values[i][4]),
                    'year_mar20': clean_numeric(values[i][5])
                })
                connection.commit()
            except Exception as e:
                print(f"Failed to insert row {i}: {e}")
