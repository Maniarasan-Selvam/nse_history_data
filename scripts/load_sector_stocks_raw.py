
sector_dict =
    {'BANK' : ['bank-private', 'bank-public'],
    'AUTO' : [ 'automobile-lcvs-hvcs', 'automobile-passenger-cars', 'automobile-auto-truck-manufacturers', 'automobile-2-3-wheelers'],
    'PHARMA' : ['pharmaceuticals-drugs'],
    'IT' : ['software' ,'it-services-consulting'],
    'CHEMICAL' : ['speciality-chemicals' ,'chemicals' ,'diversified-chemicals'],
    'FINANCE' : ['Finance-Investment', 'Finance - Stock Broking' ,'finance-nbfc' ,'finance-housing','finance-term-lending'],
    'FMCG' : ['consumer-food', 'household-personal-products' ],
    'BATTERY' : ['Batteries'],
    'CEMENT' : ['cement'],
    'STEEL' : ['iron-steel'],
    'GOLD' : ['diamond-jewellery'],
    'DIVERSIFIED' : ['Diversified']
               }

sector_stock_dict = {}

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

        sector_stock_dict[i] = data

