#Scraping xe.gr
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from zenrows import ZenRowsClient
from datetime import date
import os
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime



# RENTALS 
# Set the user-agent header
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

# Create a proxy client
client = ZenRowsClient("a45285ef2974d1cf4c9f103296a7835b19844643")
BASE_URL='https://www.xe.gr/en/property/results?transaction_name=rent&item_type=re_residence&geo_place_ids%5B%5D=ChIJ8UNwBh-9oRQR3Y1mdkU1Nic'
response = client.get(BASE_URL, headers=headers)
soup=BeautifulSoup(response.content,"html.parser")

# PAGINATION : Finding all li tags in ul and printing the text within it 
web_urls=[]
nums=[]

buttons=soup.find('ul',class_='results-pagination')
for li in buttons.find_all('li'):
    nums.append(li.text.replace('\n',''))

# Take the last element from the list of texts found in li elements
last_page=int(nums[-1])
#print(last_page*34)


#Create the urls of multiple "xe" result pages, 
for i in range(1,last_page+1):
    web_url_base="https://www.xe.gr/en/property/results?transaction_name=rent&item_type=re_residence&geo_place_ids%5B%5D=ChIJ8UNwBh-9oRQR3Y1mdkU1Nic&page="
    web_urls.append(web_url_base+str(i))
    
#print(web_urls)


# RENTALS PAGES

# Access page source code, parse, find elements and write them in csv file 

title_list=[]
level_list=[]
url_list=[]
address_list=[]
price_sqm_list=[]
construction_year_list=[]  


for web_url in web_urls:
    try:
        response=client.get(web_url,headers=headers)
    
        xe_webpage=response.text
    
        soup=BeautifulSoup(xe_webpage,"html.parser")
        print('OK')
    except:
        print(f"No response for {web_url}")

    
    
    prices=soup.find_all(name="span",class_="property-ad-price")
    titles=soup.find_all(name="div",class_="common-property-ad-title")
    levels=soup.find_all(name="div",class_="property-ad-level-container")
    url_element=soup.find_all(name="div",class_="common-property-ad-body grid-y align-justify")
    addresses=soup.find_all(name="span",class_="common-property-ad-address")
    prices_sqm=soup.find_all(name="span",class_="property-ad-price-per-sqm")
    details=soup.find_all(name='div',class_="common-property-ad-details grid-x")



    
    for price in prices:
        p=re.findall(r'\d+', price.getText())
        price_list.append(int("".join(p)))
       #price_list.append(price.getText().replace("\xa0"," ")) 
    
    for title in titles:
        t=re.findall(r'\d+', title.getText())
        title_list.append(int("".join(t)))
        #title_list.append(title.getText().replace("\n",""))
#    
#    for level in levels:
#        try:
#            #l=re.findall(r'[^A-Za-z0-9]+',level.getText())
#            l=re.findall(r'\d\w+',level.getText())
#            level_list.append("".join(l))
#        except:
#            level_list.append('Ground Floor|Basement')
#       
    for urlz in url_element:
        urls=urlz.find('a')
        url_list.append(urls.get('href'))
    
    for address in addresses:
        address_clean=re.findall(r'\((.*?)\)',address.getText())
        try:
            address_list.append(address_clean[0])
        except:
            address_list.append('None')
    
    for price_sqm in prices_sqm:
        psqm=re.findall(r'\d+', price_sqm.getText())
        price_sqm_list.append(int("".join(psqm)))

    for text in details:
   
        construction_years=text.find_all(name='div',class_="grid-x property-ad-construction-year-container")
        if construction_years:
            for years in construction_years:
                year=re.findall(r'\d+',years.getText())
                construction_year_list.append(int("".join(year)))
        else:
            construction_year_list.append("None")

#print(len(price_list))
#print(len(title_list))
#print(len(level_list))
#print(len(url_list))
#print(len(address_list))
#print(len(price_sqm_list))
#print(len(construction_year_list))
#
    
    
    
#Create dictionary with column names as keys

dict={'Sqm':title_list,'Price':price_list,'URL':url_list,'neighbourhood':address_list, 'price_sq/m':price_sqm_list, 'construction_year':construction_year_list, "date_imported":date.today()}
df=pd.DataFrame(dict)


#duplicateRows = df[df.duplicated(['Title', 'Price','neighbourhood'])]
#write to csv file
#df.to_csv(f'xe_rentals_{date.today()}',mode='a',index=False, header=False)
#
#time.sleep(1)
#
#

if not os.path.isfile('/app/csv/.csv'):
   df.to_csv(f'/app/csv/xe_rentals_{date.today()}.csv',index=False)
else: 
   df.to_csv(f'/app/csv/xe_rentals_{date.today()}.csv', mode='a', index=False, header=False)


time.sleep(1)


###########################
engine = create_engine(
'postgresql+psycopg2:'
'//postgres:'          # username for postgres
'docker'              # password for postgres
'@postgres-db:5432/'     # postgres server name and the exposed port
'postgres')

con=engine.connect()
# create an empty table xrysi_rentals 

sql = """
  create table if not exists xrysi_rentals (
  Square_meters int,
  Price int,
  URL VARCHAR(50),
  neighbourhood VARCHAR(50),
  price_per_sqm int,
  construction_year int,
  date_imported date
);
"""

#sql COPY xrysi_rentals FROM f"xe_rentals_{date.today()}.csv" DELIMITER ',' CSV HEADER;

# execute the 'sql' query
with engine.connect().execution_options(autocommit=True) as conn:
    query = conn.execute(text(sql))

# insert the dataframe data to 'xrysi_rentals' SQL table
with engine.connect().execution_options(autocommit=True) as conn:
    df.to_sql('xrysi_rentals', con=conn, if_exists='append', index= False)

#optional
print(pd.read_sql_query("""
select * from xrysi_rentals
""", con))




