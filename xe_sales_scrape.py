#Scraping xe.gr
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from zenrows import ZenRowsClient

#SALES

# Create a proxy client
BASE_URL_Sales='https://www.xe.gr/property/results?transaction_name=buy&item_type=re_residence&geo_place_ids%5B%5D=ChIJ8UNwBh-9oRQR3Y1mdkU1Nic'
response_Sales = client.get(BASE_URL_Sales, headers=headers)
soup_Sales=BeautifulSoup(response_Sales.content,"html.parser")

# PAGINATION : Finding all li tags in ul and printing the text within it 
num_Sales=[]
web_urls_Sales=[]
buttons_Sales=soup_Sales.find('ul',class_='results-pagination')
for li in buttons_Sales.find_all('li'):
    num_Sales.append(li.text.replace('\n',''))

# Take the last element from the list of texts found in li elements
last_page_Sales=int(num_Sales[-1])
print(last_page_Sales)

num_of_ads_sales=last_page_Sales*34
#print(num_of_ads_sales)


for i in range(1,last_page_Sales+1):
    web_url_Sales_base="https://www.xe.gr/en/property/results?transaction_name=buy&item_type=re_residence&geo_place_ids%5B%5D=ChIJ8UNwBh-9oRQR3Y1mdkU1Nic&page="
    web_urls_Sales.append(web_url_Sales_base+str(i))
    
    
print(web_urls_Sales)


# SALES Dataframe 
price_list=[]
title_list=[]
level_list=[]
url_list=[]
address_list=[]
price_sqm_list=[]
constru_year_list=[]


for web_url in web_urls_Sales:
    try:
        response=client.get(web_url,headers=headers)
    
        xe_webpage=response.text
    
        soup=BeautifulSoup(xe_webpage,"html.parser")
        print('OK')
    except:
        print(f"No response for {web_url}")

    
    
    prices=soup.find_all(name="span",class_="property-ad-price")
    titles=soup.find_all(name="div",class_="common-property-ad-title")
    levels=soup.find_all(name="span",class_="property-ad-level")
    url_element=soup.find_all(name="div",class_="common-property-ad-body grid-y align-justify")
    addresses=soup.find_all(name="span",class_="common-property-ad-address")
    prices_sqm=soup.find_all(name="span",class_="property-ad-price-per-sqm")
    #construction_dates=soup.find_all(name="div",class_="grid-x property-ad-construction-year-container")

    
    for price in prices:
        p=re.findall(r'\d+', price.getText())
        price_list.append(int("".join(p)))
       #price_list.append(price.getText().replace("\xa0"," ")) 
    
    for title in titles:
        t=re.findall(r'\d+', title.getText())
        title_list.append(int("".join(t)))
        #title_list.append(title.getText().replace("\n",""))
    
    for level in levels:
        y1=level.getText().replace("\n","")
        level_list.append(y1.replace(" ", ""))
        
    for urlz in url_element:
        urls=urlz.find('a')
        url_list.append(urls.get('href'))
    
    for address in addresses:
        address_clean=re.findall(r'\((.*?)\)',address.getText())
        try:
            address_list.append(address_clean[0])
        except:
            address_list.append('NA')
        #address_clean=address.getText().replace(" ", "")
        #address_list.append(address_clean.replace("\n",""))  
    
    for price_sqm in prices_sqm:
        psqm=re.findall(r'\d+', price_sqm.getText())
        price_sqm_list.append(int("".join(psqm)))
        #price_sqm_list.append((price_sqm.getText().replace("\xa0","")).replace("\n",""))

    # for constru_date in construction_dates:
    #     try:
    #         year=re.findall(r'\d+',constru_date.getText())
    #         constru_year_list.append(int("".join(year)))
    #     except:
    #         constru_year_list.append("None")

    
#Create dictionary with column names as keys
dict={'Sqm':title_list,'Price':price_list,'URL':url_list, 'neighbourhood':address_list, 'price_sq/m':price_sqm_list}

#create dataframe
df=pd.DataFrame(dict)

#duplicateRows = df[df.duplicated(['Title', 'Price','neighbourhood'])]


#write to csv file
df.to_csv(f'xe_Sales_{date.today()}',mode='a',index=False, header=False)

time.sleep(1)
