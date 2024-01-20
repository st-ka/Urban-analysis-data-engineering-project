#Scraping xe.gr
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from zenrows import ZenRowsClient


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
price_list=[]
title_list=[]
level_list=[]
url_list=[]
address_list=[]
price_sqm_list=[]
    


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
    levels=soup.find_all(name="span",class_="property-ad-level")
    url_element=soup.find_all(name="div",class_="common-property-ad-body grid-y align-justify")
    addresses=soup.find_all(name="span",class_="common-property-ad-address")
    prices_sqm=soup.find_all(name="span",class_="property-ad-price-per-sqm")
    
    for price in prices:
        p=re.findall(r'\d+', price.getText())
        price_list.append(int("".join(p)))

    
    for title in titles:
        t=re.findall(r'\d+', title.getText())
        title_list.append(int("".join(t)))

    
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

    
    for price_sqm in prices_sqm:
        psqm=re.findall(r'\d+', price_sqm.getText())
        price_sqm_list.append(int("".join(psqm)))


    
    
    
#Create dictionary with column names as keys
dict={'Sqm':title_list,'Price':price_list,'URL':url_list, 'neighbourhood':address_list, 'price_sq/m':price_sqm_list}
df=pd.DataFrame(dict)
#create dataframe


#duplicateRows = df[df.duplicated(['Title', 'Price','neighbourhood'])]
#write to csv file
df.to_csv(f'xe_rentals_{date.today()}',mode='a',index=False, header=False)

time.sleep(1)

