import requests
from time import time,ctime
from time import sleep
import random
import pandas as pd
import os
from seleniumwire import webdriver
from seleniumwire.utils import decode 
import json

driver = webdriver.Chrome()

countPage = 0
while True:
                start_time = time()
                urldriver= 'https://shopee.vn/search?keyword=camera&page={}'.format(countPage)
                driver.get(urldriver)
                countPage=countPage+1
                if(countPage==8):     
                    break
                sleep(5)
                product_id=[]
                shop_id=[]
                short_description=[]
                price=[]
                discount=[]
                discount_price=[]
                sold_product=[]
                rating_count=[]
                location=[]
                brand=[]         
                print("crawling page number ",countPage)
                for request in driver.requests:
                    if request.response:
                        if request.url.startswith('https://shopee.vn/api/v4/search/search_items?by=relevancy&keyword='):
                            response = request.response
                            body= decode(response.body, response.headers.get('Content-Encoding','Identity'))
                            decoded_body= body.decode('utf8')
                            json_data= json.loads(decoded_body)
                            item_number=len(json_data['items'])
                                
                            for i in range(0,item_number):
                                print("crawling item number ",i)  
                                product_id.append(json_data['items'][i]['item_basic']['itemid'])
                                shop_id.append(json_data['items'][i]['item_basic']['shopid'])
                                short_description.append(json_data['items'][i]['item_basic']['name'])
                                price.append(json_data['items'][i]['item_basic']['price_before_discount'])
                                discount.append(json_data['items'][i]['item_basic']['raw_discount'])
                                discount_price.append(json_data['items'][i]['item_basic']['price'])
                                sold_product.append(json_data['items'][i]['item_basic']['historical_sold'])
                                rating_count.append(json_data['items'][i]['item_basic']['item_rating']['rating_count'])
                                location.append(json_data['items'][i]['item_basic']['shop_location'])
                                brand.append(json_data['items'][i]['item_basic']['brand'])
                            # product_name= json_data['items'][0]['item_basic']['name']
                            # print(product_name) 
df = pd.DataFrame(list(zip(product_id, shop_id, short_description, price, discount, discount_price, sold_product, rating_count, location, brand)),
                                              columns =['product_id', 'shop_id', 'short_description', 'price', 'discount', 'final_price', 'sold_product', 'rating_count', 'location', 'brand'])
print("here is the data:",df)  
if os.path.exists("F:\RnD\ETL\shopee\productId_shopee.csv"):
    os.remove("F:\RnD\ETL\shopee\productId_shopee.csv")
df.to_csv("F:\RnD\ETL\shopee\productId_shopee.csv", encoding="utf-8-sig")             
                            