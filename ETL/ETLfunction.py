from ast import Delete
import numpy as np
import ast
import os
import glob                  # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime
import psycopg2

LazadaItemsUrl    = "F:\RnD\ETL\lazadaexportdata\item"            # file used to store all extracted data
TikiItemsUrl  = 'F:\RnD\ETL\ctiki\productData_dtmtb.csv'
ShopeeItemsUrl   = "F:\RnD\ETL\shopee\productId_shopee.csv"            # all event logs will be stored in this file  # file where transformed data is stored
TikiCmtUrl = "F:\RnD\ETL\ctiki\comments_data_ncds.csv"
ShopeeCmtUrl = "F:\RnD\ETL\shopee\comments_data_shopee.csv"

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_lazada_items(url):
    pageNumber=6
    df_result= pd.DataFrame()
    df_result['product_id']=""
    for i in range(0,pageNumber,1):
        print("Crawl page number {}...".format(i))
        # df_result.append(extract_from_csv(LazadaItemsUrl+"itemPage{}.csv".format(i)))
        df_data = extract_from_csv(url+"\itemPage{}.csv".format(i))
        # df_data = df_data.reset_index(drop=True) 
        # print(df_data['link_item'])
        list_product_id=[]
        # print("đây là list product_id: \n",df_result['link_item'])
        # generate id from link of items
        for link in df_data['link_item']:
            # print("đây là link: \n",link)
            link=str(link)
            # print("gia tri là: \n",link)
            position= link.find(".html")
            # print("vi tri là: \n",position)
            product_id=""
            len=1
            while(len!=11):
                product_id=product_id+link[position-len]
                len=len+1
            list_product_id.append(product_id[::-1].replace("i","")) 
        # print("đây là list product_id: \n",list_product_id)
        df_data['product_id'] = list_product_id     
        df_result = pd.concat([df_result, df_data], axis=0)
        # df_result = df_result.loc[~df_result.index.duplicated(keep='first')]
        df_result = df_result.dropna()
        df_result = df_result.reset_index(drop=True)
    # generate encoded shop id from name
    shop_id=[]
    for name in df_result['shop_name']:
         shop_id.append(int.from_bytes(name.encode('utf-8-sig'), 'little'))
    df_result['shop_id'] = shop_id
    # generate encoded brand id from name
    brand_id=[]
    for name in df_result['brand_name']:
         brand_id.append(int.from_bytes(name.encode('utf-8-sig'), 'little'))
    df_result['brand_id'] = brand_id
    # cleaning and standardization column's data
    df_result['discount_percent'] = df_result['discount_percent'].str.replace("%"+"off"," ", regex=False)          
    df_result.rename(columns = {'discount_percent':'discount'}, inplace = True)
    df_result = df_result.drop(columns=["Unnamed: 0", "link_item", "index_id", "discount_idx", "starsReview_idx", "soldProduct_idx"])
    df_result['stars_review'] = df_result['stars_review'].str.replace('-'," ", regex=False)
    df_result['stars_review'] = df_result['stars_review'].str.replace('('," ", regex=False)
    df_result['stars_review'] = df_result['stars_review'].str.replace(')'," ", regex=False)
    df_result['price'] = df_result['price'].str.replace(" ₫"," ", regex=False)
    df_result['price'] = df_result['price'].str.replace(".","", regex=False)
        # df_result['price'] = df_result['price'].astype(int)
    # df_result['discount_percent'] = df_result['discount_percent'].replace("%"+"off"," ", regex=False)
    df_result['sold_product'] = df_result['sold_product'].str.replace(" Đã bán"," ", regex=False)
        
    return df_result

# df_product= extract_lazada_items(LazadaItemsUrl)
# print(df_product['shop_id'])
# if os.path.exists("F:\RnD\ETL\etlCrawlData\datalazada.csv"):
#     os.remove("F:\RnD\ETL\etlCrawlData\datalazada.csv")
# df_product.to_csv("F:\RnD\ETL\etlCrawlData\datalazada.csv", encoding="utf-8-sig")

def extract_shopee_items(url):
    df_data = extract_from_csv(url)
    df_result = df_data
    # df_result['price'] = df_data['price']/int(10^5)
    df_result['final_price'] = df_data['final_price']/int(100000)
    df_result['rating_count'] = df_data['rating_count'].str.replace("[", "", regex=False)
    df_result['rating_count'] = df_result['rating_count'].str.replace("]", "", regex=False)
    list_rating= df_result['rating_count']
    lr = list_rating.str.split(', ')
    strlist=[]
    for ir in lr:       
            strlist.append(ir[0])
    df_result['stars_review']= strlist

    # generate encoded brand id from name
    brand_id=[]
    df_result['brand'] = df_result['brand'].replace(np.nan ,"No brand")
    for name in df_result['brand']:
         name=str(name)
         brand_id.append(int.from_bytes(name.encode('utf-8-sig'), 'little'))
    df_result['brand_id'] = brand_id  

    df_result = df_result.drop(columns=["Unnamed: 0","rating_count","price"])
    df_result.rename(columns = {'short_description':'title', 'brand':'brand_name', 'final_price':'price'}, inplace = True)
    # print("đây là kết quả: \n",df_result)
    return df_result

# df_product= extract_shopee_items(ShopeeItemsUrl)
# print("gia tri brand: \n",df_product['brand_name']) 
# if os.path.exists("F:\RnD\ETL\etlCrawlData\datashopee.csv"):
#     os.remove("F:\RnD\ETL\etlCrawlData\datashopee.csv")
# df_product.to_csv("F:\RnD\ETL\etlCrawlData\datashopee.csv", encoding="utf-8-sig")  


def extract_ctiki_items(url):
     df_result = extract_from_csv(url)
     df_result = df_result.drop(columns=["stock_item","inventory_status","sku","Unnamed: 0","short_description","list_price","discount"])
     df_result.rename(columns = {'brand':'brand_name', 'discount_rate':'discount', 'review_count':'stars_review'
                                 ,'quantity_sold':'sold_product', 'cs_id':'shop_id', 'cs_name':'shop_name', 'product_name':'title'}, inplace = True)
     return df_result
# df_product= extract_ctiki_items(TikiItemsUrl)
# # print("gia tri df: \n",df_product) 
# if os.path.exists("F:\RnD\ETL\etlCrawlData\datatiki.csv"):
#     os.remove("F:\RnD\ETL\etlCrawlData\datatiki.csv")
# df_product.to_csv("F:\RnD\ETL\etlCrawlData\datatiki.csv", encoding="utf-8-sig")  

def extract_ctiki_comment(url):
     df_cmt = extract_from_csv(url)
     df_cmt = df_cmt.drop(columns=["title","purchased_at", "Unnamed: 0"])
     df_cmt.rename(columns = {'id':'cmt_id', 'thank_count':'like_count', 'customer_id':'user_id'
                                 ,'created_at':'created_time'}, inplace = True)
     return df_cmt

# df_product= extract_ctiki_comment(TikiCmtUrl)
# # print("gia tri df: \n",df_product) 
# if os.path.exists("F:\RnD\ETL\etlCrawlData\cmttiki.csv"):
#     os.remove("F:\RnD\ETL\etlCrawlData\cmttiki.csv")
# df_product.to_csv("F:\RnD\ETL\etlCrawlData\cmttiki.csv", encoding="utf-8-sig")  

def extract_shopee_comment(url):
     df_cmt = extract_from_csv(url)
     df_cmt = df_cmt.drop(columns=["shop_id", "Unnamed: 0", "seller_service"])
     df_cmt.rename(columns = {'id':'cmt_id', 'cmt_content':'content'
                                 ,'time':'created_time','product_quality':'rating','author_username':'user_name'}, inplace = True)
    #  df_cmt.dropna()
     df_cmt['like_count'] = df_cmt['like_count'].replace(np.nan ,0)
    #  df_cmt['user_name'] = df_cmt['user_name'].dropna()
     df_cmt = df_cmt[df_cmt['user_name'].notna()]
     return df_cmt

df_product= extract_shopee_comment(ShopeeCmtUrl)
# print("gia tri df: \n",df_product['user_name']) 
if os.path.exists("F:\RnD\ETL\etlCrawlData\cmtshopee.csv"):
    os.remove("F:\RnD\ETL\etlCrawlData\cmtshopee.csv")
df_product.to_csv("F:\RnD\ETL\etlCrawlData\cmtshopee.csv", encoding="utf-8-sig") 