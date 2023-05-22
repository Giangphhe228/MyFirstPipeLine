from ast import Delete
import numpy as np
import ast
import os
import glob                  # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime
import psycopg2
# required code
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 
from psycopg2.extras import execute_values
from datetime import datetime
from psycopg2.errorcodes import UNIQUE_VIOLATION
from psycopg2 import errors

####################### url for windows
# LazadaItemsUrl    = "F:\RnD\ETL\lazadaexportdata\item"            # file used to store all extracted data
# TikiItemsUrl  = 'F:\RnD\ETL\ctiki\productData_dtmtb.csv'
# ShopeeItemsUrl   = "F:\RnD\ETL\shopee\productId_shopee.csv"            # all event logs will be stored in this file  # file where transformed data is stored
# TikiCmtUrl = "F:\RnD\ETL\ctiki\comments_data_ncds.csv"
# ShopeeCmtUrl = "F:\RnD\ETL\shopee\comments_data_shopee.csv"
# ShopeeShopUrl = "F:\RnD\ETL\shopee\shop_data_shopee.csv"

# # extracted_shopee_items_url    = "F:\RnD\ETL\etlCrawlData\datashopee.csv"            # file used to store all extracted data
# # extracted_tiki_items_url  = "F:\RnD\ETL\etlCrawlData\datatiki.csv"
# # extracted_lazada_items_url  = "F:\RnD\ETL\etlCrawlData\datalazada.csv"
# # extracted_tiki_cmt_url   = "F:\RnD\ETL\etlCrawlData\cmttiki.csv"            # all event logs will be stored in this file  # file where transformed data is stored
# # extracted_shopee_cmt_url = "F:\RnD\ETL\etlCrawlData\cmtshopee.csv" 

####################### url for ubuntu airflow
LazadaItemsUrl    = "/mnt/f/RnD/ETL/lazadaexportdata/item"            # file used to store all extracted data
TikiItemsUrl  = '/mnt/f/RnD/ETL/ctiki/productData_dtmtb.csv'
ShopeeItemsUrl   = "/mnt/f/RnD/ETL/shopee/productId_shopee.csv"            # all event logs will be stored in this file  # file where transformed data is stored
TikiCmtUrl = "/mnt/f/RnD/ETL/ctiki/comments_data_ncds.csv"
ShopeeCmtUrl = "/mnt/f/RnD/ETL/shopee/comments_data_shopee.csv"
ShopeeShopUrl = "/mnt/f/RnD/ETL/shopee/shop_data_shopee.csv"

extracted_shopee_items_url    = "/mnt/f/RnD/ETL/etlCrawlData/datashopee.csv"            # file used to store all extracted data
extracted_tiki_items_url  = "/mnt/f/RnD/ETL/etlCrawlData/datatiki.csv"
extracted_lazada_items_url  = "/mnt/f/RnD/ETL/etlCrawlData/datalazada.csv"
extracted_tiki_cmt_url   = "/mnt/f/RnD/ETL/etlCrawlData/cmttiki.csv"            # all event logs will be stored in this file  # file where transformed data is stored
extracted_shopee_cmt_url = "/mnt/f/RnD/ETL/etlCrawlData/cmtshopee.csv"

logfile_url= "/mnt/f/RnD/ETL/etlCrawlData/logfile.txt"

def rearrange_items_dataframe(dataframe=pd.DataFrame()):
     col = dataframe.pop("product_id")
     dataframe.insert(0, "product_id", col)
     col = dataframe.pop("title")
     dataframe.insert(1, "title", col)
     col = dataframe.pop("price")
     dataframe.insert(2, "price", col)
     col = dataframe.pop("discount")
     dataframe.insert(3, "discount", col)
     col = dataframe.pop("stars_review")
     dataframe.insert(4, "stars_review", col)
     col = dataframe.pop("sold_product")
     dataframe.insert(5, "sold_product", col)
     col = dataframe.pop("shop_id")
     dataframe.insert(6, "shop_id", col)
     col = dataframe.pop("brand_id")
     dataframe.insert(7, "brand_id", col)
     return dataframe

def rearrange_cmt_dataframe(dataframe=pd.DataFrame()):
     col = dataframe.pop("product_id")
     dataframe.insert(0, "product_id", col)
     col = dataframe.pop("cmt_id")
     dataframe.insert(1, "cmt_id", col)
     col = dataframe.pop("user_id")
     dataframe.insert(2, "user_id", col)
     col = dataframe.pop("content")
     dataframe.insert(3, "content", col)
     col = dataframe.pop("created_time")
     dataframe.insert(4, "created_time", col)
     col = dataframe.pop("rating")
     dataframe.insert(5, "rating", col)
     col = dataframe.pop("like_count")
     dataframe.insert(6, "like_count", col)
     col = dataframe.pop("user_name")
     dataframe.insert(7, "user_name", col)
     return dataframe

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_lazada_items(url):
    pageNumber=6
    df_result= pd.DataFrame()
    df_result['product_id']=""
    print("Extract lazada items...")
    for i in range(0,pageNumber,1):
        # print("Crawl page number {}...".format(i))
        # df_result.append(extract_from_csv(LazadaItemsUrl+"itemPage{}.csv".format(i)))
        df_data = extract_from_csv(url+"/itemPage{}.csv".format(i))
        # df_data = df_data.reset_index(drop=True) 
        list_product_id=[]
        # print("đây là list product_id: \n",df_result['link_item'])
        # generate id from link of items
        for link in df_data['link_item']:
            link=str(link)
            position= link.find(".html")
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
    df_interid = df_result[['shop_name', 'location']]
    for index, row in df_interid.iterrows():
        #  print("id day nay: ",row['shop_name'])
         value=row['shop_name']+row['location']
         shop_id.append(int.from_bytes(value.encode('utf-8-sig'), 'little'))
    df_result['shop_id'] = shop_id
    df_result['shop_id'] = df_result['shop_id'].astype(str)
    # generate encoded brand id from name
    brand_id=[]
    for name in df_result['brand_name']:
         brand_id.append(int.from_bytes(name.encode('utf-8-sig'), 'little'))
    df_result['brand_id'] = brand_id
    df_result['brand_id'] = df_result['brand_id'].astype(str)
    # cleaning and standardization column's data
    df_result['discount_percent'] = df_result['discount_percent'].str.replace("%"+"off","", regex=False)          
    df_result.rename(columns = {'discount_percent':'discount'}, inplace = True)
    df_result = df_result.drop(columns=["Unnamed: 0", "link_item", "index_id", "discount_idx", "starsReview_idx", "soldProduct_idx"])
    df_result['stars_review'] = df_result['stars_review'].str.replace('-',"", regex=False)
    df_result['stars_review'] = df_result['stars_review'].str.replace('(',"", regex=False)
    df_result['stars_review'] = df_result['stars_review'].str.replace(')',"", regex=False)
    df_result['price'] = df_result['price'].str.replace(" ₫","", regex=False)
    df_result['price'] = df_result['price'].str.replace(".","", regex=False)
        # df_result['price'] = df_result['price'].astype(int)
    # df_result['discount_percent'] = df_result['discount_percent'].replace("%"+"off"," ", regex=False)
    df_result['sold_product'] = df_result['sold_product'].str.replace(" Đã bán","", regex=False)
    df_result['sold_product'] = df_result['sold_product'].str.replace(",", "", regex=False)
    df_result['sold_product'] = df_result['sold_product'].str.replace("+", "", regex=False)
    if os.path.exists("F:\RnD\ETL\etlCrawlData\datalazada.csv"):
        os.remove("F:\RnD\ETL\etlCrawlData\datalazada.csv")
    df_result.to_csv("F:\RnD\ETL\etlCrawlData\datalazada.csv", encoding="utf-8-sig")

    # return df_result

# df_product= extract_lazada_items(LazadaItemsUrl)
# print("kieu du lieu la: ",type(df_product['shop_id']))
# if os.path.exists("F:\RnD\ETL\etlCrawlData\datalazada.csv"):
#     os.remove("F:\RnD\ETL\etlCrawlData\datalazada.csv")
# df_product.to_csv("F:\RnD\ETL\etlCrawlData\datalazada.csv", encoding="utf-8-sig")

def extract_shopee_items(url,shopurl):
    print("Extract shopee items...")
    df_data = extract_from_csv(url)
    df_shop_data=extract_from_csv(shopurl)
    df_result = df_data
    # df_result['price'] = df_data['price']/int(10^5)
    df_result['final_price'] = df_data['final_price']/int(100000)
    df_result['rating_count'] = df_data['rating_count'].str.replace("[", "", regex=False)
    df_result['rating_count'] = df_result['rating_count'].str.replace("]", "", regex=False)
    list_rating= df_result['rating_count']
    lr = list_rating.str.split(', ')
    # ShopeeShopUrl
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
    name_list=[]
    for id in df_result["shop_id"]:
        for index, row in df_shop_data.iterrows():
            if(id==row['shop_id']):
                name_list.append(row["shop_name"])
                break
    # print("day la id va ten: \n",name_list)
    df_result["shop_name"]= name_list       
    if os.path.exists("F:\RnD\ETL\etlCrawlData\datashopee.csv"):
        os.remove("F:\RnD\ETL\etlCrawlData\datashopee.csv")
    df_result.to_csv("F:\RnD\ETL\etlCrawlData\datashopee.csv", encoding="utf-8-sig")  
    # print("đây là kết quả: \n",df_result)
    # df_result["brand_id"]=df_result["brand_id"].strip()
    # df_result["brand_name"]=df_result["brand_name"].str.strip()
    # df_result["shop_id"]=df_result["shop_id"].str.strip()
    # df_result["shop_name"]=df_result["shop_name"].str.strip()

    # return df_result

# df_product= extract_shopee_items(ShopeeItemsUrl)
# print("gia tri brand: \n",df_product['brand_name']) 
# if os.path.exists("F:\RnD\ETL\etlCrawlData\datashopee.csv"):
#     os.remove("F:\RnD\ETL\etlCrawlData\datashopee.csv")
# df_product.to_csv("F:\RnD\ETL\etlCrawlData\datashopee.csv", encoding="utf-8-sig")  


def extract_ctiki_items(url):
     print("Extract tiki items...")
     df_result = extract_from_csv(url)
     df_result = df_result.drop(columns=["stock_item","inventory_status","sku","Unnamed: 0","short_description","list_price","discount"])
     df_result.rename(columns = {'id':'product_id','brand':'brand_name', 'discount_rate':'discount', 'review_count':'stars_review'
                                 ,'quantity_sold':'sold_product', 'cs_id':'shop_id', 'cs_name':'shop_name', 'product_name':'title'}, inplace = True)

    #  df_result["brand_id"]=df_result["brand_id"].str.strip()
     df_result["brand_name"]=df_result["brand_name"].str.strip()
    #  df_result["shop_id"]=df_result["shop_id"].strip()
     df_result["shop_name"]=df_result["shop_name"].str.strip()
     if os.path.exists("F:\RnD\ETL\etlCrawlData\datatiki.csv"):
        os.remove("F:\RnD\ETL\etlCrawlData\datatiki.csv")
     df_result.to_csv("F:\RnD\ETL\etlCrawlData\datatiki.csv", encoding="utf-8-sig") 

    #  return df_result
# df_product= extract_ctiki_items(TikiItemsUrl)
# # print("gia tri df: \n",df_product) 
# if os.path.exists("F:\RnD\ETL\etlCrawlData\datatiki.csv"):
#     os.remove("F:\RnD\ETL\etlCrawlData\datatiki.csv")
# df_product.to_csv("F:\RnD\ETL\etlCrawlData\datatiki.csv", encoding="utf-8-sig")  

def extract_ctiki_comment(url):
     print("Extract tiki comment...")
     df_cmt = extract_from_csv(url)
     df_cmt = df_cmt.drop(columns=["title","purchased_at", "Unnamed: 0"])
     df_cmt.rename(columns = {'id':'cmt_id', 'thank_count':'like_count', 'customer_id':'user_id'
                                 ,'created_at':'created_time','customer_name':'user_name'}, inplace = True)
    #  user_id=[]
    #  df_interid = df_cmt[['user_id', 'user_name']]
    #  for index, row in df_interid.iterrows():
    #     #  print("id day nay: ",row['shop_name'])
    #      value=row['user_id']+row['user_name']
    #      user_id.append(int.from_bytes(value.encode('utf-8-sig'), 'little'))
    #  df_result['shop_id'] = shop_id
     if os.path.exists("F:\RnD\ETL\etlCrawlData\cmttiki.csv"):
        os.remove("F:\RnD\ETL\etlCrawlData\cmttiki.csv")
     df_cmt.to_csv("F:\RnD\ETL\etlCrawlData\cmttiki.csv", encoding="utf-8-sig")  
    #  return df_cmt

# df_product= extract_ctiki_comment(TikiCmtUrl)
# # print("gia tri df: \n",df_product) 
# if os.path.exists("F:\RnD\ETL\etlCrawlData\cmttiki.csv"):
#     os.remove("F:\RnD\ETL\etlCrawlData\cmttiki.csv")
# df_product.to_csv("F:\RnD\ETL\etlCrawlData\cmttiki.csv", encoding="utf-8-sig")  

def extract_shopee_comment(url):
     print("Extract shopee comment...")
     df_cmt = extract_from_csv(url)
     df_cmt = df_cmt.drop(columns=["shop_id", "Unnamed: 0", "seller_service"])
     df_cmt.rename(columns = {'id':'cmt_id', 'cmt_content':'content'
                                 ,'time':'created_time','product_quality':'rating','author_username':'user_name'}, inplace = True)
    #  df_cmt.dropna()
     df_cmt['like_count'] = df_cmt['like_count'].replace(np.nan ,0)
    #  df_cmt['user_name'] = df_cmt['user_name'].dropna()
     df_cmt = df_cmt[df_cmt['user_name'].notna()]
     if os.path.exists("F:\RnD\ETL\etlCrawlData\cmtshopee.csv"):
        os.remove("F:\RnD\ETL\etlCrawlData\cmtshopee.csv")
     df_cmt.to_csv("F:\RnD\ETL\etlCrawlData\cmtshopee.csv", encoding="utf-8-sig") 
    #  return df_cmt

# df_product= extract_shopee_comment(ShopeeCmtUrl)
# # print("gia tri df: \n",df_product['user_name']) 
# if os.path.exists("F:\RnD\ETL\etlCrawlData\cmtshopee.csv"):
#     os.remove("F:\RnD\ETL\etlCrawlData\cmtshopee.csv")
# df_product.to_csv("F:\RnD\ETL\etlCrawlData\cmtshopee.csv", encoding="utf-8-sig") 

def load_items_data(shopee_url, tiki_url, lazada_url):

    shopee_dataframe = extract_from_csv(shopee_url)
    tiki_dataframe = extract_from_csv(tiki_url)
    lazada_dataframe = extract_from_csv(lazada_url)
    print("Load items data into database...")
    conn = None
    conn = psycopg2.connect(user = "postgres",
                            password = "123456",
                            host = "127.0.0.1",
                            # host = "localhost",
                            port = "5432",
                            database = "cameraproduct"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
    conn.autocommit = True
    cursor=None
    cursor = conn.cursor()
    # create shop table
                                        # location char(256) ,\
    sql_shop = '''ALTER TABLE IF EXISTS Product
                    DROP CONSTRAINT IF EXISTS FK_shop;\
                  DROP TABLE IF EXISTS Shop;\
                  CREATE TABLE Shop(shop_id char(256) NOT NULL primary key,\
                                    location char(256) ,\
                                    shop_name char(256));'''
    cursor.execute(sql_shop)

    # create brand table
    sql_brand = '''ALTER TABLE IF EXISTS Product
                    DROP CONSTRAINT IF EXISTS FK_brand;\
                   DROP TABLE IF EXISTS Brand;\
                   CREATE TABLE Brand(brand_id char(256) NOT NULL primary key,\
                                       brand_name char(256));'''
    cursor.execute(sql_brand)

    # create product table
    sql = '''ALTER TABLE IF EXISTS Comment
                    DROP CONSTRAINT IF EXISTS FK_product;\
             DROP TABLE IF EXISTS Product;\
             CREATE TABLE Product(product_id char(256) NOT NULL primary key,\
             title char(1000),\
             price float,\
             discount float,\
             stars_review integer,\
             sold_product integer,\
             location char(256),\
             shop_id char(256) NOT NULL,\
             brand_id char(256) NOT NULL,\
             CONSTRAINT FK_shop FOREIGN KEY (shop_id) REFERENCES Shop(shop_id),\
             CONSTRAINT FK_brand FOREIGN KEY (brand_id) REFERENCES Brand(brand_id));'''
    cursor.execute(sql)
    
    # concat all the items data from 3 platform crawler and then store in db
    # print("day la shopee : \n",shopee_dataframe)
    # print("day la tiki : \n",tiki_dataframe)
    # print("day la lazada : \n",lazada_dataframe)
    # sắp xếp lại thứ tự các cột để concat vs nhau
    shopee_dataframe = rearrange_items_dataframe(shopee_dataframe)
    tiki_dataframe = rearrange_items_dataframe(tiki_dataframe)
    lazada_dataframe = rearrange_items_dataframe(lazada_dataframe)

    df_product_patch = pd.concat([shopee_dataframe, tiki_dataframe], axis=0)
    df_product_full = pd.concat([df_product_patch, lazada_dataframe], axis=0)
    # print("size df: ",df_product_full.size)
    df_product_full['shop_id'].str.strip()
    df_product_full['shop_name'].str.strip()
    df_product_full['brand_id'].str.strip()
    df_product_full['brand_id'].str.replace("'","")
    df_product_full['brand_name'].str.strip()
    # print("gia tri brand_id: ",df_product_full['brand_id'])
    # get all shop and brand info to store
    df_sb= df_product_full.drop(columns=["product_id", "title", "price", "discount", "stars_review", "sold_product"])
    
    
    # lấy danh sách các shop để lưu 
    df_sbshop=df_sb.drop(columns=["brand_id","brand_name"])
    df_sbshop = df_sbshop.astype('str')
    df_sbshop.drop_duplicates(subset=['shop_id', 'shop_name'], keep="first", inplace=True)
    for index,row in df_sbshop.iterrows():
        # print("gia tri cua mot row la: \n",row)
        val = (row['shop_id'],row['shop_name'],row['location'])
        # # val = (row[0][0],row[0][1],row[0][2])
        cursor.execute("INSERT INTO Shop (shop_id, shop_name, location) VALUES(%s,%s,%s)"
                       ,val)
    
    
    # lấy danh sách các brand để lưu
    df_sbrand= df_sb.drop(columns=["shop_id", "shop_name", "location"])
    df_sbrand = df_sbrand.astype('str')
    df_sbrand.drop_duplicates(subset=['brand_id'], keep="first", inplace=True)
    for index,row in df_sbrand.iterrows():
        val = (row['brand_id'],row['brand_name'])
        # val = (row[0][0],row[0][1])
        cursor.execute("INSERT INTO Brand (brand_id, brand_name) VALUES(%s,%s)"
                       ,val)

    # insert product
    df_product= df_product_full.drop(columns=["shop_name", "brand_name"])
    df_product = df_product.astype('str')
    df_product.drop_duplicates(subset=['product_id'], keep="first", inplace=True)
    # print("day la df : \n",df_product)
    for index, row in df_product.iterrows():
        # print("gia tri row: ",type(row.location))
        val = (row.product_id,row.title,row.price,row.discount,row.stars_review,row.sold_product, row.location,row.shop_id,row.brand_id)
        cursor.execute("INSERT INTO Product (product_id, title, price, discount, stars_review, sold_product, location, shop_id, brand_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                       ,val)




def load_cmt_data(tiki_url, shopee_url):

    shopee_cmt = extract_from_csv(shopee_url)
    tiki_cmt = extract_from_csv(tiki_url)
    
    print("Load comments data into database...")
    conn = None
    conn = psycopg2.connect(user = "postgres",
                            password = "123456",
                            host = "127.0.0.1",
                            port = "5432",
                            database = "cameraproduct"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
    conn.autocommit = True
    cursor=None
    cursor = conn.cursor()
    cursor.execute("SET DateStyle='DMY'")
    # create user table
    # DROP TABLE IF EXISTS Comment;\
    #               CREATE TABLE Comment(cmt_id char(256) NOT NULL primary key,\
    #               product_id char(256) NOT NULL,\
    #               content char(1000),\
    #               created_time timestamp,\
    #               rating integer,\
    #               like_count integer,\
    #               user_name char(256),\
    #               user_id char(256) NOT NULL);\
    sql_user = '''ALTER TABLE IF EXISTS Comment
                    DROP CONSTRAINT IF EXISTS FK_user;\
                  DROP TABLE IF EXISTS User_detail;\
                  CREATE TABLE User_detail(user_id char(256) NOT NULL primary key,\
                                    user_name char(256));'''
    cursor.execute(sql_user)

    # create cmt table
    sql = '''DROP TABLE IF EXISTS Comment;\
    CREATE TABLE Comment(cmt_id char(256) NOT NULL primary key,\
    product_id char(256) NOT NULL,\
    content text,\
    created_time timestamp,\
    rating integer,\
    like_count integer,\
    user_name char(256),\
    user_id char(256) NOT NULL,\
    CONSTRAINT FK_user FOREIGN KEY (user_id) REFERENCES User_detail(user_id),\
    CONSTRAINT FK_product FOREIGN KEY (product_id) REFERENCES Product(product_id));'''
    cursor.execute(sql)
    
    # concat all the items data from 3 platform crawler and then store in db
    # print("day la shopee : \n",shopee_dataframe)
    # print("day la tiki : \n",tiki_dataframe)
    # print("day la lazada : \n",lazada_dataframe)
    # sắp xếp lại thứ tự các cột để concat vs nhau

    shopee_dataframe = rearrange_cmt_dataframe(shopee_cmt)
    tiki_dataframe = rearrange_cmt_dataframe(tiki_cmt)

    df_cmt_full = pd.concat([tiki_dataframe, shopee_dataframe], axis=0)

    df_sc= df_cmt_full.drop(columns=["cmt_id", "product_id", "content", "created_time", "rating", "like_count"])

    # lấy danh sách các user để lưu 
    df_sc = df_sc.astype('str')
    df_sc.drop_duplicates(subset=['user_id'], keep="first", inplace=True)
    for index,row in df_sc.iterrows():
        # print("gia tri cua mot row la: \n",row)
        val = (row['user_id'],row['user_name'])
        # # val = (row[0][0],row[0][1],row[0][2])
        cursor.execute("INSERT INTO User_detail (user_id, user_name) VALUES(%s,%s)"
                        ,val)
        

    # insert comment
    df_comment = df_cmt_full.astype('str')
    df_comment.drop_duplicates(subset=['cmt_id'], keep="first", inplace=True)
    # print("day la df : \n",df_comment)
    for index, row in df_comment.iterrows():
        # print("gia tri row: ",row)
        try:
            val = (row['cmt_id'],row['product_id'],row['content'],datetime.fromtimestamp(int(row['created_time'])),row['rating'],int(float(row['like_count'])), row['user_name'],row['user_id'])
            cursor.execute("INSERT INTO Comment (cmt_id, product_id, content, created_time, rating, like_count, user_name, user_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                        ,val)
        except(psycopg2.Error):
            continue

def log(logfile_url,message,duration=0):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    if os.path.exists(logfile_url):
        os.remove(logfile_url)
    with open(logfile_url,"a") as f:
        if(duration!=0):
            f.write(message + duration+'\n')
            f.write("*" * 50+ '\n')
        else:
            f.write(timestamp + ',' + message + '\n')  
# LazadaItemsUrl,ShopeeItemsUrl,ShopeeShopUrl,TikiItemsUrl,ShopeeCmtUrl,TikiCmtUrl
def deployTheTransformation():
    extract_lazada_items(LazadaItemsUrl)
    extract_shopee_items(ShopeeItemsUrl, ShopeeShopUrl)
    extract_ctiki_items(TikiItemsUrl)
    extract_shopee_comment(ShopeeCmtUrl)
    extract_ctiki_comment(TikiCmtUrl)
# extracted_shopee_items_url,extracted_tiki_items_url,extracted_lazada_items_url,extracted_tiki_cmt_url,extracted_shopee_cmt_url
def deployTheLoading():
    load_items_data(extracted_shopee_items_url, extracted_tiki_items_url, extracted_lazada_items_url)
    load_cmt_data(extracted_tiki_cmt_url,extracted_shopee_cmt_url)
    

# def demo():
#     print("demo day nay: \n")



# deployTheTransformation(LazadaItemsUrl,ShopeeItemsUrl,ShopeeShopUrl,TikiItemsUrl,ShopeeCmtUrl,TikiCmtUrl)
# deployTheLoading(extracted_shopee_items_url,extracted_tiki_items_url,extracted_lazada_items_url,extracted_tiki_cmt_url,extracted_shopee_cmt_url)