import pandas as pd
import requests
import time
import random
from tqdm import tqdm
import os

cookies = {
    'TIKI_GUEST_TOKEN': 'IcG0Mkj6Z3wOW9RdyYsmqa8vQf2JUoL7',
    '_ga_GSD4ETCY1D': 'GS1.1.1681108319.14.1.1681111905.60.0.0',
    'tiki_client_id': '1457150552.1680489574',
    '_hjSession_522327': 'eyJpZCI6ImNjNGI5NmRiLThhMjMtNGVjYS04OWU4LThjYTVlY2I0OGVkZiIsImNyZWF0ZWQiOjE2ODExMDgzMjAyNTcsImluU2FtcGxlIjpmYWxzZX0=',
    '_hjSessionUser_522327': 'eyJpZCI6IjE4MDU4OWU0LTRkM2YtNWFlMi1hMWQyLTJkNzBlZDNhOGM0NiIsImNyZWF0ZWQiOjE2ODA0ODk1Nzc2NTcsImV4aXN0aW5nIjp0cnVlfQ==',
    '__uidac':'1bce3386e4b2fe4311384ad3486c1686',
    'delivery_zone': 'Vk4wMzQwMjQwMTM=',
   '_hjIncludedInSessionSample_522327': '0',
    # 'TOKENS': '{%22access_token%22:%228jWSuIDBb2NGVzr6hsUZXpkP1FRin7lY%22%2C%22expires_in%22:157680000%2C%22expires_at%22:1763654224277%2C%22guest_token%22:%228jWSuIDBb2NGVzr6hsUZXpkP1FRin7lY%22}',
    'amp_99d374': 'AERaUWyShfsKpSBHm3hgjd...1gtl0abem.1gtl17v0g.22.39.5b',
    '_gcl_au': '1.1.1696312369.1680489577',
    '_trackity': '24591e51-28eb-58ae-45d0-dac00534fe96',
    '_ga_NKX31X43RV': 'GS1.1.1605974235.1.1.1605974434.0',
    '_ga': 'GA1.1.1457150552.1680489574',
    # 'ai_client_id': '11935756853.1605974227',
    # 'an_session': 'zizkzrzjzlzizqzlzqzjzdzizizqzgzmzkzmzlzrzmzgzdzizlzjzmzqzkznzhzhzkzdzhzdzizlzjzmzqzkznzhzhzkzdzizlzjzmzqzkznzhzhzkzdzjzdzhzqzdzizd2f27zdzjzdzlzmzmznzq',
    '__iid': '749',
    '__su': '0',
    '_bs': 'bb9a32f6-ab13-ce80-92d6-57fd3fd6e4c8',
    '_gid': 'GA1.2.1582131965.1681102106',

    '_hjid': '9be65b53-7a8a-426f-9acf-8679d30112a0',
    '_hjAbsoluteSessionInProgress': '1',
    '_hjIncludedInSessionSample': '1',
    'tiki_client_id': '1457150552.1680489574',
    '__gads': 'ID=ae56424189ecccbe-227eb8e1d6c400a8:T=1605974229:RT=1605974229:S=ALNI_MZFWYf2BAjzCSiRNLC3bKI-W_7YHA',
    'TKSESSID': '333ce4dc219b85c1ff7222a21ca83387',
    'TIKI_RECOMMENDATION': '627df41845c2e6d21ed7867864174848',
    '_gat': '1',
    'cto_bundle': 'nPRm_F9WWWpTVjhHZ3FJcjBFWmpYR3RRamNpTmRNWXJiNWIzTUpDMTI1WHFZU00lMkY0cUdJQWNGMGZrbkh5aUwlMkJBZzA5c3RyYnh3RkFKRkh2M002JTJCYjNncXhsejFVUjdMalduUTJIRzQlMkJpSkRHdngyZE1pYmQySUxrRGZoZUhRQ2ZtUGU4d201eDF4clhrdnFmMFpzWFdUU3VaZyUzRCUzRA',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-encoding': 'gzip, deflate, br',
    'Accept-Language': 'vi,en;q=0.9,vi-VN;q=0.8,fr-FR;q=0.7,fr;q=0.6,en-US;q=0.5',
    'Referer': 'https://tiki.vn/camera-imou-wifi-a22ep-1080p-2m-xoay-360-do-dam-thoai-2-chieu-hang-chinh-hang-p242618146.html?itm_campaign=SRC_YPD_TKA_PLA_UNK_ALL_UNK_UNK_UNK_UNK_X.131020_Y.1298111_Z.3644894_CN.A22EP&itm_medium=CPC&itm_source=tiki-ads&spid=242618149',
    'x-guest-token': 'IcG0Mkj6Z3wOW9RdyYsmqa8vQf2JUoL7',
}

params = (
    ('platform', 'web'),
    ('spid', 242618149)
    #('include', 'tag,images,gallery,promotions,badges,stock_item,variants,product_links,discount_tag,ranks,breadcrumbs,top_features,cta_desktop'),
)

def parser_product(json):
    fetchedData = dict()
    fetchedData['id'] = json.get('id')

    fetchedData['sku'] = json.get('sku')
    fetchedData['short_description'] = json.get('short_description')
    fetchedData['price'] = json.get('price')
    fetchedData['list_price'] = json.get('list_price')
    fetchedData['discount'] = json.get('discount')
    fetchedData['discount_rate'] = json.get('discount_rate')
    fetchedData['review_count'] = json.get('review_count')
    try:
        fetchedData['quantity_sold'] = json.get('quantity_sold').get('value')
        fetchedData['cs_id'] = json.get('current_seller').get('id')
        fetchedData['cs_name'] = json.get('current_seller').get('name')
        fetchedData['stock_item'] = json.get('stock_item').get('qty')
    except AttributeError:

        print("no sold items here")
        fetchedData['stock_item'] = 0
        fetchedData['cs_id'] = 0
        fetchedData['cs_name'] = ""
        fetchedData['quantity_sold']=0 
    fetchedData['inventory_status'] = json.get('inventory_status')
    
    fetchedData['product_name'] = json.get('name')
    fetchedData['brand_id'] = json.get('brand').get('id')
    fetchedData['brand_name'] = json.get('brand').get('name')
    fetchedData['location'] = "Trong nước"
    return fetchedData


df_id = pd.read_csv('F:\RnD\ETL\ctiki\productId_dtmtb.csv')
id_list = df_id.id.to_list()
# print(id_list)
result = []
for pid in tqdm(id_list, total=len(id_list)):
        try:
            response = requests.get('https://tiki.vn/api/v2/products/{}'.format(pid), headers=headers, params=params, cookies=cookies)
            if response.status_code == 200:
                print('Crawl data {} success !!!'.format(pid))
                # print(response.json())
                result.append(parser_product(response.json()))
        except ValueError:
            print(ValueError)
            continue
        # time.sleep(random.randrange(3, 5))


df_product = pd.DataFrame(result)
print(df_product)
if os.path.exists("F:\RnD\ETL\ctiki\productData_dtmtb.csv"):
    os.remove("F:\RnD\ETL\ctiki\productData_dtmtb.csv")
df_product.to_csv("F:\RnD\ETL\ctiki\productData_dtmtb.csv", encoding="utf-8-sig")
