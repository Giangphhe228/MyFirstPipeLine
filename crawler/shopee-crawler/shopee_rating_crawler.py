import requests
import pandas as pd
import time
import random
import urllib
from tqdm import tqdm
import json
from seleniumwire import webdriver
from seleniumwire.utils import decode 
import os
# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context
cookies = {
    'csrftoken': 'HT6AzjD3eUrHqECNSJuYJanmm4Rt3zmq',
    '_QPWSDCXHZQA': '1bd2211c-3c2f-45d9-e9df-6c03662c5e24',
    'AMP_TOKEN': '$NOT_FOUND',
    '_hjSession_868286': 'eyJpZCI6ImI1OTQ5Y2Y5LTM5MWItNGQ4ZS1iMjZmLWI3MTI5MzcwMGU1YyIsImNyZWF0ZWQiOjE2ODEyNjU5Njc5NTYsImluU2FtcGxlIjpmYWxzZX0=',
    '_hjAbsoluteSessionInProgress': '0',
    '_ga': 'GA1.1.2139499135.1680229886',
    'shopee_webUnique_ccd': 't4Fa1MPFfgq2ohOQPvmoFw==|meYdrRvvGei3q7awg7/K+g2vDVskhYng1wIk+NcyLvRDl1cq2uLxPq92M4GQRmj4u919G4uv90l5hstYBwdfUtOtIoJTyrEV6g2H|l/jfO27zKhicV4Ov|06|3',
    'ds': 'e7b4b6642504f8ecf967d258b26e8c93',
    '_hjIncludedInSessionSample_868286': '0',
    '_ga_M32T05RVZT': 'GS1.1.1681265827.22.1.1681268688.60.0.0',
    'SPC_IA':'-1',
    'SPC_F': 'BUfPSSye5i4WZgKk91j15PXC356QKODk',
    'REC_T_ID': '209d47b4-db10-11ea-8290-3c15fbdfdfb8',
    'G_ENABLED_IDPS': 'google',
    'SPC_CLIENTID': 'BUfPSSye5i4WZgKkhwahkvpovmrmvgll',
    'SPC_T_IV': 'IPKROwVE+qPa7BBMqQ3Rhw==',
    'SPC_T_ID': 'U/F1/+5BRu/r40FAma7moQTodixxmbVqrprBqHIJmRyRFTknaEZztuPv2YtpXMilFtwmwUAVhe0e5OOTSIHjwEPIKYjTNR/lMGxgEZSx4Vg=',
    '_gcl_dc': 'GCL.1680229883.CjkKEQjw5pShBhDOoZyb3tqu5YcBEiQAkmq_4oqn54CBhpPLX5vvaGc4L3DDrsNoRtF4f_Apamacnenw_wcB',
    '_gcl_au': '1.1.758958907.1680229883',
    '_hjid': '0e4a0030-e478-43b5-97c0-9cdcff381b0d',
    '_hjSessionUser_868286': 'eyJpZCI6ImNkYTljMWI1LTBlMWUtNTRlMC1iN2FmLTQxMTE4MzlhZDVkNSIsImNyZWF0ZWQiOjE2ODA0ODY2NDUzOTgsImV4aXN0aW5nIjp0cnVlfQ==',
    'SPC_SI': 'Bn4aZAAAAABpUHdOanAyaoY/ZwIAAAAATEZ2T216OE0=',
    '_med': 'cpc',
    '_gcl_aw': 'GCL.1681121322.CjwKCAjw586hBhBrEiwAQYEnHRGlnPEj026FK-I-BmKKvohiU-UBSXpq2Us-z8Lo0OdRXThUrkeZuBoCJlMQAvD_BwE',
    'SPC_U': '171207266',
    'SPC_T_ID': '6L/tKE/2K1BCuZucbahhjAxzx3lXxw9Z6pHQs1PBlH59pQ0/1QMBPINL2Eb2yO2pXggTDQ5JT5cEqAVMSy3rXySedRVfOhHTZrgIXFfifV1uUYJj7z2EG3EHGdYFL+x1GOWhqdVrcum6kKN21fZBpJLttJka5J+4zZ/G5wVdqow=',
    'SPC_T_IV': 'RktvejZheVBmRlRvV2xsbg==',
    'SPC_R_T_ID': '6L/tKE/2K1BCuZucbahhjAxzx3lXxw9Z6pHQs1PBlH59pQ0/1QMBPINL2Eb2yO2pXggTDQ5JT5cEqAVMSy3rXySedRVfOhHTZrgIXFfifV1uUYJj7z2EG3EHGdYFL+x1GOWhqdVrcum6kKN21fZBpJLttJka5J+4zZ/G5wVdqow=',
    'SPC_R_T_IV': 'RktvejZheVBmRlRvV2xsbg==',
    '_gac_UA-61914164-6': '1.1681121333.CjwKCAjw586hBhBrEiwAQYEnHRGlnPEj026FK-I-BmKKvohiU-UBSXpq2Us-z8Lo0OdRXThUrkeZuBoCJlMQAvD_BwE',
    'SPC_SC_TK': '21abc0af556e11c932717f00116ef7da',
    'SPC_SC_UD': '171207266',
    'SPC_STK': 'bTaWgn5itA9w31GXtvc2qmRSgHSP18EUiAvpbDw8IR+8rS8Rgu/ln2RIPe0UvlHXBuiPGPX1a0xKmODX/7S0jirfwgfa6mrbOHNuwFFhN7dBWhrOMFqKjPhIx8HFkMlDFtrYB6DSjtxvN5tzNlE91aA9bsmORm3w1B8ZJNAoL1iVet4pXS6+LcwC7/HGY5nT',
    'SC_DFP': 'mwAOoWabbmEIvGuRfHrQVMLckZpeRvJT',
    '_ga_3XVGTY3603': 'GS1.1.1681122425.1.1.1681122452.33.0.0',
    '__LOCALE__null': 'VN',

}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-encoding': 'gzip, deflate, br',
    'Accept-Language': 'vi,en;q=0.9,vi-VN;q=0.8,fr-FR;q=0.7,fr;q=0.6,en-US;q=0.5',

}

params = {
    'filter': '0',
    'flag': '1',
    'itemid': '12197987133',
    'limit': '6',
    'offset': '0',
    'type': '0',
    'shopid': '297411514'
}

def comment_parser(list):
    d = dict()
    d['product_id'] = list['itemid']
    d['shop_id'] = list['shopid']
    d['cmt_id'] = list['cmtid']
    d['user_id'] = list['userid']
    d['cmt_content'] = list['comment']
    d['time'] = list['ctime']
    d['product_quality'] = list['detailed_rating']['product_quality']
    d['seller_service']  = list['detailed_rating']['seller_service'] 
    d['like_count']  = list['like_count']
    d['author_username']  = list['author_username']
    # d['product_quality'] = list['detailed_rating']['product_quality']
    # d['seller_service']  = list['detailed_rating']['seller_service']
    return d


df_id = pd.read_csv('F:\RnD\ETL\shopee\productId_shopee.csv')
p_ids = df_id.product_id.to_list()
result = []
url='https://shopee.vn/api/v2/item/get_ratings'
for pid in tqdm(p_ids, total=len(p_ids)):
    params['itemid'] = pid
    print('Crawl comment for product {}'.format(pid))
    pagecount=0
    try:
        params['offset']=0
        checkrating = requests.get(url, headers=headers, params=params, cookies=cookies).json().get('data').get('ratings')
        
        for i in range(0, 5*6, 6):
                params['offset'] = i
                pagecount=pagecount+1
                # print('page {} response...'.format(i))
                # print(requests.get(url, headers=headers, params=params).decode('utf8').encode('latin1').decode('utf8'))
                # resolved_url = urllib.request.urlopen(url).geturl()
                # print(resolved_url)
                response = requests.get(url, headers=headers, params=params, cookies=cookies)
                # item_raw=response.json()
                # product_data=pd.json_normalize(item_raw['data'])
                # print(product_data)
                if response.status_code == 200:                  
                    obj=response.json().get('data').get('ratings')
                    print('Crawl comment page {} success!!!'.format(pagecount))
                    for comment in obj:
                        # print("page response value:",comment_parser(comment))
                        result.append(comment_parser(comment))
    except TypeError:
        print("no rating in this items!")
df_comment = pd.DataFrame(result)
print(df_comment)
if os.path.exists("F:\RnD\ETL\shopee\comments_data_shopee.csv"):
    os.remove("F:\RnD\ETL\shopee\comments_data_shopee.csv")
df_comment.to_csv("F:\RnD\ETL\shopee\comments_data_shopee.csv", encoding="utf-8-sig")
