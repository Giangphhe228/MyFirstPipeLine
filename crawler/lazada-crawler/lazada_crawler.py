import os
import numpy as np
from time import time,ctime
from selenium import webdriver
from time import sleep
import random
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import threading
from queue import Queue
# Declare browser


def startOption(driver,url):
    try:
        # driver.maximize_window()
        driver.get(url)
    except WebDriverException:
        print("ko cho nhỏ đc :)")
    return driver

def openMultiBrowsers(n):
    drivers = []
    for i in range(n):
        driver = webdriver.Chrome("chromedriver.exe")
        drivers.append(driver)
    return drivers

def loadMultiPages(driver, link):
    # for driver in drivers:
    # driver.maximize_window()
    driver.get(link)
    sleep(3)

def loadMultiBrowsers(drivers_rx,links):
    for driver in drivers_rx:
        for link in links:
            t = threading.Thread(target=loadMultiPages, args = (driver, link))
            t.start()
            links.remove(link)
            break
            # if os.path.exists('F:\RnD\ETL\exportdata\detail\detailItem{}.csv'.format(i)):
            #     os.remove('F:\RnD\ETL\exportdata\detail\detailItem{}.csv'.format(i))    
            # df.to_csv('F:\RnD\ETL\exportdata\detail\detailItem{}.csv'.format(i), encoding="utf-8-sig")


def runInParallel(func, drivers_rx, df1, df2):
    for driver in drivers_rx:  
        que = Queue()
        print("-------Running parallel---------")
        t1 = threading.Thread(target=lambda q, arg1: q.put(func(driver, df1, df2)), args=(que, driver))
        t1.start()
    try:    
        ouput = que.get()
    except:
        ouput = [] 

    return ouput

# ================================ GET link
def getLink(driver):
    elems = driver.find_elements(By.CSS_SELECTOR, ".RfADt [href]")  ###### css_link
    links = [elem.get_attribute('href') for elem in elems]
    title = [elem.text for elem in elems]
    return links,title


# ================================ GET price
def getPrice(driver):
    elems_price = driver.find_elements(By.CSS_SELECTOR , ".aBrP0") ###### css_price
    len(elems_price)
    price = [elem_price.text for elem_price in elems_price]
    return price

# ================================GET discount

# elems_discount = driver.find_elements(By.CSS_SELECTOR , ".WNoq3")
# discount_all = [elem.text for elem in elems_discount]

def getDiscount(driver, df1, len):
    # elems_discount = driver.find_elements(By.CSS_SELECTOR , ".WNoq3 ._1m41m") ###### css_discount
    # discount = [elem.text for elem in elems_discount]

    # elems_discountPercent = driver.find_elements(By.CSS_SELECTOR , ".WNoq3 .IcOsH") ###### css_discountpercent
    # discountPercent = [elem.text for elem in elems_discountPercent]
    
    discount_idx, discount_percent= [], []
    starsReview_idx, stars_review = [], []
    soldProduct_idx, sold_product = [], []
    for i in range(1, len+1):
        try:
            elems_discount_percent = driver.find_element("xpath", "/html/body/div[3]/div/div[2]/div[1]/div/div[1]/div[2]/div[{}]/div/div/div[2]/div[4]/span".format(i)) ###### xpath_discountpercent
            elems_5starsreview = driver.find_element("xpath", "/html/body/div[3]/div/div[2]/div[1]/div/div[1]/div[2]/div[{}]/div/div/div[2]/div[5]/div/span".format(i))
            elems_soldproduct = driver.find_element("xpath", "/html/body/div[3]/div/div[2]/div[1]/div/div[1]/div[2]/div[{}]/div/div/div[2]/div[5]/span[1]".format(i))
            discount_percent.append(elems_discount_percent.text)
            stars_review.append(elems_5starsreview.text)
            sold_product.append(elems_soldproduct.text)
            soldProduct_idx.append(i)
            starsReview_idx.append(i)
            discount_idx.append(i)
        except NoSuchElementException:
            print("No discount||stars Review||sold product in item " + str(i))
    df2sp= pd.DataFrame(list(zip(soldProduct_idx , sold_product)), columns = ['soldProduct_idx','sold_product'])
    df2s= pd.DataFrame(list(zip(starsReview_idx , stars_review)), columns = ['starsReview_idx','stars_review'])
    df2 = pd.DataFrame(list(zip(discount_idx , discount_percent)), columns = ['discount_idx','discount_percent'])

    df3 = df1.merge(df2, how='left', left_on='index_id', right_on='discount_idx')
    df3 = df3.merge(df2s, how='left', left_on='index_id', right_on='starsReview_idx')
    df3 = df3.merge(df2sp, how='left', left_on='index_id', right_on='soldProduct_idx')
# # ================================ GET location  
    elems_location = driver.find_elements(By.CSS_SELECTOR , ".oa6ri")  ###### css_location
    location = [elem.text for elem in elems_location]
    df3['location'] = location
    
    return df3
     
## =============================scroll down
def scroll(driver):
    pre_height = driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
    # count = 1
    while True:
        # print('-----scroll: {} times-----'.format(count))
        
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight*1/3);')
        sleep(1)  
        new_height = driver.execute_script('return document.body.scrollHeight')
        print('new_height: {}\npre_height: {}'.format(new_height, pre_height))
        
        if new_height == pre_height:
            print("While loop scroll Done!!!")
            break
        
        # count += 1
        pre_height = new_height   

# # ============================GET INFOMATION OF ALL ITEMS
def getDetailItems(driver,df1,df2):   
            print("Crawl detail")
            shop_name=""
            brand_name=""
            # moreinfor_button = driver.find_element(By.XPATH, "/html/body/div[4]/div/div[10]/div[1]/div[1]/div/div/div/div[2]/button")
            # driver.execute_script("arguments[0].scrollIntoView();",moreinfor_button)  
            # driver.execute_script("arguments[0].click();",moreinfor_button) 
            sleep(2)                                                    

            elems_shop = driver.find_elements('xpath', "/html/body/div[4]/div/div[3]/div[2]/div/div[2]/div[6]/div/div[1]/div[1]/div[2]/a")
            for elem in elems_shop:
                shop_name=elem.text
                break
            # print("shop: ",shop_name)
            elems_brand = driver.find_elements('xpath', "/html/body/div[4]/div/div[3]/div[2]/div/div[1]/div[6]/div/a[1]")  
            brand_name = [elem.text for elem in elems_brand] 
            # print("brand: ",brand_name)
            # elems_resolution = driver.find_elements(By.XPATH , "/html/body/div[4]/div/div[10]/div[1]/div[1]/div/div/div/div[1]/div[3]/div[1]/ul/li[3]/div")
            # resolution = [elem.text for elem in elems_resolution] 
            # print("reso: ",resolution)
            # elems_function= driver.find_elements(By.XPATH , "/html/body/div[4]/div/div[10]/div[1]/div[1]/div/div/div/div[1]/div[3]/div[1]/ul/li[4]/div")
            # overall_function = [elem.text for elem in elems_function] 
                 
            # next_pagination_cmt = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[4]/div/div[10]/div[1]/div[2]/div/div/div/div[3]/div[2]/div/button[2]")))
            # print("Clicked on button next page!")
            df1.append(brand_name)
            df2.append(shop_name)
            # print(df4)
            driver.close()
    # dataframe = dataframe.concat({'brand_name':df4[0], 'resolution':df4[1], 'overall_function':df4[2]}, ignore_index=True)
    # df4['link_item'] = links[0]
            return df1, df2


def runAPage(driver, pagePerTime):
    
    links,title=getLink(driver)
    price=getPrice(driver)
    df1 = pd.DataFrame(list(zip(links, title,price)), columns = ['link_item', 'title', 'price'])
    df1['index_id']= np.arange(1, len(df1) + 1)
    df3= getDiscount(driver, df1, len(title))
    df3['brand_name']=""
    df3['shop_name']=""
    # print(df3)
    cou=len(links)
    dfbrand_i4 = []
    dfshop_i4 = []
    chunked_link=[links[i:i + pagePerTime] for i in range(0, len(links), pagePerTime)]
    for chunk in chunked_link:
        cou=cou-pagePerTime
        start_time = time()
        drivers_r1 = openMultiBrowsers(pagePerTime)
        loadMultiBrowsers(drivers_r1,chunk)
        # sleep(3)     
        dfbrand_i4, dfshop_i4 = runInParallel(getDetailItems, drivers_r1, dfbrand_i4,dfshop_i4)
        print("brand detail: \n",dfbrand_i4)
        print("shop detail: \n",dfshop_i4)
        end_time = time()
        print("left over:",cou,", last time duration:",str(end_time - start_time)) 
    dfbrand = pd.DataFrame(dfbrand_i4, columns=['brand_name'])
    dfshop = pd.DataFrame(dfshop_i4, columns=['shop_name'])   
    # print("brand detail: \n",dfbrand)
    # print("shop detail: \n",dfshop)
    df3['brand_name']=dfbrand
    df3['shop_name']=dfshop
    return df3

#    
def multiPageCrawl(n):
    driver = webdriver.Chrome('F:\RnD\ETL\chromedriver.exe')
    countPage = 0
    while True:
            try:
                start_time = time()
                driver= startOption(driver, "https://www.lazada.vn/catalog/?_keyori=ss&from=input&page={}&q=camera&spm=a2o4n.tm80133073.search.go.2f14xoQTxoQTtJ".format(countPage))
                PageData= runAPage(driver, n)
                if os.path.exists('F:\RnD\ETL\lazadaexportdata\item\itemPage{}.csv'.format(countPage)):
                    os.remove('F:\RnD\ETL\lazadaexportdata\item\itemPage{}.csv'.format(countPage))
                PageData.to_csv('F:\RnD\ETL\lazadaexportdata\item\itemPage{}.csv'.format(countPage), encoding="utf-8-sig")
                end_time = time()
                print("page number:",countPage,", crawl duration:",str(end_time - start_time)) 
                countPage= countPage+1   
                # next_pagination_item_page_status = wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[3]/div/div[2]/div[1]/div/div[1]/div[3]/div/ul/li[9]/button")))
                # next_pagination_item_page_status = driver.find_element(By.XPATH, "/html/body/div[3]/div/div[2]/div[1]/div/div[1]/div[3]/div/ul/li[9]/button").get_property('data-spm-anchor-id')
            except ElementNotInteractableException:
                print("Element Not Interactable Exception!")
                break
            except TimeoutException:
                print("Element Exception!")
                break
            except NoSuchElementException:
                print("no found.")
                continue

multiPageCrawl(4)