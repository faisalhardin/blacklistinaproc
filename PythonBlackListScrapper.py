#!/usr/bin/env python
# coding: utf-8

# In[12]:


from bs4 import BeautifulSoup
import requests
import re
import functools as func
import numpy as np
import json
from contextlib import closing
#! pip install selenium
from selenium.webdriver import Firefox 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import datetime
import psycopg2


# In[13]:


def get_data_id_class(content):
    print('get data')
    data_ids = content.findAll('a', attrs={'data-id':re.compile("\d*")})
    list_data_ids = list(map(lambda x: x.get('data-id'), data_ids))
    return list_data_ids


# In[14]:


def translate_time(day, month, year):
    try:
        month_translation = {
            'jan':1,
            'feb':2,
            'mar':3,
            'apr':4,
            'mei':5,
            'jun':6,
            'jul':7,
            'agu':8,
            'sep':9,
            'okt':10,
            'nov':11,
            'des':12
        }
        month_int = month_translation[month.lower()]
    except:
        month_int = int(month)
    return datetime.date(int(year), month_int, int(day))


# In[15]:


def get_details(content, data_id, url="http://www.inaproc.id/daftar-hitam/"):
    print('get details : ', url)
    detail = content.find('div', attrs={'class':re.compile("^ui modal \w*")})
    
    while True:
        try:
            with closing(Firefox()) as driver:
                driver.get(url)
                button = driver.find_element_by_xpath("//a [@data-id='"+data_id+"']")
                button.click()
                # wait for the page to load
                element = WebDriverWait(driver, 10).until(
                EC.invisibility_of_element_located((By.XPATH, '//div [@class="ui dimmer modals page transition visible active"]'))
                )
                # store it to string variable

                page_source = driver.page_source
        except:
            continue
        break
#         driver.close()
    return page_source


# In[16]:


class DB:
    
    def connect(self):
        print("here")
        connection = psycopg2.connect(user='postgres',
                                     password = '1234',
                                     host= '127.0.0.1',
                                     port = '5432',
                                     database = 'scrapper',
                                     connect_timeout=3)
        return connection
        
    def command(self,cursor,bulk):
        pass
            
    def __init__(self, bulk):
        try:
            connection = self.connect()
            cursor = connection.cursor()
        
#             self.command(cursor, bulk)
            
#             connection.commit()
            print("commit")
        except (Exception, psycopg2.Error) as error :
            print ("Error while connecting to PostgreSQL", error)
            return 
        finally:
            #closing database connection.
            if(connection):
                
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
            return 
        
        
    
class Violations(DB):
#     __id = 1
    
    def command(self, cursor, bulk):
        cursor.execute('''INSERT INTO scrap_yard.violation (
        id_corp_info, sk_penetapan, pelanggaran,  masa_berlaku_mulai, 
        masa_berlaku_akhir, tanggal_penayangan) VALUES (%s,%s,%s,%s,%s, 
        %s)''',( bulk['data_id'], bulk['sk_penetapan'], bulk['pelanggaran'],
                  bulk['masa_berlaku_mulai'],bulk['masa_berlaku_akhir'],
                   bulk['tanggal_penayangan']))
#         Violations.__id += 1
#         cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)",
#        (100, "abc'def"))
        
class CorpInfo(DB):
    
    def command(self, cursor, bulk):
        cursor.execute('''INSERT INTO scrap_yard.corp_info (
        id, name, npwp, address,  province, city) VALUES (%s,%s,%s,%s,%s,%s)''',
                       (bulk['id'], bulk['nama_penyedia'],
                        bulk['npwp'], bulk['alamat'], 
                        bulk['provinsi'], bulk['kabupaten']))
    
        
def write_to_db(bulk, db):
    db(bulk)
#     TO DO : insert more


# In[17]:


def write_violations(soup, data_id):
    print('view violation')
#     detail = content.find_next('div', attrs={'class':re.compile('^ui dimmer')})
    violations = soup.find('table', attrs={'id':'injunctions'})
    for br in violations.find_all_next("br"):
        br.replace_with(" ")
    table_data = [[cell.text.strip() for cell in row("td")]
                         for row in violations.find_all_next("tr")][1:]
    
#     print(table_data)
    
    while len(table_data) > 0:
        masa_berlaku_mulai = table_data[1][1].split(' s/d ')[0].split(' ')
        masa_berlaku_akhir = table_data[1][1].split(' s/d ')[1].split(' ')
        tanggal_penayangan = table_data[2][1].split(' ')
        detail_pelanggaran = {
        'data_id':int(data_id),
        'sk_penetapan':table_data[0][0],
        'pelanggaran' : table_data[0][1],
        'masa_berlaku_mulai' : translate_time(masa_berlaku_mulai[0],masa_berlaku_mulai[1],masa_berlaku_mulai[2]),
        'masa_berlaku_akhir' : translate_time(masa_berlaku_akhir[0],masa_berlaku_akhir[1],masa_berlaku_akhir[2]),
        'tanggal_penayangan' : translate_time(tanggal_penayangan[0], tanggal_penayangan[1], tanggal_penayangan[2])
        }
        
        write_to_db(detail_pelanggaran, Violations)
        
#         print("len ",len(table_data))
        del table_data[0:3]
        if len(table_data)>0 and table_data[0][0] == 'SK Pencabutan':
            del table_data[0]


# In[18]:


def new_entry(content, url):
    print('new entry')
    data_ids = get_data_id_class(content)
    
    for data_id in data_ids:
#         try:
            
        while True:
            details = get_details(content, data_id, url)

            soup = BeautifulSoup(details)

            det = soup.find('table', attrs={'class':re.compile('^ui table definition')}).findAll('tr')
#             print(det)

            table_data_det = np.array([[cell.text.strip() for cell in row("td")]
                                       for row in det])
            if table_data_det[0,1] != '-':
                break

        table_data_det[:,0] = list(map(lambda x: x.replace(" ",'_').lower(), table_data_det[:,0]))
#         table_data_det.append(['id',int(data_id)])
#         table_basic_detail = json.dumps(dict(table_data_det))
        table_basic_detail = dict(table_data_det)

        table_basic_detail['id'] = int(data_id)
#         print(table_data_det, "\n\n---------------\n\n",table_basic_detail)

        ## TO DO : Sambung ke database
    
        write_to_db(table_basic_detail, CorpInfo)

        ## TO DO : Sambung ke database
        write_violations(soup, data_id)
#         except:
#             continue


# In[ ]:


base_url = 'http://www.inaproc.id/daftar-hitam/non-aktif?page='

counter = 1
hasNext = True
while hasNext:
    print('has next')
    
    while True:
        try:
            response = requests.get(base_url + str(counter), timeout=30)
            content = BeautifulSoup(response.content, "html.parser")
        except:
            continue
        break

#     print(type(content))
    new_entry(content, base_url + str(counter))
    
    counter += 1
    next_url = base_url + str(counter)
    print(next_url)
    next_link = content.find('a' , attrs={'href':next_url})
    if not next_link:
        hasNext = False


# ### Test

# In[ ]:




