#!/usr/bin/env python
# coding: utf-8

# In[16]:


#!pip install python-docx
#!pip install -U spacy
#pip install pymongo
#!pip install PyPDF2
#!pip install keybert
get_ipython().system(' pip install Tensorflow == 2.12.0')


# In[13]:


#!pip install pytextrank
#!pip install keybert
#!pip install htmldate
#!pip install datefinder
# pip install CurrencyConverter
# pip install textacy
# pip install geopy
# pip install lexnlp
#! pip install quantulum3
# pip install country-converter
#!pip install fuzzywuzzy
# pip install tensorflow==1.2.0 --ignore-installed


# In[7]:


import pandas as pd
import numpy as np
import re
import nltk
import string
import statistics
import transformers
import pymongo
from transformers import pipeline
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
import spacy
import pytextrank
from tqdm import tqdm
from keybert import KeyBERT
import pymongo
import unicodedata
from htmldate import find_date
import datefinder
from currency_converter import CurrencyConverter
import textacy
from geopy.geocoders import Nominatim
# import lexnlp.extract.en.money
from quantulum3 import parser
import country_converter as coco
import math
from fuzzywuzzy import fuzz,process
import os
import multiprocessing
import matplotlib.pyplot as plt
import tensorflow as tf
import tensorflow_hub as hub
from sklearn.metrics.pairwise import cosine_similarity

import urllib.request
import ssl
from bs4 import BeautifulSoup
import time
from docx import Document
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import json
import numpy as np
import requests
from requests.models import MissingSchema
import spacy
from urllib.request import Request, urlopen
import ssl
import pymongo
import time
from PyPDF2 import PdfReader
from io import BytesIO
import re


# In[4]:


#!pip uninstall python-docx
get_ipython().system('pip install PyPDF2')


# In[6]:


get_ipython().system('pip install tensorflow_hub')


# In[3]:


import urllib.request
import ssl
from bs4 import BeautifulSoup
import time
from docx import Document
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import json
import numpy as np
import requests
from requests.models import MissingSchema
import spacy
from urllib.request import Request, urlopen
import ssl
import pymongo
import time
from PyPDF2 import PdfReader
from io import BytesIO
import re

def dateConverter(date1):
    
    try:
        #print(date1)
        matches = datefinder.find_dates(date1)
        date = None
        for match in matches:
            date = match
    
    except:
        date = date1
        
    return date
    


class UrlProcessing:
    
    
    def __init__(self,urllist_docpath):
        self.doc = urllist_docpath
        self.urlSeg()
    
    
    #Extract URL from Word Document
    def urlList(self):
        document = Document(self.doc)
        rels = document.part.rels
        
        self.urls = []

        for rel in rels:
            if rels[rel].reltype == RT.HYPERLINK:
                self.urls.append(rels[rel]._target)
        print("Total no of urls found: {}".format(len(self.urls)))
        return self.urls
    
    
    #Existing info from mongodb
    def urlUnique(self):
        self.urlList()
        myclient = pymongo.MongoClient("mongodb://163.183.205.67:27017/")
        print('okay')
        mydb = myclient["NeoLith"]
        mycol = mydb["RunTotal288"]        
        if "RunTotal288" in mydb.list_collection_names():
            self.dbdata = pd.DataFrame(list(mycol.find({},{'_id': 0, "url_name": 1, "status": 1})))
        else:
             self.dbdata = pd.DataFrame({})
        if self.dbdata.size == 0: self.url_unique = self.urls
        else:
            self.url_unique = []
            for url in self.urls:
                if (url in self.dbdata['url_name'].values) != True: self.url_unique.append(url)
                #elif (data[data["url_name"] == url]["status"].iloc[0]) == "Fail": self.url_unique.append(url)
                else: pass
        print("Total no of new urls found: {}".format(len(self.url_unique)))
        return self.url_unique, self.dbdata
    
    
    #Seggregating them into pdf, ppt and link
    def urlSeg(self):
        self.urlUnique()
        self.url_pdf, self.url_ppt, self.url_link = [], [], []
        for url in self.url_unique:
            if url.lower().endswith(".pdf"): self.url_pdf.append(url)
            elif url.lower().endswith(".ppt") or url.lower().endswith(".pptx"): self.url_ppt.append(url)
            else : self.url_link.append(url)
        
        print("Total no of new urls found- pdf, ppt, link: {},{},{}".format(len(self.url_pdf), len(self.url_ppt), len(self.url_link)))
        return self.url_pdf, self.url_ppt, self.url_link
    
    
        
    
    
    
class WebScrap:
    
    
    def __init__(self,url):
        self.url = url
        
    def reuters(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    content.append(i.get_text())
                except:
                    print("fail")
            
            # create some data to export
            data = [self.url, soup.find_all("span", class_ = "date-line__date__23Ge-")[1].get_text(),soup.h1.get_text(), content,len(content), "Success"]
        except:
            data = [self.url]+self.fail()
        return data
        
    def greencarcongress(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    content.append(i.get_text())
                except:
                    print("fail")


            # create some data to export
            data = [self.url, soup.find_all("span", class_ = "entry-date")[0].get_text(),soup.h2.get_text().replace("\n",""), content,len(content), "Success"]
        except:
            data = [self.url]+self.fail()
        return data

    def bnamericas(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)
            
            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    content.append(i.get_text())
                except:
                    print("fail")


            # create some data to export
            data = [self.url, soup.find("div", class_ = "card__data pb-4").get_text().split(" ",5)[-1],soup.h1.get_text().replace("\n",""), content,len(content), "Success"]
        except:
            data = [self.url]+self.fail()
        return data
    
    def investingnews(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    if "About Livent" in i.get_text():
                        break
                    #print(i.get_text())
                    content.append(i.get_text())
                except:
                    print("fail")

            #Published Date
            date = soup.find("div", class_ = "social-date").get_text()

            # create some data to export
            data = [self.url, date,soup.h1.get_text().strip(), content,len(content), "Success"]
        except:
            data = [self.url]+self.fail()
        return data

    def energygov(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    #print(i.get_text())
                    content.append(i.get_text())
                except:
                    print("fail")

            #Published Date
            try:date = soup.find("div", class_ = "page-hero-date").get_text()
            except:date= "None"
            # create some data to export
            data = [self.url, date,soup.h1.get_text().strip(), content, len(content),"Success"]
        except:
            data = [self.url]+self.fail()
        return data
                
    def washingtonpost(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    if "More From Bloomberg Opinion" in i.get_text():
                        break
                    #print(i.get_text())
                    content.append(i.get_text())
                except:
                    print("fail")

            #Published Date
            try:date = soup.find("span", class_ = "wpds-c-iKQyrV").get_text().rsplit(" ",4)[0]
            except: date ="None"
            # create some data to export
            data = [self.url, date,soup.h1.get_text().strip(), content, len(content),"Success"]
        except:
            data = [self.url]+self.fail()
        return data
                
    def yahoo(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    if "FORWARD" in i.get_text() and "LOOKING" in i.get_text():
                        break
                    content.append(i.get_text())
                except:
                    print("fail")
            #Published Date

            try:date = soup.find("time").get_text().rsplit(",",1)[0]
            except: date ="None"
            # create some data to export
            data = [self.url, date,soup.find_all("h1")[1].get_text(), content, len(content),"Success"]
        except:
            data = [self.url]+self.fail()
        return data

    def cleantechnica(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    content.append(i.get_text())
                except:
                    print("fail")

            for j in ['Hi, what are you looking for?','By','Published']:
                content.remove(j)

            #Published Date       
            try:date = soup.find("time").get_text()
            except: date ="None"

            # create some data to export
            data = [self.url, date,soup.find("h1").get_text(), content, len(content),"Success"]
        except:
            data = [self.url]+self.fail()
        return data
    
    def nature(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    content.append(i.get_text())
                except:
                    print("fail")


            # create some data to export
            data = [self.url, soup.find('time').get_text(),soup.h1.get_text().replace("\n",""), content, len(content),"Success"]
        except:
            data = [self.url]+self.fail()
        return data
    
    def barrons(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    content.append(i.get_text())
                except:
                    print("fail")
            print(soup.find('time').get_text().split(" ",1)[-1].rsplit(" ",3)[0])
            # create some data to export
            data = [self.url, soup.find('time').get_text(),soup.h1.get_text().replace("\n",""), content, len(content),"Success"]
        except:
            data = [self.url]+self.fail()
        return data
    
    def magnoliareporters(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("div", id = "article-body")

            content = []

            for i in rows:
                try:
                    content.append(i.get_text())
                except:
                    print("fail")

            # create some data to export
            data = [self.url, soup.find('time').get_text().strip(),soup.h1.get_text().replace("\n",""), content,len(content), "Success"]
        except:
            data = [self.url]+self.fail()
        return data
                
    def seekingalpha(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    content.append(i.get_text())
                except:
                    print("fail")

            # create some data to export
            data = [self.url, soup.find('span',class_ = "rD-UA rD-jA U-gL").get_text().rsplit(" ",3)[0],soup.h1.get_text().replace("\n",""), content,len(content),"Success"]
        except:
            data = [self.url]+self.fail()
        return data
    
    def marketscreener(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in rows:
                try:
                    content.append(i.get_text())
                except:
                    print("fail")

            try: date = soup.find_all("div",style="align-self: center;color:black;")[0].get_text().split("|")[0].strip()
            except: date = "None"

            # create some data to export
            data = [self.url, date,soup.h1.get_text().replace("\n",""), content,len(content),"Success"]
        except:
            data = [self.url]+self.fail()
        return data
    
    def nytimes(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            content = []
            for i in soup.find_all("div",class_ ="css-s99gbd StoryBodyCompanionColumn"):
                for j in i.find_all("p"):
                    try:
                        content.append(j.get_text())
                    except:
                        print("fail")

            try: date = soup.find_all("time")[0].get_text().strip()
            except: date = "None"

            data = [self.url, date,soup.h1.get_text().replace("\n",""), content,len(content), "Success"]
        except:
            data = [self.url]+self.fail()
        return data
        
    def yahoofinance(self):
        try:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            rows = soup.find_all("p")

            content = []
            for i in soup.find_all("div",class_ ="caas-body"):

                for j in i.find_all("p"):
                    try:
                        content.append(j.get_text())
                    except:
                        print("fail")

            try: date = soup.find_all("time")[0].get_text().strip()
            except: date = "None"

            # create some data to export
            data = [self.url, date,soup.h1.get_text().replace("\n",""), content,len(content),"Success"]
        except:
            data = [self.url]+self.fail()
        return data
    
    def generalized(self):
        try:
            content = []
            hdr = {'User-Agent': 'Mozilla/5.0'}
            req = urllib.request.Request(self.url,headers=hdr)
            context = ssl._create_unverified_context()
            page = urllib.request.urlopen(req, context = context, timeout=120)
            soup = BeautifulSoup(page)

            try:
                title = soup.title.get_text()
            except:
                title = "None"

            try:
                for i in range(len(soup.find_all("p"))):
                    content.append(soup.find_all("p")[i].get_text())
            except:
                pass

            if title == "None": status = "Fail"
            else: status = "Success"
            lines = len(content)

            published_time = ''

            try:
                meta_tags = [i for i in soup.find_all("meta") if type(i.get("property", None)) == str]

                if any("published_time" in tag.get("property", None) for tag in meta_tags):
                    for i in meta_tags:
                        if "published_time" in i.get("property", None):
                            published_time = soup.find("meta", property=i.get("property", None))["content"]
                        else: pass

                elif any("published" in tag.get("property", None) for tag in meta_tags):
                    for i in meta_tags:
                        if "published" in i.get("property", None):
                            published_time = soup.find("meta", property=i.get("property", None))["content"]
                        else: pass

                else:
                    for tag in soup.find_all("div"):
                        if type(tag.get("class", None)) == list:
                            for i in tag.get("class", None):
                                if "date" in i:
                                    element = soup.find('div', {'class': i})
                                    published_time = element.get_text()

                    if published_time == '':
                        for tag in soup.find_all("div"):
                            if type(tag.get("class", None)) == list:
                                for i in tag.get("class", None):
                                    if "time" in i:
                                        element = soup.find('div', {'class': i})
                                        published_time = element.get_text()

                    if published_time == '':
                        for tag in soup.find_all("span"):
                            if type(tag.get("class", None)) == list:
                                for i in tag.get("class", None):
                                    if "date" in i:
                                        element = soup.find('span', {'class': i})
                                        published_time = element.get_text()

                    if published_time == '':
                        rows = soup.find("time")
                        try:
                            published_time = rows.get_text()
                        except:
                            pass
            except:
                print("Date Scraping Failed")

            data = [self.url, published_time,title.replace("\n",""), content,len(content),"Success"]
        except:       
            data = [self.url] + self.fail()
        return data

    
    def dateScrap(self):

        published_time = ''

        try:
            meta_tags = [i for i in soup.find_all("meta") if type(i.get("property", None)) == str]

            if any("published_time" in tag.get("property", None) for tag in meta_tags):
                for i in meta_tags:
                    if "published_time" in i.get("property", None):
                        published_time = soup.find("meta", property=i.get("property", None))["content"]
                    else: pass

            elif any("published" in tag.get("property", None) for tag in meta_tags):
                for i in meta_tags:
                    if "published" in i.get("property", None):
                        published_time = soup.find("meta", property=i.get("property", None))["content"]
                    else: pass

            else:
                for tag in soup.find_all("div"):
                    if type(tag.get("class", None)) == list:
                        for i in tag.get("class", None):
                            if "date" in i:
                                element = soup.find('div', {'class': i})
                                published_time = element.get_text()

                if published_time == '':
                    for tag in soup.find_all("div"):
                        if type(tag.get("class", None)) == list:
                            for i in tag.get("class", None):
                                if "time" in i:
                                    element = soup.find('div', {'class': i})
                                    published_time = element.get_text()

                if published_time == '':
                    for tag in soup.find_all("span"):
                        if type(tag.get("class", None)) == list:
                            for i in tag.get("class", None):
                                if "date" in i:
                                    element = soup.find('span', {'class': i})
                                    published_time = element.get_text()

                if published_time == '':
                    rows = soup.find("time")
                    try:
                        published_time = rows.get_text()
                    except:
                        pass

        except:
            print("Date Scraping Failed")

        return published_time    



    def pdfScrap(self):
        try:
            # Download the PDF file from the URL and open it
            context = ssl._create_unverified_context()
            with urllib.request.urlopen(self.url, context=context) as response:
                pdf_data = response.read()
                pdf_reader = PdfReader(BytesIO(pdf_data))

            # Extract the title from the PDF metadata
            if pdf_reader.metadata.title != None:
                if "powerpoint" not in pdf_reader.metadata.title.lower() and "microsoft" not in pdf_reader.metadata.title.lower():
                    title = pdf_reader.metadata.title
                else: title = None
            else: title = None

            date_string = pdf_reader.metadata['/CreationDate'].strip()[2:10]
            date_format = '%Y%m%d'
            date_unfrmt = datetime.strptime(date_string, date_format)
            date = datetime.strftime(date_unfrmt, '%d-%b-%Y')

            # Extract the text content of the PDF file
            content = ''
            for page in range(len(pdf_reader.pages)):
                content += pdf_reader.pages[page].extract_text().strip()

            # Count the number of lines
            lines = len(content.split('\n'))

            if title == None:            
                # Print the title
                if lines == 0:
                    # Search for the first line that contains multiple words starting with capital letters
                    for line in text_content.split('\n'):
                        if re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', line):
                            title = line
                            break

            if title == None: title = pdf_reader.pages[0].extract_text().strip().split("\n")[1]
            # create some data to export
            data = [self.url, date,title, content, lines,"Success"]

        except:
            data = [self.url] + self.fail()
        return data
    
    def fail(self):
        data_f = [None,None,None,0,"Fail"]
        return data_f
    

class DataBase:
    
    def __init__(self,data):
        self.data = data
        self.url = data[0]
        print(data[4])
        self.mongoPush()
    
    def writecsv(self):
        filename = r"C:\Users\BGogoi2\Desktop\NeoLith\TestCsv.csv"  # use name in file name
        with open(filename, 'a', newline='',encoding = 'utf-8') as file:  # overwrite existing file if it exists
            # create a CSV writer object
            writer = csv.writer(file)
            # write the row to the CSV file
            writer.writerow(self.data)
            
    def mongoPush(self):
        myclient = pymongo.MongoClient("mongodb://163.183.205.67:27017/")
        mydb = myclient["NeoLith"]
        mycol = mydb["RunTotal288"]
        if dbdata.size != 0:
            if self.url not in dbdata["url_name"].to_list():
                mycol.insert_one({"url_name" : self.data[0], "date": dateConverter(self.data[1]), "title": self.data[2], "content": self.data[3], "status":self.data[5],"lines": self.data[4]})
                print("added")
            else:
                mycol.update_one({"url_name" : self.data[0]},{"$set": {"date": dateConverter(self.data[1]),"title": self.data[2], "content": self.data[3], "status":self.data[5], "lines": self.data[4]}})
                print("updated")
        else:
            mycol.insert_one({"url_name" : self.data[0], "date": dateConverter(self.data[1]), "title": self.data[2], "content": self.data[3], "status":self.data[5],"lines": self.data[4]})
            print("added")
            

b = UrlProcessing(r"C:\NeoLith\Final_test_1\New_Data.docx")
dbdata = b.dbdata


for i in b.url_link:
    if "www.reuters.com" in i:
        print(i)
        DataBase(WebScrap(i).reuters());

    elif "www.greencarcongress.com" in i:
        print(i)
        DataBase(WebScrap(i).greencarcongress());

    elif "www.bnamericas.com" in i:
        print(i)
        DataBase(WebScrap(i).bnamericas());

    elif "investingnews.com" in i:
        print(i)
        DataBase(WebScrap(i).investingnews());    

    elif "www.energy.gov" in i:
        print(i)
        DataBase(WebScrap(i).energygov());    

    elif "www.washingtonpost.com" in i:
        print(i)
        DataBase(WebScrap(i).washingtonpost());

    elif "www.yahoo.com" in i:
        print(i)
        DataBase(WebScrap(i).yahoo());

    elif "cleantechnica.com" in i:
        print(i)
        DataBase(WebScrap(i).cleantechnica());

    elif "www.nature.com" in i:
        print(i)
        DataBase(WebScrap(i).nature());

    elif "www.barrons.com" in i:
        print(i)
        DataBase(WebScrap(i).barrons());

    elif "www.magnoliareporter.com" in i:
        print(i)
        DataBase(WebScrap(i).magnoliareporters());

    elif "seekingalpha.com" in i:
        print(i)
        DataBase(WebScrap(i).seekingalpha());

    elif "www.marketscreener.com" in i:
        print(i)
        DataBase(WebScrap(i).marketscreener());    

    elif "www.nytimes.com" in i:
        print(i)
        DataBase(WebScrap(i).nytimes());

    elif "finance.yahoo.com" in i:
        print(i)
        DataBase(WebScrap(i).yahoofinance());

    else:
        print(i)
        DataBase(WebScrap(i).generalized());

#         if count == 20:
#             break
            
            
for i in b.url_pdf:
    print(i)
    DataBase(WebScrap(i).pdfScrap())
    


# 
# 
# ### Database                  : NeoLith
# ### Web Scrapping        : New_Data
# ### Sentiment Analysis : SentimentAnalysis_New_data
# ### Content Summary   : contentSummary_New_Data
# ### Entity Extraction      : total_content_new1
# ### Similarity Analysis  : articleSimilarity_New1
# 

# In[8]:




def getUniqueUrls():
    #Get data from articleCollection
    client = pymongo.MongoClient('mongodb://163.183.205.67:27017/')
    db = client['NeoLith']
    collection = db['RunTotal288']
    projection = {'_id': 1, 'url_name': 1, 'content': 1}
    cursor = collection.find({}, projection)
    df = pd.DataFrame(list(collection.find()))
    df1 = df.dropna(subset=['content'])
    
    #Get existing info from SentimentAnalysis
    myclient = pymongo.MongoClient("mongodb://163.183.205.67:27017/")
    mydb = myclient["NeoLith"]
    mycol = mydb["SentimentAnalysis_RunTotal288"]
    urls = df1["url_name"].to_list()
    if "SentimentAnalysis_RunTotal288" in db.list_collection_names():
        dbdata = pd.DataFrame(list(mycol.find({},{'_id': 0, "url_name": 1})))
    else:
        dbdata = pd.DataFrame({})
    if dbdata.size == 0: urls_to_drop = []
    else:
        urls_to_drop = []
        for url in urls:
            if (url in dbdata['url_name'].values) == True: urls_to_drop.append(url)
            else: pass

    # get only those rows corresponding to the urls who are present in articleCollection but not in sentiment analysis
    keep_mask = ~df1['url_name'].isin(urls_to_drop)
    filtered_df = df1[keep_mask]

    return filtered_df,db,mydb

##############################################################################################################################
####################################################Sentiment Analysis########################################################
##############################################################################################################################






def list_to_string(x):
    if isinstance(x, list):
        x1 = [str(i) for i in x]
    else:
        x1 = re.findall(r'"(.*?)"', x)
    x2 = '.'.join(str(x) for x in x1)
    x2 = x2.replace("\\t","")
    x2 = x2.replace("\\n","")
    x2 = x2.encode("ascii", "ignore")
    x2 = x2.decode()
    return x2


def remove_stopwords(text):
    stop_words = set(stopwords.words('english'))
    no_stopword_text = [word for word in text.split() if not word.lower() in stop_words]
    return ' '.join(no_stopword_text)

def sentimentAnalysis():
    df['content1'] = df['content'].apply(list_to_string)
    df.dropna(subset=['content1'],inplace=True)
    df['cleaned_content1'] = df['content1'].apply(remove_stopwords)
    df.reset_index(drop=True, inplace=True)
    content_list = df["cleaned_content1"].to_list()
    df['mode_label'] = None
    df['mean_score'] = None

    for i in content_list:
        if len(i) == 0:
            continue
        sentences = re.split('\. |\? |\! |\, |\:', i)
        classifier = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english', max_length = 512, truncation = True)
        result = classifier(sentences)
        labels = [d['label'] for d in result]
        scores = [d['score'] for d in result]
        mode_label = statistics.mode(labels)
        mean_score = statistics.mean(scores)
        row_idx = content_list.index(i)
        df.at[row_idx, 'mode_label'] = mode_label
        df.at[row_idx, 'mean_score'] = mean_score



    df2 = df[['_id', 'url_name', 'cleaned_content1', 'mode_label', 'mean_score']]
    df2 = df2.dropna(subset=['_id'])
    #df2.reset_index(drop=True, inplace=True)
    new_collection = mydb['SentimentAnalysis_RunTotal288']
    new_docs = df2.to_dict('records')
    if len(new_docs) != 0: new_collection.insert_many(new_docs)
    else: print("No new record to insert")
        

##############################################################################################################################
####################################################Article Summary#########################################################
############################################################################################################################

def readDB():
    client = pymongo.MongoClient('mongodb://163.183.205.67:27017/')
    db = client['NeoLith']
    collection = db['RunTotal288']
    
    projection = {'_id': 1, 'url_name': 1, 'content': 1}
    cursor = collection.find({}, projection)
    df = pd.DataFrame(list(collection.find()))
    df1 = df.dropna(subset=['content'])
    return df,db

def list_to_string(x):
    if isinstance(x, list):
        x1 = [str(i) for i in x]
    else:
        x1 = re.findall(r'"(.*?)"', x)
    x2 = '.'.join(str(x) for x in x1)
    x2 = x2.replace("\\t","")
    x2 = x2.replace("\\r","")
    x2 = x2.replace("\\n","")
    x2 = x2.replace("|","")
    x2 = re.sub(' +', ' ', x2)
    x2 = x2.encode("ascii", "ignore")
    x2 = x2.decode()
    return x2

def summary():
    df,db = readDB()
    df1 = df[df['content'].notnull()].reset_index(drop=True)
    df1['content1'] = df1['content'].apply(list_to_string)
    nlp = spacy.load("en_core_web_sm")
    # add PyTextRank to the spaCy pipeline
    nlp.add_pipe("textrank")
    doc = nlp(df1['content1'][0])
    
    kw_model = KeyBERT(model='all-mpnet-base-v2')  # for keywords

    l = []
    k = []
    for i in tqdm(range(len(df1))):
        doc = nlp(df1['content1'][i])
        tr = doc._.textrank
        s = ""
        for sent in tr.summary(limit_phrases=15, limit_sentences=5):
            s = s + sent.text
        l.append(s)
        keywords = kw_model.extract_keywords(df1['content1'][i], keyphrase_ngram_range=(1, 3), stop_words='english', 
                                             highlight=False, top_n=10)
        keywords_list= list(dict(keywords).keys())
        k.append(keywords_list)

    df1["extractive_summary"] = l
    df1["keywords"] = k

    df2 = df1[['_id', 'url_name', 'content1', 'extractive_summary', 'keywords']]
    new_docs = df2.to_dict('records')
    mycol = mydb["contentSummary_RunTotal288"]
    if len(new_docs) != 0: mycol.insert_many(new_docs)
    else: print("No new record to insert")
        
##############################################################################################################################
####################################################Entity Extraction#########################################################
############################################################################################################################

def clean1(x, remlist = ["\\n", 'xa0','\\t',"' , '",'\  "',"', '","\\n","\\t","\\xa0",'"',"\xa0"]):
    
    sen = ""
    x = unicodedata.normalize('NFKD',x)
    
    for erm in remlist:
        s1 = x.replace(erm,'')
    s1 = s1.replace("  ","")
    s1 = s1.replace("\xa0"," ")
    s1 = s1.replace("\u200e"," ")
    s1 = s1.replace("', '","")
    s1 = s1.replace('."',".")
    s1 = s1.encode("ascii", "ignore")
    s1 = s1.decode()

    return s1

market_segment = pd.read_csv("C:\\NeoLith\\Final_test_1\\Market segmentation for LDMSNew.csv", delimiter=',', encoding='unicode_escape')
market_segment.columns = ["Mines","Companies","Ore_type","segment"]


organization = pd.read_csv("C:\\NeoLith\\Final_test_1\\Company_industryNew.csv", delimiter=',', encoding='unicode_escape')
organization.columns = ["Company","Industry"]




og_comp=""
url_last=""
sentence_number=0


def relevant_act(company_info,url):                  #text/string input
    
        
        
        global og_comp
        global url_last
        global sentence_number
        
        compOfInterest =""
        cc = coco.CountryConverter()
        #print("within function____________\t",company_info,"\n")
        
        nlk = spacy.load("en_core_web_sm")

        dox = nlk(company_info)
        #dox = pipeline(company_info)
        #print(dox)
        sentences = list(dox.sents)
        
        total_info=[]
        currency=["$"]
        symbols=["-","@","'"]
        price_mass=[]
       
        str_mon = ""
        #company names of interest
        action =[]
        money_action = ["invest","borrowed","loaned","borrow","loan","invested","buy","bought",'purchase',"debt","liability"]   # add other actions of companies of interest
        #displacy.serve(dox, style="dep")
        money_str=""
        checklist = ["MONEY","LOC","ORG","GPE","FAC","DATE"]

        #ent_dict = {}
        #end_sen = [".","?","!"]

        comp_list = list(market_segment.Companies)#["Tesla"]#,"Microsoft","Amazon"] 
        segment_list = list(market_segment.segment)
    
        org_list = list(organization.Company)
        org_industry = list(organization.Industry)
    
        
        price_pattern1 = r"(\$|usd|USD|Dollar|Dollars|dollar|dollars) *(\d)(\d|,|\.)*(\d) *(per|\/|\\)? *(kg|kgs|Kg|Kgs|kilo|kilos|kilogram|kilograms|tonne|ton|tons|tonnes)"
        price_pattern2 = r"(\d)(\d|,|\.)*(\d) *(\$|usd|USD|Dollar|Dollars|dollar|dollars) *(per|\/|\\)? *(kg|kgs|Kg|Kgs|kilo|kilos|kilogram|kilograms|tonne|ton|tons|tonnes)"
        patterns = [price_pattern1, price_pattern2]

        #print(comp_list)
        
        if url_last == url:
            
            sentence_number = sentence_number+1
            
        else:
            sentence_number = 0
        
        
        
        
        
        push_sen_list = []

        alpha=0
        
        #print( "Company from previous \t", og_comp) 
        
        for sen in sentences:       

               
            for ent in sen.ents:
                
                #print("entity____",ent.text, "label_find______",str(ent.label_), "Company", compOfInterest) 
                
                if ent.label_=='ORG':
                    alpha=0
                    #global og_comp
                    og_comp=""
                    for i in range(0,len(comp_list)):
                        if pd.isna(comp_list[i]) != True:
                            if fuzz.ratio(ent.text.lower(),comp_list[i].lower()) > 90:
                                alpha = 1
                                if fuzz.ratio(compOfInterest.upper(),ent.text.upper())<80:  
                                        compOfInterest = compOfInterest+' '+comp_list[i]
                                        #global og_comp
                                        og_comp = compOfInterest
                                #break
                    if alpha==0:
                        
                        for j in range(0,len(organization.Company)):
                            if pd.isna(organization.Company[j]) != True:
                                if fuzz.ratio(ent.text.lower(),organization.Company[j].lower()) > 90:
                                    #print("ajkhdfjsahvfjadfl______________ ",ent.text)
                                    
                                    if fuzz.ratio(compOfInterest.upper(),ent.text.upper())<80: 
                                        compOfInterest = compOfInterest+' '+organization.Company[j]
                                        #global og_comp
                                        og_comp = compOfInterest
                                    alpha = 1
                                    #break
                
                    #print("entity____",ent.text, "label_find______",str(ent.label_), "Company", compOfInterest)  
                    
            if alpha==0:
                    #global og_comp
                compOfInterest= og_comp
                if len(og_comp)!=0:
                    alpha=1
                    
                    
              
            #print("line formed____",line)
            #print("line lemma______",line_lemma)

            if alpha==1:
                
                og_comp=compOfInterest
                obj = ""
                company=""
                verb="" 
                subject=""
                found=0
                sub_segment=""
                quant_row=[]
                ents=[]
                rate_found=0
                add_row=[]
                rate_value=""
                price_mass_row=[]
                
                
                svo = (textacy.extract.subject_verb_object_triples(sen))

                for s,v,o in svo:
                    #sub_segment=""
                    subject = str(s).replace("[","").replace("]","")
                    verb = str(v).replace("[","").replace("]","")
                    obj= str(o).replace("[","").replace("]","")
                    #print((obj))
                    for z in range(0,len(comp_list)):
                        if pd.isna(comp_list[z]) != True:
                            if subject.lower() == comp_list[z].lower():
                                sub_segment = segment_list[z]
                    if len(obj)>0:
                        break                
                    
                
                try:
                    quant_value=""
                    row=[]
                    mass_value=""
                    
                    quants = parser.parse(company_info)
                    if len(quants)>0:

                        if quants[0].unit.entity.name.lower() == 'mass':
                            quant_value = quants[0].surface
                            #print(quants[0].surface,"_____",quants[0].unit.entity.name)
                            mass_value= mass_value+','+str(quant_value)
                            #row = [subject,"",verb,obj,quant_value,'','',"Mass",'',compOfInterest,sentence_number,url]
                        
                        #quant_row.append(row)
                            
                            
                except:
                    continue
                
                for pattern in patterns:
                    for match in re.finditer(pattern, dox.text):
                        start, end = match.start(), match.end()
                        span = dox.char_span(start, end, label="PRICE", alignment_mode="contract")
                        if span is None:
                            print("Skipping entity")
                        else:
                            ents.append(span)
                    if ents:
                        dox.ents = ents
                        rate_found=1
                        #print("price/tonne   ",  dox.ents)
                        
                        rate_value = rate_value+''+str(dox.ents).strip(")").strip("(")
                        
                        #print (i)
                      
                #article_date = find_date(url)
                digits=0
                p=0
                for ent in sen.ents:
                    
                    label_subject = ""
                    label_verb =""
                    industry=""
                    segment=""
                    
                    p = p+1
                    #print("entity____",ent.text, "label_find______",str(ent.label_))
                    str_rel=ent.text
                    
                    ind=0
                    label_subject=""
                    if str(ent.label_) in checklist:
                        
                        
                        if  str(ent.label_) == 'ORG':
                            for z in range(0,len(comp_list)):
                                if pd.isna(comp_list[z]) != True:

                                    if ent.text.lower() == comp_list[z].lower():
                                        segment= segment_list[z]
                                        found_segment=1
                                        #print('xxxxxxxxxxx_',ent.text)
                                    
                            for nn in range(0,len(org_list)):
                                if pd.isna(org_list[nn]) != True:
                                    if fuzz.ratio(ent.text.lower(),org_list[nn].lower()) > 90:
                                        
                                        #print('tttttttttttttt_',ent.text)
                                        
                                        industry = org_industry[nn]   
                
                                        
                        
                        if str(ent.label_)=='MONEY':
                            #print("$$$$$$_____\n",ent.label_)
                            
                            #print(list(lexnlp.extract.en.money.get_money(company_info)))
                            
                            #label_subject = str(ent.text_)
                            
                            
                            ind = company_info.find(ent.text)
                            check_str = company_info[ind-20:ind+20]
                            
                            
                            for m in list(lexnlp.extract.en.money.get_money(check_str)):
                                label_subject += str(m[0]) + ' '+str(m[1])+' '
                            
                        elif str(ent.label_)=='GPE':
                            index=0
                            address=""
                            country=""
                            
        
                            name=""
                            name= cc.convert(names = str(ent.text), to = 'name_short', not_found='not found')
                            if name != 'not found':
                                str_rel = name
                            
                            try:
                                geolocator = Nominatim(user_agent="country_name")
                                loc = geolocator.geocode(str_rel, language='en') 
                                index = str(loc.address).rfind(",") +1
                                address = str(loc.address) 
                                if index != -1:
                                    country = address[index:len(address)]
                                #print(str(ent.text),"full location______________________\n",address, type(address))

                                #print(index,"__index_____""country________",country)
                                label_subject = str(ent.text)+"|"+country
                            except:
                                label_subject = str(ent.text)

                            # u'Chicago, Cook County, Illinois, United States of America'
                            
                        
                        else:
                            label_subject = ent.text
                            
                            
                        ind = company_info.find(str_rel)
                        count_word = len(company_info.strip()[0:ind].split(' '))-1
                        
                        try:

                            verb_text,verb_token = loc_to_verb(sen[count_word])

                            label_verb=""
                            for v in verb_text:
                                if v.lower().strip() in money_action:
                                    label_verb = v
                                    break
                                else:
                                    #if label_verb.find(v)!=-1:
                                    label_verb += v+", "
                                        #print("found verb\t",v)

                            label_verb = label_verb.strip() 
                            label_verb = label_verb.rstrip(",") 
                        except:
                            print(company_info,"verb")
                            continue
                            
                        
                        type_ent=""
                        type_ent= str(ent.label_)
                        if type_ent =='GPE' or type_ent =='FAC' :
                            type_ent = 'LOC'
                        #print( "subject____ ",subject," verb______",verb," object_______",obj,
                           #" label subject____",label_subject, "label______",str(ent.label_))       
                        
                        add_row = [subject,sub_segment,verb,obj,label_subject,industry,segment,type_ent,label_verb,compOfInterest,sentence_number,url]
                        
                        #check_row = [subject,verb,url]
                        
                    if len(add_row)!=0:
                        total_info.append(add_row)
                        
                if len(mass_value)!=0:
                    mass_value=mass_value.strip(",")
                    quant_row=[subject,"",verb,obj,mass_value,'','',"measurement",'',compOfInterest,sentence_number,url]
                    print("_____measurement___________",quant_row)
                    
                    #df = pd.DataFrame(total_info,columns=['Subject','Subject_Segment','Verb','Object','Entity','Industry','Segment','Type','VerbWithEntity','CompanyOfInterest','Sentence Number','URL'])       
                    total_info.append(quant_row)
                    
                if rate_found ==1:
                    rate_value = rate_value.strip(",")
                    price_mass = [subject,"",verb,obj,rate_value,'','',"price/mass",'',compOfInterest,sentence_number,url]
                    #price_mass_row.append(price_mass)  
                      
                    #rate_row=[subject,"",verb,obj,rate_value,'','',"price/mass",'',compOfInterest,sentence_number,url]
                    print("_____rate___________",price_mass)
                    total_info.append(price_mass)
            
        #print("tttttt________",total_info)  
        url_last= url
        df = pd.DataFrame(total_info,columns=['Subject','Subject_Segment','Verb','Object','Entity','Industry','Segment','Type','VerbWithEntity','CompanyOfInterest','Line Number','URL'])       #df.drop_duplicates(keep='first',inplace=True)
        #df.reset_index(drop=True,inplace=True)
        #print("_________XXXX)______\n",df)
        return(df)                
        
                
                        
                


# In[4]:


def loc_to_verb(tok):
    verb_phrase = []
    verb_tok=[]
    
    for i in tok.ancestors:
        if i.pos_ == "VERB":
            # ...add the verb to the verb phrase list
            verb_phrase.append(i.text)
            verb_tok.append(i)
            
    return(verb_phrase,verb_tok)


def Article_entities():
    myclient = pymongo.MongoClient("mongodb://163.183.205.67:27017/")
    mydb = myclient["NeoLith"]
    mycol = mydb["RunTotal288"]
    data = list(mycol.find({},{'_id': 0, "url_name": 1, "content": 1, "category": 1}))

    mydb = myclient["NeoLith"]
    mycol = mydb["total_content_Run228New"]

    market_segment = pd.read_csv("C:\\NeoLith\\Final_test_1\\Market segmentation for LDMSNew.csv", delimiter=',', encoding='unicode_escape')
    market_segment.columns = ["Mines","Companies","Ore_type","segment"]


    organization = pd.read_csv("C:\\NeoLith\\Final_test_1\\Company_industryNew.csv", delimiter=',', encoding='unicode_escape')
    organization.columns = ["Company","Industry"]
    phi=1
    cnt=0
    for x in data:

        url = x.get('url_name')
        #if url == 'https://www.mining.com/teslas-battery-metals-bill-balloons-to-100-billion/':
            #phi=1

        if phi==1:
            draft_text=[]
            if  x.get('content')!=None: #and 'Battery/Critical Minerals' in x.get('category'):
                url_frame = pd.DataFrame()


                url = x.get('url_name')


                extract_dict={}
                tok_text = str(x.get('content'))
                tok_text = tok_text[1:len(tok_text)-1]


                #print("yyyyyyyyyyyyyyyyyyyyyy_____________",url)



                #bytes_encoded = tok_text.encode(encoding='utf-8')
                #tok_text = bytes_encoded.decode()

                tok_text = clean1(tok_text)


                #extract_frame = relevant_act(tok_text, url)



                draft_text = re.split(r'(?<!\d\.\d)(?<![A-Za-z]\$)\.(?!\d)', tok_text)

                #print(". ","DRAFT_TEXT\n",draft_text)

                url = x.get('url_name')
                #url = response.urljoin(url)

                print("url_____________",url)



                for text in draft_text:
                    text = text.strip(' ').replace("'",'').replace('"','')
                    #print("text_____og_____\n",text)

                    extract_frame=pd.DataFrame()
                    split_list=[]
                    #clean_line=""
                    #print("_ttttt_____\n",text)
                    if text.find("?")!=-1:
                        #print("???????\n")
                        split_list = text.split("?")
                    elif text.find("!")!=-1:
                        #print("!!!!!!!!\n")
                        split_list = text.split("?")
                    else:
                        split_list = []

                    if len(split_list)>1:
                        for split_text in split_list:

                            clean_line = clean1(split_text)
                            #print("******\n",split_text)
                            extract_frame = relevant_act(clean_line, str(url))
                    else:
                        clean_line = clean1(text)


                    try:
                        extract_frame = relevant_act(clean_line, str(url))

                        if extract_frame.empty:
                            continue
                        else:
                            extract_frame.drop_duplicates(keep='first',inplace=True)
                            extract_frame.reset_index(drop=True,inplace=True)

                                    #print("extracted_______",extract_frame)
                                    #check_data_object(extract_frame)

                    except:
                        print("url issue")


                    url_frame = pd.concat([url_frame,extract_frame])  

                url_frame.drop_duplicates(keep='first',inplace=True)
                url_frame.reset_index(drop=True,inplace=True)
                print("url_info_______________\n",url_frame)


                #phi=0
                #extract_frame = relevant_act(tok_text,url)

                if url_frame.empty:
                    print("Empty")
                else:
                    cnt=cnt+1
                    data_dict = url_frame.to_dict("records")
                    mycol.insert_many(data_dict)

                url_last = url

############################################################################################################################
#####################################################Similarity Analysis####################################################
############################################################################################################################

def mongoExtract():
    
    client = pymongo.MongoClient('mongodb://163.183.205.67:27017/')
    db = client['NeoLith']
    collection = db['RunTotal288']
    projection = {'_id': 1, 'url_name': 1, 'content': 1}
    cursor = collection.find({}, projection)
    df = pd.DataFrame(list(collection.find()))

    df1 = df[df['content'].notnull()].reset_index(drop=True)
    df1['content1'] = df1['content'].apply(list_to_string)
    df1 = df1[df1.content1 != ''].reset_index(drop=True)
    df1 = df1[df1.content1 != '\n']
    for i in range(0,len(df1)):
        df1['content1'][i] = df1['content1'][i].replace('.,', '.')

    for i in range(0,len(df1)):
        df1['content1'][i] = df1['content1'][i].replace('..', '.')
    df1 = df1.rename(columns={'url_name': 'URL'})


    collection = db['total_content_Run228New']
    #projection = {'_id': 1, 'url_name': 1, 'content': 1}
    cursor = collection.find({})
    df = pd.DataFrame(list(collection.find()))

    # group by 'URL' and 'Company', and aggregate the unique 'Entity' values
    df_new = df.groupby(['URL', 'CompanyOfInterest'])['Entity'].unique().reset_index()

    # create a new dataframe with 'URL', 'Company' and 'Entity' columns
    df_final = pd.DataFrame({'URL': df_new['URL'],
                             'CompanyOfInterest': df_new['CompanyOfInterest'],
                             'Entity': df_new['Entity']})
    return df1, df_final


def get_matching_urls(url1, url2,df_final):
    urls = df_final['URL'].tolist()
    if url1 in urls and url2 in urls:
        entity1 = df_final.loc[df_final['URL'] == url1, 'Entity'].tolist()[0]
        entity2 = df_final.loc[df_final['URL'] == url2, 'Entity'].tolist()[0]
        num_same_entities = len(set(entity1) & set(entity2))
        if num_same_entities >= 1:
            return True, num_same_entities
    return False, 0


def similarityAnalysis():
    df1, df_final = mongoExtract()
#     print(df_final)
    #texts = df1['extractive_summary'].values.tolist()
    paragraphs = df1['content1'].values.tolist()
    URL = df1['URL'].values.tolist()
    # Set threshold
    threshold = 0.75


    module_url = "https://tfhub.dev/google/universal-sentence-encoder/4" 
    model = hub.load(module_url)

    embeddings = model(paragraphs)
    labels = [sentence[:10] for sentence in paragraphs]

    similarity = cosine_similarity(embeddings)    
    
    data1 = {'paragraph': paragraphs, 'relation_sim': [''] * len(paragraphs), 'relation_sim_ent': [''] * len(paragraphs), 'URL': URL}
    df3 = pd.DataFrame(data1)


    for i in range(len(paragraphs)):
        for j in range(i + 1, len(paragraphs)):
            sim_score = similarity[i][j]
            if sim_score > threshold:
                url1 = URL[i]
                url2 = URL[j]
                rel_text = f"similarity between URL {i} and URL {j} is {sim_score}"
                df3.at[i, 'relation_sim'] += rel_text  + ' ; '
                df3.at[j, 'relation_sim'] += rel_text +  ' ; '
                matching_urls, num_same_entities = get_matching_urls(url1, url2,df_final)
                if matching_urls:
                    df3.at[i, 'relation_sim_ent'] += rel_text + f" (same entities: {num_same_entities})" + ' ; '
                    df3.at[j, 'relation_sim_ent'] += rel_text + f" (same entities: {num_same_entities})" + ' ; '


    df3['relation_sim_ent'] = df3['relation_sim_ent'].apply(lambda x: x[:-1] if x != '' else 'similarity <0.75 and same entities less than 2')
    df3['relation_sim'] = df3['relation_sim'].apply(lambda x: x[:-1] if x != '' else 'similarity <0.75 ')
    
    URL_list = URL
    
    similarity = similarity.tolist()

    results = []

    for i in range(len(URL_list)):
        sub_list = []
        for j in range(i + 1, len(URL_list)):
            sim_score = similarity[i][j]
            if sim_score > threshold:
                url1 = URL_list[i]
                url2 = URL_list[j]
                matching_urls, num_same_entities = get_matching_urls(url1, url2,df_final)
                print(matching_urls)
                print(num_same_entities)
                if matching_urls and num_same_entities >= 1:
                    sub_list.append({
                        "compared_url": url2,
                        "sim_score": sim_score,
                        "same_entities": num_same_entities
                    })

        if sub_list:
            results.append({
                "url": URL_list[i],
                "similarities": sub_list
            })

    with open('LDMS_similairty_dict.json', 'w') as f:
        json.dump(results, f, indent=4)

    df3.to_excel('LDMS_similarity.xlsx')
    
    #print(df3)
    #print('')

    myclient = pymongo.MongoClient("mongodb://163.183.205.67:27017/")
    mydb = myclient["NeoLith"]
    mycol = mydb["articleSimilarity_New228"]
    mycol.insert_many(results)

        
df,db,mydb = getUniqueUrls()

if __name__ == '__main__':
    p1 = multiprocessing.Process(target=sentimentAnalysis)
    p2 = multiprocessing.Process(target=summary)
    p3 = multiprocessing.Process(target=Article_entities)
    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()
    #similarityAnalysis()
    print("Process ended")


# In[9]:


#!/usr/bin/env python
# coding: utf-8

# In[1]:


import torch
import pandas as pd
import pymongo
from transformers import BertTokenizer
from imblearn.over_sampling import ADASYN
from collections import Counter
import nlpaug.augmenter.word.context_word_embs as aug

import tensorflow as tf
from transformers import BertTokenizer, TFBertForSequenceClassification

import re
import string
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import nltk

from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk import word_tokenize

from sklearn.utils import shuffle
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report


# In[6]:


import re
import unicodedata
from nltk.tokenize import sent_tokenize, word_tokenize
def clean1(x, remlist = ["\\n", 'xa0','\\t',"' , '",'\  "',"', '","\\n","\\t","\\xa0",'"',"\n"]):
#     res = ast.literal_eval(x)
#     res = ''.join(map(str, res))
    
    sen = ""
    x = unicodedata.normalize('NFKD',x)
    
    for erm in remlist:
        s1 = x.replace(erm,'')
    s1 = s1.replace("  ","")
    s1 = s1.replace("\xa0"," ")
    s1 = s1.replace("\u200e"," ")
    s1 = s1.replace("', '","")
    s1 = s1.replace(".\n","")

    return s1


# In[2]:


def convert_to_lower(text):
    return text.lower()

def remove_numbers(text):
    number_pattern = r'\d+'
    without_number = re.sub(pattern=number_pattern, repl=" ", string=text)
    return without_number

def lemmatizing(text):
    lemmatizer = WordNetLemmatizer()
    tokens = word_tokenize(text)
    for i in range(len(tokens)):
        lemma_word = lemmatizer.lemmatize(tokens[i])
        tokens[i] = lemma_word
    return " ".join(tokens)

def remove_punctuation(text):
    return text.translate(str.maketrans('', '', string.punctuation))

def remove_stopwords(text):
    removed = []
    stop_words = list(stopwords.words("english"))
    tokens = word_tokenize(text)
    for i in range(len(tokens)):
        if tokens[i] not in stop_words:
            removed.append(tokens[i])
    return " ".join(removed)

def remove_extra_white_spaces(text):
    single_char_pattern = r'\s+[a-zA-Z]\s+'
    without_sc = re.sub(pattern=single_char_pattern, repl=" ", string=text)
    return without_sc
     


# In[3]:


def augmentMyData(df, augmenter, repetitions=1, samples=150):
    augmented_texts = []
    # select only the minority class samples
    aug_df = df[df['label'] == 4].reset_index(drop=True) # removes unecessary index column
    for i in tqdm(np.random.randint(0, len(aug_df), samples)):
        # generating 'n_samples' augmented texts
        for _ in range(repetitions):
            augmented_text = augmenter.augment(aug_df['Content'].iloc[i])
            augmented_texts.append(augmented_text)
    
    data = {
        'label': 4,
        'Content': augmented_texts,
        'Category': 'Battery/Critical Minerals' 
    }
    aug_df = pd.DataFrame(data)
    df = shuffle(df.append(aug_df).reset_index(drop=True))
    return df
     


# In[4]:


def tokenize_text(text):
    #k=k+1
    #print("index of text_____________", text[:10])
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)
    return tokenizer.encode_plus(
        text,                      
        add_special_tokens=True,   
        max_length=64,             
        pad_to_max_length=True,    
        return_attention_mask=True,
        return_tensors='tf',truncation=True      
    )


# In[7]:

def trainingdata():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["NeoLith"]
    mycol = mydb["articleCollection"]
    data = list(mycol.find({},{'_id': 0, "url_name": 1, "content": 1, "category": 1}))


    label_map={'Market':0,'Academic/Enabling Technology':1,'Lithium Resources':2,'Extraction/Competitors':3,'Battery/Critical Minerals':4,}

    #labels = ['Market','Academic/Enabling Technology','Lithium Resources','Extraction/Competitors','Battery/Critical Minerals']


    #print(data[10])

    dataset = pd.DataFrame()
    cat_list=[]
    content_list=[]
    markets = ['Market','Extraction/Competitors','Academic/Enabling Technology','Lithium Resources','Battery/Critical Minerals']    
    for x in data:
        add_row=[]
        if  x.get('content')!=None:

            tok_text = str(x.get('content'))
            tok_text = tok_text[2:len(tok_text)-2]
            tok_text = clean1(tok_text)
            category = str(x.get('category'))
            category=category[2:len(category)-2]
            url= str(x.get('url_name'))

            tok_text = convert_to_lower(tok_text)
            tok_text = remove_numbers(tok_text)
            #tok_text = remove_punctuation(tok_text)
            tok_text = remove_stopwords(tok_text)
            tok_text = remove_extra_white_spaces(tok_text)
            tok_text = lemmatizing(tok_text)

            content_list.append([tok_text,category,url])


    dataset = pd.DataFrame(content_list, columns=['Content','Category','URL'])
    dataset = dataset.loc[dataset['Category'].isin(markets)]
    dataset['label'] = dataset['Category'].map(label_map)
    #print(dataset.head())
    return dataset

def predictData():
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["NeoLith"]
    mycol = mydb["RunTotal288"]
    data = list(mycol.find({},{'_id': 0, "url_name": 1, "content": 1}))
    
    dataset = pd.DataFrame()
    cat_list=[]
    content_list=[]
    #markets = ['Market','Extraction/Competitors','Academic/Enabling Technology','Lithium Resources','Battery/Critical Minerals']    
    for x in data:
        add_row=[]
        if  x.get('content')!=None:

            tok_text = str(x.get('content'))
            tok_text = tok_text[2:len(tok_text)-2]
            tok_text = clean1(tok_text)
            url= str(x.get('url_name'))

            tok_text = convert_to_lower(tok_text)
            tok_text = remove_numbers(tok_text)
            tok_text = remove_stopwords(tok_text)
            tok_text = remove_extra_white_spaces(tok_text)
            tok_text = lemmatizing(tok_text)
            content_list.append([tok_text,url])


    predict_dataset = pd.DataFrame(content_list, columns=['Content','URL'])
    #print(predict_dataset.head())
    return predict_dataset

def encodeData(dataset):    
    
    aug_df=dataset
    #print("check1_________in function________",aug_df.head())
    augmenter = aug.ContextualWordEmbsAug(model_path='bert-base-uncased', action="insert")


    #aug_df = augmentMyData(dataset, augmenter, samples=150)
    

    #print("Original: ", dataset.shape)
    #print("Augmented: ", aug_df.shape)



    aug_df['Content'].replace('',np.nan,inplace=True)

    aug_df.dropna(subset=['Content'], inplace=True)

    aug_df.reset_index(drop=True,inplace=True)


    #print("check2_________in function________",aug_df.head())
    
    from sklearn.model_selection import train_test_split
    import torch
    from transformers import BertTokenizer, BertForSequenceClassification
    from torch.utils.data import DataLoader, Dataset
    import tensorflow_addons as tfa



    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)
    
    #print("sample____________",aug_df['Content'][1])

    enc_text=[]
    for i in range(0,len(aug_df['Content'])):
        try:
            enc_text.append(tokenize_text(aug_df['Content'][i]))
        except:
            enc_text.append(np.nan)

    aug_df['encoded_text'] = enc_text

    aug_df.dropna(subset=['encoded_text'], inplace=True)

    aug_df.reset_index(drop=True,inplace=True)
    
    #print("encoded_____________",aug_df.head())
    
    return aug_df
    
    


# In[10]:


import pandas as pd
import numpy as np
import re
import nltk
import string
import statistics
import transformers
import pymongo
import spacy
import csv
import unicodedata
import Levenshtein
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from transformers import pipeline
from decimal import Decimal
from spacy.tokens import Doc, Span, Token
from spacy.language import Language
nltk.download('stopwords')
nltk.download('punkt')

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

def trainingCapex():
    
     
    model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

    capex_sentences = ["With a capital cost of $852, the new Argentinean mine is expected to produce 40,000 tonnes of battery-grade lithium annually. Commissioning of the new mine is underway, and it is expected to be in production in the first quarter of this year.",
    "The Thacker Pass mine’s capital cost is estimated at $1 billion – with $581 million for the first phase.",
    "Australia to set up $1.5 billion [~A$2 billion] loan facility for critical minerals projects.",
    "CATL announces $5 billion JV battery recycling facility, AACE Class 5 Total CAPEX estimate of US$870 Million including conservative 25% contingency of direct capital costs.",
    "Sayona closes acquisition of 60% stake in Moblan Lithium Project, Québec for US$86.5 million, following successful A$100 million capital raising.",
    "I found another source quoting the upfront CapEx at $547m, which includes $126M in contingency.",
    "Bolivian President Luis Arce said CBC would invest over $1 billion in the project's first stage, boosting infrastructure, roads and conditions needed to start up plants the country hopes will one day produce lithium cathodes and batteries.",
    "The initiatives have piqued the interest of billionaires like Mukesh Ambani, whose Reliance Industries Ltd. is building an EV battery facility as part of a broader $76 billion push into clean energy.",
    "On Wednesday, Suzuki Motor Corp. President  Toshihiro Suzuki said that the company plans to invest 100 billion rupees in EV and battery manufacturing in India.",
    "Total capital expenditure is pegged at $383.6 million, with lithium production starting in 2026, though Cleantech’s management continues to target a start of operations in late 2025.",
    "General Motors Co. last week invested $650 million in Lithium Americas Corp., which hopes to produce 80,000 metric tons of lithium from a mine in Nevada — equivalent to more than 10% of global production last year.",
    "At full build-out, with estimated average production over 20 years of 30,000 tonnes per annum of lithium, the direct capital costs are estimated at US$532 million, with indirect costs of US$205 million. A contingency of 25% was applied to direct costs (US$133 million) to yield an estimated all-in capital cost of US$870 million.",
    "Earlier this year, in July, Arena announced the closing of the agreement to purchase 100% of the SDLP project for a total of ~$22 million. This is less than a fourth of what Litica paid for ownership of a project that shares a salar with the SDLP project.",
    "In a bid to meet the growing demand for lithium products, LMN Lithium Ltd. unveiled plans for a 890 million dollar investment in its production facilities.",
    "To enhance its lithium production capabilities, the company has allocated a significant portion of its budget towards upgrading its mining equipment and adopting advanced extraction technologies. This entire investments can result to a dollar value of about 3,000 million and can also go upto 4,500 million dollar.",
    "capital cost",
    "capex",
    "installing industries"
    "build infrastructure",
    "Buying fixed assets.",
    "Expansion of buildings.",
    "Purchasing vehicles.",
    "Adding to the asset’s value through upgrading.",
    "capital expenditure",
    "capital expense"]

    opex_sentences = ["20-year mine-life producing average 30,000 tonnes per year battery-quality lithium [LHM], Operating costs US$2,599 per tonne battery quality lithium",
    "Average annual production 32.3 million kg (32,300 tonnes) LCE, Cash operating cost $5,974/tonne LCE, All-in sustaining cost $6,057/tonne LCE,",
    "According to measured indicated resources the battery grade lithium operational life is 30 years, The study estimated accumulated net cashflows tax including royalties $6.3 billion generated with operating cost $3,875 per tonne.",
    "The demonstration plant located in Union County, Arkansas with the all-in operating cost $2,599 per tonne lithium one lowest reported industry owing two key factors location-specific",
    "Global Lithium Corporation, a leading player in the lithium industry, revealed plans for a series of opex investments across its mining operations. With a total investment of $300 million, the company aims to optimize its lithium extraction processes, improve operational efficiencies, and enhance sustainability practices.",
    "In response to the surge in demand for electric vehicles, PQR Auto Manufacturing has made substantial opex investments in securing a stable supply of lithium. The company has entered into long-term agreements with lithium producers by ensuring a consistent and reliable lithium supply chain, PQR Auto Manufacturing aims to mitigate risks associated with raw material availability and maintain its production capabilities. They plan to invest very close to $5,500 per metric tonne of LCE.",
    "By adopting these measures, manufacturers not only reduce their operational expenses but also contribute to environmental sustainability, aligning their businesses with evolving market demands. The same type of trend is being observed wherein the prices soar from $500 per tonne to $800 per tonne to meet the daily expenses.",
    "As companies grapple with disruptions in raw material availability, transportation challenges, and labor shortages they are diversifying their supply chains, exploring local sourcing options, and implementing risk management strategies to ensure business continuity while minimizing operational expenses.",
    "The prices are rising everyday and today it can be estimated that to operate a 10,000 metric tonnes of lithium plant we need to have $4,500 per day in hand.",
    "The opex would grow at a pace slightly faster than prospective loan growth but slower than past opex growth. We value the bank at 0.8x FY24 P/BV for an FY24E/25E RoE profile of 8.6%/10.8%", 
    "The lithium industry also plan to keep a track of their investments around operating the daily output of lithium, currently it is estimated to be $9,800 per metric tonne in US",
    "operating costs",
    "operational expense",
    "opex",
    "License fees",
    "accounting and legal fees",
    "office supply costs",
    "rent, repair and maintenance costs",
    "day-to-day expenses",
    "recurring expenses",
    "expenditure on operations",
    "As a growing technology startup, we need to closely monitor our operational expenses to ensure sustainable growth. Our operational expenses include various components such as employee salaries, office rent, utilities, software subscriptions, and marketing expenses.",
    "Regularly reviewing our operational expenses helps us identify potential areas of optimization and cost reduction, allowing us to maximize profitability while maintaining the quality of our products and services.",
    "In the lithium market, operational expenditure refers to the costs associated with the day-to-day operations of lithium mining, processing, and production. These expenses encompass various aspects of the lithium supply chain, including exploration, extraction, processing, refining, and transportation.",
    "In lithium mining, operational expenses include exploration activities such as geological surveys, drilling, and sampling. Once a lithium deposit is identified, costs are incurred for mining operations, such as excavation, blasting, and ore extraction. These activities require heavy machinery, labor, and energy, which contribute to the overall operational expenditure.",            
    "advertising costs",
    "legal fees",
    "insurance fees",
    "telephone and other overheads",
    "property taxation expenses",
    "vehicle fuel and repair costs",
    "salary and wages",
    "raw materials and supplies",
    "revenue expenditure",
    "operating expenditure",
    "Apple reported the total cost of sales (or cost of goods soled) was $213 billion, while total operating expenses were $43.9 billion"
    ] 

    neither_sentences = ["In contrast ternary cathode materials, graphite prices drifted lower year thanks weakness steel industry, although wide disparities grades, According Benchmarks Flake Graphite Price Assessment October, China FOB 94-95% purity -100 Mesh sizes 31% past year, last trading $765 tonne +100 Mesh prices hardly moved period exchange hands $890 tonne,  Benchmark also tracks price value added products uncoated spherical graphite (99.95%, 15 micron) risen 10% 2022 average $3,065 tonne, Both mined synthetic graphite market turning point, said Miller",
    "If lithium continues to sell at $47,000 per metric ton a lot of things can be commercial. If it drops back to $10,000 a ton then a lot of those things that are commercial will just get wiped out.",
    "Bloomberg reported on April 28 that a 5,000-ton cargo of partly-processed lithium sold for a top bid of $5,650 per ton, 140% above the “insane” prices that prevailed when Musk issued his complaint. The next day, Reuters reported on rising demand and growing shortages of lithium and other critical minerals in Europe, saying that, as in the U.S.",
    "The companys highest quarterlysales, combined above-average lithium prices, helped achieve net profit $1.1 billion period, Revenue surged four times year-on-year $2.95 billion, lithium revenues growing 12 times, Prices battery metal jumped record levels Q3 $56,000 per tonne, said, Despite market fearsover possible cooling-off Chinas two-year lithium buying spree",
    "Prices for the batteries that power everything from smartphones to cars rose in 2022 for the first time since research firm BloombergNEF started tracking them — and they won’t likely drop next year. The global average price for lithium battery packs climbed 7% to $151 per kilowatt-hour, according to BNEF’s annual battery price survey.",
    "For lithium that change happened in 2018, which, for one, shows just to what extent the EV industry is still in its infancy, and two, just what an impact the additional demand from EVs will have on raw material prices. Benchmark Source, a new service from the London-HQed battery supply chain research firm, reports that lithium prices in China touched a fresh record high mid-November, just shy of $80,000 a tonne.",
    "Fastmarkets (formerly Metal Bulletin) reports 99.5% lithium battery grade spot midpoint prices cif China, Japan and Korea of US$22.50/kg (US$22,500/t), and min 56.5% lithium battery grade spot midpoint prices cif China, Japan and Korea of US$24.00/kg (US$24,000/t).",
    "Most automakers pay a negotiated price for lithium that can vary greatly from spot prices, which are trading this month near $77,500 per tonne but as recently as 2020 were trading near $6,750, according to data from Fastmarkets.",
    "What's starting to happen now is those prices are remaining sustained high. Both carbonate and hydroxide on a spot delivered basis are over $30 per kilogram. And what you're now starting to see around the rest of the world is, most of the rest of the world works on a contracted basis which lags that, is those contract prices around the world for various producers in the industry is starting to march up towards that high watermark that you're seeing in the spot market in China.",
    "lithium prices had been dropping since a peak in 2018 of around $15,000 per ton to half that price by the end of 2020, according to Edison Group. But, in large part due to the demand for EVs, the price has been growing all through 2021, hitting over $25,000 per ton by the end of last year, and now reaching over $40,000 per ton."
    "Sales volumes for lithium and derivatives totaled 41,000 tonnes, the highest quarterly volume ever reported by the company, SQM said in its earnings report. Our positive results in the lithium market were due to sales volumes and prices significantly above average, the company said. SQM also sells industrial chemicals. Average lithium prices rose to record levels during the quarter at more than $56,000 per tonne, the company said.",
    "On March 8, the nickel prices surged by 66% within a day to $48,078 at 5:42 am. Later on, the prices saw a sudden surge of 250% to over $100,000 per tonne around 6 a.m. The main reason for this sudden price hike was the Russian invasion of Ukraine. The prices spread mayhem in the market as there was a giant short position held by Xiang Guangda, the founder of Tsingshan Holding Group.",
    "Lithium also represents a big source of jobs and tax dollars for one of the poorest parts of California, where the median household income is 40% below the state average. In its excitement for the resource, the state of California has already set a tax of $400 per ton for the first 20,000 tons of Salton Sea lithium to help restore the deeply distressed area, where residents face high lung disease rates due to toxic dust the wind has picked up from the bed of the shrinking sea."
    ]

    capex_embeddings = model.encode(capex_sentences)
    opex_embeddings = model.encode(opex_sentences)
    neither_embeddings = model.encode(neither_sentences)

    capex_avg_embedding1 = np.mean(capex_embeddings, axis=0)
    opex_avg_embedding1 = np.mean(opex_embeddings, axis=0)
    neither_avg_embedding1 = np.mean(neither_embeddings, axis=0)

    capex_avg_embedding = capex_avg_embedding1.reshape(1, -1)
    opex_avg_embedding = opex_avg_embedding1.reshape(1, -1)
    neither_avg_embedding = neither_avg_embedding1.reshape(1, -1)
    
    return capex_avg_embedding, opex_avg_embedding, neither_avg_embedding

def getClassification():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['artilcleDatabase']
    collection = db['articleCollection']

    data = list(collection.find({},{'_id': 0, "url_name": 1, "content": 1}))
    sen_num = []
    capex_score = []
    opex_score = []
    neither_score =[]
    cl =[]
    sen_list = []
    url_list = []
    for x in data:
        url = x.get('url_name')
        draft_text=[]
        if  x.get('content')!=None:
            tok_text = str(x.get('content'))
            tok_text = tok_text[1:len(tok_text)-1]
            tok_text = clean1(tok_text)
            draft_text = re.split(r'(?<!\d\.\d)(?<![A-Za-z]\$)\.(?!\d)', tok_text)
            sen_number = 0

            for sentence in draft_text:
                #sentence = clean1(sentence)
                input_embedding = model.encode([sentence])
                capex_similarity = (cosine_similarity(input_embedding, capex_avg_embedding))[0][0]
                opex_similarity = (cosine_similarity(input_embedding, opex_avg_embedding))[0][0]
                neither_similarity = (cosine_similarity(input_embedding, neither_avg_embedding))[0][0]

                if ((capex_similarity > 0.4) | (opex_similarity > 0.4)):
                    if capex_similarity > opex_similarity:
                        classification = "CAPEX"
                    else:
                        classification = "OPEX"

                else:
                    classification = "Neither"

                cl.append(classification)
                neither_score.append(neither_similarity)
                capex_score.append(capex_similarity)
                opex_score.append(opex_similarity)
                sen_num.append(sen_number)
                sen_list.append(sentence)
                url_list.append(url)
                sen_number = sen_number + 1
                
    df_final = pd.DataFrame()
    df_final["URL"] = url_list
    df_final["Sentence"] = sen_list
    df_final["Line Number"] = sen_num
    df_final["Classification"] = cl
    df_final["Capex_Score"] = capex_score
    df_final["Opex_Score"] = opex_score
    df_final["Neither_Score"] = neither_score
    
    return df_final

def getMoney():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['NeoLith']
    collection_entity = db['total_content_Run228New']
    df_entity = pd.DataFrame(list(collection_entity.find()))

    df_entity_filtered = df_entity[df_entity['Type'] == 'MONEY']
    
    return df_entity_filtered


def find_mode_classification(url, line_number, window_size=2):
    min_ln = max(line_number - window_size, 0)
    max_ln = line_number + window_size
    max_ln_url = df_final[df_final['URL'] == url]['Line Number'].max()
    if max_ln_url is not None:
        max_ln = min(max_ln_url, max_ln)
    df_filtered = df_final[(df_final['URL'] == url) & (df_final['Line Number'].between(min_ln , max_ln))]
    if len(df_filtered) > 0:
        mode_classification = statistics.mode(df_filtered['Classification'])
        return mode_classification
    else:
        return None

def applyClassification(df_entity_filtered):
    df_entity_filtered['Classification'] = df_entity_filtered.apply(
    lambda row: find_mode_classification(row['URL'], int(row['Line Number'])), axis = 1)
    
    return df_entity_filtered

def mongoPushCapex(df_entity_filtered):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['NeoLith']
    collection_new = db['CapexOpexClassificationNew']
    data_class = df_entity_filtered.to_dict(orient='records')
    collection_new.insert_many(data_class)
    
    


# In[11]:


#!/usr/bin/env python
# coding: utf-8

# In[1]:


import torch
import pandas as pd
import pymongo
from transformers import BertTokenizer
from imblearn.over_sampling import ADASYN
from collections import Counter
import nlpaug.augmenter.word.context_word_embs as aug

import tensorflow as tf
from transformers import BertTokenizer, TFBertForSequenceClassification

import re
import string
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import nltk

from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk import word_tokenize

from sklearn.utils import shuffle
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report


# In[6]:


import re
import unicodedata
from nltk.tokenize import sent_tokenize, word_tokenize
def clean1(x, remlist = ["\\n", 'xa0','\\t',"' , '",'\  "',"', '","\\n","\\t","\\xa0",'"',"\n"]):
#     res = ast.literal_eval(x)
#     res = ''.join(map(str, res))
    
    sen = ""
    x = unicodedata.normalize('NFKD',x)
    
    for erm in remlist:
        s1 = x.replace(erm,'')
    s1 = s1.replace("  ","")
    s1 = s1.replace("\xa0"," ")
    s1 = s1.replace("\u200e"," ")
    s1 = s1.replace("', '","")
    s1 = s1.replace(".\n","")

    return s1


# In[2]:


def convert_to_lower(text):
    return text.lower()

def remove_numbers(text):
    number_pattern = r'\d+'
    without_number = re.sub(pattern=number_pattern, repl=" ", string=text)
    return without_number

def lemmatizing(text):
    lemmatizer = WordNetLemmatizer()
    tokens = word_tokenize(text)
    for i in range(len(tokens)):
        lemma_word = lemmatizer.lemmatize(tokens[i])
        tokens[i] = lemma_word
    return " ".join(tokens)

def remove_punctuation(text):
    return text.translate(str.maketrans('', '', string.punctuation))

def remove_stopwords(text):
    removed = []
    stop_words = list(stopwords.words("english"))
    tokens = word_tokenize(text)
    for i in range(len(tokens)):
        if tokens[i] not in stop_words:
            removed.append(tokens[i])
    return " ".join(removed)

def remove_extra_white_spaces(text):
    single_char_pattern = r'\s+[a-zA-Z]\s+'
    without_sc = re.sub(pattern=single_char_pattern, repl=" ", string=text)
    return without_sc
     


# In[3]:


def augmentMyData(df, augmenter, repetitions=1, samples=150):
    augmented_texts = []
    # select only the minority class samples
    aug_df = df[df['label'] == 4].reset_index(drop=True) # removes unecessary index column
    for i in tqdm(np.random.randint(0, len(aug_df), samples)):
        # generating 'n_samples' augmented texts
        for _ in range(repetitions):
            augmented_text = augmenter.augment(aug_df['Content'].iloc[i])
            augmented_texts.append(augmented_text)
    
    data = {
        'label': 4,
        'Content': augmented_texts,
        'Category': 'Battery/Critical Minerals' 
    }
    aug_df = pd.DataFrame(data)
    df = shuffle(df.append(aug_df).reset_index(drop=True))
    return df
     


# In[4]:


def tokenize_text(text):
    #k=k+1
    #print("index of text_____________", text[:10])
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)
    return tokenizer.encode_plus(
        text,                      
        add_special_tokens=True,   
        max_length=64,             
        pad_to_max_length=True,    
        return_attention_mask=True,
        return_tensors='tf',truncation=True      
    )


# In[7]:

def trainingdata():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["NeoLith"]
    mycol = mydb["articleCollection"]
    data = list(mycol.find({},{'_id': 0, "url_name": 1, "content": 1, "category": 1}))


    label_map={'Market':0,'Academic/Enabling Technology':1,'Lithium Resources':2,'Extraction/Competitors':3,'Battery/Critical Minerals':4,}

    #labels = ['Market','Academic/Enabling Technology','Lithium Resources','Extraction/Competitors','Battery/Critical Minerals']


    #print(data[10])

    dataset = pd.DataFrame()
    cat_list=[]
    content_list=[]
    markets = ['Market','Extraction/Competitors','Academic/Enabling Technology','Lithium Resources','Battery/Critical Minerals']    
    for x in data:
        add_row=[]
        if  x.get('content')!=None:

            tok_text = str(x.get('content'))
            tok_text = tok_text[2:len(tok_text)-2]
            tok_text = clean1(tok_text)
            category = str(x.get('category'))
            category=category[2:len(category)-2]
            url= str(x.get('url_name'))

            tok_text = convert_to_lower(tok_text)
            tok_text = remove_numbers(tok_text)
            #tok_text = remove_punctuation(tok_text)
            tok_text = remove_stopwords(tok_text)
            tok_text = remove_extra_white_spaces(tok_text)
            tok_text = lemmatizing(tok_text)

            content_list.append([tok_text,category,url])


    dataset = pd.DataFrame(content_list, columns=['Content','Category','URL'])
    dataset = dataset.loc[dataset['Category'].isin(markets)]
    dataset['label'] = dataset['Category'].map(label_map)
    #print(dataset.head())
    return dataset

def predictData():
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["NeoLith"]
    mycol = mydb["RunTotal288"]
    data = list(mycol.find({},{'_id': 0, "url_name": 1, "content": 1}))
    
    dataset = pd.DataFrame()
    cat_list=[]
    content_list=[]
    #markets = ['Market','Extraction/Competitors','Academic/Enabling Technology','Lithium Resources','Battery/Critical Minerals']    
    for x in data:
        add_row=[]
        if  x.get('content')!=None:

            tok_text = str(x.get('content'))
            tok_text = tok_text[2:len(tok_text)-2]
            tok_text = clean1(tok_text)
            url= str(x.get('url_name'))

            tok_text = convert_to_lower(tok_text)
            tok_text = remove_numbers(tok_text)
            tok_text = remove_stopwords(tok_text)
            tok_text = remove_extra_white_spaces(tok_text)
            tok_text = lemmatizing(tok_text)
            content_list.append([tok_text,url])


    predict_dataset = pd.DataFrame(content_list, columns=['Content','URL'])
    print(predict_dataset.head())
    return predict_dataset

def encodeData(dataset):    
    
    aug_df=dataset
    #print("check1_________in function________",aug_df.head())
    augmenter = aug.ContextualWordEmbsAug(model_path='bert-base-uncased', action="insert")


    #aug_df = augmentMyData(dataset, augmenter, samples=150)
    

    #print("Original: ", dataset.shape)
    #print("Augmented: ", aug_df.shape)



    aug_df['Content'].replace('',np.nan,inplace=True)

    aug_df.dropna(subset=['Content'], inplace=True)

    aug_df.reset_index(drop=True,inplace=True)


    #print("check2_________in function________",aug_df.head())
    
    from sklearn.model_selection import train_test_split
    import torch
    from transformers import BertTokenizer, BertForSequenceClassification
    from torch.utils.data import DataLoader, Dataset
    import tensorflow_addons as tfa



    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)
    
    #print("sample____________",aug_df['Content'][1])

    enc_text=[]
    for i in range(0,len(aug_df['Content'])):
        try:
            enc_text.append(tokenize_text(aug_df['Content'][i]))
        except:
            enc_text.append(np.nan)

    aug_df['encoded_text'] = enc_text

    aug_df.dropna(subset=['encoded_text'], inplace=True)

    aug_df.reset_index(drop=True,inplace=True)
    
    #print("encoded_____________",aug_df.head())
    
    return aug_df
    
    


# In[12]:


def push_data(pred_data):

    url_list = pred_data['URL'].tolist()
    label_list = pred_data['Category'].tolist()

    print(type(url_list))

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["NeoLith"]
    mycol = mydb["total_content_Run228New"]
    data = list(mycol.find({},{'_id': 0, "url_name": 1, "content": 1}))

    #markets = ['Market','Extraction/Competitors','Academic/Enabling Technology','Lithium Resources','Battery/Critical Minerals']    
    for x in data: 
        add_value ={}
        label=""
        ind=0

        if x.get('url_name') in url_list:
            #print("found")
            ind = url_list.index(x.get('url_name'))
            label = label_list[ind]

        if len(label)!=0:
            mycol.update_one({'url_name': x.get('url_name')}, {'$set': {'Category_predicted': label}})      


# In[13]:


def predictResults():
    import tensorflow as tf
    train_data = trainingdata()
    #print(train_data.head())
    train_encode = encodeData(train_data)
    #train_model = categoryModel(train_encode)
    #print("_____encoded_text__________",pred_data.head())


    aug_df = train_encode
    #print("train split____________________",aug_df.head())
    x_train_df,x_test_df,y_train_df,y_test_df = train_test_split(aug_df['encoded_text'], aug_df['label'], test_size=0.2,
                                                    random_state=42)


    model = TFBertForSequenceClassification.from_pretrained('bert-base-uncased', num_labels=5)

    optimizer = tf.keras.optimizers.Adam(learning_rate=3e-5, epsilon=1e-08)

    loss = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)

    metric = tf.keras.metrics.SparseCategoricalAccuracy('accuracy')
    #metric = tfa.metrics.F1Score(num_classes=5, average='weighted', name='f1_score')


    model.compile(optimizer=optimizer, loss=loss, metrics=[metric])

    train_input_ids = np.concatenate([encoding['input_ids'] for encoding in x_train_df])
    train_attention_masks = np.concatenate([encoding['attention_mask'] for encoding in x_train_df])


    test_input_ids = np.concatenate([encoding['input_ids'] for encoding in x_test_df])
    test_attention_masks = np.concatenate([encoding['attention_mask'] for encoding in x_test_df])


    history = model.fit((train_input_ids,train_attention_masks), y_train_df, epochs=5, batch_size=32, validation_data=((test_input_ids,test_attention_masks), y_test_df))

    #(len(train_input_ids),train_attention_masks)

    y_pred = (model.predict((test_input_ids, test_attention_masks)))


    #print(y_train_df)
    #print((test_input_ids))

    #output = model.predict(input_data)


    logits = y_pred.logits
    predicted_labels = tf.argmax(logits, axis=1)
    print(classification_report(y_test_df, predicted_labels))
    #return history



    predict_data = predictData()
    pred_data = encodeData(predict_data)

    test_input_ids_p = np.concatenate([encoding['input_ids'] for encoding in pred_data['encoded_text']])
    test_attention_masks_p = np.concatenate([encoding['attention_mask'] for encoding in pred_data['encoded_text']])

    y_predict_newData = model.predict((test_input_ids_p, test_attention_masks_p))
    logits_newData = y_predict_newData.logits
    predicted_labels_newData = tf.argmax(logits_newData, axis=1)
    #print(predicted_labels_newData)
    

    label_map = {0: 'Market',1: 'Academic/Enabling Technology',2: 'Lithium Resources',3: 'Extraction/Competitors',4: 'Battery/Critical Minerals'}
    predicted_labels = [label_map[number.numpy()] for number in predicted_labels_newData]
    pred_data['Category'] = predicted_labels
    push_data(pred_data)
    #print(predicted_labels)


# In[ ]:





# In[ ]:


##df,db,mydb = getUniqueUrls()
#SA = sentimentAnalysis()
#Summ = summary()
#Article_entities()
#similarityAnalysis()
#capex_avg_embedding, opex_avg_embedding, neither_avg_embedding = trainingCapex()
#df_final = getClassification()
#mongoPushCapex(applyClassification(getMoney()))
predictResults()


# In[ ]:


similarityAnalysis()

