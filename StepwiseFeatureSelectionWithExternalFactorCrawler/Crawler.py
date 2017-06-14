
# coding: utf-8



import sys
import requests 
import pandas as pd
import ConfigParser
import datetime
import csv
from bs4 import BeautifulSoup 
from pymongo import MongoClient
from mongoDB import mongoDB


class Crawler:
    
    def __init__(self,collection_name="Aggregate-Day",save_to_all_collection=True):
        self.df = pd.DataFrame()
        self.target_df = pd.DataFrame()
        self.exfeature = pd.DataFrame()
        
        self.config = ConfigParser.ConfigParser()
        self.config.read('config_crawler.ini')

        self.now=datetime.datetime.now()
        self.ip_address = self.config.get("Server","ip_address")
        self.from_year = self.config.get("Date","from_year")
        self.from_month = self.config.get("Date","from_month")
        self.from_day = self.config.get("Date","from_day")
        self.to_year=str(self.now.year)

        self.collection_name=collection_name
        self.svaetoAll=save_to_all_collection
        
        if(self.now.month<11):
            self.to_month=str("0"+str(self.now.month-1))
        else:
            self.to_month=str(self.now.month-1)
        self.to_day=str(self.now.day-1)
            
        self.mongo = mongoDB(self.ip_address, "External_factor")
    
    def date_replace(self,data_list):
        date=str(data_list).replace("Jan","01").replace("Feb","02").replace("Mar","03").replace("Apr","04").replace("May","05").replace("Jun","06")        .replace("Jul","07").replace("Aug","08").replace("Sep","09").replace("Oct","10").replace("Nov","11").replace("Dec","12").replace(",","")
        return date
    
    def Crawl_rate_exchange(self):

        currency_code = self.config.get("Exchange_Rate","currency_code").split(",")
        currency = self.config.get("Exchange_Rate","currency").split(",")
        index = 0
        for code in currency_code:
            data_list=[]
            doc = {}
            i=0
            count=1
            res = requests.get("http://www.exchangerate.com/past_rates.html?letter=&continent=&cid=239-USD&currency="+str(code)+"&last30=&date_from="+str(int(self.from_month)+1)+"-"+self.from_day+"-"+self.from_year+"&date_to="+str(int(self.to_month)+1)+"-"+self.to_day+"-"+self.to_year+"&action=Generate+Chart")
            soup = BeautifulSoup(res.text.encode("utf-8"), "html.parser")
            for texts in soup.findAll(face="Arial",size="-1"):
                data_list.append(texts.text)#data start from index 7 str(data_list[7][5:10])
                i+=1
                if i>=8 :
                    if count==1 :
                        doc = {"Month":int(self.date_replace(str(data_list[i-1][1:4])))}
                        doc ["Day"]=int(data_list[i-1][5:7])
                        doc["Year"]=int(data_list[i-1][8:12])
                        count=2
                    elif count==2 :
                        doc[str(currency[index])+"/USD"]=float(str(data_list[i-1]))
                        count=3
                    elif count==3 :
                        doc["USD/"+str(currency[index])]=float(str(data_list[i-1]))
                        doc["Date"] = str(doc.get("Year"))+"-"+str(doc.get("Month"))+"-"+str(doc.get("Day"))
                        count=1
                        if self.svaetoAll:
                            self.mongo.updateIfExist(self.collection_name,{"Year":doc.get("Year"),"Month":doc.get("Month"),"Day":doc.get("Day")},{"$set":doc})
                            self.mongo.updateIfExist("RateExchange-Day",{"Year":doc.get("Year"),"Month":doc.get("Month"),"Day":doc.get("Day")},{"$set":doc})
                        else:
                            self.mongo.updateIfExist("RateExchange-Day",{"Year":doc.get("Year"),"Month":doc.get("Month"),"Day":doc.get("Day")},{"$set":doc})
                        #print doc
                        doc.clear()
            index +=1
        
    def Crawl_Stock(self):
        company = self.config.get("Company_stock","company_code").split(",")
        company_name = self.config.get("Company_stock","company_name").split(",")
        
        name=0
        for com in company:	
            url="http://chart.finance.yahoo.com/table.csv?s="+com+"&a="+self.from_month+"&b="+self.from_day+"&c="+self.from_year+"&d="+self.to_month+"&e="+self.to_day+"&f="+self.to_year+"&g=d&ignore=.csv"

            csvFile = requests.get(url)
            output = open('table.csv', 'wb')
            output.write(csvFile.content)
            output.close()

            f = open('table.csv', 'r')
            for row in csv.DictReader(f):
                del(row['Open'])
                del(row['High'])
                del(row['Low'])
                del(row['Close'])
                row.setdefault(str(company_name[name])+'-Adj-Close', float(row['Adj Close']))
                del(row['Adj Close'])
                row.setdefault(str(company_name[name])+'-Volume', int(row['Volume']))
                del(row['Volume'])
                d=row["Date"].split("-")
                row["Year"]=int(d[0])
                row["Month"]=int(d[1])
                row["Day"]=int(d[2])
                if self.svaetoAll:
                    self.mongo.updateIfExist(self.collection_name,{"Year":row.get("Year"),"Month":row.get("Month"),"Day":row.get("Day")},{"$set":row})
                    self.mongo.updateIfExist("Stock-Day",{"Year":row.get("Year"),"Month":row.get("Month"),"Day":row.get("Day")},{"$set":row})
                else:
                    self.mongo.updateIfExist("Stock-Day",{"Year":row.get("Year"),"Month":row.get("Month"),"Day":row.get("Day")},{"$set":row})
            f.close()
            name+=1
            
    def Crawl_Index(self):
        company = self.config.get("Company_index","company_code").split(",")
        company_name = self.config.get("Company_index","company_name").split(",")
        
        name=0
        for com in company:	
            url="http://chart.finance.yahoo.com/table.csv?s="+com+"&a="+self.from_month+"&b="+self.from_day+"&c="+self.from_year+"&d="+self.to_month+"&e="+self.to_day+"&f="+self.to_year+"&g=d&ignore=.csv"

            csvFile = requests.get(url)
            output = open('table.csv', 'wb')
            output.write(csvFile.content)
            output.close()

            f = open('table.csv', 'r')
            for row in csv.DictReader(f):
                del(row['Open'])
                del(row['High'])
                del(row['Low'])
                del(row['Close'])
                row.setdefault(str(company_name[name])+'-Adj-Close', float(row['Adj Close']))
                del(row['Adj Close'])
                row.setdefault(str(company_name[name])+'-Volume', int(row['Volume']))
                del(row['Volume'])
                d=row["Date"].split("-")
                row["Year"]=int(d[0])
                row["Month"]=int(d[1])
                row["Day"]=int(d[2])
                if self.svaetoAll:
                    self.mongo.updateIfExist(self.collection_name,{"Year":row.get("Year"),"Month":row.get("Month"),"Day":row.get("Day")},{"$set":row})
                    self.mongo.updateIfExist("Index-Day",{"Year":row.get("Year"),"Month":row.get("Month"),"Day":row.get("Day")},{"$set":row})
                else:
                    self.mongo.updateIfExist("Index-Day",{"Year":row.get("Year"),"Month":row.get("Month"),"Day":row.get("Day")},{"$set":row})
            f.close()
            name+=1            
        
    def Set_Time(self):
        self.config.set("Date","from_year",self.to_year)
        self.config.set("Date","from_month",self.to_month)
        self.config.set("Date","from_day",self.to_day)
        self.config.write(open('Config_crawler.ini', 'wb'))
        
    def Crawl_AllExternal(self):
        self.Crawl_rate_exchange()
        self.Crawl_Index()
        self.Crawl_Stock()
        self.Set_Time()
    
if __name__ == '__main__':
    cr = Crawler()
    cr.Crawl_AllExternal()
    
       

