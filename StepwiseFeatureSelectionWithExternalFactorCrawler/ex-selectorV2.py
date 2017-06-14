# coding: utf-8

from  scipy.stats import pearsonr
import pandas as pd
import numpy as np
import datetime
import csv
from pymongo import MongoClient
from mongoDB import mongoDB
from sklearn.tree import DecisionTreeClassifier,DecisionTreeRegressor
from StepwiseRegressionV3 import StepwiseRegression as STP
from dateutil.relativedelta import relativedelta
import ConfigParser

#在Add_Target中自動將使用者的資料轉成合格的timestamp
#目前mapping是用date,如果沒設為1會mapp不到資料
#輸出的資料已經照timestamp排序,所以建議輸入照時間序排序過的資料
#     Y should contain timestamp
#     Y should be in dataframe columes = ['Date','Target']
class ExternalFactorSelection:
    
    # lag為想嘗試的最大期數, targetType為選擇判斷重要性的model(reg,regtree,clf)三種
    def __init__(self,lag=5,TargetType='reg'):

        self.lag=lag+1
        self.exfeature = pd.DataFrame()
        self.targetType=TargetType
        # self.Externalfactor_Day_collection = ['Index-Day','Stock-Day','RateExchange-Day']
        
        config = ConfigParser.ConfigParser()
        config.read('ex_selector.ini')

        self.Externalfactor_Day_collection = config.get("ExternalFactor_Collection","collection").split(",")
        self.economy_Month_collection = config.get("economy","collection").split(",")
        self.ndcData_Month_collection = config.get("ndcData","collection").split(",")
        self.statData_Month_collection = config.get("statData","Month_collection").split(",")
        self.statData_Year_collection = config.get("statData","Year_collection").split(",")
        self.domain_name = config.get("Server","ip_address")
    
    # 加入想要判別的traing data,必要欄位為'Date','Target' 
    #     TargetTimeDelta是資料本身的時間顆粒大小
    #     Y should contain timestamp
    #     Y should be in dataframe columes = ['Date','Target']   
    def Add_Target(self,Y,TargetTimeDelta):
        self.TargetTimeDelta = TargetTimeDelta
        self.target_df = Y
        self.target_df['Date']= pd.to_datetime(self.target_df['Date'])
        self.target_df = self.target_df.sort_values('Date')
        self.target_df.index=range(self.target_df.shape[0])
        month = list()
        day = list()
        year = list()
        for i in self.target_df.Date:
            day.append(i.day)
            month.append(i.month)
            year.append(i.year)
        
        if TargetTimeDelta == 'Day':
            self.__AddDayExternal()
        elif TargetTimeDelta == 'Month':
            self.target_df['Date']= pd.to_datetime(pd.DataFrame(data={'year':year,'month':month,'day':[1]*len(self.target_df)}))
            self.__AddMonthExternal()
        elif TargetTimeDelta == 'Year':
            self.target_df['Date']= pd.to_datetime(pd.DataFrame(data={'year':year,'month':[1]*len(self.target_df),'day':[1]*len(self.target_df)}))
            self.__AddYearExternal()

        self.__duplicate_targetD = list()
        for i in self.target_df['Date'].unique():
            self.__duplicate_targetD.append( self.target_df['Date'].value_counts(sort=False)[i])
    
    # Date is a dataframe contain a column named 'Date'
    def Add_PredictionDate(self,Date):
        if len(self.exfeature.columns)==0:
            print 'No feature selected'
            return
        self.Feature = pd.DataFrame()
        self.PredictDate=Date
        self.PredictDate['Date']= pd.to_datetime(self.PredictDate['Date'])
        month = list()
        day = list()
        year = list()
        for i in self.PredictDate.Date:
            day.append(i.day)
            month.append(i.month)
            year.append(i.year)
        
        self.PredictDate = self.PredictDate.sort_values('Date')
        self.PredictDate.index = range(len(self.PredictDate))

        if  self.TargetTimeDelta == 'Day':
            self.__CalculateDuplicatedDate()
            self.__AddDayFeature()
            for i in self.DelayIndex:
                self.__MappingDayFeature(int(i))
            self.Feature = self.Feature[self.exfeature.columns] 
        elif self.TargetTimeDelta == 'Month':
            self.PredictDate['Date']= pd.to_datetime(pd.DataFrame(data={'year':year,'month':month,'day':[1]*len(self.PredictDate)}))
            self.__CalculateDuplicatedDate()
            self.__AddMonthFeature()
            for i in self.DelayIndex:
                self.__MappingMonthFeature(int(i))
            self.Feature = self.Feature[self.exfeature.columns] 

        elif  self.TargetTimeDelta == 'Year':
            self.PredictDate['Date']= pd.to_datetime(pd.DataFrame(data={'year':year,'month':[1]*len(self.PredictDate),'day':[1]*len(self.PredictDate)}))
            self.__CalculateDuplicatedDate()
            self.__AddYearFeature()
            for i in self.DelayIndex:
                self.__MappingYearFeature(int(i))
            self.Feature = self.Feature[self.exfeature.columns]  

    # mapping by lag
    # ex: lag=5 --> try lag 1,2,3,4,5
    def Map(self):
        if self.TargetTimeDelta=='Day':
            for i in xrange(1,self.lag):
                print 'map lag',i
                self.__MappingDay(i)
        elif self.TargetTimeDelta=='Month':
            for i in xrange(1,self.lag):
                print 'map lag',i
                self.__MappingMonth(i)
        elif self.TargetTimeDelta=='Year':
            for i in xrange(1,self.lag):
                print 'map lag',i
                self.__MappingYear(i)

    # to fit train target and maped data 
    def fit(self):
       
        self.exfeature=self.exfeature[self.exfeature.columns[(self.exfeature != 0).any()]]
        self.exfeature= self.exfeature.fillna(0.0).replace('',0).replace('NaNQ',0.0).astype(float)
        length = self.exfeature.shape[1]
        target = self.target_df['Target']
        print '-------------------------------------------------'
        print 'After mapping features number :'+str(length)# +'- Mapping finished time:'+str(datetime.datetime.now())
        
        if self.targetType=='reg' :
            # pearson r test
            insterest_column = list()
            for c in self.exfeature.columns:
                if pearsonr(self.exfeature[c],target.astype(float))[1]<0.05:
                    insterest_column.append(c)
            self.exfeature = self.exfeature[insterest_column]
            print 'After Pearson features number :'+str(len(insterest_column))#+'- Pearson finished time:'+str(datetime.datetime.now())
            x = self.exfeature.as_matrix()
            y = self.target_df['Target'].as_matrix()
            #  stepwise regression
            St = STP(x,y,P=0.05)
            St.fit_Stepwise(KS_feature=False)
            if St.GetfeatureIndex()==None:
                return
            print 'After StepWise features number :'+str(len(St.GetfeatureIndex()))#+'- StepWise finished time:'+str(datetime.datetime.now())
            print '-------------------------------------------------'
        elif self.targetType=='regtree':
            insterest_column = list()
            for c in self.exfeature.columns:
                if pearsonr(self.exfeature[c],target.astype(float))[1]<0.05:
                    insterest_column.append(c)
            self.exfeature = self.exfeature[insterest_column]
            print 'After Pearson features number :'+str(len(insterest_column))
            if len(insterest_column)==0:
                print 'No feature selected'
                return
            regt = DecisionTreeRegressor()
            regt.fit(self.exfeature,self.target_df['Target'].astype(float))
            threshold = regt.feature_importances_[regt.feature_importances_>0]
            column_index=list()
            for j in xrange(len(regt.feature_importances_)):
                if regt.feature_importances_[j] in threshold :
                    column_index.append(j)
            print 'After RegTree features number :'+str(len(regt.feature_importances_[regt.feature_importances_>0]))
            print '-------------------------------------------------'
        else:
            clf = DecisionTreeClassifier()
            clf.fit(self.exfeature,self.target_df['Target'].astype(float))
            threshold = clf.feature_importances_[clf.feature_importances_>0]
            column_index=list()
            for j in xrange(len(clf.feature_importances_)):
                if clf.feature_importances_[j] in threshold :
                    column_index.append(j)
            print 'After CART features number :'+str(len(clf.feature_importances_[clf.feature_importances_>0]))
            print '-------------------------------------------------'
            
        # get significant index
        tempIndex=list()
        f = pd.DataFrame()

        print 'Selected features:'
        if self.targetType=='reg' :
            for i in St.GetfeatureIndex():
                tempIndex.append(list(self.exfeature)[i])
                print list(self.exfeature)[i]
            self.exfeature = St.Getfeature()
            self.exfeature.columns = tempIndex
        else:
            for i in column_index:
                tempIndex.append(list(self.exfeature)[i])
                print list(self.exfeature)[i]
                if f.shape[1]==0:
                    f=self.exfeature[[i]]
                else:
                    f=f.join(self.exfeature[[i]])    
            self.exfeature=f
            self.exfeature.columns=tempIndex
        
        self.AddFeatureIndex=list()
        self.DelayIndex=list()
        self.DbIndex = dict()

        for index in tempIndex:
            save = True
            i = -1
            while save:
                if index[i].isdigit():
                    i-=1
                else :
                    self.AddFeatureIndex.append(index[0:i])
                    self.DelayIndex.append(index[i+1:])
                    save = False
                
        self.DbIndex = dict((k,1) for k in self.AddFeatureIndex)
        self.DbIndex['_id'] = 0
        self.DelayIndex = set(self.DelayIndex)

    def __CalculateDuplicatedDate(self):
        self.__duplicate_targetD = list()
        for i in self.PredictDate['Date'].unique():
                self.__duplicate_targetD.append( self.PredictDate['Date'].value_counts(sort=False)[i])

    # to add data from mongo
    def __AddDayExternal(self):
        timedelta = datetime.timedelta(days=self.lag)
        start_time=str(self.target_df['Date'][0]-timedelta)[0:4]
        end_time=str(self.target_df['Date'][self.target_df.shape[0]-1])[0:4] 
        first = True
        
        client = MongoClient(self.domain_name)
        db = client['External_factor']
        for collection in self.Externalfactor_Day_collection:
            if first :
                first = False
                collect = db[collection]
                df = collect.find({"Year":{"$gte":int(start_time),"$lte":int(end_time)}})
                df = pd.DataFrame(list(df))#.drop('Year', 1).drop('Month', 1).drop('Day', 1)
        self.df = df
        self.df['Date'] = pd.to_datetime(pd.DataFrame(data={'Year':df['Year'],'Month':df['Month'],'Day':df['Day']})) 
        self.df = self.df.sort_values(['Date'])
        self.df = self.df.replace('',0).replace('-',0).fillna(0.0)
        self.df =self.df.drop('_id',1).drop('Year', 1).drop('Month', 1).drop('Day', 1)
        self.df.index = range(len(self.df)) 

    def __AddDayFeature(self):
        timedelta = datetime.timedelta(days=self.lag)
        start_time=str(self.PredictDate['Date'][0]-timedelta)[0:4]
        end_time=str(self.PredictDate['Date'][self.PredictDate.shape[0]-1])[0:4] 
        first = True
        self.DbIndex['Year']=1
        self.DbIndex['Month']=1
        self.DbIndex['Day']=1
        client = MongoClient(self.domain_name)
        db = client['External_factor']
        for collection in self.Externalfactor_Day_collection:
            if first :
                first = False
                collect = db[collection]
                df = collect.find({"Year":{"$gte":int(start_time),"$lte":int(end_time)}},self.DbIndex)
                df = pd.DataFrame(list(df))#.drop('Year', 1).drop('Month', 1).drop('Day', 1)

        self.df = df
        self.df['Date'] = pd.to_datetime(pd.DataFrame(data={'Year':df['Year'],'Month':df['Month'],'Day':df['Day']})) 
        self.df = self.df.sort_values(['Date'])
        self.df = self.df.replace('',0).replace('-',0).fillna(0.0)
        self.df =self.df.drop('Year', 1).drop('Month', 1).drop('Day', 1)
        self.df.index = range(len(self.df)) 
        
    def __AddMonthExternal(self):
        #month feature date to be year/month/1
        timedelta = datetime.timedelta(days=self.lag*31)
        start_time=str(self.target_df['Date'][0]-timedelta)[0:4]
        end_time=str(self.target_df['Date'][self.target_df.shape[0]-1])[0:4] 

        client = MongoClient(self.domain_name)
        db = client['ndcData']
        collect = db['month']
        df = collect.find({"year":{"$gte":start_time,"$lte":end_time}})
        df = pd.DataFrame(list(df))
        
        db2 = client['statData']
        collect2 = db2['month']
        df2 = collect2.find({"year":{"$gte":start_time,"$lte":end_time}})
        df2 = pd.DataFrame(list(df2))
       
        self.df = pd.merge(df,df2,on = ['year','month'],how ='outer')
        self.df = self.df.drop('_id_x',1).drop('_id_y',1)
        self.df = self.df.replace('',0).fillna(0.0).sort_values(['year','month'])
        self.df['Date'] = pd.to_datetime(pd.DataFrame(data={'year':self.df['year'],'month':self.df['month'],'day':[1]*len(self.df)})) 
        self.df.index = range(len(self.df)) 

    def __AddMonthFeature(self):
        timedelta = datetime.timedelta(days=self.lag*31)
        start_time=str(self.PredictDate['Date'][0]-timedelta)[0:4]
        end_time=str(self.PredictDate['Date'][self.PredictDate.shape[0]-1])[0:4] 
        self.DbIndex['year']=1
        self.DbIndex['month']=1
        
        client = MongoClient(self.domain_name)
        db = client['ndcData']
        collect = db['month']
        df = collect.find({"year":{"$gte":start_time,"$lte":end_time}},self.DbIndex)
        df = pd.DataFrame(list(df))
        
        db2 = client['statData']
        collect2 = db2['month']
        df2 = collect2.find({"year":{"$gte":start_time,"$lte":end_time}},self.DbIndex)
        df2 = pd.DataFrame(list(df2))
       
        self.df = pd.merge(df,df2,on = ['year','month'],how ='outer')
        self.df = self.df.replace('',0).fillna(0.0).sort_values(['year','month'])
        self.df['Date'] = pd.to_datetime(pd.DataFrame(data={'year':self.df['year'],'month':self.df['month'],'day':[1]*len(self.df)})) 
        self.df.index = range(len(self.df)) 

    def __AddYearExternal(self):
        timedelta = self.lag
        start_time=str(self.target_df['Date'][0].year-timedelta)
        end_time=str(self.target_df['Date'][self.target_df.shape[0]-1])
        
        
        client = MongoClient(self.domain_name)
        db = client['statData']
        collect = db['year']
        df = collect.find({"year":{"$gte":start_time,"$lte":end_time}})
        df = pd.DataFrame(list(df))

                
        self.df = df.drop('_id',1)
        self.df = self.df.replace('',0).fillna(0.0).sort_values(['year'])
        self.df['Date'] = pd.to_datetime(pd.DataFrame(data={'year':self.df['year'],'month':[1]*len(self.df),'day':[1]*len(self.df)})) 
        self.df.index = range(len(self.df)) 
    
    def __AddYearFeature(self):
        timedelta = self.lag
        start_time=str(self.target_df['Date'][0].year-timedelta)
        end_time=str(self.target_df['Date'][self.target_df.shape[0]-1])
        self.DbIndex['year']=1
        
        client = MongoClient(self.domain_name)
        db = client['statData']
        collect = db['year']
        df = collect.find({"year":{"$gte":start_time,"$lte":end_time}},self.DbIndex)
        df = pd.DataFrame(list(df))

        self.df = self.df.replace('',0).fillna(0.0).sort_values(['year'])
        self.df['Date'] = pd.to_datetime(pd.DataFrame(data={'year':self.df['year'],'month':[1]*len(self.df),'day':[1]*len(self.df)})) 
        self.df.index = range(len(self.df)) 

    # do map for different lag
    def __MappingDay(self,timedelta):
        # there is no data on weekend in mongo, therefore if date is on weekend no data 
        map_name = '-'+str(timedelta)
        timedelta = datetime.timedelta(days = timedelta)
        mapping_df = pd.DataFrame()
        t = 0 # index of each duplicated date list
        k = 0 # index of external data
        for i in self.target_df['Date'].unique():
            for r in xrange(k,len(self.df.Date)):
                # print self.df.loc[r,'Date'],pd.to_datetime(i),timedelta,pd.to_datetime(i)-timedelta
                if self.df.loc[r,'Date']==pd.to_datetime(i)-timedelta:
                    add_row = self.df.iloc[r]
                    k = r+1
                    mapping_df=mapping_df.append([add_row]*self.__duplicate_targetD[t], ignore_index=True)
                    t+=1
                    break 

        mapping_df.columns = [x+map_name for x in self.df.columns]
        mapping_df = mapping_df.drop('Date'+map_name,1)

        if self.exfeature.shape[0]==0:
            self.exfeature = mapping_df
            self.exfeature.index = range(self.exfeature.shape[0])
        else:
            mapping_df.index = self.exfeature.index
            self.exfeature = self.exfeature.join(mapping_df) 
      
    def __MappingDayFeature(self,timedelta):
        # there is no data on weekend in mongo, therefore if date is on weekend no data 
        map_name = '-'+str(timedelta)
        timedelta = datetime.timedelta(days = timedelta)
        mapping_df = pd.DataFrame()
        t = 0 # index of each duplicated date list
        k = 0 # index of external data
        for i in self.PredictDate['Date'].unique():
            for r in xrange(k,len(self.df.Date)):
                if self.df.loc[r,'Date']==pd.to_datetime(i)-timedelta:
                    add_row = self.df[r:r+1]
                    k = r+1
                    mapping_df=mapping_df.append([add_row]*self.__duplicate_targetD[t], ignore_index=True)
                    t+=1
                    break 

        mapping_df.columns = [x+map_name for x in self.df.columns]
        mapping_df = mapping_df.drop('Date'+map_name,1)
        
        if self.Feature.shape[0]==0:
            self.Feature = mapping_df
            self.Feature.index = range(self.Feature.shape[0])
        else:
            mapping_df.index = self.Feature.index
            self.Feature = self.Feature.join(mapping_df) 
    
    def __MappingMonth(self,timedelta):
        map_name = '-'+str(timedelta)
        timedelta = relativedelta(months=+timedelta)
        mapping_df = pd.DataFrame()
        t = 0 # index of each duplicated date list
        k = 0 # index of external data
        for i in self.target_df['Date'].unique():
            for r in xrange(k,len(self.df.Date)):
                if self.df.loc[r,'Date']==pd.to_datetime(i)-timedelta:
                    add_row = self.df[r:r+1]
                    k = r+1
                    mapping_df=mapping_df.append([add_row]*self.__duplicate_targetD[t], ignore_index=True)
                    t+=1
                    break 
    
        mapping_df.columns = [x+map_name for x in self.df.columns]
        mapping_df = mapping_df.drop('month'+map_name,1)
        mapping_df = mapping_df.drop('year'+map_name,1)
        mapping_df = mapping_df.drop('Date'+map_name,1)
        
        if self.exfeature.shape[0]==0:
            self.exfeature = mapping_df
            self.exfeature.index = range(self.exfeature.shape[0])
        else:
            mapping_df.index = self.exfeature.index
            self.exfeature = self.exfeature.join(mapping_df) 
              
    def __MappingMonthFeature(self,timedelta):
        map_name = '-'+str(timedelta)
        timedelta = relativedelta(months=+timedelta)
        mapping_df = pd.DataFrame()
        t = 0 # index of each duplicated date list
        k = 0 # index of external data
        for i in self.PredictDate['Date'].unique():
            for r in xrange(k,len(self.df.Date)):
                if self.df.loc[r,'Date']==pd.to_datetime(i)-timedelta:
                    add_row = self.df[r:r+1]
                    k = r+1
                    mapping_df=mapping_df.append([add_row]*self.__duplicate_targetD[t], ignore_index=True)
                    t+=1
                    break 
        mapping_df.columns = [x+map_name for x in self.df.columns]
        mapping_df = mapping_df.drop('month'+map_name,1)
        mapping_df = mapping_df.drop('year'+map_name,1)
        mapping_df = mapping_df.drop('Date'+map_name,1)

        if self.Feature.shape[0]==0:
            self.Feature = mapping_df
            self.Feature.index = range(self.Feature.shape[0])
        else:
            mapping_df.index = self.Feature.index
            self.Feature = self.Feature.join(mapping_df) 
    
    def __MappingYear(self,timedelta):
        map_name = '-'+str(timedelta)
        timedelta = relativedelta(years=+timedelta)
        mapping_df = pd.DataFrame()
        t = 0 # index of each duplicated date list
        k = 0 # index of external data
        for i in self.target_df['Date'].unique():
            for r in xrange(k,len(self.df.Date)):
                if self.df.loc[r,'Date']==pd.to_datetime(i)-timedelta:
                    add_row = self.df[r:r+1]
                    k = r+1
                    mapping_df=mapping_df.append([add_row]*self.__duplicate_targetD[t], ignore_index=True)
                    t+=1
                    break 

        mapping_df.columns = [x+map_name for x in self.df.columns]
        mapping_df = mapping_df.drop('year'+map_name,1)
        mapping_df = mapping_df.drop('Date'+map_name,1)
        
        if self.exfeature.shape[0]==0:
            self.exfeature = mapping_df
            self.exfeature.index = range(self.exfeature.shape[0])
        else:
            mapping_df.index = self.exfeature.index
            self.exfeature = self.exfeature.join(mapping_df) 

    def __MappingYearFeature(self,timedelta):
        map_name = '-'+str(timedelta)
        timedelta = relativedelta(years=+timedelta)
        mapping_df = pd.DataFrame()
        t = 0 # index of each duplicated date list
        k = 0 # index of external data
        for i in self.PredictDate['Date'].unique():
            for r in xrange(k,len(self.df.Date)):
                if self.df.loc[r,'Date']==pd.to_datetime(i)-timedelta:
                    add_row = self.df[r:r+1]
                    k = r+1
                    mapping_df=mapping_df.append([add_row]*self.__duplicate_targetD[t], ignore_index=True)
                    t+=1
                    break 

        mapping_df.columns = [x+map_name for x in self.df.columns]
        mapping_df = mapping_df.drop('year'+map_name,1)
        mapping_df = mapping_df.drop('Date'+map_name,1)
        
        if self.Feature.shape[0]==0:
            self.Feature = mapping_df
            self.Feature.index = range(self.Feature.shape[0])
        else:
            mapping_df.index = self.Feature.index
            self.Feature = self.Feature.join(mapping_df) 
    
    def Get_TrainFeature(self):
        if len(self.exfeature.columns)>0:
            return self.exfeature
        else:
            return

    def Get_TestFeature(self):
        if len(self.exfeature.columns)>0:
            return self.Feature
        else:
            return

if __name__ == '__main__':
    
    
#整理輸入資料格式
    target = pd.read_csv('DGTrainDate.csv')
    Y = pd.DataFrame([target['Date'],target['Label']]).T
    Y.columns = ['Date','Target']
#整理待預測的日期
    D = pd.read_csv('DGTestDate.csv')
    Date = pd.DataFrame(D)

    t = datetime.datetime.now()
    cr = ExternalFactorSelection(lag=4,TargetType='regtree')
    cr.Add_Target(Y,'Month')
    print 'Add full factors',str(datetime.datetime.now()-t)

    t1 = datetime.datetime.now()
    cr.Map()
    print 'Mapping Complete',str(datetime.datetime.now()-t1)
    
    t2 = datetime.datetime.now()
    cr.fit()
    print 'Model Complete',str(datetime.datetime.now()-t2)

    t3 = datetime.datetime.now()
    cr.Add_PredictionDate(Date)
    print 'Predict feature generate',str(datetime.datetime.now()-t3)
    print cr.Get_TestFeature().shape,cr.Get_TrainFeature().shape

    # print cr.Get_TrainFeature()
    # print cr.Get_TestFeature()
    cr.Add_PredictionDate(Date)
    print cr.Get_TrainFeature().shape,cr.Get_TestFeature().shape
    