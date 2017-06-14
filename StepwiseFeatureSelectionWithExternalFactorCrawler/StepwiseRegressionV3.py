
# coding: utf-8

# In[2]:

import numpy as np 
import pandas as pd
from scipy import stats
import statsmodels.api as sm
import sys
import math


# In[24]:

class StepwiseRegression(object):
    
    def __init__(self,X,y,N=20,P=0.15):
        # expected features number
        self.N=N 
        # Alpha-to-Remove features
        self.P=P
        
        i=0
        
        if type(X)==type(np.array([])) and type(y)==type(np.array([])):
            self.X=X#.astype(float)
            self.y=y.astype(float)
          
        #else:
            #print '--- Wrong input type (numpy array) ---'
            #sys.exit()
        if len(X)!=len(y):
            print '--- Wrong length of X or y ---'
            sys.exit()
        
        for x in X:
            if i==0:
                tmp = len(x)
                i=1
            if tmp!=len(x):
                print '--- Wrong length of X ---'
                sys.exit()
       
        self.delet = list()
        #for i in range(X.any(axis=0).size):   
            #if X.any(axis=0)[i]==False:
                #self.delet.append(i)
        #print 'All values are zero feature index '+str(self.delet)
        
    def fit_KS(self,P=0.05):
        sample = self.X.T
        index_list = []
        first = 0
        for i in range(sample.shape[0]):
            if stats.ks_2samp(sample[i], self.y)[1]<=P and first ==0:
                ks = np.array([sample[i]])
                first = 1
                index_list.append(i)
            elif stats.ks_2samp(sample[i], self.y)[1]<=P and first ==1:
                ks = np.vstack([ks, sample[i]])
                index_list.append(i)
        self.KS_index = index_list
        self.KS_feature = ks
        
    
    def fit_Stepwise(self,KS_feature=False):
        if KS_feature:
            self.X = self.KS_feature.T
            
        self.feature = np.array([])
        self.Index = []
        
        no_better_feature = False
        featureIndex = np.array([])
        featureNumber = 0
        featureIndexList =[]
        t = 0
        is_first_feature = True
        nextX = np.array([])
        localbestX = np.array([])
        while featureNumber<self.N and no_better_feature==False :
            max_tvalue = 0
            for i in range(self.X.shape[1]):
                
                if i not in self.delet and i not in featureIndexList :
                    
                    if is_first_feature:
                        X = self.X[0:,i]
                    else:
                        X = np.column_stack((nextX,self.X[0:,i]))                        
                    X = sm.add_constant(X)
                    res = sm.OLS(self.y,X).fit()
                
                    if res.pvalues[-1]<self.P and math.fabs(res.tvalues[-1])>max_tvalue and is_first_feature:
                        max_tvalue = math.fabs(res.tvalues[-1]) 
                        localbestX = X[0:,1:]
                        #------------------remove constants column
                        
                    elif res.pvalues[-1]<self.P and math.fabs(res.tvalues[-1])>max_tvalue and is_first_feature==False:
                        max_tvalue = math.fabs(res.tvalues[-1])                       
                        remove_feature = res.pvalues[1:] >self.P
                        remove_index = np.where(remove_feature == True)[0]
                        X = np.delete(X,0,1)
                        if len(remove_index)>0:
                            localbestX = np.delete(X,remove_index,1)
                        else:
                            localbestX=X
                            
#                     print 'feature:'+str(i)
#                     print X
#                     print localbestX
#                     print res.tvalues
#                     print res.pvalues
#                     print 'maxt:' + str(max_tvalue)
                         
                else:
                    continue
            
            nextX = localbestX
            featureIndexList =[]
            for ind in nextX.T:
                index = np.all(self.X==np.array([ind]).T,axis=0)
                featureIndex = np.where(index==True)[0]
                featureIndexList.append(int(featureIndex.item(0)))             
            if max_tvalue==0:
                no_better_feature = True
                self.feature = nextX
                self.Index = featureIndexList
            featureNumber = len(featureIndexList)                
            is_first_feature = False
            
#             feature index
            if KS_feature:
                for i in range(len(self.Index)):
                    self.Index[i]=self.KS_index[self.Index[i]]
                    
    
    def Get_KS_featureIndex(self):
        if len(self.KS_index)==0:
            print 'No feature selected'
        else:
            return self.KS_index
        
    def Get_KS_feature(self):
        if len(self.KS_feature)==0:
            print 'No feature selected'
        else:
            df = pd.DataFrame(self.KS_feature.T,columns = self.KS_index)
            #print df
            return df
    
    
    def GetfeatureIndex(self):
        if len(self.Index)==0:
            print 'No feature selected'
        else:
            return self.Index
        
    def Getfeature(self):
        if len(self.Index)==0:
            print 'No feature selected'
        else:
            df = pd.DataFrame(self.feature,columns = self.Index)
            #print df
            return df
            
        
        
        
              
        
if __name__ == "__main__":
    X = np.array([[3,4,6,9,13,13,14,15,19],
                  [5,4,8,11,12,22,24,26,23],
                  [1,6,5,8,7,11,13,15,17],
                  [2,0,0,3,0,0,0,0,0],
                  [0,0,2,3,7,8,9,12,14],
                  [12,22,23,21,21,23,24,24,24],
                  [2,3,4,4,4,6,7,8,9],
                  [2,4,13,17,19,15,17,18,20],
                  [0,0,0,0,0,0,0,0,0],
                 [0,0,0,0,0,0,0,0,6]]).T
    
    y = np.array([0,1,2,3,5,9,12,15,19])
    
    St = StepwiseRegression(X,y,P=0.05)
    St.fit_KS(P=0.15)
    St.fit_Stepwise(KS_feature=True)
    St.Get_KS_featureIndex()
    St.Get_KS_feature()
    St.GetfeatureIndex()
    St.Getfeature()
    
   

