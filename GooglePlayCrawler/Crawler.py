 # -*- coding: utf-8 -*-. 
import pandas as pd 
import requests 
from bs4 import BeautifulSoup 
from datetime import datetime
from threading import Thread
from selenium import webdriver
import time
import gzip

# get pkg support language url code
def GetLangList(id):
	lang_list = list()
	res = requests.get("https://play.google.com/store/apps/details?id="+id)
	soup = BeautifulSoup(res.text.encode("utf-8"), "html.parser")
	i=0
	for hl in soup.findAll('link', attrs={'rel':'alternate'}):
		if i>1:
			lang_list.append( hl.attrs[hl.attrs.keys()[2]])
		i+=1
	return lang_list

# crawl app information by pkgname and language code
def app_detail(id,lang):
	res = requests.get("https://play.google.com/store/apps/details?id="+id+'&hl='+lang)
	soup = BeautifulSoup(res.text.encode("utf-8"), "html.parser")
	
	#error = soup.body.findAll('div', attrs={'id':'error-section'})
	#if error > 0 : return id

	title = soup.body.find('div', attrs={'class':'id-app-title'})
	if title == None : title = ''
	else: title = title.text.encode('utf-8')

	genre = soup.body.find('span', attrs={'itemprop':'genre'})
	if genre == None : genre = ''
	else: genre = genre.text.encode('utf-8')

	score = soup.body.find('div', attrs={'class':'score'})
	if score == None : score = ''
	else: score = score.text.replace(',','').encode('utf-8')

	rev_num = soup.body.find('span', attrs={'class':'reviews-num'})
	if rev_num == None : rev_num = ''
	else: rev_num = rev_num.text.replace(',','').encode('utf-8')
	
	if len(soup.body.findAll('span', attrs={'class':'bar-number'})) == 0:
		score_number = ['']*5
	else:
		score_number = list()
		for i in soup.body.findAll('span', attrs={'class':'bar-number'}):
			score_number.append(i.text.replace(',','').encode('utf-8'))

	comp = soup.body.find('a', attrs={'class':'document-subtitle primary'})
	if comp == None : comp = ''
	else: comp = comp.text.encode('utf-8')

	publish_date  = soup.body.find('div', attrs={'itemprop':'datePublished'})
	if publish_date == None : publish_date = ''
	else: publish_date = publish_date.text.encode('utf-8')

	installed_time  = soup.body.find('div', attrs={'itemprop':'numDownloads'})
	if installed_time == None : installed_time = ''
	else: installed_time = installed_time.text.encode('utf-8')

	contentRate = soup.body.find('div', attrs={'class':'content','itemprop':'contentRating'})
	if contentRate == None : contentRate = ''
	else: contentRate = contentRate.text.encode('utf-8')

	price = soup.body.find('meta', attrs={'itemprop':'price'})
	if price == None : price = ''
	else: price = price['content'].encode('utf-8')

	similar_app = list()
	for simid in soup.body.findAll('span', attrs={'class':'preview-overlay-container'}):
		similar_app.append(simid['data-docid'].encode('utf-8'))

	# permission = list()
	# for simid in soup.body.findAll('li', attrs={'jstcache':'90','jsinstance':'*0'}):
	# 	permission.append(simid.text.encode('utf-8'))

	softwareVersion = soup.body.find('div', attrs={'class':'content','itemprop':'softwareVersion'})
	if softwareVersion == None : softwareVersion = ''
	else: softwareVersion = softwareVersion.text.encode('utf-8')

	operatingSystems = soup.body.find('div', attrs={'class':'content','itemprop':'operatingSystems'})
	if operatingSystems == None : operatingSystems = ''
	else: operatingSystems = operatingSystems.text.replace(' ','').encode('utf-8')

	ads = soup.body.find('span', attrs={'class':'ads-supported-label-msg'})
	if ads == None : ads = ''
	else: ads = ads.text.encode('utf-8')

	inapp_msg = soup.body.find('div', attrs={'class':'inapp-msg'})
	if inapp_msg == None : inapp_msg = ''
	else: inapp_msg = inapp_msg.text.encode('utf-8')

	whatsNew = soup.body.find('div', attrs={'class':'recent-change'})
	if whatsNew == None : whatsNew = ''
	else: whatsNew = whatsNew.text.encode('utf-8')

	LogoUrl = soup.body.find('img', attrs={'class':'cover-image'})
	if LogoUrl == None : LogoUrl = ''
	else: LogoUrl = LogoUrl['src'].encode('utf-8')

	mail = soup.body.find('div', attrs={'class':'content physical-address'})
	if mail == None : mail = ''
	else: mail = mail.text.encode('utf-8')

	Thumbnails = soup.body.find('img', attrs={'itemprop':'screenshot'})
	if Thumbnails == None : Thumbnails = ''
	else: Thumbnails = Thumbnails['src'].encode('utf-8')
	
	ClkUrl = "https://play.google.com/store/apps/details?id="+id

	reviewers = soup.body.find('span', attrs={'class':'reviews-num'})
	if reviewers == None : reviewers = ''
	else: reviewers = reviewers.text.encode('utf-8')
	
	Desc = soup.body.find('div', attrs={'jsname':'C4s9Ed'})
	if Desc == None : Desc = ''
	else: Desc = Desc.text.encode('utf-8')

	Rating = soup.body.find('div', attrs={'class':'current-rating'})
	if Rating == None : Rating = ''
	else: Rating = Rating['style'].replace(';','').replace('width','').replace(' ','').encode('utf-8')

	AppType = title+'/'+genre
	
	# fileSize = soup.body.find('div', attrs={'class':'current-rating'})
	# if Rating == None : Rating = ''
	# else: Rating = Rating['style'].encode('utf-8')
	Category = soup.body.find('a', attrs={'class':'document-subtitle category'})
	if Category == None : Category = ''
	else: Category = Category['href'].split('/')[-1].encode('utf-8')


	feature = {
	'PkgName':id,
	'Title':title,
	'genre':genre,
	'score':score,	
	'ratingHistogram':score_number,
	'comp':comp,
	'datePublished':publish_date,
	'numDownloads':installed_time,
	'ratingValue':contentRate,
	'contentRating':contentRate,
	'Reviewers':reviewers,
	'softwareVersion':softwareVersion,
	'GpCategory':Category,
	'GpCategory2':'',
	'similarApps':similar_app,
	'operatingSystems':operatingSystems,
	'ads':ads,
	'whatsNew':whatsNew,
	'inAppMsg':inapp_msg,
	'price':price,
	'LogoUrl':LogoUrl,
	'ClkUrl':ClkUrl,
	'mail':mail,
	'Thumbnails':Thumbnails, # screenshot
	'Desc':Desc, # description
	'Rating':Rating, #width
	'Recommenders':'', #empty in java
	'PosId':'', #empty in java
	'AppType':AppType,
	'Lan':lang,
	'Country':lang, # set by crawler
	'fileSize':'', # cant find tag due to new version
	'genre2':'', # cant find tag due to new version
	'name':title, # same as title 
	'GpTag':''# cant find tag due to new version
	}
	# print feature
	return feature

# get lang list
def ReadLangList(path='langlist.txt'):
	return pd.read_csv(path,header=None).loc[:,0].tolist()

def ReadCountryList(path='lan_region'):
	return pd.read_csv(path,sep='\t',header=None).loc[:,1].tolist()

def GetPkgurlList(url,number_of_app = 1000,num=120):
	urllist = list()
	i=1
	last_end_pkg_name =''
	for index in xrange(0,number_of_app,num):
		form_data = {'start':str(index),'num':str(num)}
		res=requests.post(url+'/collection/topselling_free',form_data)
		soup = BeautifulSoup(res.text.encode("utf-8"), "html.parser")
		total= soup.find('div',class_='id-card-list card-list two-cards')
		if total != None:
			hrefs=total.find_all('div',class_='card no-rationale square-cover apps small')
			last_end_pkg_name = hrefs[len(hrefs)-1]['data-docid']
			if last_end_pkg_name in urllist:
				return list(set(urllist))
			for href in hrefs:
				urllist.append(href['data-docid'])
				# print i,href['data-docid']
				i+=1
		else :
			return list(set(urllist))

	return list(set(urllist))

def GetTopCategory():
	res=requests.get('https://play.google.com/store/apps')
	soup = BeautifulSoup(res.text.encode("utf-8"), "html.parser")
	total= soup.findAll('a',class_='child-submenu-link')
	url_list = list()
	cate_list = list()
	for url in total:
		cate = url['href'].split('/')
		cate = cate[len(cate)-1]
		cate_list.append(cate)
		url_list.append('https://play.google.com'+url['href'])
	return url_list,cate_list

# Crawl all package by pkgname file	
def CrawlPackageByLang(fileindex1=0,fileindex2=99,file_job_num=''):
	date = datetime.now().strftime('%Y%m%d')
	totalapp =0
	for lang in ReadLangList():
		with gzip.open('.\\Data\\'+date+'_'+file_job_num+lang+'.gz', 'wb') as f:
			# FeatureList = list()
			for i in xrange(fileindex1,fileindex2+1):
				if i<=9:
					filename = 'part-0000'+str(i)	
				else :
					filename = 'part-000'+str(i)
				
				df = pd.read_csv('.\\temporary\\'+filename,sep='\t',header=None)
					
				for j in xrange(len(df)):
					pkg_name = df.loc[j,0]
					totalapp+=1
					content = app_detail(pkg_name,lang)
					f.write(str(content)+'\n')
					# FeatureList.append(content)
					print '-------',filename,'-------',totalapp,'-------'
			
		# pd.DataFrame(FeatureList).to_json('.\\Data\\'+date+'_'+lang+'.txt',orient='records',lines =True)

def CrawlPackageByCountry(fileindex1=0,fileindex2=99,file_job_num=''):
	date = datetime.now().strftime('%Y%m%d')
	totalapp = 0
	lang = ReadLangList()
	lan_index = 0
	for ctry in ReadCountryList():
		with gzip.open('.\\Data\\'+date+'_'+file_job_num+ctry+'.gz', 'wb') as f:
			# FeatureList = list()
			for i in xrange(fileindex1,fileindex2+1):
				if i<=9:
					filename = 'part-0000'+str(i)	
				else :
					filename = 'part-000'+str(i)
				
				df = pd.read_csv('.\\temporary\\'+filename,sep='\t',header=None)
					
				for j in xrange(len(df)):
					pkg_name = df.loc[j,0]
					totalapp+=1
					content = app_detail(pkg_name,lang[lan_index])
					f.write(str(content)+'\n')
					# FeatureList.append(content)
					print '-------',filename,'-------',totalapp,'-------'
		lan_index+=1
			
		# pd.DataFrame(FeatureList).to_json('.\\Data\\'+date+'_'+ctry+'.txt',orient='records',lines =True)

def CrawlPackageByTopList():
	date = datetime.now().strftime('%Y%m%d')
	url_list,cate_list = GetTopCategory()
	for i in xrange(len(url_list)):
		pkg_list = GetPkgurlList(url_list[i])
		t=1
		with gzip.open('.\\TopData\\'+date+'_'+cate_list[i]+'.gz', 'wb') as f:
			for pkg in pkg_list:
				# print pkg,cate_list[i]
				content = app_detail(pkg,'en')
				f.write(str(content)+'\n')
				print '-------',cate_list[i],'-------',t,'-------'
				t+=1
			
# crawl by pkgname
def CrawlByPkgName(pkgname='com.facebook.katana'):
	# FeatureList = list()
	with gzip.open('.\\Data\\'+pkgname+'.gz', 'wb') as f:
		for lang in ReadLangList():
			print lang
			content = app_detail(pkgname,lang)
			print content
			f.write(str(content))
			# FeatureList.append(content)
	# pd.DataFrame(FeatureList).to_json('.\\Data\\'+pkgname+'.txt',orient='records',lines =True)
	# return FeatureList

# crawl by multithread 
def MultiThreadCrawlPackage(fileindex1=0,fileindex2=99,n_job=3):
	date = datetime.now().strftime('%Y%m%d')
	job_file = (fileindex2-fileindex1+1)/n_job
	remainder = (fileindex2-fileindex1+1)%n_job
	last_index = 0 
	start_index = fileindex1
	gz_file_list = list()
	mythread = list()

	for i in xrange(n_job):
		if i != 0 :
			start_index = last_index+1 
		last_index = start_index+ job_file-1
		if remainder>0 and i <remainder:	
			last_index+=1
		thd = Thread(target=CrawlPackageByLang, name='Thd'+str(i),args=(start_index,last_index,str(i)))
		thd.start()
		mythread.append(thd)
	print mythread

	for thd in mythread:
		while thd.isAlive():
			time.sleep(5)

	# for lang in ReadLangList():
	# 	for i in xrange(n_job):
	# 		gz_file_list.append('.\\Data\\'+date+'_'+str(i)+lang+'.gz')
	
	# crawlFinish = False
	# # concate gz
	# while not crawlFinish:
	# 	for t in mythread:
	# 		if t.isAlive() and crawlFinish is True:
	# 			crawlFinish = False
 #    		if t.isAlive()is False:
 #    			# print t.isAlive(),crawlFinish
 #    			crawlFinish = True
	
	# # print crawlFinish
	# if crawlFinish:
	# 	bufferSize = 30
	# 	for lang in ReadLangList():
	# 		destFilename = '.\\Data\\'+date+'_'+lang+'.gz' 
	# 		with open(destFilename, 'wb') as destFile:
	# 		    for fileName in gz_file_list:
	# 		        with open(fileName, 'rb') as sourceFile:
	# 		            chunk = True
	# 		            while chunk:
	# 		                chunk = sourceFile.read(bufferSize)
	# 		                destFile.write(chunk)

def read_gz(path='D:\\PythonScript\\GooglePlayCrawler\\TopData\\20170703_TOOLS.gz'):
	import gzip
	f=gzip.open(path,'rb')
	file_content=f.read()
	print file_content



if __name__ == '__main__':
	start = datetime.now()
	CrawlByPkgName()
	# MultiThreadCrawlPackage(0,2)
	# CrawlPackageByTopList()
	# CrawlPackageByCountry(0,0)
	# read_gz()
	end = datetime.now()
	print (end-start)/24

#照語言存以及照國家存
