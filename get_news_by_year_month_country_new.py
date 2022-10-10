from newspaper import Article
from newspaper import Config, Article, Source
# config = Config()
# config.memoize_articles = False
# config.language = 'en'
import json
import pandas as pd
import collections
import csv
import sys
import os
import time
# from datetime import datetime
from subprocess import call
from itertools import product
from datetime import date
import datetime
'''
 NOTE: country_code is from ActionGeo_CountryCode, (similar attributes are Actor1Geo_CountryCode,Actor2Geo_CountryCode)
       event_date is from GDELT data
'''
# EVENTID AND DATE ATTRIBUTES
event_attrs = ['GlobalEventID','Day','MonthYear','Year','FractionDate',
#               ACTOR ATTRIBUTES
'Actor1Code','Actor1Name','Actor1CountryCode','Actor1KnownGroupCode','Actor1EthnicCode','Actor1Religion1Code','Actor1Religion2Code','Actor1Type1Code','Actor1Type2Code','Actor1Type3Code',
'Actor2Code','Actor2Name','Actor2CountryCode','Actor2KnownGroupCode','Actor2EthnicCode','Actor2Religion1Code','Actor2Religion2Code','Actor2Type1Code','Actor2Type2Code','Actor2Type3Code',
#               EVENT ACTION ATTRIBUTES
'IsRootEvent','EventCode','EventBaseCode','EventRootCode','QuadClass','GoldsteinScale','NumMentions','NumSources','NumArticles','AvgTone',
# EVENT GEOGRAPHY
'Actor1Geo_Type','Actor1Geo_Fullname','Actor1Geo_CountryCode','Actor1Geo_ADM1Code','Actor1Geo_ADM2Code','Actor1Geo_Lat','Actor1Geo_Long','Actor1Geo_FeatureID',
'Actor2Geo_Type','Actor2Geo_Fullname','Actor2Geo_CountryCode','Actor2Geo_ADM1Code','Actor2Geo_ADM2Code','Actor2Geo_Lat','Actor2Geo_Long','Actor2Geo_FeatureID',
'ActionGeo_Type','ActionGeo_Fullname','ActionGeo_CountryCode','ActionGeo_ADM1Code','ActionGeo_ADM2Code','ActionGeo_Lat','ActionGeo_Long','ActionGeo_FeatureID',
# DATA MANAGEMENT FIELDS
'DATEADDED','SOURCEURL']
event_dtypes = {'EventCode':str,'EventBaseCode':str,'EventRootCode':str}
mention_attrs = [
'GlobalEventID','EventTimeDate','MentionTimeDate','MentionType','MentionSourceName','MentionIdentifier',
'SentenceID','Actor1CharOffset','Actor2CharOffset','ActionCharOffset','InRawText','Confidence',
'MentionDocLen','MentionDocTone','MentionDocTranslationInfo','Extras'
]

try:
    target_year = int(sys.argv[1])
    target_month = int(sys.argv[2])
    # cut = int(sys.argv[2]) # [1-4,5-8,9-12] # [1-6,7-12] # [1-3,4-6,7-9,10-12]
    country = str(sys.argv[3])
    filepath = str(sys.argv[4])

except:
    print('Usage: year (select from 2015-2020), country (2-digit), filepath (data/masterfilelist20210929.txt)')
    exit()

print('target_year=',target_year,'country=',country,'filepath',filepath)
# get the event csv data
# filepath = 'data/masterfilelist20201027.txt'


# filelists = pd.read_csv(filepath,sep=' ',names=['size','code','path'])
# filelists
with open(filepath, newline='') as f:
    reader = csv.reader(f)
    data = list(reader)
 

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)

startdate = date(target_year, target_month,1)#.strftime("%Y%d%m")
enddate = last_day_of_month(startdate)#.strftime("%Y%d%m")
startdate = int(startdate.strftime("%Y%d%m"))
enddate = int(enddate.strftime("%Y%d%m"))
print('startdate=',startdate,'enddate=',enddate)

selected_event_files = []
for i in data:
    try:
        path = i[0].split(' ')[2]
    except:
        print('error in reading files, skipping ',i)
        continue
    
    # path = i[0].split(' ')[2]
    filename = path.split('/')[4]
    filedatetime = int(filename[:8])
    if startdate <= filedatetime <= enddate:
        # print(datetime)
        if path.find('export') > -1 and path.find('/{}'.format(str(target_year))) > -1:
            selected_event_files.append(path.split(' ')[-1])
        
print('#event_file=',len(selected_event_files),selected_event_files[0],selected_event_files[-1])

# save news into file
finished_event_files = set()
news_file = 'news/news.year.{}.month.{}.{}.json'.format(target_year,target_month,country)
store_finished_event_file = 'marker/viewed-event-file.news.year.{}.{}.txt'.format(target_year,country)
store_finished_urls = 'news/url.year.{}.month.{}.{}.txt'.format(target_year,target_month,country)

# get finished event files before starting crawling

try:
    with open(store_finished_event_file,'r') as f:
        finished_files_read = f.read().splitlines()
except:
    finished_files_read = []

try:
    with open(store_finished_urls,'r') as f:
        finished_urls_read = f.read().splitlines()
except:
    finished_urls_read = []


print('#finished_files_read=',len(finished_files_read))
print('#finished_urls_read=',len(finished_urls_read))

# exit()
for zippath in selected_event_files:
    if zippath in finished_files_read:
        print(zippath,'processed')
        continue
    
    news_collection = []
    urls_collection = []
    try:
        df = pd.read_csv(zippath,sep='\t',names=event_attrs,dtype=event_dtypes,index_col=False,compression='zip')
    except:
        print(zippath,'read file error')
        finished_event_files.add(zippath)
        with open(store_finished_event_file, 'a+') as f:
            f.write(zippath+"\n")
        continue
    df = df.loc[df['ActionGeo_CountryCode']==country] # filter the news in this country
    df.drop_duplicates(subset=['SOURCEURL'], keep='first', inplace=True)
    print(zippath,country,'#news=',len(df['SOURCEURL'].unique()),time.ctime())
    # if df.empty:
        
    # else:
    for i,row in df.iterrows():
        # if i > 2:
        #     break
        url = row['SOURCEURL']
        event_date = row['Day']
        country_code = row['ActionGeo_CountryCode']
        try: 
            article = Article(url,language='en', memoize_articles=False)
            article.download()
            article.parse()
            crawl_text = article.text
            if len(crawl_text) < 300:
                continue
            tmp = {#'publish_date':str(article.publish_date), # do not consider this, sometimes None
                "event_date":event_date,
                "country_code":country_code,
                "url":url,
                "title":article.title,"text":crawl_text}
            news_collection.append(tmp)
            urls_collection.append(url)
        except: # could be 404 error
            continue

    if len(news_collection)>0:
        print('#news collected',len(news_collection))
        output_file = open(news_file, 'a+') #, encoding='utf-8'
        for dic in news_collection:
            json.dump(dic, output_file) 
            output_file.write("\n")
        output_file.close()

        # add urls
        with open(store_finished_urls, 'a+') as f:
            for url in urls_collection:
                f.write(url+"\n")
    else:
        pass
        # print('no news fetched in',zippath)
    
    finished_event_files.add(zippath)
    with open(store_finished_event_file, 'a+') as f:
        f.write(zippath+"\n")

if len(finished_event_files) == len(selected_event_files):
    print(target_year,'complete!')
else:
    print(len(finished_event_files),'/',len(selected_event_files),'finished')