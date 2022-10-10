# from newspaper import Article
# from newspaper import Config, Article, Source
# config = Config()
# config.memoize_articles = False
# config.language = 'en'

# import torch
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
import datetime
from datetime import date
# geolocation format [City/Landmark, State, Country]
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

# try:
#     target_country_code = str(sys.argv[1])
# except:
#     print('Usage: 2-character FIPS10-4 country code (https://en.wikipedia.org/wiki/List_of_FIPS_country_codes)')
#     exit()
try:
    target_year = int(sys.argv[1])
    target_month = int(sys.argv[2])
    # cut = int(sys.argv[2]) # [1-4,5-8,9-12] # [1-6,7-12] # [1-3,4-6,7-9,10-12]
    country = str(sys.argv[3])
    filepath = str(sys.argv[4])
except:
    print('Usage: year (select from 2015-2021), month, country (2-digit), filepath (data/masterfilelist20210929.txt)')
    exit()

print('target_year=',target_year,'country=',country,'filepath',filepath)
# get the event csv data
# filepath = 'data/masterfilelist20201027.txt'

# check if some
# filelists = pd.read_csv(filepath,sep=' ',names=['size','code','path'])
# filelists
with open(filepath, newline='') as f:
    reader = csv.reader(f)
    data = list(reader)

# target_year = '2015'
selected_event_files = []

# if country == 'US':
#     print('no split')
#     startdate = '{}0301'.format(target_year)
#     enddate = '{}1208'.format(target_year)
# else:
#     print('no split')

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)

startdate = date(target_year, target_month,1)#.strftime("%Y%d%m")
enddate = last_day_of_month(startdate)#.strftime("%Y%d%m")
startdate = int(startdate.strftime("%Y%d%m"))
enddate = int(enddate.strftime("%Y%d%m"))
print('startdate=',startdate,'enddate=',enddate)


for i in data:
    try:
        path = i[0].split(' ')[2]
    except:
        print('error in reading files, skipping ',i)
        continue
    
    filename = path.split('/')[4]
    filedatetime = int(filename[:8])
    if startdate <= filedatetime <= enddate:
        # print(datetime)
        if path.find('export') > -1 and path.find('/{}'.format(str(target_year))) > -1:
            selected_event_files.append(path.split(' ')[-1])

finished_event_files = set()
target_file = 'event/event.year.{}.month.{}.{}.json'.format(target_year,target_month,country)
# target_file = 'event/event.{}.{}.{}.json'.format(target_year,cut,country)
store_finished_event_file = 'marker/viewed-event-file.event.year.{}.{}.txt'.format(target_year,country)
check_finished_url_file = 'news/url.year.{}.month.{}.{}.txt'.format(target_year,target_month,country)


try:
    with open(store_finished_event_file,'r') as f:
        finished_files_read = f.read().splitlines()
except:
    finished_files_read = []
print('#finished_files_read=',len(finished_files_read))
# get urls
try:
    with open(check_finished_url_file,'r') as f:
        finished_urls_read = f.read().splitlines()
except:
    finished_urls_read = []
print('#finished_urls_read=',len(finished_urls_read))

print('#event file',len(selected_event_files),selected_event_files[0],selected_event_files[-1])
# exit()

for i in range(len(selected_event_files)):
    eventpath = selected_event_files[i]
    mentionpath = eventpath.replace('export','mentions')
    if eventpath in finished_files_read:
        print(eventpath,'processed')
        continue

    # break
    try:
        df = pd.read_csv(eventpath,sep='\t',names=event_attrs,dtype=event_dtypes,index_col=False,compression='zip')
    except:
        print(eventpath,'read file error')
        finished_event_files.add(eventpath)
        with open(store_finished_event_file, 'a+') as f:
            f.write(eventpath+"\n")
        continue
    df = df.loc[df['ActionGeo_CountryCode']==country]
    df.drop_duplicates(subset=['EventCode','EventBaseCode','SOURCEURL'], keep='first', inplace=True)
    print(eventpath,country,'#event=',len(df),time.ctime())
    try:
        mentiondf = pd.read_csv(mentionpath,sep='\t',names=mention_attrs,index_col=False,compression='zip')
        if not mentiondf.empty:
            mentiondf.sort_values(by=['Confidence','InRawText'], axis=0, ascending=False, inplace=True, na_position='last')
    except:
        mentiondf = pd.DataFrame() 
    event_collection = []
    for i,row in df.iterrows():
        eventid = row['GlobalEventID']
        if mentiondf.empty:
            SentenceID = -1
            MentionIdentifier = ''
        else:
            mentions_filtered = mentiondf.loc[mentiondf['GlobalEventID']==eventid]
            if mentions_filtered.empty:
                SentenceID = -1
                MentionIdentifier = ''
            else:
                SentenceID = mentions_filtered.iloc[0]['SentenceID']
                MentionIdentifier = mentions_filtered.iloc[0]['MentionIdentifier']
                for j,men_row in mentions_filtered.iterrows():
                    if men_row['MentionIdentifier'] in finished_urls_read: #  find the one has news article
                        SentenceID = men_row['SentenceID']
                        MentionIdentifier = men_row['MentionIdentifier']
                        break
        tmp = {
            "GlobalEventID":str(row['GlobalEventID']),"event_date":int(row['Day']),
            "Actor1Code":str(row['Actor1Code']),"Actor1Name":str(row['Actor1Name']),
            "Actor2Code":str(row['Actor2Code']),"Actor2Name":str(row['Actor2Name']),
            "IsRootEvent":int(row['IsRootEvent']),"EventCode":str(row['EventCode']),
            "QuadClass":int(row['QuadClass']),"GoldsteinScale":float(row['GoldsteinScale']),
            "NumMentions":int(row['NumMentions']),"NumArticles":int(row['NumArticles']),
            "AvgTone":float("{:.4f}".format(row['AvgTone'])),"ActionGeo_Type":int(row['ActionGeo_Type']),
            "ActionGeo_Fullname":str(row['ActionGeo_Fullname']),"ActionGeo_CountryCode":str(row['ActionGeo_CountryCode']),
            "SOURCEURL":str(row['SOURCEURL']),"SentenceID":int(SentenceID),"MentionIdentifier":str(MentionIdentifier)
        }
        event_collection.append(tmp)
        # break
    # print(event_collection)
    # exit()

    if len(event_collection)>0:
        print('#event collected',len(event_collection))
        output_file = open(target_file, 'a+') #, encoding='utf-8'
        for dic in event_collection:
            # try:
            #     json.dump(dic, output_file) 
            # except:
            #     print(dic,type(dic))
            #     exit()
            json.dump(dic, output_file) 
            output_file.write("\n")
        output_file.close()
    else:
        pass
        # print('no event fetched in',eventpath)
    
    finished_event_files.add(eventpath)
    with open(store_finished_event_file, 'a+') as f:
        f.write(eventpath+"\n")


if len(finished_event_files) == len(selected_event_files):
    print(target_year,'complete!')
else:
    print(len(finished_event_files),'/',len(selected_event_files),'finished')
 


'''
# find events containing a url # 410580706
for i in range(len(selected_event_files)):
    eventpath = selected_event_files[i]
    mentionpath = eventpath.replace('export','mentions')
    try:
        # df = pd.read_csv(eventpath,sep='\t',names=event_attrs,dtype=event_dtypes,index_col=False,compression='zip')
        # df = df.loc[(df['ActionGeo_CountryCode']==country) & (df['SOURCEURL']==theurl)]
        # if not df.empty:
        #     print(df.values)
        mentiondf = pd.read_csv(mentionpath,sep='\t',names=mention_attrs,index_col=False,compression='zip')
        mentiondf = mentiondf.loc[(mentiondf['GlobalEventID']==410580706)]
        # mentiondf = mentiondf.loc[(mentiondf['MentionIdentifier']==theurl)]
        if not mentiondf.empty:
            print(mentiondf.values)
            print(mentiondf.columns)
    except:
        pass
''' 