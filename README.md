# gdelt_data_preprocess
Collect events and news from GDELT

### 1. Create folders to save data ###
Create folders at the project root, i.e., ``event``, ``news``, ``marker``. ``marker` saves some temporary information to prevent duplicate collection in case of interruption.

### 2. Download GDELT source file ###
Download the file [masterfilelist.txt](http://data.gdeltproject.org/gdeltv2/masterfilelist.txt) and store it at the project root.

### 3. Run Python code to collect data ###
*First crawl news*
```python
python get_news_by_year_month_country.py 2020 1 US masterfilelist.txt

python get_news_by_year_month_country.py 2017 12 NI masterfilelist.txt
```
*Then collect events*
```python
python get_events_by_year_month_country.py 2020 1 US masterfilelist.txt

python get_events_by_year_country.py 2017 12 NI masterfilelist.txt
```
