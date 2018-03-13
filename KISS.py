import requests # запросы к сайту с данными
from bs4 import BeautifulSoup # обработка сайта
import json # обработка скачиваемых данных
import re # обработка текста и сайта
from ast import literal_eval # обработка текста

from tqdm import tqdm_notebook # progress bar

url = "http://stat.gibdd.ru/"

## Скачиваем регионы

regions = {}

# делаем запрос к stat.gibdd.ru
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')          

#ищем скрипт с кодами ОКТМО регионов и выкачиваем их
scripts = soup.find_all("script")

for script in scripts:
    if script.string is not None and "downloadRegListData" in script.string:
        string = script.string
        p = re.compile(r"downloadRegListData = (.*?);", re.MULTILINE)
        m = p.search(string)
        data = m.groups()[0]
        jdata = json.loads(data)
        for federal_district in jdata[0]["Nodes"]:
            for region in federal_district["Nodes"]:
                regions[region['Text']] = region['Value']
                
#делаем запросы по регионам и качаем коды ОКТМО для районов и городов
for region in tqdm_notebook(regions):
    region_code = regions[region]
    
    payload = {
        "maptype":1,
        "region":region_code,
        "pok":"1"
    }
    
    regions[region] = []
    
    area_request = requests.post("http://stat.gibdd.ru/map/getMainMapData", json=payload)
    area_response = area_request.json()['metabase']
    export = json.loads(literal_eval(area_response)[0]['maps'])
    for area in export:
        regions[region].append({"region":region,"area":area["name"],"region_code":region_code,"area_code":area["id"]})

with open("regions.json", 'w') as data_file:
    json.dump(regions, data_file, ensure_ascii=False)


## Качаем карточки ДТП по районам

with open("regions.json") as data_file:    
    regions = json.load(data_file)
        
# искомые даты в формате месяц.год
dates = [
    "01.2015",
    "02.2015",
    "03.2015",
    "04.2015",
    "05.2015",
    "06.2015",
    "07.2015",
    "08.2015",
    "09.2015",
    "10.2015",
    "11.2015",
    "12.2015",
    "01.2016",
    "02.2016",
    "03.2016",
    "04.2016",
    "05.2016",
    "06.2016",
    "07.2016",
    "08.2016",
    "09.2016",
    "10.2016",
    "11.2016",
    "12.2016",
    "01.2017",
    "02.2017",
    "03.2017",
    "04.2017",
    "05.2017",
    "06.2017",
    "07.2017",
    "08.2017",
    "09.2017",
    "10.2017",
    "11.2017",
    "12.2017"
]

# функция импорта данных со stat.gibdd.ru
def get_crashes_data(region_name, region_code, area_name, area_code, dates):
    local_crashes = []
    
    for date in tqdm_notebook(dates, leave=False, desc=area_name):
        payload = {}

        payload["data"] = '{"date":["MONTHS:' + date + '"],"ParReg":"' + region_code + '","order":{"type":"1","fieldName":"dat"},"reg":"' + area_code + '","ind":"1","st":"1","en":"10000"}'
        
        response = requests.post("http://stat.gibdd.ru/map/getDTPCardData", json=payload)
        
        try:
            response = response.json()
            export = literal_eval(response['data'])
            local_crashes.extend(export['tab'])
        except:
            pass

    for i in local_crashes:
        i['region'] = region_name
        i['area'] = area_name
        
    return local_crashes

auto_crashes = []

for region in tqdm_notebook(regions,desc="Россия"):
    # фильтр по региону
    if region == "Алтайский край":
        for area in tqdm_notebook(regions[region],desc=region,leave=False):
            # фильтр по району
            if area['area'] == "г.Барнаул":
                auto_crashes.extend(get_crashes_data(region, area["region_code"], area["area"], area["area_code"], dates))
                
with open("auto-crashes.json", 'w') as data_file:
    json.dump(auto_crashes, data_file, ensure_ascii=False)

