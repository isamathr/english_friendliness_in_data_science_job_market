import numpy as np
import pandas as pd
from langdetect import detect
import re
import time
import concurrent.futures
from bs4 import BeautifulSoup
import requests 
from tqdm import tqdm
from datetime import datetime

def clean_language(language):
    language = language.split("(")[0].strip()
    return language.rstrip(",")

def get_official_languages():
    url = "https://en.wikipedia.org/wiki/List_of_official_languages_by_country_and_territory"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")

    table = soup.find("table", class_="wikitable")
    rows = table.find_all("tr")[1:]

    languages_by_country = {}

    for row in rows:
        columns = row.find_all("td")
        if len(columns) >= 2:
            country = columns[0].find("a").text.strip()
            languages_cell = columns[1]
            languages_with_context = list(languages_cell.stripped_strings)
            languages = [clean_language(lang) for lang in languages_with_context]
            languages = [lang for lang in languages if lang]
            
            if country == "Mexico" or country == "Bolivia" or country == "Uruguay":
                cleaned_languages = ["Spanish"]
            elif country == "Switzerland":
                cleaned_languages = ["French", "German", "Italian", "Romansh"]
            elif country == "Palestine":
                cleaned_languages = ["Arabic"]
            elif country == "Slovenia":
                cleaned_languages = ["Slovenian"]
            else:
                cleaned_languages = [clean_language(language) for language in languages]
                cleaned_languages = [language for language in cleaned_languages if language and language != "None"]
                cleaned_languages = [language for language in cleaned_languages if language != "de facto" and "has" not in language.lower()]
                cleaned_languages = [language for language in cleaned_languages if all(char.isalpha() or char.isspace() for char in language) or "language" in language.lower()]
            
            languages_by_country[country] = cleaned_languages
    languages_by_country['Macedonia (FYROM)'] = ['Macedonian', 'Albanian']
    languages_by_country['Czechia'] = ['Czech','Slovak']
    return languages_by_country

def get_iso_codes():
    url = "https://en.wikipedia.org/wiki/ISO_639-1_codes"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    soup1 = BeautifulSoup(soup.prettify())

    elements = soup1.find('table',{'id':'Table'}).find('tbody').find_all('tr')

    t, s = [], []
    for i in elements:
        title = i.find("a")
        symbol = i.find("span")
        t.append(title.text.strip())
        s.append(symbol.text.strip())
    del t[0], s[0]
    
    res = {s[i]: t[i] for i in range(len(s))}
    return res

def language_detection(text):
    try:
        src_lang = detect(text)
        return src_lang
    except:
        return "Unknown"

def translate(text, lang):
    try:
        translator = Translator()
        translated = translator.translate(text, src=lang, dest='en')
        return translated.text
    except:
        return "Cannot be translated"

def apply_translation(row):
    progress_bar.update(1)
    if row['language'] != 'en':
        return translate(row['job_description'], row['language']) 
    else:
        row['job_description']

def of_lang(x,of_langs):
    if x in of_langs:
        return list(of_langs[x])
    else:
        return "Not available"

def local_language(lang,country,of_langs):
    if lang == "Unknown":
        return "NaN"
    if of_langs[country] == [None]:
        return np.nan
    else:
        if languages[lang] in list(of_langs[country]):
            return 'Local'
        else:
            return 'Non local'

def find_language_reqs(description,local,country,of_lang,language,languages):
    if language == 'Unknown':
        return "NaN"
    elif languages[language] in of_lang:
        return "Yes"
    else:
        text = description.split()
        c = 0
        index = []
        for idx, value in enumerate(text):
            if value in of_lang:
                index.append(idx)
                c =+ 1
        if language != 'en' and languages[language] not in of_lang:
            return "NaN"
        if c == 0:
            return "No"
        else:
            for i in range(len(of_lang)):
                possibilities = [
                of_lang[i] + ", English",
                "English, " + of_lang[i],
            
                of_lang[i] + " & English",
                "English & " + of_lang[i],
            
                of_lang[i] + " and English",
                "English and " + of_lang[i],
                
                of_lang[i] + " and the English",
                "English and the " + of_lang[i],
            
                of_lang[i] + " language",
                of_lang[i] + " Language",
                "Fluent in " + of_lang[i],
                "fluent " + of_lang[i],
                "Fluent " + of_lang[i],
                
                of_lang[i] + " skills",
                "knowledge of " + of_lang[i],
            
                "know " + of_lang[i],
                "Proficiency in " + of_lang[i],
                "proficiency in " + of_lang[i],
                
                of_lang[i] + " is a pre",
                of_lang[i] + " speaking is a pre",
                of_lang[i] + " is a must",
                of_lang[i] + " speaker",
                of_lang[i] + " proficiency",
                "command of " + of_lang[i],
                '•' + " " + of_lang[i] + " " + '•',
                of_lang[i] + " (B2)",
                of_lang[i] + " (C1)"
                ]
            
                for j in range(len(possibilities)):
                    if possibilities[j] in description:
                        return "Yes"
            return "No"

def english_official(row):
    if 'English' in row['official_language']:
        return 'No'
    else:
        return row['language_reqs']

merging_data = True

## Getting official languages and their iso codes
languages = get_iso_codes()
of_langs = get_official_languages()

## Detect language of the job posting
data['language'] = data['job_description'].apply(language_detection)

# Finds official languages of each country
data['official_language'] = data.apply(lambda x: of_lang(x['search_country'],of_langs),axis=1)

data['local'] = data.apply(lambda x: local_language(x["language"],x["search_country"],of_langs),axis=1)

## Data cleaning
data['search_time'] = pd.to_datetime(data['search_time'])
data['date'] = data['search_time'].dt.date
data['time'] = data['search_time'].dt.time

## Finding language requirements
data['language_reqs'] = data.apply(lambda x:find_language_reqs(x['job_description'],x['local'],x['search_country'],x['official_language'],x['language'],languages),axis=1)
data['language_reqs'] = data.apply(english_official,axis=1)

if merging_data == True:
    df = pd.read_table('',sep=',') ## Add your filename
    df_full = pd.concat([df,data])
    filename = '' ## Filename in Google drive
    df_full = df_full[df_full['language_reqs']!='NaN']
    df_full[["job_title_clean","company_name","date","search_country",
        "language","local","language_reqs"]].to_csv(filename,index_label="id",encoding='utf-8-sig')
if merging_data == False:
    filename = ''  ## Add your file
    data = data[data['language_reqs']!='NaN']
    data[["job_title_clean","company_name","date","search_country",
        "language","local","language_reqs"]].to_csv(filename,index_label="id",encoding='utf-8-sig')
