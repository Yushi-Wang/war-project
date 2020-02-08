# -*- coding: utf-8 -*-
"""
Created on Fri Jan 18 11:47:17 2019
This is the code file for dumping wikipedia articles and then extracting infoboxes from wikipedia
@author: leo
"""

import datetime
import pandas as pd
import calendar
import wget
import unicodecsv
from lxml import etree as ET
from urllib import error
import re
import bz2file
import pyparsing
import numpy as np

now = datetime.datetime.now()
date_part = calendar.month_name[now.month].lower() + str(now.year)
date_part = "july2018"

dump_date = '20190101'
main_path = "D:/learning/Arash/war_participants/"
wd_umea = 'http://ftp.acc.umu.se/mirror/wikimedia.org/dumps/'
page_props_dir = main_path + 'page_props/'
page_table_dir = main_path + 'page_tables/'
wd_virginia = 'https://dumps.wikimedia.org/'
languages_path = main_path + "languages.tsv"
articles_dir = main_path + 'articles/'



date = dump_date
skip_lang = True

def strip_tag_name(t):
    t = elem.tag
    idx = k = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t

lang = "en"
fname_sqlgz = articles_dir + lang + '.sql.gz'

#This downloads the main wikipedia text file
if False:
    try:
        url = wd_umea + lang + 'wiki/' + date + '/' + lang + 'wiki-' + date + '-pages-articles-multistream.xml.bz2'
        wget.download(url, out=fname_sqlgz, bar=None)
    except (error.HTTPError, error.URLError) as e:
        print("Using alternative mirror for " + lang)
        url = wd_virginia + lang + 'wiki/' + date + '/' + lang + 'wiki-' + date +'-pages-articles-multistream.xml.bz2'
        wget.download(url, out=fname_sqlgz)

counter = 0
with open(articles_dir + lang + '_sent.tsv', 'w') as f:
    writer = pd.DataFrame(columns=('wid','tmp'))
    allid=pd.DataFrame(columns=('wid','title'))
    with bz2file.open(fname_sqlgz, 'r') as in_file:
        context = ET.iterparse(in_file, events=('end',))
        ns = 0
        flush_next = 1
        index = 0
        for event, elem in context:
            if flush_next == 1:
                
                title = ''
                ns = 0
                text = ''
                wid = ''
                flush_next = 0
            tag = strip_tag_name(elem.tag)
            if tag == 'id' and wid == '':
                
                wid = elem.text
            if tag == 'title':
                title = elem.text
            if tag == 'ns':
                ns = int(elem.text)
            if tag == 'text':
                text = elem.text
                if text == None:
                    text = ''
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            if tag == 'page' and event == 'end':
                flush_next = 1
                
        #context = ET.iterparse(in_file, events=('end',))
        #ns = 0
        #flush_next = 1
        #index = 0
        #for event, elem in context:
            #if flush_next == 1:
                #title = ''
                #ns = 0
                #text = ''
                #flush_next = 0
            #tag = strip_tag_name(elem.tag)
            #if tag == 'id':
                #wid = elem.text
            #if tag == 'title':
                #title = elem.text
            #if tag == 'ns':
                #ns = int(elem.text)
            #if tag == 'text':
                #text = elem.text
                #if text == None:
                    #text = ''
           # elem.clear()
           # while elem.getprevious() is not None:
                #del elem.getparent()[0]
            #if tag == 'page' and event == 'end':
                #flush_next = 1
            
            if ns == 0 and flush_next == 1 and title.find("/Archive") == -1:
                if text.startswith('#REDIRECT'):
                    continue
                
               
                text = text.lower()
                
                
                if '{{infobox military conflict' in text:
                    print(title)
                    
                    text=text.replace('\n', '')            
                    text=text.replace('\r', '') 
                    text=re.findall(r'({{infobox military conflict.*)',text)
                    text=text[0]
                    allid=allid.append(pd.DataFrame({'wid':[wid],'title':[title]}),ignore_index=True)
                    #counter = counter + 1
           # if counter==1:
                #break 
                    ccount = 0
                    str_list = ''
                    for c in text:
                        str_list=str_list+c
                        if c=='{':
                            ccount=ccount+1
                            
                        if c=='}':
                            ccount=ccount-1
                        if ccount==0:
                            break
                    writer=writer.append(pd.DataFrame({'wid':[wid],'tmp':[str_list]}),ignore_index=True)    
                    counter = counter + 1
                    print(counter)
            #if counter==50: 
                #break
                    
                    
                    
                    
            
writer=pd.merge(allid,writer,on='wid',how='left')

writer.to_csv(main_path+'articles/infobox/input/infobox_new.csv')   
