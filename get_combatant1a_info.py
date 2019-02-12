# -*- coding: utf-8 -*-
"""
Created on Sun Jan 27 17:22:46 2019

@author: leo
"""
import numpy as np
import pandas as pd
import sys
import re
import random
import datetime
import datetime
import calendar
from os import mkdir
main_path = "D:/learning/Arash/war_participants/articles/infobox/"
try:
    mkdir(main_path)
except FileExistsError:
    pass
writer=pd.read_csv(main_path+'input/infobox_new.csv') 


def function0(a,b):
    if a!=0:
        return a
    else:
        return b
info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]

#############combatant1
combatant1a.drop('combatant1a',axis=1, inplace=True)
combatant1a['comb1_1a']=combatant1a['tmp'].str.extract(r'combatant1(.*?)combatant1a',expand=False)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)combatant',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)strength',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)casualties',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['no_support']=combatant1a['comb1_1a'].str.extract(r'(.*?)support',expand=False)
combatant1a['no_support']=combatant1a['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1a['combatant1_nosup']=combatant1a.apply(lambda x: function(x.no_support,x.comb1_1a), axis=1)

combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
combatant1a['combatant1_nosup']=combatant1a['combatant1_nosup'].fillna(0)
rcount=0

for row in combatant1a.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][5]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
    if row[1][5]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][5]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant1_infobox['second']=combatant1_infobox['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)
combatant1_infobox_second=combatant1_infobox.loc[combatant1_infobox['second']!=0]
combatant1_infobox_first=combatant1_infobox.loc[combatant1_infobox['second']==0]

combatant1_infobox_2nd = pd.DataFrame(columns=('wid','combatant1_nosup'))
rcount=0

for row in combatant1_infobox_second.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:
        combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][3]]}),ignore_index=True) 
    if row[1][3]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][3]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0
combatant1_infobox_2nd.loc[combatant1_infobox_2nd.wid==145401,'combatant1_nosup']='[[mexico]]'
combatant1_infobox_2nd.loc[combatant1_infobox_2nd.wid==1800030,'combatant1_nosup']='[[zimbabwe rhodesia]]'

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd.loc[combatant1_infobox_2nd.wid==44035,'combatant1_nosup']='[[north german confederation]]'
combatant1_infobox_2nd['middle']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox_2nd['big']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant1_infobox_first.drop('second',axis=1, inplace=True)
combatant1_infobox=combatant1_infobox_first.append(combatant1_infobox_2nd)

combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)
###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='kia']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='startplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='notelist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='noflag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='no flag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='surrendered']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullet']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='!']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='plainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='endplainlist']

combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)
wid_list_after=combatant1_infobox.loc[:,'wid']
wid_list_after.drop_duplicates(inplace=True)
wid_list_after=list(wid_list_after)
no_brackets=combatant1a[~combatant1a.wid.isin(wid_list_after)]

no_brackets.loc[no_brackets.wid==9002153,'combatant1_nosup']='luikvlag'
no_brackets['combatant1_nosup']=no_brackets['combatant1_nosup'].str.strip()
no_brackets['step1']=no_brackets['combatant1_nosup'].str.extract(r'(.*)\|$',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step1']=no_brackets.apply(lambda x: function(x.step1,x.combatant1_nosup), axis=1)
no_brackets['step1']=no_brackets['step1'].str.strip()
no_brackets['step2']=no_brackets['step1'].str.extract(r'^=(.*)',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step2']=no_brackets.apply(lambda x: function(x.step2,x.step1), axis=1)
no_brackets['step2']=no_brackets['step2'].str.strip()
no_brackets=no_brackets.rename(columns={'step2':'participants'})

no_brackets=no_brackets.loc[:,['wid','participants']]
participant1_infobox=combatant1_infobox.loc[:,['wid','participants']]
participant1a_1_infobox=participant1_infobox.append(no_brackets)
participant1a_1_infobox['side']=1
participant1a_1_infobox.to_csv(main_path+'output/participant1a_1_infobox.csv')

######################combatant1a

info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]

combatant1a.drop('combatant1a',axis=1, inplace=True)
combatant1a['comb1_1a']=combatant1a['tmp'].str.extract(r'combatant1a(.*?)combatant2',expand=False)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')
combatant1a.loc[combatant1a.wid==57536311,'comb1_1a']='[[prussia]]'
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.strip()
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=|',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('= |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=  |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].fillna(0)
combatant1a=combatant1a[combatant1a['comb1_1a']!=0]
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)combatant',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)strength',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)casualties',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['no_support']=combatant1a['comb1_1a'].str.extract(r'(.*?)support',expand=False)
combatant1a['no_support']=combatant1a['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1a['combatant1_nosup']=combatant1a.apply(lambda x: function(x.no_support,x.comb1_1a), axis=1)

combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
combatant1a['combatant1_nosup']=combatant1a['combatant1_nosup'].fillna(0)
rcount=0

for row in combatant1a.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][5]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
    if row[1][5]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][5]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{ref\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sup\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant1_infobox['second']=combatant1_infobox['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)
combatant1_infobox_second=combatant1_infobox.loc[combatant1_infobox['second']!=0]
combatant1_infobox_first=combatant1_infobox.loc[combatant1_infobox['second']==0]

combatant1_infobox_2nd = pd.DataFrame(columns=('wid','combatant1_nosup'))
rcount=0

for row in combatant1_infobox_second.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:
        combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][3]]}),ignore_index=True) 
    if row[1][3]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][3]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0
                
combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['middle']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox_2nd['big']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant1_infobox_first.drop('second',axis=1, inplace=True)
combatant1_infobox=combatant1_infobox_first.append(combatant1_infobox_2nd)

combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)

###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='kia']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='startplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='notelist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='noflag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='no flag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='surrendered']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullet']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='!']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='plainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='endplainlist']

combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)
wid_list_after=combatant1_infobox.loc[:,'wid']
wid_list_after.drop_duplicates(inplace=True)
wid_list_after=list(wid_list_after)
no_brackets=combatant1a[~combatant1a.wid.isin(wid_list_after)]#after checking these items, we can see that we don't need to care them

participant1a_1a_infobox=combatant1_infobox.loc[:,['wid','participants']]

participant1a_1a_infobox['side']='1a'
participant1a_1a_infobox.to_csv(main_path+'output/participant1a_1a_infobox.csv')


######################combatant2

info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]

combatant1a.drop('combatant1a',axis=1, inplace=True)
combatant1a['comb1_1a']=combatant1a['tmp'].str.extract(r'combatant2(?![ab])(.*?)combatant',expand=False)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')





combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.strip()
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=|',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('= |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=  |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].fillna(0)
combatant1a=combatant1a[combatant1a['comb1_1a']!=0]
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)combatant',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)strength',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)casualties',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['no_support']=combatant1a['comb1_1a'].str.extract(r'(.*?)support',expand=False)
combatant1a['no_support']=combatant1a['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1a['combatant1_nosup']=combatant1a.apply(lambda x: function(x.no_support,x.comb1_1a), axis=1)

combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
combatant1a['combatant1_nosup']=combatant1a['combatant1_nosup'].fillna(0)
rcount=0

for row in combatant1a.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][5]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
    if row[1][5]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][5]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{ref\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sup\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant1_infobox['second']=combatant1_infobox['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)
combatant1_infobox_second=combatant1_infobox.loc[combatant1_infobox['second']!=0]
combatant1_infobox_first=combatant1_infobox.loc[combatant1_infobox['second']==0]

combatant1_infobox_2nd = pd.DataFrame(columns=('wid','combatant1_nosup'))
rcount=0

for row in combatant1_infobox_second.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:
        combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][3]]}),ignore_index=True) 
    if row[1][3]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][3]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0
                
combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['middle']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox_2nd['big']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant1_infobox_first.drop('second',axis=1, inplace=True)
combatant1_infobox=combatant1_infobox_first.append(combatant1_infobox_2nd)

combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)

###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='kia']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='startplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='plainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='endplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='notelist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='noflag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='no flag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='surrendered']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullet']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='!']

combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)
wid_list_after=combatant1_infobox.loc[:,'wid']
wid_list_after.drop_duplicates(inplace=True)
wid_list_after=list(wid_list_after)
no_brackets=combatant1a[~combatant1a.wid.isin(wid_list_after)]
no_brackets.loc[no_brackets.wid==39447818,'combatant1_nosup']='umayyad|sheikhdom of mohammerah'
no_brackets['combatant1_nosup']=no_brackets['combatant1_nosup'].str.strip()
no_brackets['step1']=no_brackets['combatant1_nosup'].str.extract(r'(.*)\|$',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step1']=no_brackets.apply(lambda x: function(x.step1,x.combatant1_nosup), axis=1)
no_brackets['step1']=no_brackets['step1'].str.strip()
no_brackets['step2']=no_brackets['step1'].str.extract(r'^=(.*)',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step2']=no_brackets.apply(lambda x: function(x.step2,x.step1), axis=1)
no_brackets['step2']=no_brackets['step2'].str.strip()
no_brackets=no_brackets.rename(columns={'step2':'participants'})

no_brackets=no_brackets.loc[:,['wid','participants']]
participant1_infobox=combatant1_infobox.loc[:,['wid','participants']]
participant1a_2_infobox=participant1_infobox.append(no_brackets)
participant1a_2_infobox['side']=2
participant1a_2_infobox.to_csv(main_path+'output/participant1a_2_infobox.csv')

#######################combatant2a

info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]

combatant1a.drop('combatant1a',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['tmp'].str.extract(r'combatant2a(.*)',expand=False)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)commander',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)combatant',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)strength',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)casualties',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.strip()
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=|',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('= |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=  |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].fillna(0)

combatant1a=combatant1a[combatant1a['comb1_1a']!=0]
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['no_support']=combatant1a['comb1_1a'].str.extract(r'(.*?)support',expand=False)
combatant1a['no_support']=combatant1a['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1a['combatant1_nosup']=combatant1a.apply(lambda x: function(x.no_support,x.comb1_1a), axis=1)
combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
combatant1a['combatant1_nosup']=combatant1a['combatant1_nosup'].fillna(0)
rcount=0

for row in combatant1a.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][5]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
    if row[1][5]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][5]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{ref\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sup\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant1_infobox['second']=combatant1_infobox['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)
combatant1_infobox_second=combatant1_infobox.loc[combatant1_infobox['second']!=0]
combatant1_infobox_first=combatant1_infobox.loc[combatant1_infobox['second']==0]

combatant1_infobox_2nd = pd.DataFrame(columns=('wid','combatant1_nosup'))
rcount=0

for row in combatant1_infobox_second.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:
        combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][3]]}),ignore_index=True) 
    if row[1][3]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][3]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0
                
combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['middle']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox_2nd['big']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant1_infobox_first.drop('second',axis=1, inplace=True)
combatant1_infobox=combatant1_infobox_first.append(combatant1_infobox_2nd)

combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)
combatant1_infobox['big_3']=combatant1_infobox['big_3'].str.strip()
###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='kia']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='startplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='plainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='endplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='notelist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='noflag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='no flag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='surrendered']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullet']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='!']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullets = yes']



combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)

wid_list_after=combatant1_infobox.loc[:,'wid']
wid_list_after.drop_duplicates(inplace=True)
wid_list_after=list(wid_list_after)
no_brackets=combatant1a[~combatant1a.wid.isin(wid_list_after)]

participant1a_2a_infobox=combatant1_infobox.loc[:,['wid','participants']]

participant1a_2a_infobox['side']='2a'
participant1a_2a_infobox.to_csv(main_path+'output/participant1a_2a_infobox.csv')


#############combatant3
info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]

combatant1a.drop('combatant1a',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['tmp'].str.extract(r'combatant3(?![ab])(.*)',expand=False)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)commander',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)combatant',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)strength',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)casualties',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.strip()
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=|',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('= |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=  |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].fillna(0)

combatant1a=combatant1a[combatant1a['comb1_1a']!=0]
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['no_support']=combatant1a['comb1_1a'].str.extract(r'(.*?)support',expand=False)
combatant1a['no_support']=combatant1a['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1a['combatant1_nosup']=combatant1a.apply(lambda x: function(x.no_support,x.comb1_1a), axis=1)
combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
combatant1a['combatant1_nosup']=combatant1a['combatant1_nosup'].fillna(0)
rcount=0

for row in combatant1a.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][5]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
    if row[1][5]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][5]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{ref\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sup\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)
combatant1_infobox['big_3']=combatant1_infobox['big_3'].str.strip()
###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='kia']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='startplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='plainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='endplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='notelist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='noflag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='no flag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='surrendered']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullet']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='!']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullets = yes']

combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)

participant1a_3_infobox=combatant1_infobox.loc[:,['wid','participants']]
participant1a_3_infobox['side']=3
participant1a_3_infobox.to_csv(main_path+'output/participant1a_3_infobox.csv')

#######################combatant3a
info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]

combatant1a.drop('combatant1a',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['tmp'].str.extract(r'combatant3a(.*)',expand=False)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)commander',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)combatant',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)strength',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)casualties',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.strip()
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=|',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('= |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=  |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].fillna(0)

combatant1a=combatant1a[combatant1a['comb1_1a']!=0]
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['no_support']=combatant1a['comb1_1a'].str.extract(r'(.*?)support',expand=False)
combatant1a['no_support']=combatant1a['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1a['combatant1_nosup']=combatant1a.apply(lambda x: function(x.no_support,x.comb1_1a), axis=1)
combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
combatant1a['combatant1_nosup']=combatant1a['combatant1_nosup'].fillna(0)
rcount=0

for row in combatant1a.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][5]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
    if row[1][5]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][5]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{ref\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sup\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)
combatant1_infobox['big_3']=combatant1_infobox['big_3'].str.strip()
###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='kia']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='startplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='plainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='endplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='notelist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='noflag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='no flag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='surrendered']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullet']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='!']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullets = yes']

combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)

participant1a_3a_infobox=combatant1_infobox.loc[:,['wid','participants']]
participant1a_3a_infobox['side']='3a'
participant1a_3a_infobox.to_csv(main_path+'output/participant1a_3a_infobox.csv')

#######################combatant1b
info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]

combatant1a.drop('combatant1a',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['tmp'].str.extract(r'combatant1b(.*)',expand=False)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)commander',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)combatant',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)strength',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)casualties',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.strip()
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=|',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('= |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=  |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].fillna(0)

combatant1a=combatant1a[combatant1a['comb1_1a']!=0]
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['no_support']=combatant1a['comb1_1a'].str.extract(r'(.*?)support',expand=False)
combatant1a['no_support']=combatant1a['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1a['combatant1_nosup']=combatant1a.apply(lambda x: function(x.no_support,x.comb1_1a), axis=1)
combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
combatant1a['combatant1_nosup']=combatant1a['combatant1_nosup'].fillna(0)
rcount=0

for row in combatant1a.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][5]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
    if row[1][5]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][5]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{ref\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sup\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant1_infobox['second']=combatant1_infobox['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)
combatant1_infobox_second=combatant1_infobox.loc[combatant1_infobox['second']!=0]
combatant1_infobox_first=combatant1_infobox.loc[combatant1_infobox['second']==0]

combatant1_infobox_2nd = pd.DataFrame(columns=('wid','combatant1_nosup'))
rcount=0

for row in combatant1_infobox_second.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:
        combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][3]]}),ignore_index=True) 
    if row[1][3]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][3]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['middle']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox_2nd['big']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant1_infobox_first.drop('second',axis=1, inplace=True)
combatant1_infobox=combatant1_infobox_first.append(combatant1_infobox_2nd)

combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)

###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='kia']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='startplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='notelist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='noflag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='no flag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='surrendered']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullet']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='!']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='plainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='endplainlist']

combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)
wid_list_after=combatant1_infobox.loc[:,'wid']
wid_list_after.drop_duplicates(inplace=True)
wid_list_after=list(wid_list_after)
no_brackets=combatant1a[~combatant1a.wid.isin(wid_list_after)]#after checking these items, we can see that we don't need to care them

participant1a_1b_infobox=combatant1_infobox.loc[:,['wid','participants']]

participant1a_1b_infobox['side']='1b'
participant1a_1b_infobox.to_csv(main_path+'output/participant1a_1b_infobox.csv')

#######################combatant2b
info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]

combatant1a.drop('combatant1a',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['tmp'].str.extract(r'combatant2b(.*)',expand=False)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)commander',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)combatant',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)strength',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)casualties',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.strip()
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=|',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('= |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=  |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].fillna(0)

combatant1a=combatant1a[combatant1a['comb1_1a']!=0]
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['no_support']=combatant1a['comb1_1a'].str.extract(r'(.*?)support',expand=False)
combatant1a['no_support']=combatant1a['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1a['combatant1_nosup']=combatant1a.apply(lambda x: function(x.no_support,x.comb1_1a), axis=1)
combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
combatant1a['combatant1_nosup']=combatant1a['combatant1_nosup'].fillna(0)
rcount=0

for row in combatant1a.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][5]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
    if row[1][5]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][5]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{ref\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sup\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant1_infobox['second']=combatant1_infobox['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)
combatant1_infobox_second=combatant1_infobox.loc[combatant1_infobox['second']!=0]
combatant1_infobox_first=combatant1_infobox.loc[combatant1_infobox['second']==0]

combatant1_infobox_2nd = pd.DataFrame(columns=('wid','combatant1_nosup'))
rcount=0

for row in combatant1_infobox_second.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:
        combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][3]]}),ignore_index=True) 
    if row[1][3]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][3]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox_2nd=combatant1_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['middle']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox_2nd['big']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant1_infobox_first.drop('second',axis=1, inplace=True)
combatant1_infobox=combatant1_infobox_first.append(combatant1_infobox_2nd)

combatant1_infobox['x']=combatant1_infobox['big'].str.extract(r'^abbr\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox['big']=combatant1_infobox.apply(lambda x: function0(x.x,x.big), axis=1)
combatant1_infobox.drop('x',axis=1, inplace=True)


combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)

###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='kia']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='startplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='notelist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='noflag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='no flag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='surrendered']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullet']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='!']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='plainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='endplainlist']

combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)
wid_list_after=combatant1_infobox.loc[:,'wid']
wid_list_after.drop_duplicates(inplace=True)
wid_list_after=list(wid_list_after)
no_brackets=combatant1a[~combatant1a.wid.isin(wid_list_after)]#after checking these items, we can see that we don't need to care them

participant1a_2b_infobox=combatant1_infobox.loc[:,['wid','participants']]

participant1a_2b_infobox['side']='2b'
participant1a_2b_infobox.to_csv(main_path+'output/participant1a_2b_infobox.csv')

#######################combatant3b
info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]

combatant1a.drop('combatant1a',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['tmp'].str.extract(r'combatant3b(.*)',expand=False)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)commander',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)combatant',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)strength',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_2']=combatant1a['comb1_1a'].str.extract(r'(.*?)casualties',expand=False)
combatant1a=combatant1a.fillna(0)
combatant1a['comb1_1a']=combatant1a.apply(lambda x: function0(x.comb1_2,x.comb1_1a), axis=1)
combatant1a.drop('comb1_2',axis=1, inplace=True)

combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.strip()
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=|',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('= |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].replace('=  |',np.nan)
combatant1a['comb1_1a']=combatant1a['comb1_1a'].fillna(0)

combatant1a=combatant1a[combatant1a['comb1_1a']!=0]
combatant1a['comb1_1a']=combatant1a['comb1_1a'].str.replace(r'<ref.*?>.*?</ref>','')

combatant1a['no_support']=combatant1a['comb1_1a'].str.extract(r'(.*?)support',expand=False)
combatant1a['no_support']=combatant1a['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1a['combatant1_nosup']=combatant1a.apply(lambda x: function(x.no_support,x.comb1_1a), axis=1)
combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
combatant1a['combatant1_nosup']=combatant1a['combatant1_nosup'].fillna(0)
rcount=0

for row in combatant1a.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][5]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
    if row[1][5]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][5]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0
                
combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{ref\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sup\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)
combatant1_infobox['big_3']=combatant1_infobox['big_3'].str.strip()
###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='kia']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='startplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='plainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='endplainlist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='notelist']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='noflag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='no flag']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='surrendered']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullet']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='!']
combatant1_infobox=combatant1_infobox[combatant1_infobox['big_3']!='bullets = yes']

combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)

participant1a_3b_infobox=combatant1_infobox.loc[:,['wid','participants']]
participant1a_3b_infobox['side']='3b'
participant1a_3b_infobox.to_csv(main_path+'output/participant1a_3b_infobox.csv')


