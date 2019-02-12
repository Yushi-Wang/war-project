# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24 20:11:40 2019

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



#################################participants （combatant2) information
info_combatant2=writer.loc[:,['wid','title','tmp']]
info_combatant2['combatant1a']=info_combatant2['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant2['combatant1a']=info_combatant2['combatant1a'].fillna(0)
combatant1a=info_combatant2[info_combatant2['combatant1a']!=0]####need to be treated specially

info_combatant2=info_combatant2[info_combatant2['combatant1a']==0]
info_combatant2.drop('combatant1a',axis=1, inplace=True)
info_combatant2['combatant2']=info_combatant2['tmp'].str.extract('((?<=combatant2).*?(?=commander))',expand=False)
info_combatant2['combatant2']=info_combatant2['combatant2'].fillna(0)
missing=info_combatant2[info_combatant2['combatant2']==0]#leave these wars aside, focus on those nonmissing wars
info_combatant2_nonmissing=info_combatant2[info_combatant2['combatant2']!=0]

####clean missing
missing['combatant1']=missing['tmp'].str.extract('((?<=combatant2).*?(?=combatant1))',expand=False)

missing['units']=missing['tmp'].str.extract('((?<=combatant2).*?(?=units))',expand=False)
missing['casualties']=missing['units'].str.extract('(.*?)casualties',expand=False)
missing=missing.fillna(0)
def function0(a,b):
    if a!=0:
        return a
    else:
        return b
missing['units']=missing.apply(lambda x: function0(x.casualties,x.units), axis=1)
missing.drop('casualties',axis=1, inplace=True)
missing['strength']=missing['units'].str.extract('(.*?)strength',expand=False)
missing=missing.fillna(0)
missing['units']=missing.apply(lambda x: function0(x.strength,x.units), axis=1)
missing.drop('strength',axis=1, inplace=True)

missing['combatant3']=missing['tmp'].str.extract(r'((?<=combatant2).*?(?=combatant3))',expand=False)
missing=missing.fillna(0)
def function1(a,b,c):
    if a!=0:
        return a
    elif b!=0:
        return b
    else:
        return c
missing['combatant2']=missing.apply(lambda x: function1(x.combatant1,x.combatant3,x.units), axis=1)

missing.drop(['combatant1','units','combatant3'],axis=1, inplace=True)

missing['strength']=missing['tmp'].str.extract(r'((?<=combatant2).*?(?=strength))',expand=False)
missing=missing.fillna(0)
missing['combatant2']=missing.apply(lambda x: function0(x.combatant2,x.strength), axis=1)
missing.drop('strength',axis=1, inplace=True)

missing['casualties']=missing['tmp'].str.extract(r'((?<=combatant2).*?(?=casualties))',expand=False)
missing=missing.fillna(0)
missing['combatant2']=missing.apply(lambda x: function0(x.combatant2,x.casualties), axis=1)
missing.drop('casualties',axis=1, inplace=True)

missing['campaignbox']=missing['tmp'].str.extract(r'((?<=combatant2).*?(?=\{\{campaignbox))',expand=False)
missing=missing.fillna(0)
missing['combatant2']=missing.apply(lambda x: function0(x.combatant2,x.campaignbox), axis=1)
missing.drop('campaignbox',axis=1, inplace=True)

missing['end']=missing['tmp'].str.extract(r'(?<=combatant2)(.*)',expand=False)
missing=missing.fillna(0)
missing['combatant2']=missing.apply(lambda x: function0(x.combatant2,x.end), axis=1)
missing.drop('end',axis=1, inplace=True)

###clean non-missing
info_combatant2_nonmissing['combatant3']=info_combatant2_nonmissing['combatant2'].str.extract('(.*)combatant3',expand=False)
info_combatant2_nonmissing=info_combatant2_nonmissing.fillna(0)
info_combatant2_nonmissing['combatant2']=info_combatant2_nonmissing.apply(lambda x: function0(x.combatant3,x.combatant2), axis=1)
info_combatant2_nonmissing.drop('combatant3',axis=1, inplace=True)

info_combatant2_nonmissing['combatant1']=info_combatant2_nonmissing['combatant2'].str.extract(r'(.*)combatant1',expand=False)
info_combatant2_nonmissing=info_combatant2_nonmissing.fillna(0)
info_combatant2_nonmissing['combatant2']=info_combatant2_nonmissing.apply(lambda x: function0(x.combatant1,x.combatant2), axis=1)
info_combatant2_nonmissing.drop('combatant1',axis=1, inplace=True)

info_combatant2_nonmissing['strength']=info_combatant2_nonmissing['combatant2'].str.extract(r'(.*?)strength',expand=False)
info_combatant2_nonmissing=info_combatant2_nonmissing.fillna(0)
info_combatant2_nonmissing['combatant2']=info_combatant2_nonmissing.apply(lambda x: function0(x.strength,x.combatant2), axis=1)
info_combatant2_nonmissing.drop('strength',axis=1, inplace=True)

info_combatant2_nonmissing['strength']=info_combatant2_nonmissing['combatant2'].str.extract(r'(.*?)casualties',expand=False)
info_combatant2_nonmissing=info_combatant2_nonmissing.fillna(0)
info_combatant2_nonmissing['combatant2']=info_combatant2_nonmissing.apply(lambda x: function0(x.strength,x.combatant2), axis=1)
info_combatant2_nonmissing.drop('strength',axis=1, inplace=True)
info_combatant2=info_combatant2_nonmissing.append(missing)

#####remove support
info_combatant2['no_support']=info_combatant2['combatant2'].str.extract(r'(.*?)support',expand=False)
info_combatant2['no_support']=info_combatant2['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
info_combatant2['combatant2_nosup']=info_combatant2.apply(lambda x: function(x.no_support,x.combatant2), axis=1)
#remove<ref></ref>
info_combatant2['with<ref>']=info_combatant2['combatant2_nosup'].str.extract(r'(<ref.*?>.*?</ref>)',expand=False)
info_combatant2['without<ref>']=info_combatant2['combatant2_nosup'].str.replace(r'<ref.*?>.*?</ref>','')

######begin to extract the information in {{}} and [[]]
combatant2_infobox = pd.DataFrame(columns=('wid','combatant2_nosup'))
info_combatant2['without<ref>']=info_combatant2['without<ref>'].fillna(0)
rcount=0

for row in info_combatant2.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][7]==0:
        combatant2_infobox=combatant2_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant2_nosup':[row[1][7]]}),ignore_index=True) 
    if row[1][7]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][7]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                combatant2_infobox=combatant2_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant2_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

####further cleaning


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^{{refn.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]

combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^{{ref\|.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]

combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]

combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]

combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{resize\|.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{efn\|.*}})',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\[\*\]$)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{\*\}\}$)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{\-\}\}$)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{br\}\}$)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{plainlist\}\}$)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{collapsible list\}\}$)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{#tag:)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{cn\|)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{endplainlist\}\}$)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{notetag\|.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]

combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{cref2\|.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]

combatant2_infobox['x1']=combatant2_infobox['combatant2_nosup'].str.extract(r'(^\{\{executed\}\}$)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)          
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['x1']==0]
combatant2_infobox=combatant2_infobox.loc[:,['wid','combatant2_nosup']]


combatant2_infobox['middle']=combatant2_infobox['combatant2_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant2_infobox['big']=combatant2_infobox['combatant2_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

####second decompression
combatant2_infobox['second']=combatant2_infobox['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0)
combatant2_infobox_second=combatant2_infobox.loc[combatant2_infobox['second']!=0]
combatant2_infobox_first=combatant2_infobox.loc[combatant2_infobox['second']==0]

combatant2_infobox_2nd = pd.DataFrame(columns=('wid','combatant2_nosup'))
rcount=0

for row in combatant2_infobox_second.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:
        combatant2_infobox_2nd=combatant2_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant2_nosup':[row[1][3]]}),ignore_index=True) 
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
                combatant2_infobox_2nd=combatant2_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant2_nosup':[text]}),ignore_index=True)
                text=''
                sm=0
                
combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^{{refn.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]

combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^{{ref\|.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]

combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]

combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{resize\|.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]

combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]



combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{efn\|.*}})',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]



combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]



combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\[\*\]$)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{\*\}\}$)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{\-\}\}$)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{br\}\}$)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{plainlist\}\}$)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{collapsible list\}\}$)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{#tag:)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{cn\|)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{endplainlist\}\}$)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{notetag\|.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{cref2\|.*)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]


combatant2_infobox_2nd['x1']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'(^\{\{executed\}\}$)',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)          
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[combatant2_infobox_2nd['x1']==0]
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup']]

combatant2_infobox_2nd['middle']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant2_infobox_2nd['big']=combatant2_infobox_2nd['combatant2_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant2_infobox_2nd['second']=combatant2_infobox_2nd['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant2_infobox_2nd['third']=combatant2_infobox_2nd['second'].str.extract(r'^\[\[file:.*?\]\] (\[\[.*\]\])',expand=False)

def function2(a,b):
    if a!=0:
        return a
    else:
        return b
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)  
combatant2_infobox_2nd['second']=combatant2_infobox_2nd.apply(lambda x: function2(x.third,x.second), axis=1) 

combatant2_infobox_2nd['second_middle']=combatant2_infobox_2nd['second'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant2_infobox_2nd['second_big']=combatant2_infobox_2nd['second'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant2_infobox_2nd=combatant2_infobox_2nd.fillna(0)  
combatant2_infobox_2nd['middle']=combatant2_infobox_2nd.apply(lambda x: function2(x.second_middle,x.middle), axis=1)              
combatant2_infobox_2nd['big']=combatant2_infobox_2nd.apply(lambda x: function2(x.second_big,x.big), axis=1)              
combatant2_infobox_2nd=combatant2_infobox_2nd.loc[:,['wid','combatant2_nosup','middle','big']]
combatant2_infobox_first=combatant2_infobox_first.loc[:,['wid','combatant2_nosup','middle','big']]
combatant2_infobox=combatant2_infobox_first.append(combatant2_infobox_2nd) 

####get the country out
combatant2_infobox['big_ex']=combatant2_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant2_infobox['x']=combatant2_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant2_infobox.loc[combatant2_infobox['x']==1]
combatant2_infobox['big_2']=combatant2_infobox.apply(lambda x: function2(x.big_ex,x.big), axis=1)
combatant2_infobox['big_eex']=combatant2_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant2_infobox=combatant2_infobox.fillna(0) 
combatant2_infobox['big_3']=combatant2_infobox.apply(lambda x: function2(x.big_eex,x.big_2), axis=1)

###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='kia']
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='startplainlist']
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='notelist']
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='noflag']
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='no flag']
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='surrendered']
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='bullet']
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='!']
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='plainlist']
combatant2_infobox=combatant2_infobox[combatant2_infobox['big_3']!='endplainlist']




combatant2_infobox['participants']=combatant2_infobox.apply(lambda x: function2(x.middle,x.big_3), axis=1)



##########consider those that have participants information that is not in the brackets
wid_list_after=combatant2_infobox.loc[:,'wid']
wid_list_after.drop_duplicates(inplace=True)
wid_list_after=list(wid_list_after)
no_brackets=info_combatant2[~info_combatant2.wid.isin(wid_list_after)]
"""
no_brackets['step1']=no_brackets['without<ref>'].str.extract(r'=(.*)',expand=False)
no_brackets['step1']=no_brackets['step1'].str.strip()
no_brackets['step1']=no_brackets['step1'].replace('|',np.nan)
no_brackets['step1']=no_brackets['step1'].replace("'''",np.nan)
no_brackets['step1']=no_brackets['step1'].replace("",np.nan)
no_brackets['step2']=no_brackets['step1'].str.extract(r'^\{\{flagicon.*?\}\}(.*)',expand=False)

no_brackets=no_brackets.fillna(0)
no_brackets['step2']=no_brackets.apply(lambda x: function2(x.step2,x.step1), axis=1)

no_brackets['step3']=no_brackets['step2'].str.extract(r'^\{\{flagdeco.*?\}\}(.*)',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step3']=no_brackets.apply(lambda x: function2(x.step3,x.step2), axis=1)


no_brackets['step4']=no_brackets['step3'].str.extract(r'^\[\[file.*?\]\](.*)',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step4']=no_brackets.apply(lambda x: function2(x.step4,x.step3), axis=1)

no_brackets['step5']=no_brackets['step4'].str.extract(r'^\[\[image.*?\]\](.*)',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step5']=no_brackets.apply(lambda x: function2(x.step5,x.step4), axis=1)


no_brackets['step6']=no_brackets['step5'].str.extract(r'(.*)\|$',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step6']=no_brackets.apply(lambda x: function2(x.step6,x.step5), axis=1)
no_brackets['step6']=no_brackets['step6'].str.strip()

no_brackets=no_brackets.loc[:,['wid','title','tmp','combatant2','no_support','combatant2_nosup','with<ref>','without<ref>','step6']]

no_brackets['step7']=no_brackets['step6'].str.extract(r"(.*)<br />'''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step7']=no_brackets.apply(lambda x: function2(x.step7,x.step6), axis=1)

no_brackets['step8']=no_brackets['step7'].str.extract(r"(.*)<br/>'''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step8']=no_brackets.apply(lambda x: function2(x.step8,x.step7), axis=1)

no_brackets['step9']=no_brackets['step8'].str.extract(r"(.*)<br>'''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step9']=no_brackets.apply(lambda x: function2(x.step9,x.step8), axis=1)

no_brackets['step10']=no_brackets['step9'].str.extract(r"(.*)\{\{-\}\}''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step10']=no_brackets.apply(lambda x: function2(x.step10,x.step9), axis=1)

no_brackets['step11']=no_brackets['step10'].str.extract(r"(.*)<br>''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step11']=no_brackets.apply(lambda x: function2(x.step11,x.step10), axis=1)

no_brackets['step12']=no_brackets['step11'].str.extract(r"(.*)<br/>''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step12']=no_brackets.apply(lambda x: function2(x.step12,x.step11), axis=1)

no_brackets['step13']=no_brackets['step12'].str.extract(r"^<center>(.*)",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step13']=no_brackets.apply(lambda x: function2(x.step13,x.step12), axis=1)

no_brackets['step14']=no_brackets['step13'].str.extract(r"^'''(.*)",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step14']=no_brackets.apply(lambda x: function2(x.step14,x.step13), axis=1)
no_brackets['step14']=no_brackets['step14'].str.strip()

no_brackets['step15']=no_brackets['step14'].str.extract(r"(.*)'''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step15']=no_brackets.apply(lambda x: function2(x.step15,x.step14), axis=1)

no_brackets['step16']=no_brackets['step15'].str.extract(r"(.*)\}\}$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step16']=no_brackets.apply(lambda x: function2(x.step16,x.step15), axis=1)

no_brackets=no_brackets.loc[:,['wid','title','tmp','combatant2','no_support','combatant2_nosup','with<ref>','without<ref>','step6','step16']]

no_brackets['step17']=no_brackets['step16'].str.extract(r"(.*)'''military$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step17']=no_brackets.apply(lambda x: function2(x.step17,x.step16), axis=1)
no_brackets['step17']=no_brackets['step17'].str.strip()

no_brackets['step18']=no_brackets['step17'].str.extract(r"(.*)<br>$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step18']=no_brackets.apply(lambda x: function2(x.step18,x.step17), axis=1)


no_brackets['step19']=no_brackets['step18'].str.extract(r"^''(.*)''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step19']=no_brackets.apply(lambda x: function2(x.step19,x.step18), axis=1)


no_brackets=no_brackets.loc[:,['wid','title','tmp','combatant2','no_support','combatant2_nosup','with<ref>','without<ref>','step6','step19']]







no_brackets.loc[no_brackets.wid==7712,'step19']='[[soviet union]][[united kingdom]]'
no_brackets.loc[no_brackets.wid==18569,'step19']='nazi germany'
no_brackets.loc[no_brackets.wid==69980,'step19']='empire of japan'
no_brackets.loc[no_brackets.wid==387114,'step19']='[[southern jeob]][[northern jeob]]'
no_brackets.loc[no_brackets.wid==877358,'step19']='[[kingdom of great britain]]<br>[[kingdom of ireland]]'
no_brackets.loc[no_brackets.wid==990954,'step19']='marshal of the army of god and holy church|army of god and holy church'
no_brackets.loc[no_brackets.wid==1557605,'step19']='[[northern military administration office army]][[korean independence army]]'
no_brackets.loc[no_brackets.wid==1561462,'step19']='[[national patriotic front of liberia|npfl]]*[[independent national patriotic front of liberia|inpfl]]'
no_brackets.loc[no_brackets.wid==2427487,'step19']='[[local anti-coalition militants]]*[[local pro-taliban nationals]]'
no_brackets.loc[no_brackets.wid==2731409,'step19']='pakistan'
no_brackets.loc[no_brackets.wid==3110360,'step19']='[[forces of licinius]]<br />[[eastern empire]]'
no_brackets.loc[no_brackets.wid==3128569,'step19']='[[rebel mercenaries]]<br/>[[rebelling libyan towns and cities]]'
no_brackets.loc[no_brackets.wid==3194709,'step19']='[[house of blois]]<br>[[france]]'
no_brackets.loc[no_brackets.wid==3562168,'step19']='[[iraqi rebels]]* [[shia tribesmen]]* [[sunni tribesmen]]* [[kurdish and tyari tribesmen]]'
no_brackets.loc[no_brackets.wid==4052710,'step19']='[[scottish royalists]]*[[danish & german mercenaries]]*[[orcadian infantry]]'
no_brackets.loc[no_brackets.wid==5460904,'step19']='[[neqab organization]]*[[iran patriotic officers (nupa)]]'
no_brackets.loc[no_brackets.wid==5908130,'step19']='[[brahmins of kandalur salai]]<br />[[kulasekhara/kodungallur chera]]'
no_brackets.loc[no_brackets.wid==6012083,'step19']='south korea'
no_brackets.loc[no_brackets.wid==6113376,'step19']='emzar kvitsiani factions'
no_brackets.loc[no_brackets.wid==6871959,'step19']="[[arms of the crown of castile (15th century)|joanna's supporters]][[kingdom of portugal]][[kingdom of france]]"
no_brackets.loc[no_brackets.wid==7082595,'step19']='[[national liberation front (south yemen)|nlf]]*[[front for the liberation of occupied south yemen|flosy]]'
no_brackets.loc[no_brackets.wid==12160787,'step19']="[[oʻahu army]]<br />[[kaʻiana's defector army]]"
no_brackets.loc[no_brackets.wid==12582371,'step19']='[[maredudd ap gruffydd]] [[idwal ap gruffydd]]'
no_brackets.loc[no_brackets.wid==12900447,'step19']='[[empire of brazil]] [[rebels]]*[[balaios]]*[[african slaves]]'
no_brackets.loc[no_brackets.wid==14221798,'step19']='[[bandeira cabanagem]] [[rebels]]* [[cabanos]]* [[indians]] * [[slaves]] *[[merchants]]*[[farmers]]'
no_brackets.loc[no_brackets.wid==16963918,'step19']='[[briganti]]<br>[[south italian brigands]]'
no_brackets.loc[no_brackets.wid==23959815,'step19']='[[bajouris]][[chitralis]][[swatis]][[diri]]'
no_brackets.loc[no_brackets.wid==24447488,'step19']='[[portugal]][[spain]][[philip ii of spain]]' 
no_brackets.loc[no_brackets.wid==25354549,'step19']='[[revolutionary council]]*[[sudanese communists]]*[[rebel military units]]'
no_brackets.loc[no_brackets.wid==25756665,'step19']='[[pro-chinese indonesian rebels]]<br>[[chinese volunteers]]'
no_brackets.loc[no_brackets.wid==27216529,'step19']='[[vietnam]] [[viet minh cadres]]'
no_brackets.loc[no_brackets.wid==27231044,'step19']='[[vietnam]] [[viet minh cadres]]'
no_brackets.loc[no_brackets.wid==28370538,'step19']='[[rebel mercenaries]]<br>[[rebelling libyan towns and cities]]'
no_brackets.loc[no_brackets.wid==28376685,'step19']='[[congress of guatemala]]<br> [[constitutional court]]<br> [[supreme court of justice]]<br>[[attorney general of the nation]]<br>[[chief attorney general]]<br>[[supreme electoral tribunal]]<br>[[guatemalans]]'
no_brackets.loc[no_brackets.wid==28855482,'step19']='albanian rebels'
no_brackets.loc[no_brackets.wid==31098786,'step19']='[[iraqi shia tribesmen]]<br>[[ikha party]]'
no_brackets.loc[no_brackets.wid==32055995,'step19']='[[ottoman empire]] [[karamanli dynasty]]'
no_brackets.loc[no_brackets.wid==33701873,'step19']='[[kingdom of scotland]][[protestant lairds of fife]]'
no_brackets.loc[no_brackets.wid==33982330,'step19']='republic of florence'
no_brackets.loc[no_brackets.wid==34575776,'step19']='[[dharug nation]]* [[eora nation]]* [[tharawal nation]]* [[gandangara nationirish-convict sympathisers]]'
no_brackets.loc[no_brackets.wid==34946913,'step19']='[[germany ]][[ 902nd artillery regiment z.v. (motorized)]]'
no_brackets.loc[no_brackets.wid==35178468,'step19']='[[flagicon|mali]][[national committee for the restoration of democracy and state (cnrdr)]]'
no_brackets.loc[no_brackets.wid==35389678,'step19']='[[french]][[angevins]]'
no_brackets.loc[no_brackets.wid==36117478,'step19']='[[venezuela]][[military rebels]]'
no_brackets.loc[no_brackets.wid==36117587,'step19']='[[venezuela]][[military rebels]]'
no_brackets.loc[no_brackets.wid==37353977,'step19']='[[dutch west india company]] [[akan kingdom]] [[cabess terra (akan kingdom)]][[twifo (akan kingdom)]]'
no_brackets.loc[no_brackets.wid==38997709,'step19']='[[northern military administration office army]][[the provisional government of the republic of korea]] [[korean independence army]]'
no_brackets.loc[no_brackets.wid==39329986,'step19']='øyskjegg party'
no_brackets.loc[no_brackets.wid==40167515,'step19']='[[senate]]<br>[[citizens of aquileia]]'
no_brackets.loc[no_brackets.wid==40487629,'step19']='[[rival claimants to mataram throne]] [[rebel princes]]'
no_brackets.loc[no_brackets.wid==41818998,'step19']='[[n3/pandemic legion]]* [[northern coalition]]* [[nulli secunda]]* [[fraternity.]][[pandemic legion]]'
no_brackets.loc[no_brackets.wid==41941440,'step19']='[[libyan republican alliance]][[forces loyal to general haftar]]'
no_brackets.loc[no_brackets.wid==42448870,'step19']='[[venezuela]] [[conservative government]]'
no_brackets.loc[no_brackets.wid==42662962,'step19']='[[independent state of croatia]] [[ustaše]] [[nazi germany]][[bosanski novi]]'
no_brackets.loc[no_brackets.wid==42855446,'step19']='tlingit tribe from kake village, alaska'
no_brackets.loc[no_brackets.wid==43469223,'step19']='[[alliance for the sovereign and patriotic congo (apclh)]]<br>[[mai mai cheka]]'
no_brackets.loc[no_brackets.wid==43698378,'step19']='[[lesotho army]]<br>[[government of lesotho]]'
no_brackets.loc[no_brackets.wid==43741457,'step19']='[[abdali afghans]]<br/>[[sangani rebels]]'
no_brackets.loc[no_brackets.wid==46185791,'step19']="[[ottoman empire]] [[albanian mob from orahovac]]<br>[[ottoman ''askeri'']]"
no_brackets.loc[no_brackets.wid==46659741,'step19']='confederate states'
no_brackets.loc[no_brackets.wid==47866526,'step19']='shangdu group(loyalists)'
no_brackets.loc[no_brackets.wid==51172839,'step19']="[[''sasna tsrrer'' armed group]]<br />[[anti-government protesters]]"
no_brackets.loc[no_brackets.wid==52005143,'step19']='[[rebel army under nominal command of earl of argyll]]<br>[[standard of the duke of monmouth]][[duke of monmouth]]'
no_brackets.loc[no_brackets.wid==52310203,'step19']='katanga'
no_brackets.loc[no_brackets.wid==52689882,'step19']='[[makassarese itinerant fighters]][[rival claimants to mataram throne]]'
no_brackets.loc[no_brackets.wid==52809917,'step19']='[[demak sultanate]]<br>[[majapahit defenders from trowulan]]'
no_brackets.loc[no_brackets.wid==53559919,'step19']='florence'
no_brackets.loc[no_brackets.wid==53738087,'step19']='luba militias'
no_brackets.loc[no_brackets.wid==53796572,'step19']='katanga'
no_brackets.loc[no_brackets.wid==53816952,'step19']='[[argos]]<br>[[hysiae]]'
no_brackets.loc[no_brackets.wid==55418603,'step19']='kingdom of scotland'
no_brackets.loc[no_brackets.wid==57074502,'step19']='[[the crown of castile (15th century)]] [[juana]]'
no_brackets.loc[no_brackets.wid==57194858,'step19']='syrian opposition'
no_brackets.loc[no_brackets.wid==58188866,'step19']='[[venezuela]][[military dissidents]]'
no_brackets.loc[no_brackets.wid==59184744,'step19']="opponents to the commissioners:'''<br />[[royalist france.]] [[french ''grand blancs'' royalist settlers]]<br />[[france]][[french ''petit blancs'' republican settlers]]<br />[[haiti]] [[slaves of armed settlers]]"
no_brackets.loc[no_brackets.wid==49403106,'step19']='[[mahipala ii]][[rampala]]'
no_brackets.loc[no_brackets.wid==53849907,'step19']='[[saadiens]]<br/> [[alaouites]]'

no_brackets['step20']=no_brackets['step19'].str.extract(r"(\[\[)",expand=False)
no_brackets['step20']=no_brackets['step20'].fillna(0)
no_btacket_1=no_brackets[no_brackets['step20']==0]
no_btacket_2=no_brackets[no_brackets['step20']!=0]

no_brackets_1=no_btacket_1.loc[:,['wid','step19']]
no_brackets_1=no_brackets_1.rename(columns={'step19':'participants'})

no_brackets_2= pd.DataFrame(columns=('wid','participants'))
rcount=0
for row in no_btacket_2.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][9]==0:
        no_brackets_2=no_brackets_2.append(pd.DataFrame({'wid':[row[1][0]],'participants':[row[1][9]]}),ignore_index=True) 
    if row[1][9]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][9]:
            
            
            if c=='{' or c=='[':
                counter=counter+1
               
            if c=='}' or c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                no_brackets_2= no_brackets_2.append(pd.DataFrame({'wid':[row[1][0]],'participants':[text]}),ignore_index=True)
                text=''
                sm=0
                
no_brackets_2['x1']=no_brackets_2['participants'].str.extract(r'(^\[\[file:.*)',expand=False)
no_brackets_2=no_brackets_2.fillna(0)          
no_brackets_2=no_brackets_2.loc[no_brackets_2['x1']==0]

no_brackets_2['x1']=no_brackets_2['participants'].str.extract(r'(^{{flagicon.*)',expand=False)
no_brackets_2=no_brackets_2.fillna(0)          
no_brackets_2=no_brackets_2.loc[no_brackets_2['x1']==0]

no_brackets_2=no_brackets_2.loc[:,['wid','participants']]
no_brackets_2['middle']=no_brackets_2['participants'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
no_brackets_2=no_brackets_2.loc[:,['wid','middle']]
no_brackets_2=no_brackets_2.rename(columns={'middle':'participants'})

info_nobrackets=no_brackets_1.append(no_brackets_2)
"""
participant2_infobox=combatant2_infobox.loc[:,['wid','participants']]
#participant2_infobox=participant2_infobox.append(info_nobrackets)
participant2_infobox['side']=2
participant2_infobox=participant2_infobox.replace(0,np.nan)
participant2_infobox.drop_duplicates(inplace=True)
participant2_infobox=participant2_infobox[participant2_infobox['participants']!='executed']
participant2_infobox['participants']=participant2_infobox['participants'].str.strip()
participant2_infobox['participants']=participant2_infobox['participants'].replace('|',np.nan)
participant2_infobox['participants']=participant2_infobox['participants'].replace("'''",np.nan)
participant2_infobox['participants']=participant2_infobox['participants'].replace("",np.nan)

participant2_infobox.to_csv(main_path+'output/participant2_infobox.csv')

