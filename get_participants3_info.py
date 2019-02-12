# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 15:46:03 2019

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
info_combatant3=writer.loc[:,['wid','title','tmp']]
info_combatant3['combatant1a']=info_combatant3['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant3['combatant1a']=info_combatant3['combatant1a'].fillna(0)
combatant1a=info_combatant3[info_combatant3['combatant1a']!=0]
info_combatant3=info_combatant3[info_combatant3['combatant1a']==0]####need to be treated speciallyinfo_combatant2=info_combatant2[info_combatant2['combatant1a']==0]
info_combatant3.drop('combatant1a',axis=1, inplace=True)
info_combatant3['combatant3']=info_combatant3['tmp'].str.extract('(combatant3)',expand=False)
info_combatant3['combatant3']=info_combatant3['combatant3'].fillna(0)
info_combatant3=info_combatant3[info_combatant3['combatant3']!=0]
info_combatant3['combatant3']=info_combatant3['tmp'].str.extract('((?<=combatant3).*?(?=commander))',expand=False)
info_combatant3=info_combatant3.fillna(0)
missing=info_combatant3[info_combatant3['combatant3']==0]#leave these wars aside, focus on those nonmissing wars
info_combatant3_nonmissing=info_combatant3[info_combatant3['combatant3']!=0]

####clean missing

def function0(a,b):
    if a!=0:
        return a
    else:
        return b
missing['strength']=missing['tmp'].str.extract(r'((?<=combatant3).*?(?=combatant4))',expand=False)
missing=missing.fillna(0)
missing['combatant3']=missing.apply(lambda x: function0(x.combatant3,x.strength), axis=1)
missing.drop('strength',axis=1, inplace=True)

missing['strength']=missing['tmp'].str.extract(r'((?<=combatant3).*?(?=strength))',expand=False)
missing=missing.fillna(0)
missing['combatant3']=missing.apply(lambda x: function0(x.combatant3,x.strength), axis=1)
missing.drop('strength',axis=1, inplace=True)

missing['casualties']=missing['tmp'].str.extract(r'((?<=combatant3).*?(?=casualties))',expand=False)
missing=missing.fillna(0)
missing['combatant3']=missing.apply(lambda x: function0(x.combatant3,x.casualties), axis=1)
missing.drop('casualties',axis=1, inplace=True)

missing['units']=missing['tmp'].str.extract(r'((?<=combatant3).*?(?=units))',expand=False)
missing=missing.fillna(0)
missing['combatant3']=missing.apply(lambda x: function0(x.combatant3,x.units), axis=1)
missing.drop('units',axis=1, inplace=True)

missing['end']=missing['tmp'].str.extract(r'(?<=combatant3)(.*)',expand=False)
missing=missing.fillna(0)
missing['combatant3']=missing.apply(lambda x: function0(x.combatant3,x.end), axis=1)
missing.drop('end',axis=1, inplace=True)

missing['casualties']=missing['combatant3'].str.extract(r'(.*?)casualties',expand=False)
missing=missing.fillna(0)
missing['combatant3']=missing.apply(lambda x: function0(x.casualties,x.combatant3), axis=1)
missing.drop('casualties',axis=1, inplace=True)

missing['casualties']=missing['combatant3'].str.extract(r'(.*?)units1',expand=False)
missing=missing.fillna(0)
missing['combatant3']=missing.apply(lambda x: function0(x.casualties,x.combatant3), axis=1)
missing.drop('casualties',axis=1, inplace=True)

###clean non-missing
info_combatant3_nonmissing['combatant4']=info_combatant3_nonmissing['combatant3'].str.extract('(.*)combatant4',expand=False)
info_combatant3_nonmissing=info_combatant3_nonmissing.fillna(0)
info_combatant3_nonmissing['combatant3']=info_combatant3_nonmissing.apply(lambda x: function0(x.combatant4,x.combatant3), axis=1)
info_combatant3_nonmissing.drop('combatant4',axis=1, inplace=True)

info_combatant3_nonmissing['combatant2']=info_combatant3_nonmissing['combatant3'].str.extract('(.*)combatant2',expand=False)
info_combatant3_nonmissing=info_combatant3_nonmissing.fillna(0)
info_combatant3_nonmissing['combatant3']=info_combatant3_nonmissing.apply(lambda x: function0(x.combatant2,x.combatant3), axis=1)
info_combatant3_nonmissing.drop('combatant2',axis=1, inplace=True)

info_combatant3_nonmissing['combatant1']=info_combatant3_nonmissing['combatant3'].str.extract(r'(.*)combatant1',expand=False)
info_combatant3_nonmissing=info_combatant3_nonmissing.fillna(0)
info_combatant3_nonmissing['combatant3']=info_combatant3_nonmissing.apply(lambda x: function0(x.combatant1,x.combatant3), axis=1)
info_combatant3_nonmissing.drop('combatant1',axis=1, inplace=True)

info_combatant3_nonmissing['strength']=info_combatant3_nonmissing['combatant3'].str.extract(r'(.*?)strength',expand=False)
info_combatant3_nonmissing=info_combatant3_nonmissing.fillna(0)
info_combatant3_nonmissing['combatant3']=info_combatant3_nonmissing.apply(lambda x: function0(x.strength,x.combatant3), axis=1)
info_combatant3_nonmissing.drop('strength',axis=1, inplace=True)

info_combatant3_nonmissing['strength']=info_combatant3_nonmissing['combatant3'].str.extract(r'(.*?)casualties',expand=False)
info_combatant3_nonmissing=info_combatant3_nonmissing.fillna(0)
info_combatant3_nonmissing['combatant3']=info_combatant3_nonmissing.apply(lambda x: function0(x.strength,x.combatant3), axis=1)
info_combatant3_nonmissing.drop('strength',axis=1, inplace=True)

info_combatant3_nonmissing['strength']=info_combatant3_nonmissing['combatant3'].str.extract(r'(.*?)units1',expand=False)
info_combatant3_nonmissing=info_combatant3_nonmissing.fillna(0)
info_combatant3_nonmissing['combatant3']=info_combatant3_nonmissing.apply(lambda x: function0(x.strength,x.combatant3), axis=1)
info_combatant3_nonmissing.drop('strength',axis=1, inplace=True)
info_combatant3=info_combatant3_nonmissing.append(missing)

info_combatant3['combatant3']=info_combatant3['combatant3'].str.strip()
info_combatant3['combatant3']=info_combatant3['combatant3'].replace('=|',np.nan)
info_combatant3['combatant3']=info_combatant3['combatant3'].replace('= |',np.nan)
info_combatant3['combatant3']=info_combatant3['combatant3'].replace('=  |',np.nan)
info_combatant3=info_combatant3.fillna(0)
info_combatant3=info_combatant3[info_combatant3['combatant3']!=0]

#####remove support
info_combatant3['no_support']=info_combatant3['combatant3'].str.extract(r'(.*?)support',expand=False)
info_combatant3['no_support']=info_combatant3['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
info_combatant3['combatant3_nosup']=info_combatant3.apply(lambda x: function(x.no_support,x.combatant3), axis=1)
#remove<ref></ref>
info_combatant3['with<ref>']=info_combatant3['combatant3_nosup'].str.extract(r'(<ref.*?>.*?</ref>)',expand=False)
info_combatant3['without<ref>']=info_combatant3['combatant3_nosup'].str.replace(r'<ref.*?>.*?</ref>','')

######begin to extract the information in {{}} and [[]]
combatant3_infobox = pd.DataFrame(columns=('wid','combatant3_nosup'))
info_combatant3['without<ref>']=info_combatant3['without<ref>'].fillna(0)
rcount=0

for row in info_combatant3.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][7]==0:
        combatant3_infobox=combatant3_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant3_nosup':[row[1][7]]}),ignore_index=True) 
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
                combatant3_infobox=combatant3_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant3_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

####further cleaning


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^{{refn.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]

combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^{{ref\|.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]

combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]

combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{resize\|.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{efn\|.*}})',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\[\*\]$)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{\*\}\}$)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{\-\}\}$)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{br\}\}$)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{plainlist\}\}$)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{collapsible list\}\}$)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{#tag:)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{cn\|)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{endplainlist\}\}$)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{notetag\|.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{cref2\|.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]


combatant3_infobox['x1']=combatant3_infobox['combatant3_nosup'].str.extract(r'(^\{\{executed\}\}$)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)          
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['x1']==0]
combatant3_infobox=combatant3_infobox.loc[:,['wid','combatant3_nosup']]

combatant3_infobox['middle']=combatant3_infobox['combatant3_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant3_infobox['big']=combatant3_infobox['combatant3_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

####second decompression
combatant3_infobox['second']=combatant3_infobox['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0)
combatant3_infobox_second=combatant3_infobox.loc[combatant3_infobox['second']!=0]
combatant3_infobox_first=combatant3_infobox.loc[combatant3_infobox['second']==0]

combatant3_infobox_2nd = pd.DataFrame(columns=('wid','combatant3_nosup'))
rcount=0

for row in combatant3_infobox_second.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:
        combatant3_infobox_2nd=combatant3_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant3_nosup':[row[1][3]]}),ignore_index=True) 
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
                combatant3_infobox_2nd=combatant3_infobox_2nd.append(pd.DataFrame({'wid':[row[1][0]],'combatant3_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^{{refn.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]

combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^{{ref\|.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]

combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]

combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{resize\|.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]

combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{efn\|.*}})',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]

combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\[\*\]$)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{\*\}\}$)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{\-\}\}$)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{br\}\}$)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{plainlist\}\}$)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{collapsible list\}\}$)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{#tag:)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{cn\|)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{endplainlist\}\}$)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{notetag\|.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{cref2\|.*)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]


combatant3_infobox_2nd['x1']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'(^\{\{executed\}\}$)',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.fillna(0)          
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[combatant3_infobox_2nd['x1']==0]
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup']]

combatant3_infobox_2nd['middle']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant3_infobox_2nd['big']=combatant3_infobox_2nd['combatant3_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant3_infobox_2nd=combatant3_infobox_2nd.loc[:,['wid','combatant3_nosup','middle','big']]
combatant3_infobox_first=combatant3_infobox_first.loc[:,['wid','combatant3_nosup','middle','big']]

combatant3_infobox=combatant3_infobox_first.append(combatant3_infobox_2nd)

####get the country out
combatant3_infobox['big_ex']=combatant3_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant3_infobox['x']=combatant3_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant3_infobox.loc[combatant3_infobox['x']==1]
combatant3_infobox['big_2']=combatant3_infobox.apply(lambda x: function(x.big_ex,x.big), axis=1)
combatant3_infobox['big_eex']=combatant3_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant3_infobox=combatant3_infobox.fillna(0) 
combatant3_infobox['big_3']=combatant3_infobox.apply(lambda x: function(x.big_eex,x.big_2), axis=1)

###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='kia']
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='startplainlist']
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='notelist']
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='noflag']
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='no flag']
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='surrendered']
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='bullet']
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='!']
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='plainlist']
combatant3_infobox=combatant3_infobox[combatant3_infobox['big_3']!='endplainlist']




combatant3_infobox['participants']=combatant3_infobox.apply(lambda x: function(x.middle,x.big_3), axis=1)


"""
##########consider those that have participants information that is not in the brackets
wid_list_after=combatant3_infobox.loc[:,'wid']
wid_list_after.drop_duplicates(inplace=True)
wid_list_after=list(wid_list_after)
no_brackets=info_combatant3[~info_combatant3.wid.isin(wid_list_after)]
no_brackets['step1']=no_brackets['without<ref>'].str.extract(r'=(.*)',expand=False)
no_brackets['step1']=no_brackets['step1'].str.strip()
no_brackets['step1']=no_brackets['step1'].replace('|',np.nan)
no_brackets['step1']=no_brackets['step1'].replace("'''",np.nan)
no_brackets['step1']=no_brackets['step1'].replace("",np.nan)
no_brackets['step2']=no_brackets['step1'].str.extract(r'^\{\{flagicon.*?\}\}(.*)',expand=False)

no_brackets=no_brackets.fillna(0)
no_brackets['step2']=no_brackets.apply(lambda x: function(x.step2,x.step1), axis=1)

no_brackets['step3']=no_brackets['step2'].str.extract(r'^\{\{flagdeco.*?\}\}(.*)',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step3']=no_brackets.apply(lambda x: function(x.step3,x.step2), axis=1)


no_brackets['step4']=no_brackets['step3'].str.extract(r'^\[\[file.*?\]\](.*)',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step4']=no_brackets.apply(lambda x: function(x.step4,x.step3), axis=1)

no_brackets['step5']=no_brackets['step4'].str.extract(r'^\[\[image.*?\]\](.*)',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step5']=no_brackets.apply(lambda x: function(x.step5,x.step4), axis=1)


no_brackets['step6']=no_brackets['step5'].str.extract(r'(.*)\|$',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step6']=no_brackets.apply(lambda x: function(x.step6,x.step5), axis=1)
no_brackets['step6']=no_brackets['step6'].str.strip()

no_brackets=no_brackets.loc[:,['wid','title','tmp','combatant3','no_support','combatant3_nosup','with<ref>','without<ref>','step6']]

no_brackets['step7']=no_brackets['step6'].str.extract(r"(.*)<br />'''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step7']=no_brackets.apply(lambda x: function(x.step7,x.step6), axis=1)

no_brackets['step8']=no_brackets['step7'].str.extract(r"(.*)<br/>'''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step8']=no_brackets.apply(lambda x: function(x.step8,x.step7), axis=1)

no_brackets['step9']=no_brackets['step8'].str.extract(r"(.*)<br>'''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step9']=no_brackets.apply(lambda x: function(x.step9,x.step8), axis=1)

no_brackets['step10']=no_brackets['step9'].str.extract(r"(.*)\{\{-\}\}''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step10']=no_brackets.apply(lambda x: function(x.step10,x.step9), axis=1)

no_brackets['step11']=no_brackets['step10'].str.extract(r"(.*)<br>''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step11']=no_brackets.apply(lambda x: function(x.step11,x.step10), axis=1)

no_brackets['step12']=no_brackets['step11'].str.extract(r"(.*)<br/>''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step12']=no_brackets.apply(lambda x: function(x.step12,x.step11), axis=1)

no_brackets['step13']=no_brackets['step12'].str.extract(r"^<center>(.*)",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step13']=no_brackets.apply(lambda x: function(x.step13,x.step12), axis=1)

no_brackets['step14']=no_brackets['step13'].str.extract(r"^'''(.*)",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step14']=no_brackets.apply(lambda x: function(x.step14,x.step13), axis=1)
no_brackets['step14']=no_brackets['step14'].str.strip()

no_brackets['step15']=no_brackets['step14'].str.extract(r"(.*)'''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step15']=no_brackets.apply(lambda x: function(x.step15,x.step14), axis=1)

no_brackets['step16']=no_brackets['step15'].str.extract(r"(.*)\}\}$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step16']=no_brackets.apply(lambda x: function(x.step16,x.step15), axis=1)

no_brackets=no_brackets.loc[:,['wid','title','tmp','combatant3','no_support','combatant3_nosup','with<ref>','without<ref>','step6','step16']]

no_brackets['step17']=no_brackets['step16'].str.extract(r"(.*)'''military$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step17']=no_brackets.apply(lambda x: function(x.step17,x.step16), axis=1)
no_brackets['step17']=no_brackets['step17'].str.strip()

no_brackets['step18']=no_brackets['step17'].str.extract(r"(.*)<br>$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step18']=no_brackets.apply(lambda x: function(x.step18,x.step17), axis=1)


no_brackets['step19']=no_brackets['step18'].str.extract(r"^''(.*)''$",expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step19']=no_brackets.apply(lambda x: function(x.step19,x.step18), axis=1)


no_brackets=no_brackets.loc[:,['wid','title','tmp','combatant3','no_support','combatant3_nosup','with<ref>','without<ref>','step6','step19']]

no_brackets.loc[no_brackets.wid==613055,'step19']= '[[guatemala]] [[guatemalan exile rebels]]'
no_brackets.loc[no_brackets.wid==29896432,'step19']='[[flagicon|mexico]][[ mexican patriots]]'
no_brackets.loc[no_brackets.wid==55798924,'step19']=0

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
participant3_infobox=combatant3_infobox.loc[:,['wid','participants']]
#participant3_infobox=participant3_infobox.append(info_nobrackets)
participant3_infobox['side']=3
participant3_infobox=participant3_infobox.replace(0,np.nan)
participant3_infobox.drop_duplicates(inplace=True)
participant3_infobox=participant3_infobox[participant3_infobox['participants']!='executed']
participant3_infobox['participants']=participant3_infobox['participants'].str.strip()
participant3_infobox['participants']=participant3_infobox['participants'].replace('|',np.nan)
participant3_infobox['participants']=participant3_infobox['participants'].replace("'''",np.nan)
participant3_infobox['participants']=participant3_infobox['participants'].replace("",np.nan)

participant3_infobox.to_csv(main_path+'output/participant3_infobox.csv')
