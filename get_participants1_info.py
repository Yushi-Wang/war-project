# -*- coding: utf-8 -*-
"""
Created on Thu Jan 17 13:21:48 2019

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



#################################participants （combatant1) information
info_combatant1=writer.loc[:,['wid','title','tmp']]
info_combatant1['combatant1a']=info_combatant1['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant1['combatant1a']=info_combatant1['combatant1a'].fillna(0)
combatant1a=info_combatant1[info_combatant1['combatant1a']!=0]####need to be treated specially

info_combatant1=info_combatant1[info_combatant1['combatant1a']==0]
info_combatant1['combatant1']=info_combatant1['tmp'].str.extract('((?<=combatant1).*?(?=combatant2))',expand=False)
info_combatant1['combatant1']=info_combatant1['combatant1'].fillna(0)
missing=info_combatant1[info_combatant1['combatant1']==0]#leave these wars aside, focus on those nonmissing wars
info_combatant1_nonmissing=info_combatant1[info_combatant1['combatant1']!=0]
missing['combatant1']=missing['tmp'].str.extract('((?<=combatant1).*?(?=commander))',expand=False)
missing['strength']=missing['combatant1'].str.extract(r'(.*?)strength',expand=False)
missing=missing.fillna(0)
def function0(a,b):
    if a!=0:
        return a
    else:
        return b
missing['combatant1']=missing.apply(lambda x: function0(x.strength,x.combatant1), axis=1)
missing=missing.loc[:,['wid','title','tmp','combatant1a','combatant1']]
info_combatant1=info_combatant1_nonmissing.append(missing)

info_combatant1['comb3']=info_combatant1['combatant1'].str.extract('^(.*)combatant3',expand=False)#no combatant2
info_combatant1['commander']=info_combatant1['combatant1'].str.extract('^(.*?)commander',expand=False)
puzzle1=info_combatant1[info_combatant1['comb3']!=0]
puzzle2=info_combatant1[info_combatant1['commander']!=0]
info_combatant1=info_combatant1.fillna(0)

info_combatant1=info_combatant1.fillna(0)

info_combatant1['combatant1']=info_combatant1.apply(lambda x: function0(x.comb3,x.combatant1), axis=1)
info_combatant1['combatant1']=info_combatant1.apply(lambda x: function0(x.commander,x.combatant1), axis=1)

info_combatant1=info_combatant1.loc[:,['wid','title','tmp','combatant1']]



#info_combatant1['collapsible']=info_combatant1['combatant1'].str.extract('({{collapsible list.*)',expand=False)
###first, make sure what the "support''aid' look like
writer_support=info_combatant1_nonmissing.loc[info_combatant1_nonmissing['combatant1'].str.contains('support')]
writer_support['support']=writer_support['combatant1'].str.extract(r'}}(.*support.*)')
writer_aid=info_combatant1_nonmissing.loc[info_combatant1_nonmissing['combatant1'].str.contains(' aid')]
writer_aid['aid']=writer_aid['combatant1'].str.extract(r'( aid.*)')
####only 43 wars has 'aid' in their infoboxes, so just ignore them
writer_support['support_com']=writer_support['combatant1'].str.extract(r'}}(.*support.*combatant)')
support_combatant=writer_support.loc[writer_support.support_com.notnull()]##These wars has combatant after 'support', need to retreat
    
    
#####remove support
info_combatant1['no_support']=info_combatant1['combatant1'].str.extract(r'(.*?)support',expand=False)
info_combatant1['no_support']=info_combatant1['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
info_combatant1['combatant1_nosup']=info_combatant1.apply(lambda x: function(x.no_support,x.combatant1), axis=1)
#remove<ref></ref>
info_combatant1['with<ref>']=info_combatant1['combatant1_nosup'].str.extract(r'(<ref.*?>.*?</ref>)',expand=False)
info_combatant1['without<ref>']=info_combatant1['combatant1_nosup'].str.replace(r'<ref.*?>.*?</ref>','')

######begin to extract the information in {{}} and [[]]
combatant1_infobox = pd.DataFrame(columns=('wid','combatant1_nosup'))
info_combatant1['without<ref>']=info_combatant1['without<ref>'].fillna(0)
rcount=0

for row in info_combatant1.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][7]==0:
        combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[row[1][5]]}),ignore_index=True) 
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
                combatant1_infobox=combatant1_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant1_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

####further cleaning


combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{refn.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{ref\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[file:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{resize\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{efn\|.*}})',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\[\*\]$)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{\*\}\}$)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{\-\}\}$)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{br\}\}$)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{plainlist\}\}$)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{collapsible list\}\}$)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{#tag:)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{cn\|)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{endplainlist\}\}$)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['x1']=combatant1_infobox['combatant1_nosup'].str.extract(r'(^\{\{notetag\|.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0)          
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['x1']==0]
combatant1_infobox=combatant1_infobox.loc[:,['wid','combatant1_nosup']]

combatant1_infobox['middle']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox['big']=combatant1_infobox['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

####second decompression
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


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{refn.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{ref\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^{{sup\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

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



combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]



combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{resize\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]



combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{sfn\|.*}})',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]




combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{efn\|.*}})',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]




combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\[image:.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]



combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\[\*\]$)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]




combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{\*\}\}$)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]




combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{\-\}\}$)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{br\}\}$)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]



combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{plainlist\}\}$)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{collapsible list\}\}$)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{#tag:)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{cn\|)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]


combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{endplainlist\}\}$)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['x1']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'(^\{\{notetag\|.*)',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)          
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[combatant1_infobox_2nd['x1']==0]
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup']]

combatant1_infobox_2nd['middle']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox_2nd['big']=combatant1_infobox_2nd['combatant1_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant1_infobox_2nd['second']=combatant1_infobox_2nd['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
combatant1_infobox_2nd['third']=combatant1_infobox_2nd['second'].str.extract(r'^\[\[file:.*?\]\] (\[\[.*\]\])',expand=False)

def function2(a,b):
    if a!=0:
        return a
    else:
        return b
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)  
combatant1_infobox_2nd['second']=combatant1_infobox_2nd.apply(lambda x: function2(x.third,x.second), axis=1)              

combatant1_infobox_2nd['second_middle']=combatant1_infobox_2nd['second'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant1_infobox_2nd['second_big']=combatant1_infobox_2nd['second'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
combatant1_infobox_2nd=combatant1_infobox_2nd.fillna(0)  
combatant1_infobox_2nd['middle']=combatant1_infobox_2nd.apply(lambda x: function2(x.second_middle,x.middle), axis=1)              
combatant1_infobox_2nd['big']=combatant1_infobox_2nd.apply(lambda x: function2(x.second_big,x.big), axis=1)              
combatant1_infobox_2nd=combatant1_infobox_2nd.loc[:,['wid','combatant1_nosup','middle','big']]
combatant1_infobox_first=combatant1_infobox_first.loc[:,['wid','combatant1_nosup','middle','big']]
combatant1_infobox=combatant1_infobox_first.append(combatant1_infobox_2nd)

####get the country out
combatant1_infobox['big_ex']=combatant1_infobox['big'].str.extract(r'^.*?\|(.*)',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 

def function3(a,b):
    if a!=0 and b==0:
        return 1
combatant1_infobox['x']=combatant1_infobox.apply(lambda x: function3(x.big,x.big_ex), axis=1)
x=combatant1_infobox.loc[combatant1_infobox['x']==1]
combatant1_infobox['big_2']=combatant1_infobox.apply(lambda x: function2(x.big_ex,x.big), axis=1)
combatant1_infobox['big_eex']=combatant1_infobox['big_2'].str.extract(r'^(.*?)\|',expand=False)
combatant1_infobox=combatant1_infobox.fillna(0) 
combatant1_infobox['big_3']=combatant1_infobox.apply(lambda x: function2(x.big_eex,x.big_2), axis=1)
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




combatant1_infobox['participants']=combatant1_infobox.apply(lambda x: function2(x.middle,x.big_3), axis=1)
####consider those which have no participant information(missing)
combatant1_infobox_nonmissing=combatant1_infobox[combatant1_infobox['participants']!=0]
missing_2=combatant1_infobox[combatant1_infobox['participants']==0]
missing_wid=list(missing_2['wid'])

infobox_missing=writer[writer.wid.isin(missing_wid)]



##########consider those that have participants information that is not in the brackets
#wid_list_after=combatant1_infobox.loc[:,'wid']
#wid_list_after.drop_duplicates(inplace=True)
#wid_list_after=list(wid_list_after)
#no_brackets=info_combatant1[~info_combatant1.wid.isin(wid_list_after)]
#no_brackets['step1']=no_brackets['without<ref>'].str.extract(r'=(.*)',expand=False)
#no_brackets['step1']=no_brackets['step1'].str.strip()
#no_brackets['step1']=no_brackets['step1'].replace('|',np.nan)
#no_brackets['step1']=no_brackets['step1'].replace("'''",np.nan)
#no_brackets['step1']=no_brackets['step1'].replace("",np.nan)
#no_brackets['step2']=no_brackets['step1'].str.extract(r'^\{\{flagicon.*?\}\}(.*)',expand=False)

#no_brackets.loc[15154,'step1']='{{flagicon|kingdom of france|naval}}kingdom of france|'
"""
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

no_brackets.loc[15527,'step5']='benghazi defense brigades'

no_brackets['step6']=no_brackets['step5'].str.extract(r'(.*)\|$',expand=False)
no_brackets=no_brackets.fillna(0)
no_brackets['step6']=no_brackets.apply(lambda x: function2(x.step6,x.step5), axis=1)
no_brackets['step6']=no_brackets['step6'].str.strip()

no_brackets=no_brackets.loc[:,['wid','title','tmp','combatant1','no_support','combatant1_nosup','with<ref>','without<ref>','step6']]

no_brackets['step7']=no_brackets['step6'].str.extract(r"(.*)<br />'''$",expand=False)

no_brackets=no_brackets.fillna(0)
no_brackets['step7']=no_brackets.apply(lambda x: function2(x.step7,x.step6), axis=1)

no_brackets.loc[185,'step7']='republic of china'
no_brackets.loc[517,'step7']='[[new zealand government]] [[british army]]'
no_brackets.loc[533,'step7']='[[new zealand government]]{{flagicon|united kingdom}} [[british army]][[file:unitedtribesunofficial.svg|23px]] [[māori allies]]'
no_brackets.loc[574,'step7']='[[new zealand government]][[british army]][[waikato militia]]'
no_brackets.loc[668,'step7']='[[french army]] [[preveza greek civil guard]][[souliotes]]'
no_brackets.loc[984,'step7']='revolutionary serbia'
no_brackets.loc[1120,'step7']="[[court attackers]][[ white locals]][[nash's white paramilitary]]"
no_brackets.loc[1278,'step7']='swedish separatists'
no_brackets.loc[1425,'step7']='[[irish republic (1798)|irish republic]][[society of united irishmen|united irishmen]][[defenders (ireland)|defenders]][[first french republic]]'
no_brackets.loc[1509,'step7']='[[united states]][[thailand]]'
no_brackets.loc[3131,'step7']='[[argentina]][[australia]][[canada]][[czech republic]][[france]][[hungary]][[new zealand]][[poland]][[romania]][[united kingdom]][[united states]][[kuwait]]'
no_brackets.loc[3216,'step7']='[[forces of constantine]][[western empire]]'
no_brackets.loc[3402,'step7']='[[house of montfort]][[england]]'
no_brackets.loc[3696,'step7']='[[australia&nbsp]][[new zealand]][[thailand]][[brazil]][[canada]][[france]][[germany]][[ireland]][[italy]][[jordan]][[kenya]][[malaysia]][[norway]][[pakistan]][[philippines]][[portugal]][[singapore]][[south korea]][[united kingdom]][[united states]]'
no_brackets.loc[4174,'step7']='philippines'
no_brackets.loc[4898,'step7']= "[[isabella's supporters]][[crown of aragon]]"
no_brackets.loc[5042,'step7']='polish insurgents'
no_brackets.loc[5378,'step7']='england'
no_brackets.loc[6012,'step7']='venezuela'
no_brackets.loc[6291,'step7']='[[united states]][[iraq]]'
no_brackets.loc[6482,'step7']='polish insurgents'
no_brackets.loc[6530,'step7']='france|pro-toussaint forces'
no_brackets.loc[6927,'step7']='followers of æthelwold ætheling'
no_brackets.loc[8580,'step7']='scottish rebels'
no_brackets.loc[8714,'step7']='portugal|supporters of antónio prior of crato'
no_brackets.loc[8793,'step7']='[[australia]][[new zealand]]'
no_brackets.loc[8934,'step7']='[[australia]][[new zealand]]'
no_brackets.loc[9092,'step7']='zapatistas'
no_brackets.loc[9764,'step7']='muslim rebels'
no_brackets.loc[9810,'step7']='ottoman empire|pro-reform factions'
no_brackets.loc[10998,'step7']="[[arab socialist ba'ath party]][[ba'ath national guard militia]]"
no_brackets.loc[11091,'step7']='[[united states army]][[civilian volunteers]]'
no_brackets.loc[11181,'step7']='[[burgundy]][[cods]]'
no_brackets.loc[11191,'step7']='[[united states army]][[civilian volunteers]]'
no_brackets.loc[11574,'step7']='[[zintan fighters]][[guntrara tribe]]'
no_brackets.loc[11614,'step7']='[[student organization for black unity]][[student protesters]][[rioting locals]]'
no_brackets.loc[11706,'step7']='[[eguafo]] [[john cabess]],[[royal african company]][[denkyira (akan kingdom)]] [[fante (akan kingdom)]] [[asebu (akan kingdom)]]'
no_brackets.loc[12190,'step7']='[[luxembourg]][[france]]'
no_brackets.loc[12413,'step7']='[[the parsig faction]][[the nimruzi faction]]'
no_brackets.loc[12531,'step7']='polish insurgents'
no_brackets.loc[12542,'step7']='democratic republic of congo'
no_brackets.loc[12630,'step7']="[[clusterfuck coalition (cfc)]] * [[goonswarm federation]]* [[razor alliance]]* [[fidelas constans]]* [[gentlemen's agreement]]* [[fatal ascension]]* [[spacemonkey's alliance]]*[[halloween coalition]]* [[solar fleet]]* [[black legion]]* [[against all authorities]]* [[darkness of despair]]"
no_brackets.loc[12747,'step7']="venezuela|federal rebels"
no_brackets.loc[12794,'step7']='[[detachment of volunteers]] [[former members of reserve military units]]* [[serb villagers from kijevo]], [[vidovići]], [[tramošnja]], [[kozica and other neighboring places]]'
no_brackets.loc[12982,'step7']='[[lesotho police]][[government of lesotho]]'
no_brackets.loc[13035,'step7']='[[illyrians]][[lyncestians]]'
no_brackets.loc[13134,'step7']='[[syrian government]][[infoboxhez|hezbollah]][[popular mobilization forces|pmf]]'
no_brackets.loc[13409,'step7']='[[kingdom of meath]]<br>[[kingdom of leinster]]<br>[[kingdom of connacht]]<br>[[kingdom of osraige]]'
no_brackets.loc[13448,'step7']="earl of thomond's supporters"
no_brackets.loc[13668,'step7']='phu mi bun movement'
no_brackets.loc[13731,'step7']='[[khanbaliq group]]'
no_brackets.loc[13982,'step7']='[[divya]]*[[ruddak]]*[[vima]]'
no_brackets.loc[14101,'step7']='glinskis and their supporters'
no_brackets.loc[14184,'step7']='aristobulus'
no_brackets.loc[14221,'step7']="[[dār fertit|fertit]][[sudan people's liberation movement-in-opposition|splm-io]] [[national salvation front|nas]]"
no_brackets.loc[14386,'step7']='[[united nations operation in the congo|onuc]]'
no_brackets.loc[14415,'step7']='swedish separatists'
no_brackets.loc[14513,'step7']='[[kamwina nsapu militia]]* [[various sub-groups]]'
no_brackets.loc[14591,'step7']='milan'
no_brackets.loc[14616,'step7']='[[heraclea pontica]][[theodosia]]'
no_brackets.loc[14645,'step7']='[[pygmy batwa militias]][[ "perci"]]'
no_brackets.loc[14651,'step7']='[[united nations operation in the congo|onuc]]'
no_brackets.loc[14837,'step7']='faction of prince zhao'
no_brackets.loc[15136,'step7']='[[imghad tuareg self-defense group and allies|gatia]] [[movement for the salvation of azawad|msa]]'
no_brackets.loc[15154,'step7']='kingdom of france'
no_brackets.loc[15197,'step7']='arms of the crown of castile (15th century)|isabella'
no_brackets.loc[15437,'step7']='choctaw eastern division'
no_brackets.loc[15444,'step7']='[[seventh brigade]][[kani militia]]<br>[[somoud brigade]]<br>[[libyan tribesmen]]'

no_brackets['step8']=no_brackets['step7'].str.extract(r"(\[\[)",expand=False)
no_brackets['step8']=no_brackets['step8'].fillna(0)
no_btacket_1=no_brackets[no_brackets['step8']==0]
no_btacket_2=no_brackets[no_brackets['step8']!=0]

no_brackets_1=no_btacket_1.loc[:,['wid','step7']]
no_brackets_1=no_brackets_1.rename(columns={'step7':'participants'})



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
participant1_infobox=combatant1_infobox.loc[:,['wid','participants']]
#participant1_infobox=participant1_infobox.append(info_nobrackets)
participant1_infobox['side']=1
participant1_infobox=participant1_infobox.replace(0,np.nan)
participant1_infobox.drop_duplicates(inplace=True)
participant1_infobox=participant1_infobox[participant1_infobox['participants']!='executed']
participant1_infobox.to_csv(main_path+'output/participant1_infobox.csv')














