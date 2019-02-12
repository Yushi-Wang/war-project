# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 21:39:08 2019

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
def function0(a,b):
    if a!=0:
        return a
    else:
        return b
info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant1a']=info_combatant4['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant4['combatant1a']=info_combatant4['combatant1a'].fillna(0)
combatant1a=info_combatant4[info_combatant4['combatant1a']!=0]
info_combatant4=info_combatant4[info_combatant4['combatant1a']==0]####need to be treated speciallyinfo_combatant2=info_combatant2[info_combatant2['combatant1a']==0]
info_combatant4.drop('combatant1a',axis=1, inplace=True)
info_combatant4['combatant4']=info_combatant4['tmp'].str.extract('(combatant4)',expand=False)
info_combatant4['combatant4']=info_combatant4['combatant4'].fillna(0)
info_combatant4=info_combatant4[info_combatant4['combatant4']!=0]
info_combatant4['combatant4']=info_combatant4['tmp'].str.extract('((?<=combatant4).*?(?=commander))',expand=False)
info_combatant4=info_combatant4.fillna(0)

info_combatant4['combatant5']=info_combatant4['tmp'].str.extract(r'((?<=combatant4).*?(?=combatant5))',expand=False)
info_combatant4=info_combatant4.fillna(0)
info_combatant4['combatant4']=info_combatant4.apply(lambda x: function0(x.combatant5,x.combatant4), axis=1)
info_combatant4.drop('combatant5',axis=1, inplace=True)
info_combatant4.loc[info_combatant4.wid==312905,'combatant4']= '[[lebanon]][[united nations]] [[multinational force in lebanon]][[united states]][[france]][[italy]] [[arab deterrent force]][[saudi arabia]][[sudan]][[uae]][[libya]][[south yemen]] '

info_combatant4['combatant4']=info_combatant4['combatant4'].str.strip()
info_combatant4['combatant4']=info_combatant4['combatant4'].replace('=|',np.nan)
info_combatant4['combatant4']=info_combatant4['combatant4'].replace('= |',np.nan)
info_combatant4['combatant4']=info_combatant4['combatant4'].replace('=  |',np.nan)
info_combatant4=info_combatant4.fillna(0)
info_combatant4=info_combatant4[info_combatant4['combatant4']!=0]
#####remove support
info_combatant4['no_support']=info_combatant4['combatant4'].str.extract(r'(.*?)support',expand=False)
info_combatant4['no_support']=info_combatant4['no_support'].fillna(0)
def function(a,b):
    if a!=0:
        return a
    else:
        return b
info_combatant4['combatant4_nosup']=info_combatant4.apply(lambda x: function(x.no_support,x.combatant4), axis=1)
#remove<ref></ref>
info_combatant4['with<ref>']=info_combatant4['combatant4_nosup'].str.extract(r'(<ref.*?>.*?</ref>)',expand=False)
info_combatant4['without<ref>']=info_combatant4['combatant4_nosup'].str.replace(r'<ref.*?>.*?</ref>','')

######begin to extract the information in {{}} and [[]]
combatant4_infobox = pd.DataFrame(columns=('wid','combatant4_nosup'))
info_combatant4['without<ref>']=info_combatant4['without<ref>'].fillna(0)
rcount=0

for row in info_combatant4.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][7]==0:
        combatant4_infobox=combatant4_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant4_nosup':[row[1][7]]}),ignore_index=True) 
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
                combatant4_infobox=combatant4_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant4_nosup':[text]}),ignore_index=True)
                text=''
                sm=0

combatant4_infobox['x1']=combatant4_infobox['combatant4_nosup'].str.extract(r'(^\{\{small\|.*)',expand=False)
combatant4_infobox=combatant4_infobox.fillna(0)          
combatant4_infobox=combatant4_infobox.loc[combatant4_infobox['x1']==0]
combatant4_infobox=combatant4_infobox.loc[:,['wid','combatant4_nosup']]


combatant4_infobox['x1']=combatant4_infobox['combatant4_nosup'].str.extract(r'(^{{flagicon.*)',expand=False)
combatant4_infobox=combatant4_infobox.fillna(0)          
combatant4_infobox=combatant4_infobox.loc[combatant4_infobox['x1']==0]
combatant4_infobox=combatant4_infobox.loc[:,['wid','combatant4_nosup']]


combatant4_infobox['x1']=combatant4_infobox['combatant4_nosup'].str.extract(r'(^{{flagdeco.*)',expand=False)
combatant4_infobox=combatant4_infobox.fillna(0)          
combatant4_infobox=combatant4_infobox.loc[combatant4_infobox['x1']==0]
combatant4_infobox=combatant4_infobox.loc[:,['wid','combatant4_nosup']]

combatant4_infobox['middle']=combatant4_infobox['combatant4_nosup'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
combatant4_infobox['big']=combatant4_infobox['combatant4_nosup'].str.extract(r'^\{\{(.*)\}\}$',expand=False)

combatant4_infobox['x1']=combatant4_infobox['big'].str.extract(r'nowrap\|\{\{flag\|(lebanon)\}\}',expand=False)
combatant4_infobox=combatant4_infobox.fillna(0)          
combatant4_infobox['big']=combatant4_infobox.apply(lambda x: function0(x.x1,x.big), axis=1)
combatant4_infobox.drop('x1',axis=1, inplace=True)
combatant4_infobox['participants']=combatant4_infobox.apply(lambda x: function(x.middle,x.big), axis=1)
participant4_infobox=combatant4_infobox.loc[:,['wid','participants']]
participant4_infobox['side']=4
participant4_infobox.to_csv(main_path+'output/participant4_infobox.csv')


###############participants5
info_combatant5=writer.loc[:,['wid','title','tmp']]
info_combatant5['combatant1a']=info_combatant5['tmp'].str.extract('(combatant1a)',expand=False)
info_combatant5['combatant1a']=info_combatant5['combatant1a'].fillna(0)
combatant1a=info_combatant5[info_combatant5['combatant1a']!=0]
info_combatant5=info_combatant5[info_combatant5['combatant1a']==0]####need to be treated speciallyinfo_combatant2=info_combatant2[info_combatant2['combatant1a']==0]
info_combatant5.drop('combatant1a',axis=1, inplace=True)
info_combatant5['combatant5']=info_combatant5['tmp'].str.extract('(combatant5)',expand=False)
info_combatant5['combatant5']=info_combatant5['combatant5'].fillna(0)
info_combatant5=info_combatant5[info_combatant5['combatant5']!=0]
info_combatant5['combatant5']=info_combatant5['tmp'].str.extract('((?<=combatant5).*?(?=commander))',expand=False)
info_combatant5['x']=info_combatant5['tmp'].str.extract('((?<=combatant5).*?(?=combatant6))',expand=False)
info_combatant5.loc[info_combatant5.wid==234655,'combatant5']= '[[Atlixco]]'
info_combatant5.loc[info_combatant5.wid==37353977,'combatant5']= 0
info_combatant5=info_combatant5[info_combatant5['combatant5']!=0]

participant5_infobox = pd.DataFrame(columns=('wid','participants'))

rcount=0

for row in info_combatant5.iterrows():
    #print(row[1][0])
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:
        participant5_infobox=participant5_infobox.append(pd.DataFrame({'wid':[row[1][0]],'combatant4_nosup':[row[1][3]]}),ignore_index=True) 
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
                participant5_infobox=participant5_infobox.append(pd.DataFrame({'wid':[row[1][0]],'participants':[text]}),ignore_index=True)
                text=''
                sm=0
participant5_infobox['participants']=participant5_infobox['participants'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
participant5_infobox['side']=5
participant5_infobox.to_csv(main_path+'output/participant5_infobox.csv')
