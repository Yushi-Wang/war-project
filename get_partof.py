# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 22:20:14 2019

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

info_partof=writer.loc[:,['wid','title','tmp']]
def function0(a,b):
    if a!=0:
        return a
    else:
        return b

info_partof['tmp']=info_partof['tmp'].str.replace(r'\{\{Pufc\|1=\|date=.*?\}\}','')
info_partof['tmp']=info_partof['tmp'].str.replace(r'\{\{Dubious\|date=.*?\}\}','')
info_partof['tmp']=info_partof['tmp'].str.replace(r'\{\{page needed\|date=.*?\}\}','')


info_partof['partof']=info_partof['tmp'].str.extract(r'(.*)',expand=False)


info_partof['partof']=info_partof['partof'].str.replace(r'<ref[^/]*?>.*?<\/ref>','')
info_partof['partof']=info_partof['partof'].str.replace(r'<ref.*?>','')
info_partof['partof']=info_partof['partof'].str.replace(r'<sup>.*?<\/sup>','')
info_partof['partof']=info_partof['partof'].str.replace(r'<small[^/]*?>.*?<\/small>','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{cite web\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{cn\|date.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{small *\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{sfnp\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{\#tag:ref\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{citation needed\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{efn *\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{sfn *\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{refn *\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\[https\:\/\/.*?\]','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{in\b\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\<\!\-\-.*?\-\-\>','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{cref2\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{nowrap\|\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{resize\|.*?\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'09\:02–09\:40','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{rp\|136\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{rp\|10\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{rp\|178\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{rp\|267\}\}','')
info_partof['partof']=info_partof['partof'].str.replace(r'\{\{rp\|244-5\}\}','')

info_partof['partof']=info_partof['partof'].str.replace(r'\[\[fief\]\]','')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[history of \[\[france\]\]|french\]\]','[[france]]')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[universal transverse mercator coordinate system\|utm grid\]\]','')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[file\:.*?\]\]','') 
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[image\:.*?\]\]','') 

info_partof['partof']=info_partof['partof'].str.extract(r'(\| *part *of *=.*)',expand=False)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *place *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *territory *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *result *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *results *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *coordinates *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *casus *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *cause *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *conflict *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *image_size *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)



info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *map_type *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *image *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)



info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *campaign *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)


info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *causes *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)


info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *time *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *lat_deg *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *caption *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *commander1 *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *combatant1 *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *status *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *casus belli *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *date *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *territorial changes *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *width *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *notes *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *closest_city *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *latitude *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *latd *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *lat *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *map_label *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *presidency *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *strength1 *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *casus beli *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *sicilyresult *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *casualties3 *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *target *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)


info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *methods *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)

info_partof['comb1_2']=info_partof['partof'].str.extract(r'(.*?)\| *color_scheme *=',expand=False)
info_partof=info_partof.fillna(0)
info_partof['partof']=info_partof.apply(lambda x: function0(x.comb1_2,x.partof), axis=1)
info_partof.drop('comb1_2',axis=1, inplace=True)


info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bislamic conquest of persia\b(?![\|\)\]])','[[islamic conquest of persia]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bvietnam war\b(?![\|\)\]])','[[vietnam war]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bunification of hawaii\b(?![\|\)\]])','[[unification of hawaii]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\benglish civil war\b(?![\|\)\]])','[[english civil war]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bworld war i\b(?![\|\)\]])','[[world war i]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bfinnish war\b(?![\|\)\]])','[[finnish war]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bamerican civil war\b(?![\|\)\]])','[[american civil war]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\breconquista\b(?![\|\)\]])','[[reconquista]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bmughal-maratha wars\b(?![\|\)\]])','[[mughal-maratha wars]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bworld war ii\b(?![\|\)\]])','[[world war ii]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bfrench and indian war\b(?![\|\)\]])','[[french and indian war]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bsecond world war\b(?![\|\)\]])','[[second world war]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bmexican drug war\b(?![\|\)\]])','[[mexican drug war]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bmaratha war of independence\b(?![\|\)\]])','[[maratha war of independence]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bkomenda wars\b(?![\|\)\]])','[[komenda wars]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bzulu civil war\b(?![\|\)\]])','[[zulu civil war]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bsaxon wars\b(?![\|\)\]])','[[saxon wars]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bpontiac\'s war\b(?![\|\)\]])','[[pontiac\'s war]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bmoro conflict\b(?![\|\)\]])','[[moro conflict]]')
info_partof['partof']=info_partof['partof'].str.replace(r'(?<!\[)\bargentine civil wars\b(?![\|\)\]])','[[argentine civil wars]]')




info_partof['partof']=info_partof['partof'].str.replace(r'\[\[khalid ibn al-walid\]\]','khalid ibn al-walid')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[justinian i|justinian\]\]','justinian i|justinian')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[scottish clan\]\]','scottish clan')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[clan mackay\]\]','clan mackay')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[owain glyndŵr\]\]','owain glyndŵr')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[caracalla\]\]','caracalla')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[limes moesiae]\]\]','limes moesiae')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[cyrus the great\]\]','cyrus the great')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[muhammad ali of egypt\]\]','muhammad ali of egypt')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[bakumatsu\]\]','bakumatsu')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[muslim\]\]','muslim')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[quraish\]\]','quraish')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[bergen\]\]','bergen')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[augustus\]\]','augustus')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[piracy\]\]','piracy')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[alps\]\]','alps')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[sigismund of luxembourg|sigismund\]\]','sigismund of luxembourg|sigismund')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[mughal empire|mughal\]\]','mughal empire|mughal')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[deccan sultanates|deccan\]\]','deccan sultanates')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[babur\]\]','babur')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[muhammad shaybani\]\]','muhammad shaybani')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[dutch east india company|dutch colonial government\]\]','dutch east india company|dutch colonial government')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[muhammad ali of egypt\]\]','muhammad ali of egypt')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[impact of the arab spring\]\]','')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[arab winter\]\]','arab winter')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[timurid dynasty|timurid\]\]','timurid dynasty|timurid')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[aq qoyunlu\]\]','aq qoyunlu')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[uí néill\]\]','uí néill')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[travancore\]\]','travancore')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[east india company|british\]\]','east india company|british')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[humayun\]\]','humayun')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[chimurenga\]\]','chimurenga')








info_partof.loc[info_partof.wid==4602307,'partof']='[[civil conflict in the philippines|insurgency in the philippines]];<br /> [[north borneo dispute]]'
info_partof.loc[info_partof.wid==6819126,'partof']=''
info_partof.loc[info_partof.wid==7129204,'partof']=''
info_partof.loc[info_partof.wid==7708630,'partof']=''
info_partof.loc[info_partof.wid==11619484,'partof']=''
info_partof.loc[info_partof.wid==12260432,'partof']=''
info_partof.loc[info_partof.wid==13401314,'partof']=''
info_partof.loc[info_partof.wid==15187796,'partof']='[[somaliland campaign]]'
info_partof.loc[info_partof.wid==18648925,'partof']=''
info_partof.loc[info_partof.wid==33885114,'partof']='[[mughal-maratha wars]]'
info_partof.loc[info_partof.wid==39125595,'partof']='[[ukrainian-soviet war]]'
info_partof.loc[info_partof.wid==55466965,'partof']='[[rhodesian bush war]]'

partof_infobox = pd.DataFrame(columns=('wid','title','partof'))  # generate a new dataset
info_partof['partof']=info_partof['partof'].fillna(0) 
rcount=0 
for row in info_partof.iterrows():
   
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:                                  #because variable 'children' is the forth column in this assuming dataset
        partof_infobox=partof_infobox.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],'partof':[row[1][3]]}),ignore_index=True) 
    if row[1][3]!=0:
        counter=0
        text=''
        sm=0
        for c in row[1][3]:
            
            
            if  c=='[':
                counter=counter+1
               
            if  c==']':
                counter=counter-1
                sm=sm+1
                
            if counter>0:
                text=text+c
                
            if counter==0 and sm>0:
                text=text+c
                partof_infobox=partof_infobox.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],'partof':[text]}),ignore_index=True)
                text=''
                sm=0 

partof_infobox['partof']=partof_infobox['partof'].str.replace(r'\|.*','')
partof_infobox['partof']=partof_infobox['partof'].str.replace(r'\[\[','')
partof_infobox['partof']=partof_infobox['partof'].str.replace(r'\]\]','')
partof_infobox['partof']=partof_infobox['partof'].replace(0,np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('minamoto',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('taira',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('quraysh (tribe)',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('quraysh tribe',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('constantine i (emperor)',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('constantine i',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('iyasu v of ethiopia',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('wars of cyrus the great',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('arctic warfare',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('colombian war of independence',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('african slave trade',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('safavid empire',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('english east india company',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('template:formation of malaysia',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('nanboku-cho',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('haidamaky',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('safavid',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('brian ború',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('byzantium under the komnenos dynasty',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('template:campaignbox 19th-century formosan conflicts',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('nanboku-chō',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('argentine front',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('2008 uefa cup',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('abbasid',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('aftermath of the libyan civil war',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('decolonisation',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('ecuadorian-peruvian territorial dispute',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('operation apostle',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('tafari makonnen',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('eastern jin',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('spanish colonization of the philippines',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('montenegrin-ottoman wars',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('dutthagamani',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('transfer of sovereignty over hong kong',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('first chaco war',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('first caco war',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('kuomintang insurgency',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('piracy in asia',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('umayyad-turgesh wars',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('second battle of arras',np.nan) #from wiki infobox, Battle of the Scarpe (1918) was a part of second battle of arras, but battle of arras was happened in 1917 
partof_infobox['partof']=partof_infobox['partof'].replace('military history of burma#toungoo empire',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('car nicobar class fast attack craft#operation island watch',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('template:campaignbox later israelite campaigns',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('october counteroffensive of the southern front',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('post-invasion iraq, 2003-2006',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('richard marshal',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('umayyad_caliphate#hisham_and_the_limits_of_military_expansion',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('duchy of vasconia',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('marca hispanica',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('kingdom_of_georgia#final_disintegration',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('second armistice at compiègne',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('sudan-south sudan relations',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('omani colonization of africa',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('military history of mexico',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('military of guatemala#history',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('formation of malaysia',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('latinokratia',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('anatolian beyliks',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('barbarian invasions',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('tulunid',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('rise of the kano sultanate',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('military history of burma',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('efforts to control driftnetting',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('sandino rebellion',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('expansion of the ottoman empire',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('greece-turkey relations',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('norse colonization of the americas',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('hungarian-venetian wars',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('official ira',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('provisional ira',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('vietnamese-laotian wars',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('occupation of denmark',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('ismaili-seljuq relations',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('fiji expeditions (disambiguation)',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('heeckerens and bronckhorsts',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('skieringers and fetkeapers',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('List of Special Operations Executive operations',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('matabele war (disambiguation)',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('rise of nationalism under the ottoman empire',np.nan)
partof_infobox['partof']=partof_infobox['partof'].replace('wars of neo-assyria',np.nan)







partof_infobox['partof']=partof_infobox['partof'].str.strip()

partof_infobox=partof_infobox[partof_infobox['wid']!=288520]
partof_infobox=partof_infobox[partof_infobox['wid']!=160665]
partof_infobox=partof_infobox[partof_infobox['wid']!=160664]
partof_infobox=partof_infobox[partof_infobox['wid']!=560948]
partof_infobox=partof_infobox[partof_infobox['wid']!=2150520]
partof_infobox=partof_infobox[partof_infobox['wid']!=16315254]
partof_infobox=partof_infobox[partof_infobox['wid']!=207630]
partof_infobox=partof_infobox[partof_infobox['wid']!=205658]
partof_infobox=partof_infobox[partof_infobox['wid']!=338949]
partof_infobox=partof_infobox[partof_infobox['wid']!=896446]


partof_infobox['partof']=partof_infobox['partof'].str.replace(r'\#.*','')
partof_infobox['partof']=partof_infobox['partof'].str.replace(r'\&ndash\;','-')
partof_infobox['partof']=partof_infobox['partof'].str.replace(r'\&nbsp\;',' ')

partof_infobox['partof']=partof_infobox['partof'].fillna(0)
partof_infobox=partof_infobox[partof_infobox['partof']!=0]
partof_infobox.to_csv(main_path+'output/partof_infobox.csv',index=False)
