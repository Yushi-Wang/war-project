# -*- coding: utf-8 -*-
"""
Created on Sun Mar 31 20:59:21 2019

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

info_location=writer.loc[:,['wid','title','tmp']]
def function0(a,b):
    if a!=0:
        return a
    else:
        return b

info_location['tmp']=info_location['tmp'].str.replace(r'\{\{Pufc\|1=\|date=.*?\}\}','')
info_location['tmp']=info_location['tmp'].str.replace(r'\{\{Dubious\|date=.*?\}\}','')
info_location['tmp']=info_location['tmp'].str.replace(r'\{\{page needed\|date=.*?\}\}','')


info_location['location']=info_location['tmp'].str.extract(r'(.*)',expand=False)


info_location['location']=info_location['location'].str.replace(r'<ref[^/]*?>.*?<\/ref>','')
info_location['location']=info_location['location'].str.replace(r'<ref.*?>','')
info_location['location']=info_location['location'].str.replace(r'<sup>.*?<\/sup>','')
info_location['location']=info_location['location'].str.replace(r'<small[^/]*?>.*?<\/small>','')
info_location['location']=info_location['location'].str.replace(r'\{\{cite web\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{cn\|date.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{small *\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{sfnp\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{\#tag:ref\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{citation needed\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{efn *\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{sfn *\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{refn *\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\[https\:\/\/.*?\]','')
info_location['location']=info_location['location'].str.replace(r'\{\{in\b\}\}','')
info_location['location']=info_location['location'].str.replace(r'\<\!\-\-.*?\-\-\>','')
info_location['location']=info_location['location'].str.replace(r'\{\{cref2\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{nowrap\|\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{resize\|.*?\}\}','')
info_location['location']=info_location['location'].str.replace(r'09\:02–09\:40','')
info_location['location']=info_location['location'].str.replace(r'\{\{rp\|136\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{rp\|10\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{rp\|178\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{rp\|267\}\}','')
info_location['location']=info_location['location'].str.replace(r'\{\{rp\|244-5\}\}','')

info_location['location']=info_location['location'].str.replace(r'\[\[fief\]\]','')
info_location['location']=info_location['location'].str.replace(r'\[\[history of \[\[france\]\]|french\]\]','[[france]]')
info_location['location']=info_location['location'].str.replace(r'\[\[universal transverse mercator coordinate system\|utm grid\]\]','')
info_location['location']=info_location['location'].str.replace(r'\[\[file\:.*?\]\]','')





info_location['location']=info_location['location'].str.extract(r'(\| *place *=.*)',expand=False)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *territory *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *result *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *results *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *coordinates *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *casus *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *cause *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *conflict *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)


info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *part *of *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *image_size *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)



info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *map_type *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *image *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)



info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *campaign *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)


info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *causes *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)


info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *time *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *lat_deg *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *location *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *caption *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *commander1 *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *combatant1 *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *status *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *casus belli *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *date *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *territorial changes *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *width *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *notes *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *closest_city *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *latitude *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *latd *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *lat *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *map_label *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *presidency *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *strength1 *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *casus beli *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *sicilyresult *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *casualties3 *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)

info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *target *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)


info_location['comb1_2']=info_location['location'].str.extract(r'(.*?)\| *methods *=',expand=False)
info_location=info_location.fillna(0)
info_location['location']=info_location.apply(lambda x: function0(x.comb1_2,x.location), axis=1)
info_location.drop('comb1_2',axis=1, inplace=True)



info_location['location']=info_location['location'].str.replace(r'(?<!\[)china(?!])','[[china]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)england(?!])','[[england]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)paris(?!])','[[paris]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)france(?!])','[[france]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)united kingdom(?!])','[[united kingdom]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)north america(?!])','[[north america]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)italy(?!])','[[italy]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)portugal(?!])','[[portugal]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)new zealand(?!])','[[new zealand]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)mexico(?!])','[[mexico]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)batangas(?!])','[[batangas]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)greece(?!])','[[greece]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bmacedon\b(?!])','[[macedon]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)scotland(?!])','[[scotland]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)massachusetts(?!])','[[massachusetts]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)japan(?!])','[[japan]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)united states(?!])','[[united states]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)south africa(?!])','[[south africa]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)namibia(?!])','[[namibia]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)central europe(?!])','[[central europe]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)florida(?!])','[[florida]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)yugoslavia(?!])','[[yugoslavia]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)libya(?!])','[[libya]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)canada(?!])','[[canada]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)samarra(?!])','[[samarra]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)afghanistan(?!])','[[afghanistan]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)norway(?!])','[[norway]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)taiwan(?!])','[[taiwan]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)north america(?!])','[[north america]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)india(?![n\]])','[[india]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)netherlands(?!])','[[netherlands]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)switzerland(?!])','[[switzerland]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)ireland(?!])','[[ireland]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)crimea(?!])','[[crimea]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)jordan(?!])','[[jordan]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)iraq(?!])','[[iraq]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)baghdad(?!])','[[baghdad]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)south america(?!])','[[south america]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)south vietnam(?!])','[[south vietnam]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)syria(?!])','[[syria]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)germany(?!])','[[germany]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)nyasaland(?!])','[[nyasaland]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\blithuania\b(?!])','[[lithuania]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)peru(?!])','[[peru]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)philippines(?!])','[[philippines]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)brignais(?!])','[[brignais]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)ethiopia(?!])','[[ethiopia]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)illinois(?!])','[[illinois]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)korea(?!])','[[korea]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)stockholm(?!])','[[stockholm]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)romania(?!])','[[romania]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)turkey(?!])','[[turkey]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)kentucky(?!])','[[kentucky]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)south carolina(?!])','[[south carolina]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\brussia\b(?!])','[[russia]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)finland(?!])','[[finland]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)denmark(?!])','[[denmark]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)hungary(?!])','[[hungary]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)singapore(?!])','[[singapore]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bburma\b(?!])','[[burma]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\biceland\b(?!])','[[iceland]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bsudan\b(?!])','[[sudan]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\barmenia\b(?!])','[[armenia]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bhawaii\b(?!])','[[hawaii]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)great britain(?!])','[[great britain]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bmississippi\b(?!])','[[mississippi]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\btennessee\b(?!])','[[tennessee]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bpoland\b(?!])','[[poland]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bnorthern vietnam\b(?!])','[[northern vietnam]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bmontenegro\b(?!])','[[montenegro]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bscandinavia\b(?!])','[[scandinavia]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bbalkans\b(?!])','[[balkans]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\blohgarh\b(?!])','[[lohgarh]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bbelgium\b(?!])','[[belgium]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\badoni\b(?!])','[[adoni]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bnargund\b(?!])','[[nargund]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bvenezuela\b(?!])','[[venezuela]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\barkansas\b(?!])','[[arkansas]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bzambia\b(?!])','[[zambia]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\blaos\b(?!])','[[laos]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bhispaniola\b(?!])','[[hispaniola]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bbulgaria\b(?!])','[[bulgaria]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bthiruchendur\b(?!])','[[thiruchendur]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bguanajuato\b(?!])','[[guanajuato]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bjalisco\b(?!])','[[jalisco]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bixtlahuaca\b(?!])','[[ixtlahuaca]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bsan luis potosí\b(?!])','[[san luis potosí]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bcolima\b(?!])','[[colima]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bbangladesh\b(?!])','[[bangladesh]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bpoonch\b(?!])','[[poonch]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bsarmatia\b(?!])','[[sarmatia]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bbosporus\b(?!])','[[bosporus]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\blauro\b(?!])','[[lauro]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bargolis\b(?!])','[[argolis]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bthogur\b(?!])','[[thogur]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\banatolia\b(?!])','[[anatolia]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bseoul\b(?!])','[[seoul]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\balgiers\b(?!])','[[algiers]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bmesopotamia \b(?!])','[[mesopotamia ]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bbeirut\b(?!])','[[beirut]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bghana\b(?!])','[[ghana]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bbuenos aires\b(?!])','[[buenos aires]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bdardania\b(?!])','[[dardania]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bcosta rica\b(?!])','[[costa rica]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\baquitaine\b(?!])','[[aquitaine]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bgascony\b(?!])','[[gascony]]')
info_location['location']=info_location['location'].str.replace(r'(?<!\[)\bpanamá\b(?!])','[[panamá]]')


info_location.loc[info_location.wid==771,'location']='[[eastern north america]], [[caribbean sea]], [[indian subcontinent]], [[africa]], the [[atlantic ocean]], and the [[indian ocean]]'
info_location.loc[info_location.wid==54422,'location']='[[china]]'
info_location.loc[info_location.wid==732124,'location']='[[kotka]]'
info_location.loc[info_location.wid==7690881,'location']='[[Pakchon]], [[North Korea]]'
info_location.loc[info_location.wid==32604382,'location']='[[West Virginia]]'
info_location.loc[info_location.wid==35901554,'location']='[[Western Virginia]]'
info_location.loc[info_location.wid==52724615,'location']='[[south china sea]]'
info_location.loc[info_location.wid==40219703,'location']='[[edirne]]'
info_location.loc[info_location.wid==43179791,'location']='[[britain(place name)]]'
info_location.loc[info_location.wid==47414490,'location']='[[al-ghab plain]], [[idlib governorate|idlib]] and [[hama governorate|hama]] governorates, [[syria]]'

 

location_infobox = pd.DataFrame(columns=('wid','title','location'))  # generate a new dataset
info_location['location']=info_location['location'].fillna(0) 
rcount=0 
for row in info_location.iterrows():
   
    rcount=rcount+1
    print(rcount)
   
    if row[1][3]==0:                                  #because variable 'children' is the forth column in this assuming dataset
        location_infobox=location_infobox.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],'location':[row[1][3]]}),ignore_index=True) 
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
                location_infobox=location_infobox.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],'location':[text]}),ignore_index=True)
                text=''
                sm=0 

location_infobox['location']=location_infobox['location'].str.replace(r'\|.*','')
location_infobox['location']=location_infobox['location'].str.replace(r'\[\[','')
location_infobox['location']=location_infobox['location'].str.replace(r'\]\]','')
location_infobox['location']=location_infobox['location'].replace(0,np.nan)
location_infobox['location']=location_infobox['location'].str.strip()

location_infobox=location_infobox[location_infobox['wid']!=288520]
location_infobox=location_infobox[location_infobox['wid']!=160665]
location_infobox=location_infobox[location_infobox['wid']!=160664]
location_infobox=location_infobox[location_infobox['wid']!=560948]
location_infobox=location_infobox[location_infobox['wid']!=2150520]
location_infobox=location_infobox[location_infobox['wid']!=16315254]
location_infobox=location_infobox[location_infobox['wid']!=207630]
location_infobox=location_infobox[location_infobox['wid']!=205658]

location_infobox.to_csv(main_path+'output/location_infobox.csv',index=False)













