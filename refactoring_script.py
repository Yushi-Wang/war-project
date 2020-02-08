# -*- coding: utf-8 -*-
"""
Title: WAR
Aim:This script is for getting the information of conflicts and revolutions in the human history from wikipedia, including the participants
,commanders, time and locations.
For catching the infoboxes from wikipedia articles, using gather_infoboxes_new.py instead.
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
pariticipants_path=main_path+'output/wikiWAR_participant_infobox.csv'
commanders_path=main_path+'output/wikiWAR_commander_infobox.csv'
time_path=main_path+'output/wikiWAR_info_time.csv'
location_path=main_path+'output/wikiWAR_location_infobox.csv'
partof_infobox_path=main_path+'output/partof_infobox.csv'
partof_path=main_path+'output/wikiWAR_partof_infobox_iteration.csv'
qid_subject_path='D:/learning/Arash/war_participants/Fabian_06292019_WAR/Data/A1_00_qidSubject.dta'
try:
    mkdir(main_path)
except FileExistsError:
    pass

writer=pd.read_csv(main_path+'input/infobox_new.csv') 
wid_to_qid=pd.read_csv(main_path+'input/wid_to_qid.tsv',delimiter='\t')
qid_subject=pd.read_stata(qid_subject_path)
qid_subject['subject']=qid_subject['subject'].str.encode('latin-1').str.decode('utf-8')

#######drop those that are not wars at all
writer=writer[writer['wid']!=288520]
writer=writer[writer['wid']!=160665]
writer=writer[writer['wid']!=160664]
writer=writer[writer['wid']!=560948]
writer=writer[writer['wid']!=2150520]
writer=writer[writer['wid']!=16315254]
writer=writer[writer['wid']!=207630]
writer=writer[writer['wid']!=205658]
writer=writer[writer['wid']!=338949]
writer=writer[writer['wid']!=896446]
writer=writer[writer['wid']!=10343280]##this one is tricky. The page of it was delete in January 2019 and the log shows that the arguers believe this one is only a legendary war without 
                                      ## any warrant. So I delete it from the dataset
writer=writer[writer['wid']!=44131689]
writer=writer[writer['wid']!=17677848]
writer=writer[writer['wid']!=4902286] 

writer=writer[writer['wid']!=826616]                    
#######
def function0(a,b):
    if a!=0:
        return a
    else:
        return b




def direct(file,var,ex):
    file['comb1_2']=file[var].str.extract(ex,expand=False).fillna(0)
    file['shadow']=file[var]
    file[var]=file.apply(lambda x: function0(x.comb1_2,x.shadow), axis=1)
    file.drop(['comb1_2','shadow'],axis=1, inplace=True)
#
#######the funtion for positioning
def direct2(file,var):
    direct(file,var,r'(.*?)\| *combatant[123456]')
    direct(file,var,r'(.*?)\| *commander[123456]')
    direct(file,var,r'(.*?)\| *strength *=')
    direct(file,var,r'(.*?)\| *casualties *=')
    direct(file,var,r'(.*?)\| *campaignbox *=')
    direct(file,var,r'(.*?)\| *units *[123456]')
    direct(file,var,r'(.*?)\| *conflict *=')
    direct(file,var,r'(.*?)\| *formations[12345678] *=')
    direct(file,var,r'(.*?)\| *place *=')
    direct(file,var,r'(.*?)\| *casus *=')
    direct(file,var,r'(.*?)\| *coordinates *=')
    direct(file,var,r'(.*?)\| *part *of *=')
    direct(file,var,r'(.*?)\| *image_size *=')
    direct(file,var,r'(.*?)\| *result *=')
    direct(file,var,r'(.*?)\| *results *=')
    direct(file,var,r'(.*?)\| *map_type *=')
    direct(file,var,r'(.*?)\| *image *=')
    direct(file,var,r'(.*?)\| *territory *=')
    direct(file,var,r'(.*?)\| *campaign *=')
    direct(file,var,r'(.*?)\| *causes *=')
    direct(file,var,r'(.*?)\| *cause *=')
    direct(file,var,r'(.*?)\| *time *=')
    direct(file,var,r'(.*?)\| *lat_deg *=')
    direct(file,var,r'(.*?)\| *location *=')
    direct(file,var,r'(.*?)\| *caption *=')
    direct(file,var,r'(.*?)\| *status *=')
    direct(file,var,r'(.*?)\| *casus belli *=')
    direct(file,var,r'(.*?)\| *date *=')
    direct(file,var,r'(.*?)\| *territorial changes *=')
    direct(file,var,r'(.*?)\| *width *=')
    direct(file,var,r'(.*?)\| *notes *=')
    direct(file,var,r'(.*?)\| *closest_city *=')
    direct(file,var,r'(.*?)\| *latitude *=')
    direct(file,var,r'(.*?)\| *latd *=')
    direct(file,var,r'(.*?)\| *lat *=')
    direct(file,var,r'(.*?)\| *map_label *=')
    direct(file,var,r'(.*?)\| *presidency *=')
    direct(file,var,r'(.*?)\| *strength[12345678] *=')
    direct(file,var,r'(.*?)\| *milstrength[12345678] *=')
    direct(file,var,r'(.*?)\| *polstrength[12345678] *=')
    
    direct(file,var,r'(.*?)\| *casus beli *=')
    direct(file,var,r'(.*?)\| *sicilyresult *=')
    direct(file,var,r'(.*?)\| *casualties[12345678] *=')
    direct(file,var,r'(.*?)\| *target *=')
    direct(file,var,r'(.*?)\| *methods *=')
    direct(file,var,r'(.*?)\| *color_scheme *=')
    direct(file,var,r'(.*?)\| *followed *by *=')
    direct(file,var,r'(.*?) *commander[123456] *=')
    direct(file,var,r'(.*?)\| *batailles *=')
    
########

def releasewithbig(file_in,file_out,var,i):
    
    rcount=0
    for row in file_in.iterrows():
        rcount=rcount+1
        print(rcount)
        if row[1][i]==0:
            file_out=file_out.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],var:[row[1][i]]}),ignore_index=True)
        if row[1][i]!=0 :
            counter=0
            text=''
            sm=0
            for c in row[1][i]:
                if c=='{' or c=='[':
                    counter=counter+1
                if c=='}' or c==']':
                    counter=counter-1
                    sm=sm+1
                if counter>0:
                    text=text+c
                
                if counter==0 and sm>0:
                    text=text+c
                    file_out=file_out.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],var:[text]}),ignore_index=True)
                    text=''
                    sm=0
    return file_out

       
def releasewithoutbig(file_in,file_out,var,i):
    
    rcount=0
    for row in file_in.iterrows():
        rcount=rcount+1
        print(rcount)
        if row[1][i]==0:
            file_out=file_out.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],var:[row[1][i]]}),ignore_index=True)
        if row[1][i]!=0 :
            counter=0
            text=''
            sm=0
            for c in row[1][i]:
                if c=='[':
                    counter=counter+1
                if c==']':
                    counter=counter-1
                    sm=sm+1
                if counter>0:
                    text=text+c
                
                if counter==0 and sm>0:
                    text=text+c
                    file_out=file_out.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],var:[text]}),ignore_index=True)
                    text=''
                    sm=0
    return file_out
   
    





writer['tmp']=writer['tmp'].str.replace(r'\{\{Pufc\|1=\|date=.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{pufc\|1=\|date=.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{Dubious\|date=.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{dubious\|date=.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{page needed\|date=.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{deadlink *\|date=.*?\}\}','')


writer['tmp']=writer['tmp'].str.replace(r'<ref name=(.*?)\/>.*?<ref name=\1>','') ##Attention: to refine the results of commanders, I make a little change here thus cause a chaos in the time part. Athough other parts of this data sets shuold not be interupted so serious as time part was, but we still need to be careful whether the results of other parts have been changed due to this change
writer['tmp']=writer['tmp'].str.replace(r'<ref[^/]*?>.*?<\/ref>','')
writer['tmp']=writer['tmp'].str.replace(r'<ref.*?>','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{ref *label.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{smallsup\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'<sup>.*?<\/sup>','')
writer['tmp']=writer['tmp'].str.replace(r'<small[^/]*?>.*?<\/small>','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{cite web\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{cite dcb *\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{smaller *\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{r *\|group.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{cref *\|.*?\}\}','')


writer['tmp']=writer['tmp'].str.replace(r'\{\{cn\|date.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{cn\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{small *\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{sfnp\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{\#tag:ref\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{citation needed\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{efn *\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{sfn *\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{refn *\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{ref *\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{sup *\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{notetag *\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\[https\:\/\/.*?\]','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{in\b\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\<\!\-\-.*?\-\-\>','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{cref2\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{nowrap\|\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{resize\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'117 years ago','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{vague\|date\=september 2016\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'09\:02–09\:40','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{rp\|136\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{rp\|10\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{rp\|178\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{rp\|267\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{rp\|244-5\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{rp\|.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{spaces\|\d+\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{space\|\d+\}\}','')

writer['tmp']=writer['tmp'].str.replace(r'\{\{pad\|\d+px\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{deletable image.*?\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{nbsp\|\d*\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{flagicon image\|\|size=\d+px\}\}','')
writer['tmp']=writer['tmp'].str.replace(r'\{\{flagicon image\|socialist red flag\.svg\}\}','')



"""
###################participants information##########################
"""

def participant(file_in,file_out,var,side):
    direct2(file_in,var)
    
    file_in[var]=file_in[var].replace('=|',np.nan)
    file_in[var]=file_in[var].replace('= |',np.nan)
    file_in[var]=file_in[var].replace('=  |',np.nan)
    
    
    ######remove those only offered supports
    
    file_in[var]=file_in[var].str.replace(r'support.*?----','')
    file_in[var]=file_in[var].str.replace(r'support.*?----','')
    file_in[var]=file_in[var].str.replace(r'support.*?----','')
    file_in[var]=file_in[var].str.replace(r'support.*','')
    #####
    #####remove the ambiguous information
    file_in[var]=file_in[var].str.replace(r'\{\{flagicon image.*?\}\}','')
    file_in[var]=file_in[var].str.replace(r'\[\[file:.*?\]\]','')
    file_in[var]=file_in[var].str.replace(r'\[\[image:.*?\]\]','')
    file_in[var]=file_in[var].str.replace(r'\{\{\*\}\}','')
    file_in[var]=file_in[var].str.replace(r'\[\[\*\]','')
    file_in[var]=file_in[var].str.replace(r'\{\{\-\}\}','')
    file_in[var]=file_in[var].str.replace(r'\{\{br\}\}','')
    file_in[var]=file_in[var].str.replace(r'\{\{plainlist\}\}','')
    file_in[var]=file_in[var].str.replace(r'\{\{collapsible list\}\}','')
    file_in[var]=file_in[var].str.replace(r'\{\{endplainlist\}\}','')
    file_in[var]=file_in[var].str.replace(r'\{\{plainlist\}\}','')
    file_in[var]=file_in[var].str.replace(r'\{\{harv\|.*?\}\}','')
    #####
    #####release the the contents of the braces and square brackets
    file_in[var]=file_in[var].fillna(0)
    file_out = pd.DataFrame(columns=('wid','title','combatant'))
    file_out=releasewithbig(file_in,file_out,'combatant',3)
    file_out['middle']=file_out['combatant'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
    file_out['big']=file_out['combatant'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
    ###second round
    file_out['second']=file_out['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
    file_out=file_out.fillna(0)
    combatant_infobox_second=file_out.loc[file_out['second']!=0]
    combatant_infobox_first=file_out.loc[file_out['second']==0]
    
    combatant_infobox_2nd = pd.DataFrame(columns=('wid','title','combatant'))
    combatant_infobox_2nd =releasewithbig(combatant_infobox_second,combatant_infobox_2nd,'combatant',4)
    combatant_infobox_2nd['middle']=combatant_infobox_2nd['combatant'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
    combatant_infobox_2nd['big']=combatant_infobox_2nd['combatant'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
    
    combatant_infobox_2nd['second']=combatant_infobox_2nd['big'].str.extract(r'(\{\{.*\}\}|\[\[.*\]\])',expand=False)
    combatant_infobox_2nd=combatant_infobox_2nd.fillna(0)
    try:   
        combatant_infobox_2nd['second_middle']=combatant_infobox_2nd['second'].str.extract(r'^\[\[(.*)\]\]$',expand=False)
        combatant_infobox_2nd=combatant_infobox_2nd.fillna(0)
        combatant_infobox_2nd['middle']=combatant_infobox_2nd.apply(lambda x: function0(x.second_middle,x.middle), axis=1)              
    except (AttributeError,ValueError):
        pass
    try:
        combatant_infobox_2nd['second_big']=combatant_infobox_2nd['second'].str.extract(r'^\{\{(.*)\}\}$',expand=False)
        combatant_infobox_2nd=combatant_infobox_2nd.fillna(0)  
        combatant_infobox_2nd['big']=combatant_infobox_2nd.apply(lambda x: function0(x.second_big,x.big), axis=1)          
    except (AttributeError,ValueError):
        pass
    
    combatant_infobox_2nd['big']=combatant_infobox_2nd['big'].str.replace(r'size=\d+px\|','') ##special case: flagcountry|size=21px|bermuda|1910
                  
    combatant_infobox_2nd=combatant_infobox_2nd.loc[:,['wid','title','combatant','middle','big']]
    combatant_infobox_first=combatant_infobox_first.loc[:,['wid','title','combatant','middle','big']]
    file_out=combatant_infobox_first.append(combatant_infobox_2nd,ignore_index=True)
    ###
    ####
    ####get the country out
    try:
        file_out['big_ex']=file_out['big'].str.extract(r'^.*?\|(.*)',expand=False)
        file_out=file_out.fillna(0) 
    
    
        file_out['big_2']=file_out.apply(lambda x: function0(x.big_ex,x.big), axis=1)
        file_out['big_eex']=file_out['big_2'].str.extract(r'^(.*?)\|',expand=False)
        file_out=file_out.fillna(0) 
        file_out['big_3']=file_out.apply(lambda x: function0(x.big_eex,x.big_2), axis=1)
    
        file_out=file_out[file_out['big_3']!='unbulleted list']###we can see from dataframe x that there are several words have nothing to do with participants, just remove them
        file_out=file_out[file_out['big_3']!='kia']
        file_out=file_out[file_out['big_3']!='startplainlist']
        file_out=file_out[file_out['big_3']!='notelist']
        file_out=file_out[file_out['big_3']!='noflag']
        file_out=file_out[file_out['big_3']!='no flag']
        file_out=file_out[file_out['big_3']!='surrendered']
        file_out=file_out[file_out['big_3']!='bullet']
        file_out=file_out[file_out['big_3']!='!']
        file_out=file_out[file_out['big_3']!='plainlist']
        file_out=file_out[file_out['big_3']!='endplainlist']
    except (AttributeError,ValueError):
        pass
    
    file_out['middle']=file_out['middle'].str.replace(r'\|.*','') 
    file_out=file_out.fillna(0)
    try:
        file_out['participants']=file_out.apply(lambda x: function0(x.middle,x.big_3), axis=1)
    except (AttributeError,ValueError):
        file_out['participants']=file_out.loc[:,['middle']]
    file_out=file_out.loc[:,['wid','title','participants']]
    file_out['side']=side
    file_out=file_out.replace(0,np.nan)
    file_out.drop_duplicates(inplace=True)
    file_out=file_out[file_out['participants']!='executed']
    file_out=file_out[file_out['participants']!='1']
    file_out=file_out[file_out['participants']!='a']
    file_out=file_out[file_out['participants']!='aa']
    file_out=file_out[file_out['participants']!='aaa']
    file_out=file_out[file_out['participants']!='aaaa']
    file_out['participants']=file_out['participants'].str.replace(r'#.*','') 
    file_out['participants']=file_out['participants'].str.strip()
    file_out['participants']=file_out['participants'].replace('|',np.nan)
    file_out['participants']=file_out['participants'].replace("'''",np.nan)
    file_out['participants']=file_out['participants'].replace("",np.nan)
    return file_out

##############combatant1 ####################
info_combatant1=writer.loc[:,['wid','title','tmp']]
info_combatant1['combatant1']=info_combatant1['tmp'].str.extract(r'combatant1(.*)',expand=False)
combatant1_infobox = pd.DataFrame(columns=('wid','title','combatant'))
combatant1_infobox=participant(info_combatant1,combatant1_infobox,'combatant1',1)
combatant1_infobox=combatant1_infobox.fillna(0)
combatant1_infobox=combatant1_infobox.loc[combatant1_infobox['participants']!=0]
#############################################
##############combatant2 ####################
info_combatant2=writer.loc[:,['wid','title','tmp']]
info_combatant2['combatant2']=info_combatant2['tmp'].str.extract(r'combatant2(.*)',expand=False)
combatant2_infobox = pd.DataFrame(columns=('wid','title','combatant'))
combatant2_infobox=participant(info_combatant2,combatant2_infobox,'combatant2',2)
combatant2_infobox=combatant2_infobox.fillna(0)
combatant2_infobox=combatant2_infobox.loc[combatant2_infobox['participants']!=0]
#############################################
##############combatant3 ####################
info_combatant3=writer.loc[:,['wid','title','tmp']]
info_combatant3['combatant3']=info_combatant3['tmp'].str.extract(r'combatant3(.*)',expand=False)
combatant3_infobox = pd.DataFrame(columns=('wid','title','combatant'))
combatant3_infobox=participant(info_combatant3,combatant3_infobox,'combatant3',3)
combatant3_infobox=combatant3_infobox.fillna(0)
combatant3_infobox=combatant3_infobox.loc[combatant3_infobox['participants']!=0]

#############################################
##############combatant4 ####################
info_combatant4=writer.loc[:,['wid','title','tmp']]
info_combatant4['combatant4']=info_combatant4['tmp'].str.extract(r'combatant4(.*)',expand=False)
info_combatant4.loc[info_combatant4.wid==312905,'combatant4']= '[[lebanon]][[united nations]] [[multinational force in lebanon]][[united states]][[france]][[italy]] [[arab deterrent force]][[saudi arabia]][[sudan]][[uae]][[libya]][[south yemen]] '
combatant4_infobox = pd.DataFrame(columns=('wid','title','combatant'))
combatant4_infobox=participant(info_combatant4,combatant4_infobox,'combatant4',4)
combatant4_infobox=combatant4_infobox.fillna(0)
combatant4_infobox=combatant4_infobox.loc[combatant4_infobox['participants']!=0]
#############################################
##############combatant5 ####################
info_combatant5=writer.loc[:,['wid','title','tmp']]
info_combatant5['combatant5']=info_combatant5['tmp'].str.extract(r'combatant5(.*)',expand=False)
info_combatant5.loc[info_combatant5.wid==234655,'combatant5']= '[[Atlixco]]'
info_combatant5.loc[info_combatant5.wid==37353977,'combatant5']= 0
info_combatant5=info_combatant5[info_combatant5['combatant5']!=0]
combatant5_infobox = pd.DataFrame(columns=('wid','title','combatant'))
combatant5_infobox=participant(info_combatant5,combatant5_infobox,'combatant5',5)
combatant5_infobox=combatant5_infobox.fillna(0)
combatant5_infobox=combatant5_infobox.loc[combatant5_infobox['participants']!=0]
#############################################
##############combatant1a ####################
info_combatant1a=writer.loc[:,['wid','title','tmp']]
info_combatant1a['combatant1a']=info_combatant1a['tmp'].str.extract(r'combatant1a(.*)',expand=False)
combatant1a_infobox = pd.DataFrame(columns=('wid','title','combatant'))
combatant1a_infobox=participant(info_combatant1a,combatant1a_infobox,'combatant1a','1a')
combatant1a_infobox=combatant1a_infobox.fillna(0)
combatant1a_infobox=combatant1a_infobox.loc[combatant1a_infobox['participants']!=0]
#############################################
##############combatant1b ####################
info_combatant1b=writer.loc[:,['wid','title','tmp']]
combatant1b_infobox = pd.DataFrame(columns=('wid','title','combatant'))
info_combatant1b['combatant1b']=info_combatant1b['tmp'].str.extract(r'combatant1b(.*)',expand=False)    
combatant1b_infobox=participant(info_combatant1b,combatant1b_infobox,'combatant1b','1b')
combatant1b_infobox=combatant1b_infobox.fillna(0)
combatant1b_infobox=combatant1b_infobox.loc[combatant1b_infobox['participants']!=0]
#############################################
##############combatant2a ####################
info_combatant2a=writer.loc[:,['wid','title','tmp']]
info_combatant2a['combatant2a']=info_combatant2a['tmp'].str.extract(r'combatant2a(.*)',expand=False)
combatant2a_infobox = pd.DataFrame(columns=('wid','title','combatant'))
combatant2a_infobox=participant(info_combatant2a,combatant2a_infobox,'combatant2a','2a')
combatant2a_infobox=combatant2a_infobox.fillna(0)
combatant2a_infobox=combatant2a_infobox.loc[combatant2a_infobox['participants']!=0]
#############################################
##############combatant2b ####################
info_combatant2b=writer.loc[:,['wid','title','tmp']]
combatant2b_infobox = pd.DataFrame(columns=('wid','title','combatant'))
info_combatant2b['combatant2b']=info_combatant2b['tmp'].str.extract(r'combatant2b(.*)',expand=False)    
combatant2b_infobox=participant(info_combatant2b,combatant2b_infobox,'combatant2b','2b')
combatant2b_infobox=combatant2b_infobox.fillna(0)
combatant2b_infobox=combatant2b_infobox.loc[combatant2b_infobox['participants']!=0]
#############################################
##############combatant3a ####################
info_combatant3a=writer.loc[:,['wid','title','tmp']]
info_combatant3a['combatant3a']=info_combatant3a['tmp'].str.extract(r'combatant3a(.*)',expand=False)
combatant3a_infobox = pd.DataFrame(columns=('wid','title','combatant'))
combatant3a_infobox=participant(info_combatant3a,combatant3a_infobox,'combatant3a','3a')
combatant3a_infobox=combatant3a_infobox.fillna(0)
combatant3a_infobox=combatant3a_infobox.loc[combatant3a_infobox['participants']!=0]
#############################################
##############combatant3b ####################
info_combatant3b=writer.loc[:,['wid','title','tmp']]
combatant3b_infobox = pd.DataFrame(columns=('wid','title','combatant'))
info_combatant3b['combatant3b']=info_combatant3b['tmp'].str.extract(r'combatant3b(.*)',expand=False)    
combatant3b_infobox=participant(info_combatant3b,combatant3b_infobox,'combatant3b','3b')
combatant3b_infobox=combatant3b_infobox.fillna(0)
combatant3b_infobox=combatant3b_infobox.loc[combatant3b_infobox['participants']!=0]
#############################################
###############aggregate all the participants into one dataset###################
war_participant_infobox=combatant1_infobox.append(combatant2_infobox,ignore_index=True)
war_participant_infobox=war_participant_infobox.append(combatant3_infobox,ignore_index=True)
war_participant_infobox=war_participant_infobox.append(combatant4_infobox,ignore_index=True)
war_participant_infobox=war_participant_infobox.append(combatant5_infobox,ignore_index=True)
war_participant_infobox=war_participant_infobox.append(combatant1a_infobox,ignore_index=True)
war_participant_infobox=war_participant_infobox.append(combatant1b_infobox,ignore_index=True)
war_participant_infobox=war_participant_infobox.append(combatant2a_infobox,ignore_index=True)
war_participant_infobox=war_participant_infobox.append(combatant2b_infobox,ignore_index=True)
war_participant_infobox=war_participant_infobox.append(combatant3a_infobox,ignore_index=True)
war_participant_infobox=war_participant_infobox.append(combatant3b_infobox,ignore_index=True)

war_participant_infobox=war_participant_infobox.sort_index(by = ["wid",'side'])

war_participant_infobox['x1']=war_participant_infobox['participants'].str.extract(r'(war$)',expand=False)
war_participant_infobox=war_participant_infobox.fillna(0)          
war_participant_infobox=war_participant_infobox.loc[war_participant_infobox['x1']==0]
war_participant_infobox.drop('x1',axis=1, inplace=True)

war_participant_infobox['participants']=war_participant_infobox['participants'].str.strip()
war_participant_infobox=war_participant_infobox.replace(0,np.nan)
war_participant_infobox.to_csv(pariticipants_path,index=False)


"""
###################commander information##########################
"""



def matchcommandercountry(file_in):
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('*', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<br>', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<br/>', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<br />', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<br / >', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<hr>', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('\{\{br\}\}', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<br/ >', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('(?<=\]) *\| *(?=\[)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('(?<=\]) *\| *(?=\{)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('(?<=\}) *\| *(?=\[)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('(?<=\}) *\| *(?=\{)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('\| *(?=\{)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('\| *(?=\[)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('----', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<hr/>a\)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<hr/>b\)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<hr/>c\)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<hr/>d\)', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('<hr/>', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
  
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('\{\{-\}\}', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    
    file_in=file_in.drop('commander', axis=1).join(file_in['commander'].str.split('\| *\d{1,2} *=', expand=True).stack().reset_index(level=1, drop=True).rename('commander'))
    file_in.drop_duplicates(inplace=True)
    return file_in

def condition_b(file_in,ex):
    file_in['condition_b_1']=file_in['commander'].str.extract(ex,expand=False).fillna(0)
    file_in['shadow']=file_in['condition_b']
    file_in['condition_b']=file_in.apply(lambda x: function0(x.condition_b_1,x.shadow), axis=1)
    file_in.drop(['condition_b_1','shadow'],axis=1, inplace=True)
def commandercondition_bracket(file_in):
    file_in['condition_b']=file_in['commander'].str.extract(r'\[\[(death by natural causes.*?)]\]',expand=False)
    condition_b(file_in, r'\[\[(death by natural causes.*?)\]\]')
    condition_b(file_in, r'\[\[([^[]*?\|\†)\]\]')
    condition_b(file_in, r'\[\[([^[]*?\|\ⱶ)\]\]')
    condition_b(file_in, r'\[\[([^[]*?\|\+)\]\]')
    condition_b(file_in, r'\[\[(wounded in action)\|\]\]')
    condition_b(file_in, r'\[\[(executed by hanging\|\☠)\]\]')
    condition_b(file_in, r'\[\[(\†)\]\]')
    condition_b(file_in, r'\[\[(pow)\]\]')

def commandercleaning(file_in):
    file_in['commander']=file_in['commander'].str.strip()
    file_in['commander']=file_in['commander'].str.replace(r"'''",'')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{efn\|.*?\}\}','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{sfn\|.*?\}\}','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{refn\|.*?\}\}','')
    
    
    file_in['commander']=file_in['commander'].str.replace(r'<ref[^/]*?>.*?<\/ref>','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ *file:.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'<small[^/]*?>.*?<\/small>','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{cite web\|.*?\}\}','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{cite dcb *\|.*?\}\}','')

    file_in['commander']=file_in['commander'].str.strip()
    file_in['image']=file_in['commander'].str.extract(r'\[\[image:(.*?)\]\]',expand=False)
    
    file_in['commander']=file_in['commander'].str.replace(r'\[\[image:.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{small\|.*?\}\}','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[1st belorussian front\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[1st combat evaluation group.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[1st florida cavalry regiment.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[\d+th army.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[\d+th regiment of foot.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[\d+th air force\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[\d+th infantry division.*?\]\]','')
    
    
    
    
    file_in['commander']=file_in['commander'].str.replace(r'\[\[mraf\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[tsar\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lord\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[sir\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[qing dynasty\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'<!--.*?-->','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[list of.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\(.*?\)','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[death by natural causes.*?]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[death by natural causes.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[[^[]*?\|†\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[[^[]*?\|#\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[madame\|m<sup>me</sup>\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[madame\|m\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[[^[]*?\|ⱶ\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[[^[]*?\|\+\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[captain \|naval captain\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general officer\|gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major general\|majgen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major general\|maj\.gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general\|lt.gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant general\|lt.gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[germanic kingship\|king\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[mercenaries\]\]','')
    
    file_in['commander']=file_in['commander'].str.replace(r'\[\[admiral\|adm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[admiral \|adm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[admiral\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[rear admiral\]\]','')
    
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commander\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commander\|comm\]\]','')
    
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general\]\]','')
    
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general \|gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general \|gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[grand master  \|grand master\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[captain \|capt\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[president of the united states\|president\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[vice admiral\|vadm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[maj\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brig\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{#tag:.*?\}\}','')
    file_in['commander']=file_in['commander'].str.replace(r':\{\{.*','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[holy league \|holy league\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[navy\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[2nd belorussian front\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[1st ukrainian front\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ii corps \|ii corps\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[iii corps \|iii corps\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[iv corps \|iv corps\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[meiji emperor\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general\|gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lt\. gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant general\|lt\. gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commander \|cdr\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[sub-lieutenant\|sub-lt\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[befehlshaber der u-boote\|bdu\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[tan son nhut\|det\. 15\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[tbd\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{collapsible list.*?\}\}','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[military leadership in the american civil war#the union\|and others\.\.\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[llandaff\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[sheriff\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[marshal of france\|maréchal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[wounded in action\|\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prince regent\|prince-regent\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prime minister of the imperial cabinet\|prime minister\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[high commissioners for palestine and transjordan\|high commissioner\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commander-in-chief\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air commodore\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[haganah\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air chief marshal\|acm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[colonel\|col\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ban *|ban *\]\]','')
    
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commanding officer\|co\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[governor general of india\|gov\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prime minister of india|pm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air marshal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant general\|lt\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major general\|maj\.gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[maharaja of jammu and kashmir\|maharaja\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant colonel\|lt\. col\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brevet \|bvt\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[leader of the opposition \|leader of the opposition\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[captain general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[hassan uprising\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general\|gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air marshal\|am\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[vice-admiral\|adm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major\|maj\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[group captain\|gp capt\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[consul\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant general|lt\.gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major general\|maj\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[grand hetman of lithuania\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[vice admiral\|v\.adm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general \|lt-gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[captain \|captain\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general officer\|general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[roman consul\|consul\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[vice-admiral\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[papal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[mercenarie\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[cavalry\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major-general \|mg\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadier-general \|bg\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadier general\|brig\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[marshal of poland\|marshal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[field marshal \|field marshal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[colonel general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[doctor \|dr\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[bishop of clogher \|lord bishop of clogher\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ghazi \|gazi\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major-general \|major general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prime minister of bangladesh\|premier\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[rhondda\|glyn rhondda\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[vice admiral\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[colonel\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[imam\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commander\|cdr\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[earl of carrick\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[maj gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[field crown hetman\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[voivode\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brig\. gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[rulers of moldavia\|voivode of moldavia\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[khan \|khan\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[president of france|president\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadier general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[group captain\|gp\.capt\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air marshal\|air mshl\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[executed by hanging\|☠\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[counter admiral\|\'\'contre-amiral\'\'\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general\|ltg\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[colonel\|col\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lt\. colonel\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[regional command south\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major-general\|mgen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadier-general\|bgen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[count of flanders\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prisoner of war\|p\.o\.w\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lt\. general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[maj\. general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadier\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[maha vir chakra\|mvc\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[shah\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-colonel\|lt\.col\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[grand hetman\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[regimentarz\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[hetmans of ukrainian cossacks\|hetman\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general\|lgen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brig\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadier\|brig\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[admiral \|admiral\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[†\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air vice-marshal\|avm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[counter admiral\|.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major general \|major general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[theme \|thema\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[byzantine navy\|fleets\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[group captain|gp capt\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[wing commander \|wg cdr\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant colonel\|lt col\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[high king\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[mouse\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[al-qassim province\|qassim region\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commodore \|commodore\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[governor-general of the dutch east indies\|governor-general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[korvettenkapitän\|kkpt\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[maj\. general\|maj\. gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lt\. general\|lt\. gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brig\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[col\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[transylvania\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general officer commanding\|goc\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[24th infantry division \|24th infantry division\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major-general\|mg\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadier-general\|b\.gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[imperial commissioner \|imperial commissioner\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[jiangsu\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[viceroy of huguang\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[common era\|ce\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[gov\. liu rong\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[mayor of the palace\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[hms castor \|hms \'\'castor\'\'\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[hms north star \|\'\'hms north star\'\'\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[hms calliope \|\'\'hms calliope\'\'\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[hms elphinstone\|heics \'\'elphinstone\'\'\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[islamic republican party\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[maj\. gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general\|lgen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air marshal\|air-mshl\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major-general\|mgen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadier|brig\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[maharashtra police\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commander \|cdr\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general\|ltg\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadier-general\|bgen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[political resident\|p\/a\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant colonel\|lt\.col\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant colonel\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[first lieutenant\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ban of slavonia\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[rnvr\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[oberstleutnant\|otl\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major general\|maj\. gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[military cross\|mc\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[mullah\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major-general\|m\.gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[rear-admiral\|radm]\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[admiral\|adm\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general \|lt\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general \|lt gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[vice admiral\|vice adm\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air vice-marshal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[rear admiral \|rear admiral\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[maréchal-des-logis\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[quartermaster sergeant\|sergent-fourrier\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[corporal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant \|lieutenant\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[provost of edinburgh\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prime minister\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general field marshal\|gfm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[grand vizier\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigade general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[admiral|adm.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ranks in the french navy#officiers généraux — flag officers\|adm\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[governor\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[indian army\|ia\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ban \|ban\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[kapitänleutnant\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[senior superintendent of police\|ssp\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[sindh police\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[inspector general\|ig sindh\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[additional inspector general of police\|aig\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major-general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[standing nato maritime group 1\|standing nato maritime group one\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commodore \|cdre\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[field marshal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant\|lt\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major-general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'title = \[\[','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prime minister of pakistan\|prime minister\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[chief of army staff \|army chief\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[chairman joint chiefs of staff committee\|chairman jcsc\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[inter-services intelligence\|isi\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[chief of naval staff \|naval chief\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air mshl\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lt\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[colonel \|col\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[contre-amiral\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant general\|lieutenant general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[kapitänleutnant\|kl\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[count of barcelona\|count\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[south wales borderers\|24th of foot\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[president of bangladesh\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[cibyrrhaeots\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[chief of air staff \|air chief\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[international freedom battalion\|ifb\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[communist party of turkey\/marxist–leninist\|tkp\/ml\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[field marshal\|fm\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[field marshal\|fm\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[military leadership in the american civil war#the confederacy\|and others\.\.\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prince\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[state of vietnam\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[army group centre\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[jiang \|general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[grand pensionary\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[daimyō\|lord\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[w:zh:.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[colonel general\|generaloberst\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[general officer\|gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major general\|mgen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[vice-admiral\|vadm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[rear-admiral\|radm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air vice marshal\|avm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air commodore\|air cdre\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[governor general of pakistan\|gov\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant colonel\|col\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant colonel\|lieutenant colone\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[divisional general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant colonel\|lieutenant colonel\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r' of \[\[.*?\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\,','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant colonel\|ltn-col\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[archduke\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[master-general of the ordnance\|mgo\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[col\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[giritli hüseyin pasha\|hussein\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commodore |contre-amiral\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[republic of venice\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[grand duchy of tuscany\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[duchy of modena and reggio\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[mercenary\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[modena\|modenese\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[high duke of poland\|duke\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prime minister of poland\|prime minister\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[marshal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[army general \|general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[sultan\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[mosu\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[halab|aleppo\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[colonel \|colonel\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[viceroy\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commandant-general\|commandant-general\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[constable of castile\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[#revolt of february 1522\|1522 revolt\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[sardar\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[flight lieutenant\|flt\. lt.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[wing commander \|wing commander\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[captain \|cpt\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[bač, serbia\|bács\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ras \|ras\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[rear-admiral\|rear admiral\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[hetmans of polish-lithuanian commonwealth\|hetman\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major general \|maj\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general\|lit\. gen\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[:wikt:ông\|ông\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[:wikt:thượng thư\|thượng thư\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\|notes      = a: .*','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[jarl in sweden\|jarl\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ghent\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[mawlawi \|maulvi\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[grand prince of the hungarians\|grand prince\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[italian co-belligerent army\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[air chief marshal\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[fakir\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[great crown hetman\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[horka \|horka\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[prince of orange\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[khagan\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[toqui\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[rear-admiral\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[earl\|jarl\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lt\. col\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant\|lt\.\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commander\|cdr\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[four star general\|gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[four-star admiral\|adm\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[lieutenant-general\|gen\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[brigadeführer\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[war in the vendée\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[peshwa\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[monarch\|king\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[bey\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[a\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[b\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[c\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[d\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[battle of vistula lagoon\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[pow\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[bey\|beg\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[bishop\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[army of the danube\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[guangzhou\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'<\/ref>.*','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[flag lieutenant\|flag-lieut\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[ss-ogruf\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commandant \|commandant\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[commandant\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[major \|maj\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[saint-lô\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[france\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[obergruppenführer\|ss-ogruf\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[governor of worcester\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[co-belligerence\|co-belligerent\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[kingdom of georgia\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[amir\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[taliban insurgency\|taliban insurgents\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[horka \|harka\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[korvettenkapitän\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[people\'s liberation army\|pla\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[oberführer\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[chief of army staff of the bangladesh army\|chief of army staff\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[walī\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[walī\|wali\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[armée du var\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[feldzeugmeister\]\]','')
    file_in['commander']=file_in['commander'].str.replace(r'\[\[gauleiter\]\]','')

    file_in['commander']=file_in['commander'].str.strip()
    return file_in

def releasebracket(file_in,file_out):
    rcount=0
    for row in file_in.iterrows():
        #print(row[1][0])
        rcount=rcount+1
        print(rcount)
       
        if row[1][6]==0:
            file_out=file_out.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],'tmp':[row[1][2]],'commander':[row[1][6]],'condition_b':[0],'image':[0]}),ignore_index=True) 
        if row[1][6]!=0:
            counter=0
            text=''
            sm=0
            for c in row[1][6]:
                
                
                if c=='[':
                    counter=counter+1
                   
                if c==']':
                    counter=counter-1
                    sm=sm+1
                    
                if counter>0:
                    text=text+c
                    
                if counter==0 and sm>0:
                    text=text+c
                    file_out=file_out.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],'tmp':[row[1][2]],'commander':[text],'condition_b':[0],'image':[0]}),ignore_index=True)
                    text=''
                    sm=0
    return file_out

def splictcommandercountry(file_in):
    file_in['linked']=file_in['commander'].str.extract(r'\[\[(.*?)\]\]',expand=False)
    file_in=file_in.fillna(0)
    file_in=file_in[file_in['linked']!=0]
    file_in['country']=file_in['commander'].str.extract(r'\{\{(.*?)\}\} *\[\[',expand=False)
    file_in=file_in.fillna(0)
    df2=file_in['country'].str.split('}}',expand=True)
    df2=df2.fillna(0)
    file_in=file_in.drop('country',axis=1).join(df2)
    return file_in

def splictcommanderkia(file_in):
    file_in['commander']=file_in['commander'].str.replace(r'\{\{endplainlist\}\}','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{clear\}\}','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{cref\|g\}\}','')
    file_in['commander']=file_in['commander'].str.replace(r'\{\{midsize\|}\}','')
    file_in['condition']=file_in['commander'].str.extract(r'\]\] *\{\{(.*)\}\}',expand=False)
    file_in=file_in.fillna(0)
    df1=file_in['condition'].str.split('{{',expand=True)
    df1=df1.fillna(0)
    file_in=file_in.drop('condition',axis=1).join(df1)
    return file_in

def cleanimage(file_in):
    file_in['image']=file_in['image'].str.strip()
    file_in=file_in.fillna(0)
    file_in['image']=file_in['image'].str.replace(r'\.svg.*','')
    file_in['image']=file_in['image'].str.replace(r'\.png.*','')
    file_in['image']=file_in['image'].str.replace(r'\.jpg.*','')
    file_in['image']=file_in['image'].str.replace(r'\.gif.*','')
    file_in['image']=file_in['image'].str.replace(r'flagicon image *\|','')
    file_in['image']=file_in['image'].str.replace(r'flagdeco *\|','')
    file_in['image']=file_in['image'].str.replace(r'_',' ')
    file_in['image']=file_in['image'].str.replace(r'flag of','')
    file_in['image']=file_in['image'].str.strip()
    file_in=file_in.fillna(0)
    file_in['image']=file_in['image'].str.replace(r'vexilloid of the','')
    file_in['image']=file_in['image'].str.replace('\(.*?\)','')
    file_in['image']=file_in['image'].str.replace('\d','')
    file_in['image']=file_in['image'].str.replace(r'-','')
    file_in['image']=file_in['image'].str.replace(r'coat of','')
    file_in['image']=file_in['image'].str.replace(r'royal arms of','')
    file_in['image']=file_in['image'].str.strip()
    file_in=file_in.fillna(0)
    file_in['image']=file_in['image'].str.replace(r'arms of','')
    file_in['image']=file_in['image'].str.replace(r'naval jack  stars','')
    file_in['image']=file_in['image'].str.replace(r'the republic of the rif','rif republic')
    file_in['image']=file_in['image'].str.replace(r'^the','')
    file_in['image']=file_in['image'].str.replace(r'naval ensign of','')
    file_in['image']=file_in['image'].replace('red flag',0)
    file_in['image']=file_in['image'].str.replace(r'flag','')
    file_in['image']=file_in['image'].str.replace(r'philippine revolution  magdalo','philippine')
    file_in['image']=file_in['image'].str.strip()
    file_in=file_in.fillna(0)
    file_in['image']=file_in['image'].str.replace(r'coa$','')
    file_in['image']=file_in['image'].str.replace(r'th century','')
    file_in['image']=file_in['image'].str.replace(r'banner of arms','')
    file_in['image']=file_in['image'].str.replace(r'army','')
    file_in['image']=file_in['image'].str.replace(r'arms','')
    file_in['image']=file_in['image'].str.strip()
    file_in=file_in.fillna(0)
    file_in['image']=file_in['image'].str.replace(r'former','')
    file_in=file_in.fillna(0)
    file_in['image']=file_in['image'].replace(r'socialist red',0)
    file_in['image']=file_in['image'].replace(r'm',0)
    file_in['image']=file_in['image'].replace(r'a ppp','ppp')
    
    file_in['image']=file_in['image'].str.replace(r'naval jack of the','')
    file_in['image']=file_in['image'].str.strip()
    file_in=file_in.fillna(0)
    file_in['image']=file_in['image'].str.replace(r'pre$','')
    file_in=file_in.fillna(0)
    file_in['image']=file_in['image'].str.replace(r'^green harp','')
    file_in['image']=file_in['image'].replace(r'splanasir','spla-nasir')
    file_in['image']=file_in['image'].str.replace(r'^asafia','')
    file_in['image']=file_in['image'].replace(r'philippine revolution  kkk','philippine')
    file_in['image']=file_in['image'].str.replace(r'alternative$','')
    file_in['image']=file_in['image'].replace(r'black',0)
    file_in['image']=file_in['image'].replace(r'white',0)
    file_in['image']=file_in['image'].replace(r'ounr','oun-r')
    file_in['image']=file_in['image'].replace(r'united states forces&nbsp;– iraq','united states')
    file_in['image']=file_in['image'].replace(r'bandera cntait','bandera cnt-ait')
    file_in['image']=file_in['image'].replace(r'afrikanerweerstandsbeweging','afrikaner-weerstandsbeweging')
    file_in['image']=file_in['image'].replace(r'alqaeda','al-qaeda')
    file_in['image']=file_in['image'].replace(r'pakistani','pakistan')
    file_in['image']=file_in['image'].str.replace(r'used model$','')
    file_in['image']=file_in['image'].str.replace(r'^ancient  ','')
    file_in['image']=file_in['image'].str.replace(r'^state  ','')
    file_in['image']=file_in['image'].str.replace(r'asymmetric','')
    file_in['image']=file_in['image'].replace(r'byzantine imperial ,','byzantine empire')
    file_in['image']=file_in['image'].str.replace(r',$','')
    file_in['image']=file_in['image'].str.replace(r'alislam','al-islam')
    file_in['image']=file_in['image'].str.replace(r"people's liberation   the",'')
    file_in['image']=file_in['image'].str.replace(r"naval jack of",'')
    file_in['image']=file_in['image'].replace(r'icon image',0)
    file_in['image']=file_in['image'].replace(r'infoboxhez',0)
    file_in['image']=file_in['image'].replace(r'\|size=px',0)
    file_in['image']=file_in['image'].replace('',0)
    file_in['image']=file_in['image'].replace(r'philippine alternate','philippine')
    file_in['image']=file_in['image'].replace(r'war  the imperial japanese','japan')
    file_in['image']=file_in['image'].replace(r'lesser   the kingdom of bohemia','kingdom of bohemia')
    file_in['image']=file_in['image'].str.replace(r'logo of the','')
    file_in['image']=file_in['image'].str.strip()
    file_in['image']=file_in['image'].str.replace(r'^the','')
    file_in['image']=file_in['image'].str.replace(r', small, based on palavestra','')
    file_in['image']=file_in['image'].str.replace(r'civil and  france','france')
    file_in['image']=file_in['image'].str.replace(r'chinese red','china')
    file_in['image']=file_in['image'].str.replace(r'中國工農紅軍軍旗','china')
    file_in['image']=file_in['image'].str.replace(r'naval jack  stripes','')
    file_in['image']=file_in['image'].str.replace(r'president of','')
    file_in['image']=file_in['image'].str.replace(r'logo of','')
    file_in['image']=file_in['image'].str.replace(r'governorgeneral of','')
    file_in['image']=file_in['image'].str.replace(r'prime minister of','') 
    file_in['image']=file_in['image'].str.strip()
    file_in['image']=file_in['image'].str.replace(r' logo$','')
    file_in['image']=file_in['image'].str.strip()
    file_in=file_in.fillna(0)
    return file_in

def cleancommanderof(file_in,var):
    file_in['image']=file_in[var].str.extract(r'(flagicon image.*)',expand=False)
    file_in=file_in.fillna(0)
    
    file_in=cleanimage(file_in)
    
    file_in['shadow']=file_in[var]
    file_in[var]=file_in.apply(lambda x: function0(x.image,x.shadow), axis=1)
    file_in.drop(['image','shadow'],axis=1, inplace=True)
    file_in[var]=file_in[var].str.replace(r'^.*?\|','')
    file_in[var]=file_in[var].str.replace(r'\|.*','')
    file_in=file_in.fillna(0)
    return file_in
def cleancondition(file_in,var):
    file_in[var]=info_commander[var].str.replace(r'\|alt=yes','')
    file_in[var]=info_commander[var].str.replace(r'flag.*','')
    file_in[var]=info_commander[var].str.replace(r'\&nbsp\;','')
    file_in[var]=info_commander[var].str.replace(r'kia2','kia')
    file_in=file_in.replace('',np.nan)
    file_in=file_in.fillna(0)
    return file_in



###################commander1 ##########################
info_commander=writer.loc[:,['wid','title','tmp']]
info_commander['commander']=info_commander['tmp'].str.extract(r'commander1(.*)',expand=False)
direct2(info_commander,'commander')

info_commander=matchcommandercountry(info_commander)

info_commander.drop_duplicates(inplace=True)
info_commander=info_commander.reset_index(drop=True)
info_commander.loc[32666,'commander']=''
info_commander=info_commander.fillna(0)
missing=info_commander[info_commander['commander']==0]
info_commander=info_commander[info_commander['commander']!=0]

#info_commander.drop(['conditon_b_1','conditon_b'],axis=1, inplace=True)
####catch the kia information from the square brackets
commandercondition_bracket(info_commander)
####
info_commander=commandercleaning(info_commander)
####whether there are more than 2 square brackets in a cell
info_commander['brackets']=info_commander['commander'].str.extract(r'\[\[(.*?)\]\].*?\[\[',expand=False)
info_commander=info_commander.fillna(0)
info_commander_nob=info_commander[info_commander['brackets']==0]
brackets=info_commander[info_commander['brackets']!=0]
brackets=brackets.fillna(0)

brackets['commander']=brackets['commander'].str.replace(r'\]\] *\{\{flag',']]<br>{{flag')
brackets['commander']=brackets['commander'].str.replace(r'\]\]\; *\{\{flagicon',']]<br>{{flagicon')   
brackets['commander']=brackets['commander'].str.replace(r'\]\] *and *\[\[',']]<br>[[')
brackets['commander']=brackets['commander'].str.replace(r'\]\] *\[\[',']]<br>[[')
brackets['commander']=brackets['commander'].str.replace(r'\{\{kia\}\}[^$]','{{kia}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\{\{wia\}\}[^$]','{{wia}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\{\{surrendered\}\}[^$]','{{surrendered}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\{\{executed\}\}[^$]','{{executed}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\]\] *\}\} *\{\{flagdeco\|',']]}}<br>{{flagdeco|')
brackets['commander']=brackets['commander'].str.replace(r'\]\] *\}\} *\{\{flagicon\|',']]}}<br>{{flagicon|')


brackets=matchcommandercountry(brackets)

brackets['brackets2']=brackets['commander'].str.extract(r'\[\[(.*?)\]\].*?\[\[',expand=False)
brackets=brackets.fillna(0)
info_commander_nob2=brackets[brackets['brackets2']==0]
brackets2=brackets[brackets['brackets2']!=0]
brackets2=brackets2.fillna(0)
####
####the items in the dataset"brackets" are special cases which we cannot treat them in a normal way, so just catch the commanders(which are in the square brackets) from this dataset without getting their nationalities
brackets_infobox = pd.DataFrame(columns=('wid','title','tmp','commander','condition_b','image'))
brackets_infobox=releasebracket(brackets2,brackets_infobox)

info_commander_nob.drop('brackets',axis=1, inplace=True)
info_commander_nob2.drop(['brackets','brackets2'],axis=1, inplace=True)
info_commander_nob2=info_commander_nob2.loc[:,['wid','title','tmp','commander','condition_b','image']]
info_commander_nob=info_commander_nob.append(info_commander_nob2,ignore_index=True)
info_commander=info_commander_nob.append(brackets_infobox,ignore_index=True)
info_commander=info_commander.reset_index(drop=True)


####
####split the commanders and the countries(forces) they served and then match them
info_commander=splictcommandercountry(info_commander)

info_commander.rename(columns={0:'commander_of_1',1:'commander_of_2',2:'commander_of_3'},inplace=True)
info_commander['commander_of_1']=info_commander['commander_of_1'].str.replace(r'\{\{','')
info_commander['commander_of_2']=info_commander['commander_of_2'].str.replace(r'\{\{','')
info_commander['commander_of_3']=info_commander['commander_of_3'].str.replace(r'\{\{','')
info_commander['commander_of_1']=info_commander['commander_of_1'].str.replace(r'\}\}','')
info_commander['commander_of_2']=info_commander['commander_of_2'].str.replace(r'\}\}','')
info_commander['commander_of_3']=info_commander['commander_of_3'].str.replace(r'\}\}','')
info_commander=info_commander.fillna(0)

####
####split the comanders and the kia information and then match them
info_commander=splictcommanderkia(info_commander)

info_commander.rename(columns={0:'condition_1',1:'condition_2',2:'condition_3'},inplace=True)
info_commander['condition_1']=info_commander['condition_1'].str.replace(r'\{\{','')
info_commander['condition_2']=info_commander['condition_2'].str.replace(r'\{\{','')
info_commander['condition_3']=info_commander['condition_3'].str.replace(r'\{\{','')
info_commander['condition_1']=info_commander['condition_1'].str.replace(r'\}\}','')
info_commander['condition_2']=info_commander['condition_2'].str.replace(r'\}\}','')
info_commander['condition_3']=info_commander['condition_3'].str.replace(r'\}\}','')

info_commander=cleancondition(info_commander, 'condition_1')
info_commander=cleancondition(info_commander, 'condition_2')
info_commander=cleancondition(info_commander, 'condition_3')
info_commander=info_commander.replace('',np.nan)
info_commander=info_commander.fillna(0)
##combine condition_b and condition_123
info_commander['condition_1']=info_commander.apply(lambda x: function0(x.condition_1,x.condition_b), axis=1)
info_commander.drop('condition_b',axis=1, inplace=True)
##

####
#####clean variable 'image'
info_commander=cleanimage(info_commander)

image=info_commander[info_commander['image']!=0]

info_commander['commander_of_1']=info_commander.apply(lambda x: function0(x.image,x.commander_of_1), axis=1)
info_commander.drop('image',axis=1, inplace=True)
info_commander['linked']=info_commander['linked'].str.replace(r'\|.*','')
####
####clean variable variable 'commander_of_1'
info_commander=cleancommanderof(info_commander,'commander_of_1')
####
####clean variable variable 'commander_of_2'
info_commander=cleancommanderof(info_commander,'commander_of_2')
####
####clean variable variable 'commander_of_3'
info_commander=cleancommanderof(info_commander,'commander_of_3')
####
missing['linked']=0
missing['commander_of_1']=0
missing['commander_of_2']=0
missing['commander_of_3']=0
missing['condition_1']=0
missing['condition_2']=0
missing['condition_3']=0

commander1_infobox=info_commander.append(missing,ignore_index=True)
commander1_infobox=commander1_infobox.loc[:,['wid','title','linked','commander_of_1','commander_of_2','commander_of_3','condition_1','condition_2','condition_3']]
commander1_infobox.rename(columns={'linked':'commander'},inplace=True)
commander1_infobox['commander_of_4']=0
commander1_infobox['side']=1
commander1_infobox=commander1_infobox.loc[:,['wid','title','commander','commander_of_1','commander_of_2','commander_of_3','commander_of_4','condition_1','condition_2','condition_3','side']]
commander1_infobox['commander']=commander1_infobox['commander'].str.strip()
commander1_infobox['commander_of_1']=commander1_infobox['commander_of_1'].str.strip()
commander1_infobox['commander_of_2']=commander1_infobox['commander_of_2'].str.strip()
commander1_infobox['commander_of_3']=commander1_infobox['commander_of_3'].str.strip()
commander1_infobox['condition_1']=commander1_infobox['condition_1'].str.strip()
commander1_infobox['condition_2']=commander1_infobox['condition_2'].str.strip()
commander1_infobox['condition_3']=commander1_infobox['condition_3'].str.strip()
commander1_infobox=commander1_infobox.replace('',np.nan)
commander1_infobox=commander1_infobox.replace(0,np.nan)







##############################################################
########################commander2 ###########################
info_commander=writer.loc[:,['wid','title','tmp']]
info_commander['commander']=info_commander['tmp'].str.extract(r'commander2(.*)',expand=False)
direct2(info_commander,'commander')

info_commander=matchcommandercountry(info_commander)

info_commander.drop_duplicates(inplace=True)
info_commander=info_commander.reset_index(drop=True)
info_commander=info_commander.fillna(0)
missing=info_commander[info_commander['commander']==0]
info_commander=info_commander[info_commander['commander']!=0]

####catch the kia information from the square brackets
commandercondition_bracket(info_commander)
####
info_commander=commandercleaning(info_commander)
####whether there are more than 2 square brackets in a cell
info_commander['brackets']=info_commander['commander'].str.extract(r'\[\[(.*?)\]\].*?\[\[',expand=False)
info_commander=info_commander.fillna(0)
info_commander_nob=info_commander[info_commander['brackets']==0]
brackets=info_commander[info_commander['brackets']!=0]
brackets=brackets.fillna(0)

brackets['commander']=brackets['commander'].str.replace(r'\]\] *\{\{flag',']]<br>{{flag')
brackets['commander']=brackets['commander'].str.replace(r'\]\]\; *\{\{flagicon',']]<br>{{flagicon')   
brackets['commander']=brackets['commander'].str.replace(r'\]\] *and *\[\[',']]<br>[[')
brackets['commander']=brackets['commander'].str.replace(r'\]\] *\[\[',']]<br>[[')
brackets['commander']=brackets['commander'].str.replace(r'\{\{kia\}\}[^$]','{{kia}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\{\{wia\}\}[^$]','{{wia}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\{\{surrendered\}\}[^$]','{{surrendered}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\{\{executed\}\}[^$]','{{executed}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\]\] *\}\} *\{\{flagdeco\|',']]}}<br>{{flagdeco|')
brackets['commander']=brackets['commander'].str.replace(r'\]\] *\}\} *\{\{flagicon\|',']]}}<br>{{flagicon|')
brackets['commander']=brackets['commander'].str.replace(r'\| units involved.*','')


brackets=matchcommandercountry(brackets)

brackets['brackets2']=brackets['commander'].str.extract(r'\[\[(.*?)\]\].*?\[\[',expand=False)
brackets=brackets.fillna(0)
info_commander_nob2=brackets[brackets['brackets2']==0]
brackets2=brackets[brackets['brackets2']!=0]
brackets2=brackets2.fillna(0)
####
####the items in the dataset"brackets" are special cases which we cannot treat them in a normal way, so just catch the commanders(which are in the square brackets) from this dataset without getting their nationalities
brackets_infobox = pd.DataFrame(columns=('wid','title','tmp','commander','condition_b','image'))
brackets_infobox=releasebracket(brackets2,brackets_infobox)

info_commander_nob.drop('brackets',axis=1, inplace=True)
info_commander_nob2.drop(['brackets','brackets2'],axis=1, inplace=True)
info_commander_nob2=info_commander_nob2.loc[:,['wid','title','tmp','commander','condition_b','image']]
info_commander_nob=info_commander_nob.append(info_commander_nob2,ignore_index=True)
info_commander=info_commander_nob.append(brackets_infobox,ignore_index=True)
info_commander=info_commander.reset_index(drop=True)


####
####split the commanders and the countries(forces) they served
info_commander=splictcommandercountry(info_commander)
####
info_commander.rename(columns={0:'commander_of_1',1:'commander_of_2',2:'commander_of_3',3:'commander_of_4'},inplace=True)
info_commander['commander_of_1']=info_commander['commander_of_1'].str.replace(r'\{\{','')
info_commander['commander_of_2']=info_commander['commander_of_2'].str.replace(r'\{\{','')
info_commander['commander_of_3']=info_commander['commander_of_3'].str.replace(r'\{\{','')
info_commander['commander_of_4']=info_commander['commander_of_4'].str.replace(r'\{\{','')
info_commander['commander_of_1']=info_commander['commander_of_1'].str.replace(r'\}\}','')
info_commander['commander_of_2']=info_commander['commander_of_2'].str.replace(r'\}\}','')
info_commander['commander_of_3']=info_commander['commander_of_3'].str.replace(r'\}\}','')
info_commander['commander_of_4']=info_commander['commander_of_4'].str.replace(r'\}\}','')
info_commander=info_commander.fillna(0)
####

####split the comanders and the kia information and then match them
info_commander['commander']=info_commander['commander'].str.replace(r'\|sa’ad ud-din khan\|nisar muhammad khan\|khwaja ashura\|.*','')
info_commander=splictcommanderkia(info_commander)


info_commander.rename(columns={0:'condition_1',1:'condition_2'},inplace=True)
info_commander['condition_1']=info_commander['condition_1'].str.replace(r'\{\{','')
info_commander['condition_2']=info_commander['condition_2'].str.replace(r'\{\{','')

info_commander['condition_1']=info_commander['condition_1'].str.replace(r'\}\}','')
info_commander['condition_2']=info_commander['condition_2'].str.replace(r'\}\}','')


info_commander=cleancondition(info_commander, 'condition_1')
info_commander=cleancondition(info_commander, 'condition_2')

info_commander=info_commander.replace('',np.nan)
info_commander=info_commander.fillna(0)
##combine condition_b and condition_12
info_commander['condition_1']=info_commander.apply(lambda x: function0(x.condition_1,x.condition_b), axis=1)
info_commander.drop('condition_b',axis=1, inplace=True)
##

####

#####clean variable 'image'
info_commander=cleanimage(info_commander)

info_commander['commander_of_1']=info_commander.apply(lambda x: function0(x.image,x.commander_of_1), axis=1)
info_commander.drop('image',axis=1, inplace=True)
info_commander['linked']=info_commander['linked'].str.replace(r'\|.*','')
####
####clean variable variable 'commander_of_1'
info_commander=cleancommanderof(info_commander,'commander_of_1')
####
####clean variable variable 'commander_of_2'
info_commander=cleancommanderof(info_commander,'commander_of_2')
####
####clean variable variable 'commander_of_3'

info_commander['commander_of_3']=info_commander['commander_of_3'].str.replace(r'^.*?\|','')
info_commander['commander_of_3']=info_commander['commander_of_3'].str.replace(r'\|.*','')
info_commander=info_commander.fillna(0)
####
####clean variable variable 'commander_of_4'

info_commander['commander_of_4']=info_commander['commander_of_4'].str.replace(r'^.*?\|','')
info_commander['commander_of_4']=info_commander['commander_of_4'].str.replace(r'\|.*','')
info_commander=info_commander.fillna(0)
####
missing['linked']=0
missing['commander_of_1']=0
missing['commander_of_2']=0
missing['commander_of_3']=0
missing['commander_of_4']=0
missing['condition_1']=0
missing['condition_2']=0


commander2_infobox=info_commander.append(missing,ignore_index=True)
commander2_infobox=commander2_infobox.loc[:,['wid','title','linked','commander_of_1','commander_of_2','commander_of_3','commander_of_4','condition_1','condition_2']]
commander2_infobox.rename(columns={'linked':'commander'},inplace=True)
commander2_infobox['condition_3']=0
commander2_infobox['side']=2
commander2_infobox['commander_of_1']=commander2_infobox['commander_of_1'].replace('red flag.svg',0)
commander2_infobox['commander_of_1']=commander2_infobox['commander_of_1'].replace(r'black flag.svg',0)
commander2_infobox=commander2_infobox.loc[:,['wid','title','commander','commander_of_1','commander_of_2','commander_of_3','commander_of_4','condition_1','condition_2','condition_3','side']]
commander2_infobox['commander']=commander2_infobox['commander'].str.strip()
commander2_infobox['commander_of_1']=commander2_infobox['commander_of_1'].str.strip()
commander2_infobox['commander_of_2']=commander2_infobox['commander_of_2'].str.strip()
commander2_infobox['commander_of_3']=commander2_infobox['commander_of_3'].str.strip()
commander2_infobox['commander_of_4']=commander2_infobox['commander_of_4'].str.strip()
commander2_infobox['condition_1']=commander2_infobox['condition_1'].str.strip()
commander2_infobox['condition_2']=commander2_infobox['condition_2'].str.strip()
commander2_infobox=commander2_infobox.replace('',np.nan)
commander2_infobox=commander2_infobox.replace(0,np.nan)

##############################################################
########################commander3 ###########################
info_commander=writer.loc[:,['wid','title','tmp']]
info_commander['commander']=info_commander['tmp'].str.extract(r'commander3(.*)',expand=False)
direct2(info_commander,'commander')

info_commander=matchcommandercountry(info_commander)

info_commander.drop_duplicates(inplace=True)
info_commander=info_commander.reset_index(drop=True)
info_commander=info_commander.fillna(0)
missing=info_commander[info_commander['commander']==0]
info_commander=info_commander[info_commander['commander']!=0]
####catch the kia information from the square brackets
commandercondition_bracket(info_commander)
####
info_commander=commandercleaning(info_commander)
####whether there are more than 2 square brackets in a cell
info_commander['brackets']=info_commander['commander'].str.extract(r'\[\[(.*?)\]\].*?\[\[',expand=False)
info_commander=info_commander.fillna(0)
info_commander_nob=info_commander[info_commander['brackets']==0]
brackets=info_commander[info_commander['brackets']!=0]
brackets=brackets.fillna(0)

brackets['commander']=brackets['commander'].str.replace(r'\]\] *\{\{flag',']]<br>{{flag')
brackets['commander']=brackets['commander'].str.replace(r'\]\]\; *\{\{flagicon',']]<br>{{flagicon')   
brackets['commander']=brackets['commander'].str.replace(r'\]\] *and *\[\[',']]<br>[[')
brackets['commander']=brackets['commander'].str.replace(r'\]\] *\[\[',']]<br>[[')
brackets['commander']=brackets['commander'].str.replace(r'\{\{kia\}\}[^$]','{{kia}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\{\{wia\}\}[^$]','{{wia}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\{\{surrendered\}\}[^$]','{{surrendered}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\{\{executed\}\}[^$]','{{executed}}<br>')
brackets['commander']=brackets['commander'].str.replace(r'\]\] *\}\} *\{\{flagdeco\|',']]}}<br>{{flagdeco|')
brackets['commander']=brackets['commander'].str.replace(r'\]\] *\}\} *\{\{flagicon\|',']]}}<br>{{flagicon|')
brackets['commander']=brackets['commander'].str.replace(r'\| units involved.*','')

brackets=matchcommandercountry(brackets)

brackets['brackets2']=brackets['commander'].str.extract(r'\[\[(.*?)\]\].*?\[\[',expand=False)
brackets=brackets.fillna(0)
info_commander_nob2=brackets[brackets['brackets2']==0]
brackets2=brackets[brackets['brackets2']!=0]
brackets2=brackets2.fillna(0)
####
####the items in the dataset"brackets" are special cases which we cannot treat them in a normal way, so just catch the commanders(which are in the square brackets) from this dataset without getting their nationalities
brackets_infobox = pd.DataFrame(columns=('wid','title','tmp','commander','condition_b','image'))
brackets_infobox=releasebracket(brackets2,brackets_infobox)

info_commander_nob.drop('brackets',axis=1, inplace=True)
info_commander_nob2.drop(['brackets','brackets2'],axis=1, inplace=True)
info_commander_nob2=info_commander_nob2.loc[:,['wid','title','tmp','commander','condition_b','image']]
info_commander_nob=info_commander_nob.append(info_commander_nob2,ignore_index=True)
info_commander=info_commander_nob.append(brackets_infobox,ignore_index=True)
info_commander=info_commander.reset_index(drop=True)


####
####split the commanders and the countries(forces) they served
info_commander=splictcommandercountry(info_commander)
####
info_commander.rename(columns={0:'commander_of_1',1:'commander_of_2'},inplace=True)
info_commander['commander_of_1']=info_commander['commander_of_1'].str.replace(r'\{\{','')
info_commander['commander_of_2']=info_commander['commander_of_2'].str.replace(r'\{\{','')
info_commander['commander_of_1']=info_commander['commander_of_1'].str.replace(r'\}\}','')
info_commander['commander_of_2']=info_commander['commander_of_2'].str.replace(r'\}\}','')
info_commander=info_commander.fillna(0)
####
####split the comanders and the kia information and then match them
info_commander=splictcommanderkia(info_commander)

info_commander.rename(columns={0:'condition_1'},inplace=True)
info_commander['condition_1']=info_commander['condition_1'].str.replace(r'\{\{','')
info_commander['condition_1']=info_commander['condition_1'].str.replace(r'\}\}','')

info_commander=cleancondition(info_commander, 'condition_1')

info_commander=info_commander.replace('',np.nan)
info_commander=info_commander.fillna(0)
##combine condition_b and condition_123
info_commander['condition_1']=info_commander.apply(lambda x: function0(x.condition_1,x.condition_b), axis=1)
info_commander.drop('condition_b',axis=1, inplace=True)
##

####

#####clean variable 'image'
info_commander=cleanimage(info_commander)

info_commander['commander_of_1']=info_commander.apply(lambda x: function0(x.image,x.commander_of_1), axis=1)
info_commander.drop('image',axis=1, inplace=True)
info_commander['linked']=info_commander['linked'].str.replace(r'\|.*','')
####
####clean variable variable 'commander_of_1'
info_commander=cleancommanderof(info_commander,'commander_of_1')
####
####clean variable variable 'commander_of_2'
info_commander=cleancommanderof(info_commander,'commander_of_2')
####
commander3_infobox=info_commander.loc[:,['wid','title','linked','commander_of_1','commander_of_2','condition_1']]
commander3_infobox.rename(columns={'linked':'commander'},inplace=True)
commander3_infobox['commander_of_3']=0
commander3_infobox['commander_of_4']=0
commander3_infobox['condition_2']=0
commander3_infobox['condition_3']=0
commander3_infobox['side']=3
commander3_infobox['commander_of_1']=commander3_infobox['commander_of_1'].replace('red flag.svg',0)
commander3_infobox=commander3_infobox.loc[:,['wid','title','commander','commander_of_1','commander_of_2','commander_of_3','commander_of_4','condition_1','condition_2','condition_3','side']]
commander3_infobox=commander3_infobox.replace('',np.nan)
commander3_infobox=commander3_infobox.replace(0,np.nan)
commander3_infobox['commander']=commander3_infobox['commander'].str.strip()
commander3_infobox['commander_of_1']=commander3_infobox['commander_of_1'].str.strip()
commander3_infobox['commander_of_2']=commander3_infobox['commander_of_2'].str.strip()
commander3_infobox['condition_1']=commander3_infobox['condition_1'].str.strip()

##############################################################
########################commander4 ###########################
info_commander=writer.loc[:,['wid','title','tmp']]
info_commander['commander']=info_commander['tmp'].str.extract(r'commander4(.*)',expand=False)
direct2(info_commander,'commander')

info_commander=matchcommandercountry(info_commander)

info_commander.drop_duplicates(inplace=True)
info_commander=info_commander.reset_index(drop=True)
info_commander=info_commander.fillna(0)
missing=info_commander[info_commander['commander']==0]
info_commander=info_commander[info_commander['commander']!=0]
####catch the kia information from the square brackets
commandercondition_bracket(info_commander)
####
info_commander=commandercleaning(info_commander)
####whether there are more than 2 square brackets in a cell
info_commander['brackets']=info_commander['commander'].str.extract(r'\[\[(.*?)\]\].*?\[\[',expand=False)
info_commander=info_commander.fillna(0)
info_commander_nob=info_commander[info_commander['brackets']==0]
brackets=info_commander[info_commander['brackets']!=0]
brackets=brackets.fillna(0)
try:    
    brackets['commander']=brackets['commander'].str.replace(r'\]\] *\{\{flag',']]<br>{{flag')
    brackets['commander']=brackets['commander'].str.replace(r'\]\] *and *\[\[',']]<br>[[')
    brackets['commander']=brackets['commander'].str.replace(r'\{\{kia\}\}[^$]','{{kia}}<br>')
    brackets['commander']=brackets['commander'].str.replace(r'\{\{wia\}\}[^$]','{{wia}}<br>')
    brackets['commander']=brackets['commander'].str.replace(r'\{\{surrendered\}\}[^$]','{{surrendered}}<br>')
    brackets['commander']=brackets['commander'].str.replace(r'\]\]\}\} *\{\{flagdeco\|',']]}}<br>{{flagdeco|')
    brackets=matchcommandercountry(brackets)
except (AttributeError):
    pass
brackets['brackets2']=brackets['commander'].str.extract(r'\[\[(.*?)\]\].*?\[\[',expand=False)
brackets=brackets.fillna(0)
info_commander_nob2=brackets[brackets['brackets2']==0]
brackets2=brackets[brackets['brackets2']!=0]
brackets2=brackets2.fillna(0)
####
####the items in the dataset"brackets" are special cases which we cannot treat them in a normal way, so just catch the commanders(which are in the square brackets) from this dataset without getting their nationalities
brackets_infobox = pd.DataFrame(columns=('wid','title','tmp','commander','condition_b','image'))
brackets_infobox=releasebracket(brackets2,brackets_infobox)

info_commander_nob.drop('brackets',axis=1, inplace=True)
info_commander_nob2.drop(['brackets','brackets2'],axis=1, inplace=True)
info_commander_nob2=info_commander_nob2.loc[:,['wid','title','tmp','commander','condition_b','image']]
info_commander_nob=info_commander_nob.append(info_commander_nob2,ignore_index=True)
info_commander=info_commander_nob.append(brackets_infobox,ignore_index=True)
info_commander=info_commander.reset_index(drop=True)


####
####split the commanders and the countries(forces) they served and then match them
info_commander=splictcommandercountry(info_commander)

info_commander.rename(columns={0:'commander_of_1'},inplace=True)
info_commander['commander_of_1']=info_commander['commander_of_1'].str.replace(r'\{\{','')
info_commander['commander_of_1']=info_commander['commander_of_1'].str.replace(r'\}\}','')
info_commander=info_commander.fillna(0)

####
####split the comanders and the kia information and then match them
info_commander=splictcommanderkia(info_commander)

info_commander.rename(columns={0:'condition_1'},inplace=True)
info_commander['condition_1']=info_commander['condition_1'].str.replace(r'\{\{','')
info_commander['condition_1']=info_commander['condition_1'].str.replace(r'\}\}','')
info_commander=info_commander.fillna(0)

info_commander=cleancondition(info_commander, 'condition_1')
info_commander=info_commander.replace('',np.nan)
info_commander=info_commander.fillna(0)
##combine condition_b and condition_123
info_commander['condition_1']=info_commander.apply(lambda x: function0(x.condition_1,x.condition_b), axis=1)
info_commander.drop('condition_b',axis=1, inplace=True)
##

####
#####clean variable 'image'
try:
    info_commander=cleanimage(info_commander)
except (AttributeError):
    pass

image=info_commander[info_commander['image']!=0]

info_commander['commander_of_1']=info_commander.apply(lambda x: function0(x.image,x.commander_of_1), axis=1)
info_commander.drop('image',axis=1, inplace=True)
info_commander['linked']=info_commander['linked'].str.replace(r'\|.*','')
####
####clean variable variable 'commander_of_1'
info_commander=cleancommanderof(info_commander,'commander_of_1')
####
########


info_commander['commander_of_1']=info_commander['commander_of_1'].replace(r'black flag.svg',0)
info_commander['commander_of_1']=info_commander['commander_of_1'].replace(r'shababflag.svg',0)
commander4_infobox=info_commander.loc[:,['wid','title','linked','commander_of_1','condition_1']]
commander4_infobox.rename(columns={'linked':'commander'},inplace=True)
commander4_infobox['commander_of_2']=0
commander4_infobox['commander_of_3']=0
commander4_infobox['commander_of_4']=0
commander4_infobox['condition_2']=0
commander4_infobox['condition_3']=0
commander4_infobox['side']=4
commander4_infobox=commander4_infobox.loc[:,['wid','title','commander','commander_of_1','commander_of_2','commander_of_3','commander_of_4','condition_1','condition_2','condition_3','side']]
commander4_infobox['commander']=commander4_infobox['commander'].str.strip()
commander4_infobox['commander_of_1']=commander4_infobox['commander_of_1'].str.strip()
commander4_infobox['condition_1']=commander4_infobox['condition_1'].str.strip()
commander4_infobox=commander4_infobox.replace('',np.nan)
commander4_infobox=commander4_infobox.replace(0,np.nan)


##############################################################
########################commander5 ###########################
info_commander=writer.loc[:,['wid','title','tmp']]
info_commander['commander']=info_commander['tmp'].str.extract(r'commander5(.*)',expand=False)
direct2(info_commander,'commander')

info_commander=matchcommandercountry(info_commander)

info_commander.drop_duplicates(inplace=True)
info_commander=info_commander.reset_index(drop=True)
info_commander=info_commander.fillna(0)
info_commander=info_commander[info_commander['commander']!=0]
####
info_commander['linked']=info_commander['commander'].str.extract(r'\[\[(.*?)\]\]',expand=False)
info_commander=info_commander.fillna(0)
info_commander=info_commander[info_commander['linked']!=0]
info_commander['linked']=info_commander['linked'].str.replace(r'\|.*','')



commander5_infobox=info_commander.loc[:,['wid','title','linked']]
commander5_infobox.rename(columns={'linked':'commander'},inplace=True)
commander5_infobox['commander_of_1']=0
commander5_infobox['commander_of_2']=0
commander5_infobox['commander_of_3']=0
commander5_infobox['commander_of_4']=0
commander5_infobox['condition_1']=0
commander5_infobox['condition_2']=0
commander5_infobox['condition_3']=0
commander5_infobox['side']='5'
commander5_infobox['commander']=commander5_infobox['commander'].str.strip()
commander5_infobox=commander5_infobox.replace(0,np.nan)

##############################################################
########################aggregate all the commanders into one dataset ###########################
war_commander_infobox=commander1_infobox.append(commander2_infobox,ignore_index=True)
war_commander_infobox=war_commander_infobox.append(commander3_infobox,ignore_index=True)
war_commander_infobox=war_commander_infobox.append(commander4_infobox,ignore_index=True)
war_commander_infobox=war_commander_infobox.append(commander5_infobox,ignore_index=True)

####add the battles that have no commander information at all
comlistwid=war_commander_infobox.loc[:,'wid']
comlistwid.drop_duplicates(inplace=True)
comlistwid=list(comlistwid)
missing=writer[~writer['wid'].isin(comlistwid)]
missing=missing.loc[:,['wid','title']]
missing['commander']=0
missing['commander_of_1']=0
missing['commander_of_2']=0
missing['commander_of_3']=0
missing['commander_of_4']=0
missing['condition_1']=0
missing['condition_2']=0
missing['condition_3']=0
missing['side']=0
missing=missing.replace(0,np.nan)
war_commander_infobox=war_commander_infobox.append(missing,ignore_index=True)
####


war_commander_infobox=war_commander_infobox.sort_index(by = ["wid",'side'])

war_commander_infobox['x1']=war_commander_infobox['commander'].str.extract(r'(war$)',expand=False)
war_commander_infobox=war_commander_infobox.fillna(0)          
war_commander_infobox=war_commander_infobox.loc[war_commander_infobox['x1']==0]
war_commander_infobox.drop('x1',axis=1, inplace=True)

war_commander_infobox['commander_of_1']=war_commander_infobox['commander_of_1'].replace(r'black flag.svg',0)
war_commander_infobox['commander_of_1']=war_commander_infobox['commander_of_1'].replace(r'black_flag.svg',0)
war_commander_infobox['commander_of_1']=war_commander_infobox['commander_of_1'].replace(r'socialist red flag.svg',0)
war_commander_infobox['commander_of_1']=war_commander_infobox['commander_of_1'].replace(r'red flag.svg',0)
war_commander_infobox['commander_of_1']=war_commander_infobox['commander_of_1'].replace(r'm-26-7.svg','m-26-7')
war_commander_infobox['commander_of_1']=war_commander_infobox['commander_of_1'].replace(r'infoboxhez.png','hezbollah')
war_commander_infobox['commander_of_1']=war_commander_infobox['commander_of_1'].replace(r'beylik of aydin flag.png','beylik of aydin')


####whether kia
war_commander_infobox['kia_1']=war_commander_infobox['condition_1'].str.extract(r'(kia)',expand=False).replace('kia',1)
war_commander_infobox['kia_2']=war_commander_infobox['condition_2'].str.extract(r'(kia)',expand=False).replace('kia',1)
war_commander_infobox['kia_3']=war_commander_infobox['condition_3'].str.extract(r'(kia)',expand=False).replace('kia',1)
war_commander_infobox=war_commander_infobox.fillna(0)
war_commander_infobox['kia']=war_commander_infobox['kia_1']+war_commander_infobox['kia_2']+war_commander_infobox['kia_3']
war_commander_infobox.loc[war_commander_infobox.kia>=1,'kia']=1

war_commander_infobox['kia_1']=war_commander_infobox['condition_1'].str.extract(r'(killed in action)',expand=False).replace('killed in action',1)
war_commander_infobox['kia_2']=war_commander_infobox['condition_2'].str.extract(r'(killed in action)',expand=False).replace('killed in action',1)
war_commander_infobox['kia_3']=war_commander_infobox['condition_3'].str.extract(r'(killed in action)',expand=False).replace('killed in action',1)
war_commander_infobox=war_commander_infobox.fillna(0)
war_commander_infobox['killed']=war_commander_infobox['kia_1']+war_commander_infobox['kia_2']+war_commander_infobox['kia_3']
war_commander_infobox.loc[war_commander_infobox.killed>=1,'killed']=1

war_commander_infobox['kia_1']=war_commander_infobox['condition_1'].str.extract(r'(†)',expand=False).replace('†',1)
war_commander_infobox['kia_2']=war_commander_infobox['condition_2'].str.extract(r'(†)',expand=False).replace('†',1)
war_commander_infobox['kia_3']=war_commander_infobox['condition_3'].str.extract(r'(†)',expand=False).replace('†',1)
war_commander_infobox=war_commander_infobox.fillna(0)
war_commander_infobox['cross']=war_commander_infobox['kia_1']+war_commander_infobox['kia_2']+war_commander_infobox['kia_3']
war_commander_infobox.loc[war_commander_infobox.cross>=1,'cross']=1

war_commander_infobox['kia_1']=war_commander_infobox['condition_1'].str.extract(r'(assassinat)',expand=False).replace('assassinat',1)
war_commander_infobox['kia_2']=war_commander_infobox['condition_2'].str.extract(r'(assassinat)',expand=False).replace('assassinat',1)
war_commander_infobox['kia_3']=war_commander_infobox['condition_3'].str.extract(r'(assassinat)',expand=False).replace('assassinat',1)
war_commander_infobox=war_commander_infobox.fillna(0)
war_commander_infobox['assassination']=war_commander_infobox['kia_1']+war_commander_infobox['kia_2']+war_commander_infobox['kia_3']
war_commander_infobox.loc[war_commander_infobox.assassination>=1,'assassination']=1

war_commander_infobox['dead_in_battle']=war_commander_infobox['kia']+war_commander_infobox['killed']+war_commander_infobox['cross']+war_commander_infobox['assassination']
war_commander_infobox.loc[war_commander_infobox.dead_in_battle>=1,'dead_in_battle']=1
war_commander_infobox.loc[war_commander_infobox.dead_in_battle==0,'dead_in_battle']=-1
war_commander_infobox.drop(['kia_1','kia_2','kia_3','kia','killed','cross','assassination'],axis=1, inplace=True)
####
war_commander_infobox.loc[war_commander_infobox.commander==0, 'side']=0
war_commander_infobox.loc[war_commander_infobox.commander==0, 'dead_in_battle']=0

war_commander_infobox=war_commander_infobox[war_commander_infobox['commander']!='about 10105200-10170000 warriors and civilians killed during the war .additional 1000000 clavery 10000 elephants and 5000 horses killed during the war.']
war_commander_infobox=war_commander_infobox[war_commander_infobox['commander']!='commander']




war_commander_infobox=war_commander_infobox.replace('0',np.nan)
war_commander_infobox=war_commander_infobox.replace(0,np.nan)
war_commander_infobox['commander']=war_commander_infobox['commander'].str.strip()
war_commander_infobox['commander_of_1']=war_commander_infobox['commander_of_1'].str.strip()
war_commander_infobox['commander_of_2']=war_commander_infobox['commander_of_2'].str.strip()
war_commander_infobox['commander_of_3']=war_commander_infobox['commander_of_3'].str.strip()
war_commander_infobox['commander_of_4']=war_commander_infobox['commander_of_4'].str.strip()
war_commander_infobox['condition_1']=war_commander_infobox['condition_1'].str.strip()
war_commander_infobox['condition_2']=war_commander_infobox['condition_2'].str.strip()
war_commander_infobox['condition_3']=war_commander_infobox['condition_3'].str.strip()

war_commander_infobox=war_commander_infobox.loc[:,['wid','title','commander','commander_of_1','commander_of_2','commander_of_3','commander_of_4','side','dead_in_battle','condition_1','condition_2','condition_3']]


war_commander_infobox.to_csv(commanders_path,index=False)
####################################Match the wids and commanders to  qids ####################################     

war_commander_infobox=pd.read_csv(commanders_path)
####wid to qid
war_commander_infobox=pd.merge(war_commander_infobox,wid_to_qid,on='wid',how='left')
war_commander_infobox.rename(columns={'qid': 'war_qid'}, inplace=True)
####
####commanders' names to qid
qid_subject.rename(columns={'subject': 'commander'}, inplace=True)

qid_subject['commander']=qid_subject['commander'].str.lower()# convert each value of 'subject' to lowercase  
qid_subject['commander']=qid_subject['commander'].str.replace(r'_',' ') # replace "_" to " "
qid_subject['commander']=qid_subject['commander'].str.replace(r'–','-') # replace "–" to "-"
war_commander_infobox=pd.merge(war_commander_infobox,qid_subject,on='commander',how='left')

person_infobox=pd.read_csv("D:\learning\Arash\info_person\input\infobox_person.csv")
person_infobox=person_infobox.loc[:,['wid','title']]
person_infobox['title']=person_infobox['title'].str.lower()
person_infobox['title']=person_infobox['title'].str.strip()
person_infobox.rename(columns={'title': 'commander'}, inplace=True)
person_infobox.rename(columns={'wid': 'commander_wid'}, inplace=True)

war_commander_infobox=pd.merge(war_commander_infobox,person_infobox,on='commander',how='left')
wid_to_qid.rename(columns={'qid': 'com_qid'}, inplace=True)
wid_to_qid.rename(columns={'wid': 'commander_wid'}, inplace=True)
war_commander_infobox=pd.merge(war_commander_infobox,wid_to_qid,on='commander_wid',how='left')

war_commander_infobox=war_commander_infobox.fillna(0)

war_commander_infobox['commander_qid']=war_commander_infobox.apply(lambda x: function0(x.qid,x.com_qid), axis=1)



#war_commander_infobox['dot']=war_commander_infobox['commander_of_3'].str.extract(r'(\.)',expand=False)
#war_commander_infobox['dot']=war_commander_infobox['dot'].fillna(0)
#dot=war_commander_infobox[war_commander_infobox['dot']!=0]

qidAltSubject_en=pd.read_stata("D:/learning/Arash/war_participants/Fabian_06292019_WAR/Data/A1_00_qidAltSubject_en.dta")
qidAltSubject_en['altSubject']=qidAltSubject_en['altSubject'].str.encode('latin-1').str.decode('utf-8')####Atention: This step is needed or there will be a lot of items unmatched
qidAltSubject_en['altSubject']=qidAltSubject_en['altSubject'].str.lower()
qidAltSubject_en['altSubject']=qidAltSubject_en['altSubject'].str.replace(r'_',' ')
qidAltSubject_en.drop_duplicates(inplace=True)
qidAltSubject_en.rename(columns={'altSubject': 'commander'}, inplace=True)



war_commander_infobox=pd.merge(war_commander_infobox,qidAltSubject_en,on='commander',how='left')
war_commander_infobox.drop_duplicates(inplace=True)

#collect those commanders that are matched to two different qids
duplicates=war_commander_infobox.loc[:,['commander','qid']]
duplicates.drop_duplicates(inplace=True)
duplicates_2=duplicates.loc[:,['commander']]
group=duplicates['qid'].groupby(duplicates['commander'])
g=group.count()
g=g.reset_index()# change the index to column
g.rename(columns={'country': 'has_country'}, inplace=True)
gg=g[g['qid']==2]
gg=gg['commander']
gg=list(gg)

name_with_2qids=war_commander_infobox[war_commander_infobox['commander'].isin(gg)]
name_with_2qids.to_csv(main_path+"tem/name_with_2qids.csv",index=False)
#
war_commander_infobox=war_commander_infobox.fillna(0)
war_commander_infobox['commander_qid']=war_commander_infobox.apply(lambda x: function0(x.commander_qid,x.qid), axis=1)
##




##collect those commanders that are not matched
commander_noqid=war_commander_infobox[war_commander_infobox['commander_qid']==0]
commander_noqid=commander_noqid.loc[:,['commander']]
commander_noqid.drop_duplicates(inplace=True)
commander_noqid=commander_noqid.fillna(0)
commander_noqid=commander_noqid[commander_noqid['commander']!=0]
commander_noqid.to_csv(main_path+"tem/commanderlist_noqid.csv",index=False)
##
war_commander_infobox=war_commander_infobox.loc[:,['wid','war_qid','commander','commander_qid','commander_of_1','commander_of_2','commander_of_3','commander_of_4','side','dead_in_battle','condition_1','condition_2','condition_3']]
war_commander_infobox=war_commander_infobox.replace('0',np.nan)
war_commander_infobox=war_commander_infobox.replace(0,np.nan)
war_commander_infobox.to_csv(commanders_path,index=False)
####
#######################################################################################
"""
###################time information##########################
"""
writer_time=pd.read_csv(main_path+'input/infobox_new.csv') 


#######drop those that are not wars at all
writer_time=writer_time[writer_time['wid']!=288520]
writer_time=writer_time[writer_time['wid']!=160665]
writer_time=writer_time[writer_time['wid']!=160664]
writer_time=writer_time[writer_time['wid']!=560948]
writer_time=writer_time[writer_time['wid']!=2150520]
writer_time=writer_time[writer_time['wid']!=16315254]
writer_time=writer_time[writer_time['wid']!=207630]
writer_time=writer_time[writer_time['wid']!=205658]
writer_time=writer_time[writer_time['wid']!=338949]
writer_time=writer_time[writer_time['wid']!=896446]
writer_time=writer_time[writer_time['wid']!=10343280]##this one is quicky. The page of it was delete in January 2019 and the log shows that the arguers believe this one is only a legendary war without 
                                      ## any warrant. So I delete it from the dataset
writer_time=writer_time[writer_time['wid']!=44131689]
writer_time=writer_time[writer_time['wid']!=17677848]
writer_time=writer_time[writer_time['wid']!=4902286] 
info_time=writer_time.loc[:,['wid','title','tmp']]
info_time['time']=info_time['tmp'].str.extract(r'(.*)',expand=False)

####Clean
info_time['time']=info_time['time'].str.replace(r'\{\{Pufc\|1=\|date=.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{pufc\|1=\|date=.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{Dubious\|date=.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{dubious\|date=.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{page needed\|date=.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{deadlink *\|date=.*?\}\}','')

info_time['time']=info_time['time'].str.replace(r'<ref[^/]*?>.*?<\/ref>','')
info_time['time']=info_time['time'].str.replace(r'<ref.*?>','')
info_time['time']=info_time['time'].str.replace(r'<sup>.*?<\/sup>','')
info_time['time']=info_time['time'].str.replace(r'<small[^/]*?>.*?<\/small>','')
info_time['time']=info_time['time'].str.replace(r'\{\{cite web\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{cn\|date.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{small *\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{sfnp\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{\#tag:ref\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{citation needed\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{efn *\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{sfn *\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{refn *\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\[https\:\/\/.*?\]','')
info_time['time']=info_time['time'].str.replace(r'\(.*?\)','')
info_time['time']=info_time['time'].str.replace(r'\{\{in\b\}\}','')
info_time['time']=info_time['time'].str.replace(r'\<\!\-\-.*?\-\-\>','')
info_time['time']=info_time['time'].str.replace(r'\{\{cref2\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{nowrap\|\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{resize\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'117 years ago','')
info_time['time']=info_time['time'].str.replace(r'\{\{vague\|date\=september 2016\}\}','')
info_time['time']=info_time['time'].str.replace(r'09\:02–09\:40','')
info_time['time']=info_time['time'].str.replace(r'\{\{rp\|136\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{rp\|10\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{rp\|178\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{rp\|267\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{rp\|244-5\}\}','')
info_time['time']=info_time['time'].str.replace(r'\{\{rp\|.*?\}\}','')
info_time['time']=info_time['time'].str.replace(r'anti-communist resistance continued until mid-1950s','')
info_time['time']=info_time['time'].str.replace(r'\'\'\'main incidents\:\'\'\' 1948, 1958–59\, 1963–69\, 1973–77\, 2004–2012\, 2012–present','')
info_time['time']=info_time['time'].str.replace(r'\{\{collapsible list\| bullets = yes\| title = peace treaties\}\}','')
info_time['time']=info_time['time'].str.replace(r'\'\'\'ratification effective\:\'\'\' may 12, 1784','')
info_time['time']=info_time['time'].str.replace(r'\[\[nagorno-karabakh war\|main phase\]\]\: 1988–94','')
info_time['time']=info_time['time'].str.replace(r'low level resurgence since june 22\, 2002','')
info_time['time']=info_time['time'].str.replace(r'\{\{age in months\, weeks and days\|month1=12\|day1=14\|year1=2008\|month2=03\|day2=15\|year2=2009\}\}','')
info_time['time']=info_time['time'].str.replace(r'military events\:<br\/> 15 april – 1 august 1919','')
info_time['time']=info_time['time'].str.replace(r'0500--1800','')
info_time['time']=info_time['time'].str.replace(r'though more likely after 1513','')
info_time['time']=info_time['time'].str.replace(r'\{\{years or months ago\|1811\}\}','')
info_time['time']=info_time['time'].str.replace(r'\&nbsp\;','')
info_time['time']=info_time['time'].str.replace(r'b\.c\.','bc')
info_time['time']=info_time['time'].str.replace(r' or \d+','')
info_time['time']=info_time['time'].str.replace(r' or \d+','')
info_time['time']=info_time['time'].str.replace(r'\[\[treaty of peace and friendship \|signed october 20, 1904\]\]','')
info_time['time']=info_time['time'].str.replace(r'in the 8th year of \[\[ashoka\]\]\'s coronation of 269 bce\.','')
info_time['time']=info_time['time'].str.replace(r'\/possibly 629','')
info_time['time']=info_time['time'].str.replace(r'\, possibly 1486','')
info_time['time']=info_time['time'].str.replace(r'\{\{Deletable image-caption\|.*?\}\}','')


####
#######positioning
info_time['time']=info_time['time'].str.extract(r'\| *date *=(.*)',expand=False)
direct2(info_time,'time')
######
######Modify part of the data so that time information can be obtained using unified steps
info_time.loc[info_time.wid==1168834,'time']='January – February 627'
info_time.loc[info_time.wid==1793994,'time']='5 – 25 june 1857'
info_time.loc[info_time.wid==2263093,'time']='unknown'
info_time.loc[info_time.wid==4764461,'time']='| date        = {{start and end dates|1914|7|28|1918|11|11|df=yes}}'
info_time.loc[info_time.wid==5152115,'time']='1200-1300'
info_time.loc[info_time.wid==6609643,'time']='|date=50 &ndash; 562 ce'
info_time.loc[info_time.wid==21296534,'time']='20 May 1795'
info_time.loc[info_time.wid==26758615,'time']='1974'
info_time.loc[info_time.wid==26758977,'time']='1966'
info_time.loc[info_time.wid==26759074,'time']='1965'
info_time.loc[info_time.wid==26760640,'time']='1961'
info_time.loc[info_time.wid==26760892,'time']='1963'
info_time.loc[info_time.wid==26762302,'time']='1964'
info_time.loc[info_time.wid==26769868,'time']='1962'
info_time.loc[info_time.wid==26773094,'time']='1960'
info_time.loc[info_time.wid==26773392,'time']='1959'
info_time.loc[info_time.wid==26775366,'time']='1975'
info_time.loc[info_time.wid==27213409,'time']='1955'
info_time.loc[info_time.wid==27215416,'time']='1956'
info_time.loc[info_time.wid==27216529,'time']='1957'
info_time.loc[info_time.wid==27231044,'time']='1958'
info_time.loc[info_time.wid==27418958,'time']=' 15 bc'
info_time.loc[info_time.wid==26041974,'time']=' | date  = july 27 –  august 2 , 1948'
info_time.loc[info_time.wid==41961583,'time']='1795'
info_time.loc[info_time.wid==43918430,'time']='7 october 1916'
info_time.loc[info_time.wid==3129531,'time']='|date= february, 274'
info_time.loc[info_time.wid==10675891,'time']='|date = 546 bc'
info_time.loc[info_time.wid==36645711,'time']='|date=811–813/819 ce'
info_time.loc[info_time.wid==3070332,'time']='|date= 1986–  present '
info_time.loc[info_time.wid==57276762,'time']='1916'
info_time.loc[info_time.wid==11610470,'time']='|date        = {{start-date|display=26 may 1798}}   '
info_time.loc[info_time.wid==50770062,'time']='1834'
info_time.loc[info_time.wid==58420551,'time']='1107–1108'
info_time.loc[info_time.wid==58114566,'time']='1852'
info_time.loc[info_time.wid==55506469,'time']='1553'
info_time.loc[info_time.wid==51057151,'time']='1784–1788'
info_time.loc[info_time.wid==50597919,'time']='567'
info_time.loc[info_time.wid==49103542,'time']='1892'
info_time.loc[info_time.wid==46918148,'time']='1815'
info_time.loc[info_time.wid==39084732,'time']='1990–1998'
info_time.loc[info_time.wid==36562765,'time']='824'
info_time.loc[info_time.wid==35901554,'time']='1862'
info_time.loc[info_time.wid==32786931,'time']='2011'
info_time.loc[info_time.wid==31641517,'time']='1260'
info_time.loc[info_time.wid==31442596,'time']='1609'
info_time.loc[info_time.wid==29372071,'time']='June 1862'
info_time.loc[info_time.wid==26745290,'time']='1973'
info_time.loc[info_time.wid==26744981,'time']='1972'
info_time.loc[info_time.wid==26728703,'time']='1971'
info_time.loc[info_time.wid==26715036,'time']='1970'
info_time.loc[info_time.wid==26584768,'time']='1969'
info_time.loc[info_time.wid==26560819,'time']='1967'
info_time.loc[info_time.wid==26467539,'time']='1968'
info_time.loc[info_time.wid==19491752,'time']='2008'
info_time.loc[info_time.wid==16287447,'time']='2008'
info_time.loc[info_time.wid==15177482,'time']='2008'
info_time.loc[info_time.wid==7469104,'time']='1622'
info_time.loc[info_time.wid==6536580,'time']='1831–1833'
info_time.loc[info_time.wid==1090966,'time']='1585–1604'
info_time.loc[info_time.wid==8427030,'time']='1386'
info_time.loc[info_time.wid==51306,'time']='268'
info_time.loc[info_time.wid==545708,'time']='october 552'
info_time.loc[info_time.wid==3070865,'time']='december 629'
info_time.loc[info_time.wid==3168245,'time']='late 316'
info_time.loc[info_time.wid==5353292,'time']='december 730'
info_time.loc[info_time.wid==12536326,'time']='december 25, 1656'
info_time.loc[info_time.wid==30166560,'time']='late december 1767'
info_time.loc[info_time.wid==56082093,'time']='june 1990- ongoing'
info_time.loc[info_time.wid==1425816,'time']='1641-1649 '
info_time.loc[info_time.wid==9882635,'time']='428 bc - 427 bc'
info_time.loc[info_time.wid==12369276,'time']='1620–1621 '
info_time.loc[info_time.wid==12369423,'time']='1672–1676 '
info_time.loc[info_time.wid==37547678,'time']='1511–1529 '
info_time.loc[info_time.wid==41411193,'time']='384 bc 383 bc'
info_time.loc[info_time.wid==46443654,'time']='157 bc – 63 bc'
info_time.loc[info_time.wid==358109,'time']='1655–1660'
info_time.loc[info_time.wid==562348,'time']='1814–1816'
info_time.loc[info_time.wid==859729,'time']='1521–1523'
info_time.loc[info_time.wid==932578,'time']='1676-1681'
info_time.loc[info_time.wid==1082829,'time']='1474–1477'
info_time.loc[info_time.wid==1551938,'time']='1644–1651'
info_time.loc[info_time.wid==1552734,'time']='1874–1875'
info_time.loc[info_time.wid==1644215,'time']='1862-1877'
info_time.loc[info_time.wid==1718324,'time']='1888–1889'
info_time.loc[info_time.wid==1762785,'time']='1878–1888'
info_time.loc[info_time.wid==2644151,'time']='1494–1498'
info_time.loc[info_time.wid==3016271,'time']='1521–1526'
info_time.loc[info_time.wid==3021179,'time']='1551–1559'
info_time.loc[info_time.wid==3148079,'time']='1817–1818'
info_time.loc[info_time.wid==3190523,'time']='615 616'
info_time.loc[info_time.wid==3385619,'time']='1550-1551'
info_time.loc[info_time.wid==3719774,'time']='1542–1546'
info_time.loc[info_time.wid==4065344,'time']='1536–1538'
info_time.loc[info_time.wid==4998125,'time']='1627-1675'
info_time.loc[info_time.wid==6271027,'time']='1436–1449'
info_time.loc[info_time.wid==7612766,'time']='1586–1587'
info_time.loc[info_time.wid==7629610,'time']='1552–1555'
info_time.loc[info_time.wid==7629681,'time']='1449–1450'
info_time.loc[info_time.wid==8057579,'time']='1615-1618'
info_time.loc[info_time.wid==8949264,'time']='1503–1505'
info_time.loc[info_time.wid==10019205,'time']='1657–1658'
info_time.loc[info_time.wid==14071182,'time']='|date=1258, 1285 and 1287–1288'
info_time.loc[info_time.wid==14076996,'time']='1469–1474'
info_time.loc[info_time.wid==15055916,'time']='1727–1729'
info_time.loc[info_time.wid==16069987,'time']='1939; 1944–1948'
info_time.loc[info_time.wid==16574218,'time']='1911-1943'
info_time.loc[info_time.wid==19312285,'time']='1883–1886'
info_time.loc[info_time.wid==20218335,'time']='1942–1945'
info_time.loc[info_time.wid==22555547,'time']='1235–1279'
info_time.loc[info_time.wid==23574984,'time']='1635–1636'
info_time.loc[info_time.wid==25280621,'time']='1449-1453'
info_time.loc[info_time.wid==27262729,'time']='1821–1829'
info_time.loc[info_time.wid==28134949,'time']='1950–1958'
info_time.loc[info_time.wid==28754698,'time']='1002–1018'
info_time.loc[info_time.wid==28762449,'time']='1139 1140'
info_time.loc[info_time.wid==28895462,'time']='1807-1808'
info_time.loc[info_time.wid==29544077,'time']='1470–1471'
info_time.loc[info_time.wid==30160824,'time']='977 978'
info_time.loc[info_time.wid==31165684,'time']='1845–1850'
info_time.loc[info_time.wid==31421973,'time']='1672-1673'
info_time.loc[info_time.wid==32056003,'time']='1835–1858'
info_time.loc[info_time.wid==34731349,'time']='1331-1333'
info_time.loc[info_time.wid==35537896,'time']='1757-1758'
info_time.loc[info_time.wid==37678384,'time']='1658-1667'
info_time.loc[info_time.wid==40267239,'time']='1940–1942'
info_time.loc[info_time.wid==40505725,'time']='1679–1684'
info_time.loc[info_time.wid==41031412,'time']='1953-1956'
info_time.loc[info_time.wid==41217313,'time']='1122–1124'
info_time.loc[info_time.wid==42044219,'time']='1830–1850'
info_time.loc[info_time.wid==42557169,'time']='1403–1404'
info_time.loc[info_time.wid==42559756,'time']='1285–1286'
info_time.loc[info_time.wid==44940436,'time']='643 644'
info_time.loc[info_time.wid==45189568,'time']='1501–1512'
info_time.loc[info_time.wid==47161375,'time']='1368–1372'
info_time.loc[info_time.wid==47549546,'time']='1862-1877'
info_time.loc[info_time.wid==52790295,'time']='1820–1824'
info_time.loc[info_time.wid==52908598,'time']='1449–1454'
info_time.loc[info_time.wid==12833892,'time']='229 bc /228 bc –222 bc'
info_time.loc[info_time.wid==13534547,'time']='11th century bc'
info_time.loc[info_time.wid==18513015,'time']='1386'
info_time.loc[info_time.wid==20953140,'time']='january 16, 1865'
info_time.loc[info_time.wid==31562353,'time']='\|date=may 629 ad'
info_time.loc[info_time.wid==36787426,'time']='535 bc /518 bc –323 bce'
info_time.loc[info_time.wid==44994160,'time']='344 bc –343 bc /342 bc'
info_time.loc[info_time.wid==45299658,'time']='911 bc -870 bc'
info_time.loc[info_time.wid==50474473,'time']='march 20th, 1973'
info_time.loc[info_time.wid==55352287,'time']='\|date=221 bc –220 bc /219 bc'
info_time.loc[info_time.wid==23574669,'time']='\| date        = august 8, 1850 &mdash; august 15, 1850'
info_time.loc[info_time.wid==43986502,'time']='13 june 2014 – present'
info_time.loc[info_time.wid==867792,'time']='18 july 390 bc'
#########
info_time['year']=info_time['time'].str.findall('(\d{3,4} *(bc)?)')
x=info_time.set_index('wid').year.apply(pd.Series).stack().reset_index(level=0).rename(columns={0:'year'})
x=x.set_index('wid').year.apply(pd.Series).stack().reset_index(level=0).rename(columns={0:'year'})
x=x[x['year']!='']
x=x[x['year']!='bc']


########################deal with the year that is smaller than 100
info_time_merge1=pd.merge(info_time,x,on='wid',how='left')####Have checked whether there is any date in a form like 34-123
info_time_merge1=info_time_merge1.fillna(0)
smaller_100=info_time_merge1[(info_time_merge1['year_y']==0)|(info_time_merge1['wid']==46443654)]



smaller_100['time']=smaller_100['time'].str.replace(r'–','– ')
smaller_100['time']=smaller_100['time']+' '
smaller_100['time']=' '+smaller_100['time']
smaller_100['year2']=smaller_100['time'].str.findall('[= ;-<]([3456789][1234567890][ –&-<](bc)?)')
y=smaller_100.set_index('wid').year2.apply(pd.Series).stack().reset_index(level=0).rename(columns={0:'year2'})
y=y.set_index('wid').year2.apply(pd.Series).stack().reset_index(level=0).rename(columns={0:'year2'})
y=y[y['year2']!='']
y=y[y['year2']!='bc']

info_time_merge2=pd.merge(smaller_100,y,on='wid',how='left')

info_time_merge2.loc[info_time_merge2.wid==39880,'year2_y']='9'
info_time_merge2.loc[info_time_merge2.wid==288520,'year2_y']='19'
info_time_merge2.loc[info_time_merge2.wid==551236,'year2_y']='16'
info_time_merge2.loc[info_time_merge2.wid==2647722,'year2_y']='29 bc'
insertRow = pd.DataFrame([[2647722,'Cantabrian Wars','','|date= 29– 19 bc ','','','','19 bc']],columns = ['wid','title','tmp','time','year_x','year_y','year2_x','year2_y'])
above = info_time_merge2.loc[:61]
below = info_time_merge2.loc[62:]
info_time_merge2 = above.append(insertRow,ignore_index=True).append(below,ignore_index=True)
info_time_merge2.loc[info_time_merge2.wid==3127340,'year2_y']='11 bc'
info_time_merge2.loc[info_time_merge2.wid==3163655,'year2_y']='23'
info_time_merge2.loc[info_time_merge2.wid==6749892,'year2_y']='6'
insertRow = pd.DataFrame([[6749892,'Bellum Batonianum','','|date= ad 6– 9 ','','','','9']],columns = ['wid','title','tmp','time','year_x','year_y','year2_x','year2_y'])
above = info_time_merge2.loc[:117]
below = info_time_merge2.loc[118:]
info_time_merge2 = above.append(insertRow,ignore_index=True).append(below,ignore_index=True)
info_time_merge2.loc[info_time_merge2.wid==24599541,'year2_y']='16 bc'
info_time_merge2.loc[info_time_merge2.wid==27418958,'year2_y']='15 bc'
info_time_merge2.loc[info_time_merge2.wid==35561081,'year2_y']='28'
info_time_merge2.loc[info_time_merge2.wid==36184449,'year2_y']='25 bc'
info_time_merge2.loc[info_time_merge2.wid==36186223,'year2_y']='25 bc'
info_time_merge2.loc[info_time_merge2.wid==36368076,'year2_y']='63 bc'
info_time_merge2.loc[info_time_merge2.wid==51420036,'year2_y']='40'
insertRow = pd.DataFrame([[51420036,'Roman–Bosporan War','','|date=circa. 40-49 ad ','','','','49']],columns = ['wid','title','tmp','time','year_x','year_y','year2_x','year2_y'])
above = info_time_merge2.loc[:317]
below = info_time_merge2.loc[318:]
info_time_merge2 = above.append(insertRow,ignore_index=True).append(below,ignore_index=True)
info_time_merge2.loc[info_time_merge2.wid==56374860,'year2_y']='12 bc'
insertRow = pd.DataFrame([[56374860,'Early Imperial campaigns in Germania','','|date=12 bc –  ad 16 ','','','','16']],columns = ['wid','title','tmp','time','year_x','year_y','year2_x','year2_y'])
above = info_time_merge2.loc[:333]
below = info_time_merge2.loc[334:]
info_time_merge2 = above.append(insertRow,ignore_index=True).append(below,ignore_index=True)
info_time_merge2.loc[info_time_merge2.wid==58276855,'year2_y']='15'
info_time_merge2.loc[info_time_merge2.wid==58276973,'year2_y']='16'
info_time_merge2.loc[info_time_merge2.wid==4541066,'year2_y']='36 bc'


info_time_merge2.drop(['year_x','year_y','year2_x','title','tmp','time'],axis=1, inplace=True)
info_time_merge2['year2_y']=info_time_merge2['year2_y'].str.replace(r'–','')
info_time_merge2['year2_y']=info_time_merge2['year2_y'].str.replace(r'&','')
info_time_merge2['year2_y']=info_time_merge2['year2_y'].str.replace(r'-','')
info_time_merge2['year2_y']=info_time_merge2['year2_y'].str.replace(r'<','')


#info_time_merge3: after adding the year information of those that happened between 100bc to 100 ad
info_time_merge3=pd.merge(info_time_merge1,info_time_merge2,on='wid',how='left')
info_time_merge3['year2_y']=info_time_merge3['year2_y'].fillna(0)
info_time_merge3['year']=info_time_merge3.apply(lambda x: function0(x.year_y,x.year2_y), axis=1)
info_time_merge3.drop(['year_x','year_y','year2_y'],axis=1, inplace=True)
#changing xx bc into a nagative int
info_time_merge3['temp']=info_time_merge3['year'].str.extract(r'(bc)',expand=False)
info_time_merge3['year']=info_time_merge3['year'].str.replace(r'bc','')
def function(a,b):
    if b=='bc':
        return '-'+a
    else:
        return a
info_time_merge3['year']=info_time_merge3.apply(lambda x: function(x.year,x.temp), axis=1)
info_time_merge3.drop('temp',axis=1, inplace=True)
info_time_merge3['year'] = pd.to_numeric(info_time_merge3['year'], errors='ignore')



info_time_mark= pd.DataFrame(columns=('wid','title','tmp','time','year','mark'))
l_wid=771
i=0
for row in info_time_merge3.iterrows():
    wid=row[1][0]
    if wid==l_wid:
        i=i+1
    else:
        i=1
    info_time_mark=info_time_mark.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],'tmp':[row[1][2]],'time':[row[1][3]],'year':[row[1][4]],'mark':[i]}),ignore_index=True) 
    l_wid=wid

info_time_mark=info_time_mark.loc[:,['wid','year','mark']]
info_time_merge4=info_time_mark.pivot('wid','mark','year')
info_time_merge4=info_time_merge4.reset_index()
info_time_merge4=pd.merge(info_time,info_time_merge4,on='wid',how='left')
info_time_merge4.drop('year',axis=1, inplace=True)


#######deal with: 123-122 bc
info_time_merge5=info_time_merge4.fillna(9999)
#info_time_merge5['x']=info_time_merge5['time'].str.extract(r'(\}\}\| *date)',expand=False)
#info_time_merge5=info_time_merge5.fillna(9999)
#xxxx=info_time_merge5[info_time_merge5['x']!=9999]

info_time_merge5_1vs2=info_time_merge5[info_time_merge5[1]>info_time_merge5[2]]
info_time_merge5_1vs2[1]=-info_time_merge5_1vs2[1]
info_time_merge5_2vs1=info_time_merge5[info_time_merge5[1]<=info_time_merge5[2]]
info_time_merge5=info_time_merge5_2vs1.append(info_time_merge5_1vs2,ignore_index=True)
info_time_merge5_3vs4=info_time_merge5[info_time_merge5[3]>info_time_merge5[4]]
info_time_merge5_3vs4[3]=-info_time_merge5_3vs4[3]
info_time_merge5_4vs3=info_time_merge5[info_time_merge5[3]<=info_time_merge5[4]]
info_time_merge5=info_time_merge5_4vs3.append(info_time_merge5_3vs4,ignore_index=True)
info_time_merge5_5vs6=info_time_merge5[info_time_merge5[5]>info_time_merge5[6]]
info_time_merge5_5vs6[5]=-info_time_merge5_5vs6[5]
info_time_merge5_6vs5=info_time_merge5[info_time_merge5[5]<=info_time_merge5[6]]
info_time_merge5=info_time_merge5_6vs5.append(info_time_merge5_5vs6,ignore_index=True)

info_time_merge5['start_year']=info_time_merge5[[1,2,3,4,5,6,7,8,9,10,11,12,13,14]].min(axis=1)
info_time_merge5=info_time_merge5.replace(9999,np.nan)
info_time_merge5=info_time_merge5.fillna(-9999)
info_time_merge5['end_year']=info_time_merge5[[1,2,3,4,5,6,7,8,9,10,11,12,13,14]].max(axis=1)
info_time_merge5=info_time_merge5.replace(-9999,np.nan)
info_time_merge5['century']=info_time_merge5['time'].str.extract('(century)',expand=False)
century=info_time_merge5[info_time_merge5['century']=='century']
info_time_merge5=info_time_merge5[info_time_merge5['century']!='century']
century=century.fillna(0)
century=century[century[1]==0]
century['time']=century['time'].str.replace(r'th century','')
century['time']=century['time'].str.replace(r'nd century','')
century['time']=century['time'].str.replace(r'st century','')
century['time']=century['time'].str.replace(r'rd century','')
century=century.loc[:,['wid','title','tmp','time','century']]
century['year']=century['time'].str.findall('(\d+ *(bc)?)')
x=century.set_index('wid').year.apply(pd.Series).stack().reset_index(level=0).rename(columns={0:'year'})
x=x.set_index('wid').year.apply(pd.Series).stack().reset_index(level=0).rename(columns={0:'year'})
x=x[x['year']!='']
x=x[x['year']!='bc']
century=century.loc[:,['wid','title','tmp','time','century']]
century=pd.merge(century,x,on='wid',how='left')
century['temp']=century['year'].str.extract(r'(bc)',expand=False)
century['year']=century['year'].str.replace(r'bc','')
century['year']=century.apply(lambda x: function(x.year,x.temp), axis=1)
century.drop('temp',axis=1, inplace=True)
century['year'] = pd.to_numeric(century['year'], errors='ignore')

info_time_mark= pd.DataFrame(columns=('wid','title','tmp','time','century','year','mark'))
l_wid=century.loc[0,'wid']
i=0
for row in century.iterrows():
    wid=row[1][0]
    if wid==l_wid:
        i=i+1
    else:
        i=1
    info_time_mark=info_time_mark.append(pd.DataFrame({'wid':[row[1][0]],'title':[row[1][1]],'tmp':[row[1][2]],'time':[row[1][3]],'century':[row[1][4]],'year':[row[1][5]],'mark':[i]}),ignore_index=True) 
    l_wid=wid

info_time_mark=info_time_mark.loc[:,['wid','year','mark']]
info_time_mark=info_time_mark.pivot('wid','mark','year')
info_time_mark=info_time_mark.reset_index()
info_time_cen=pd.merge(century,info_time_mark,on='wid',how='left')
info_time_cen.drop('year',axis=1, inplace=True)
info_time_cen.drop_duplicates(inplace=True)
info_time_cen.rename(columns={1: 'start_year',2:'end_year'}, inplace=True) 
def function2(a):
    if a<0:
        return a*100+50
    else:
        return a*100-50
info_time_cen['start_year']=info_time_cen.apply(lambda x: function2(x.start_year), axis=1)
info_time_cen['end_year']=info_time_cen.apply(lambda x: function2(x.end_year), axis=1)
info_time_cen=info_time_cen.fillna(0)
info_time_cen['end_year']=info_time_cen.apply(lambda x: function0(x.end_year,x.start_year), axis=1)
 


info_time_merge5=info_time_merge5.loc[:,['wid','title','tmp','time','century','start_year','end_year']]
info_time_merge5=info_time_merge5.fillna(0)
info_time_merge5['century']=info_time_merge5.start_year.apply(lambda x: 0 if x==0 else 'year') 
info_time_merge5=info_time_merge5.replace(0,np.nan)

info_time_final=info_time_merge5.append(info_time_cen,ignore_index=True)
info_time_final.rename(columns={'century':'precision'},inplace=True)
##############deal with those conflicts that is still ongoing
info_time_final['present']=info_time_final['time'].str.extract(r'(present|ongoing)',expand=False)
info_time_final['present']=info_time_final['present'].fillna(0)
def functionp(a,b):
    if a!=0:
        return 0
    else:
        return b
        
info_time_final['end_year']=info_time_final.apply(lambda x: functionp(x.present,x.end_year), axis=1)

present=info_time_final[info_time_final['present']!=0]
no_end_year=info_time_final[info_time_final['end_year']==0] #####if end year equals to 0, means that the conflict is still ongoing
info_time_final=info_time_final.loc[:,['wid','title','start_year','end_year','precision']]

info_time_final['start_year']=info_time_final['start_year'].replace(0,np.nan)
info_time_final['end_year']=info_time_final['end_year'].replace(0,np.nan)

info_time.loc[info_time.wid==4548076,'start_year']=-119
info_time.loc[info_time.wid==4548076,'end_year']=-119
info_time.loc[info_time.wid==3167454,'start_year']=-30
info_time.loc[info_time.wid==50818289,'end_year']=-63
info_time.loc[info_time.wid==2800682,'start_year']=1536
info_time.loc[info_time.wid==2800682,'end_year']=1883
info_time.loc[info_time.wid==1536983,'end_year']=1739

info_time_final.to_csv(time_path,index=False)


"""
###################################### location information #########################################
"""
info_location=writer.loc[:,['wid','title','tmp']]
info_location['location']=info_location['tmp'].str.extract(r'(.*)',expand=False)


info_location['location']=info_location['location'].str.replace(r'\[\[fief\]\]','')
info_location['location']=info_location['location'].str.replace(r'\[\[history of \[\[france\]\]|french\]\]','[[france]]')
info_location['location']=info_location['location'].str.replace(r'\[\[universal transverse mercator coordinate system\|utm grid\]\]','')
info_location['location']=info_location['location'].str.replace(r'\[\[file\:.*?\]\]','')
####
#######positioning
info_location['location']=info_location['location'].str.extract(r'\| *place *=(.*)',expand=False)
direct2(info_location,'location')
#######Modify part of the data so that location information can be obtained using unified steps
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
########
 
############release the the contents of the square brackets
location_infobox = pd.DataFrame(columns=('wid','title','location'))  # generate a new dataset
info_location['location']=info_location['location'].fillna(0) 

location_infobox = releasewithoutbig(info_location,location_infobox,'location',3)
############

location_infobox['location']=location_infobox['location'].str.replace(r'\|.*','')
location_infobox['location']=location_infobox['location'].str.replace(r'\[\[','')
location_infobox['location']=location_infobox['location'].str.replace(r'\]\]','')
location_infobox['location']=location_infobox['location'].replace(0,np.nan)
location_infobox['location']=location_infobox['location'].str.strip()
location_infobox.to_csv(location_path,index=False)


"""
###################################### partof information #########################################
"""
info_partof=writer.loc[:,['wid','title','tmp']]
info_partof['partof']=info_partof['tmp'].str.extract(r'(.*)',expand=False)

info_partof['partof']=info_partof['partof'].str.replace(r'\[\[fief\]\]','')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[history of \[\[france\]\]|french\]\]','[[france]]')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[universal transverse mercator coordinate system\|utm grid\]\]','')
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[file\:.*?\]\]','') 
info_partof['partof']=info_partof['partof'].str.replace(r'\[\[image\:.*?\]\]','')
####
#######positioning
info_partof['partof']=info_partof['partof'].str.extract(r'\| *part *of *=(.*)',expand=False)
direct2(info_partof,'partof')
#######Modify part of the data so that partof information can be obtained using unified steps
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
#############
############release the the contents of the square brackets
partof_infobox = pd.DataFrame(columns=('wid','title','partof'))  # generate a new dataset
info_partof['partof']=info_partof['partof'].fillna(0)
partof_infobox = releasewithoutbig(info_partof,partof_infobox,'partof',3)
############

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
partof_infobox.to_csv(partof_infobox_path,index=False)
########################### get the structure that Arash suggested ###########################
wid_to_qid['qid']=wid_to_qid['qid'].astype(int)
wid_to_qid['wid']=wid_to_qid['wid'].astype(int)
qid_subject['qid']=qid_subject['qid'].astype(int)


qid_subject['subject']=qid_subject['subject'].str.lower()# convert each value of 'subject' to lowercase  
qid_subject.rename(columns={'subject': 'partof'}, inplace=True) 
qid_subject['partof']=qid_subject['partof'].str.replace(r'_',' ') # replace "_" to " "
qid_subject['partof']=qid_subject['partof'].str.replace(r'–','-') # replace "–" to "-"

partof=partof_infobox.loc[:,['wid','title','partof']]

partof['partof']=partof['partof'].str.replace(r'–','-') # replace "–" to "-"
partof['partof']=partof['partof'].str.replace(r'  ',' ') # replace "  " to " "



partof['partof']=partof['partof'].replace('first world war','world war i')
partof['partof']=partof['partof'].replace('second world war','world war ii')
partof['partof']=partof['partof'].replace('roman-germanic wars','germanic wars')
partof['partof']=partof['partof'].replace('anglo-french war (1213–14)','anglo-french war (1213–1214)')
partof['partof']=partof['partof'].replace('persian gulf conflict (disambiguation)','gulf war (disambiguation)')
partof['partof']=partof['partof'].replace('american theater of world war ii','american theater (world war ii)')
partof['partof']=partof['partof'].replace('american war of independence','american revolutionary war')
partof['partof']=partof['partof'].replace('german unification wars','unification of germany')
partof['partof']=partof['partof'].replace('russo-turkish wars','history of the russo-turkish wars')
partof['partof']=partof['partof'].replace('cross-strait conflict','cross-strait relations')
partof['partof']=partof['partof'].replace('byzantine-sassanid war of 602-628','Byzantine–Sasanian War of 602–628')
partof['partof']=partof['partof'].replace('song-yuan wars','Mongol conquest of the Song dynasty')
partof['partof']=partof['partof'].replace('polish-ottoman war (1683-1699)','Polish–Ottoman War (1683–99)')
partof['partof']=partof['partof'].replace('war of austrian succession','War of the Austrian Succession')
partof['partof']=partof['partof'].replace('franco-spanish war (1635-1659)','franco-spanish war (1635-59)')
partof['partof']=partof['partof'].replace('franco-spanish war (1635)','franco-spanish war (1635-59)')
partof['partof']=partof['partof'].replace('anglo-spanish war (1779-1783)','Spain and the American Revolutionary War')
partof['partof']=partof['partof'].replace('anglo-spanish war (1779-83)','Spain and the American Revolutionary War')
partof['partof']=partof['partof'].replace('french revolutionary war','French Revolutionary Wars')
partof['partof']=partof['partof'].replace('war of the french revolution','French Revolutionary Wars')
partof['partof']=partof['partof'].replace('anglo-spanish war (1585)','Anglo-Spanish War (1585–1604)')
partof['partof']=partof['partof'].replace('thirty years war','Thirty Years\' War')
partof['partof']=partof['partof'].replace('western front of world war ii','Western Front (World War II)')
partof['partof']=partof['partof'].replace('al-anfal campaign','Anfal genocide')
partof['partof']=partof['partof'].replace('rebellions of 1837','Rebellions of 1837–1838')
partof['partof']=partof['partof'].replace('persian wars','Greco-Persian Wars')
partof['partof']=partof['partof'].replace('italian war of 1494-1498','italian war of 1494-98')
partof['partof']=partof['partof'].replace('operation husky','Allied invasion of Sicily')
partof['partof']=partof['partof'].replace('crusade','Crusades')
partof['partof']=partof['partof'].replace('intercommunal violence in mandatory palestine','Intercommunal conflict in Mandatory Palestine')
partof['partof']=partof['partof'].replace('1383-85 crisis','1383-1385 portuguese interregnum')
partof['partof']=partof['partof'].replace('tunisia campaign','Tunisian Campaign')
partof['partof']=partof['partof'].replace('qing conquest of the ming','Transition from Ming to Qing')
partof['partof']=partof['partof'].replace('korean-jurchen conflicts','Korean–Jurchen border conflicts')
partof['partof']=partof['partof'].replace('battle of balaklava','Battle of Balaclava')
partof['partof']=partof['partof'].replace('indian wars','American Indian Wars')
partof['partof']=partof['partof'].replace('polish-russian war (1605-1618)','Polish–Muscovite War (1605–18)')
partof['partof']=partof['partof'].replace('insurgency in the philippines','Civil conflict in the Philippines')
partof['partof']=partof['partof'].replace('second italo-abyssinian war','Second Italo-Ethiopian War')
partof['partof']=partof['partof'].replace('campaigns of world war ii','world war ii')
partof['partof']=partof['partof'].replace('greco-turkish war (disambiguation)','greco-turkish war')
partof['partof']=partof['partof'].replace('protestant reformation','reformation')
partof['partof']=partof['partof'].replace('zulu civil war','Ndwandwe–Zulu War')
partof['partof']=partof['partof'].replace('war of the american independence','American Revolutionary War')
partof['partof']=partof['partof'].replace('indo-pakistani wars','Indo-Pakistani wars and conflicts')
partof['partof']=partof['partof'].replace('breton war of succession','War of the Breton Succession')
partof['partof']=partof['partof'].replace('polish-swedish war (1600-1611)','polish-swedish war (1600-11)')
partof['partof']=partof['partof'].replace('indo-pakistan war of 1971','Indo-Pakistani War of 1971')
partof['partof']=partof['partof'].replace('mediterranean and middle east theatre','Mediterranean and Middle East theatre of World War II')
partof['partof']=partof['partof'].replace('french invasion of egypt (1798)','French campaign in Egypt and Syria')
partof['partof']=partof['partof'].replace('italian war of 1551-1559','italian war of 1551-59')
partof['partof']=partof['partof'].replace('dunmore\'s war','Lord Dunmore\'s War')
partof['partof']=partof['partof'].replace('unosom ii','United Nations Operation in Somalia II')
partof['partof']=partof['partof'].replace('fifth coalition','War of the Fifth Coalition')
partof['partof']=partof['partof'].replace('byzantine-latin wars','Nicaean–Latin wars')
partof['partof']=partof['partof'].replace('epirote-nicaean conflict','Epirote–Nicaean conflict (1257–59)')
partof['partof']=partof['partof'].replace('operation pointblank','Pointblank directive')
partof['partof']=partof['partof'].replace('french invasion of russia (1812)','french invasion of russia')
partof['partof']=partof['partof'].replace('war of the grand alliance','Nine Years\' War')
partof['partof']=partof['partof'].replace('war on terrorism','War on Terror')
partof['partof']=partof['partof'].replace('operation iraqi freedom','Iraq War')
partof['partof']=partof['partof'].replace('bolivar in new granada','Bolívar\'s campaign to liberate New Granada')
partof['partof']=partof['partof'].replace('anglo-dutch war','anglo-dutch wars')
partof['partof']=partof['partof'].replace('roman servile wars','servile wars')
partof['partof']=partof['partof'].replace('fourth syrian war','syrian wars')
partof['partof']=partof['partof'].replace('iraq war (2003-11)','iraq war')
partof['partof']=partof['partof'].replace('strategic bombing in world war ii','Strategic bombing during World War II')
partof['partof']=partof['partof'].replace('rif war (1920)','rif war')
partof['partof']=partof['partof'].replace('battle of malaya','Malayan Campaign')
partof['partof']=partof['partof'].replace('third samnite war','Samnite Wars')
partof['partof']=partof['partof'].replace('russo-turkish war, 1768-1774','Russo-Turkish War (1768–1774)')
partof['partof']=partof['partof'].replace('decolonization of africa','Decolonisation of Africa')
partof['partof']=partof['partof'].replace('sennacherib\'s campaign in judah','Sennacherib\'s campaign in the Levant')
partof['partof']=partof['partof'].replace('mormon war (1838)','1838 Mormon War')
partof['partof']=partof['partof'].replace('polish-austrian war','Austro-Polish War')
partof['partof']=partof['partof'].replace('byzantine-seljuk wars','Byzantine–Seljuq wars')
partof['partof']=partof['partof'].replace('nader\'s campaigns','Campaigns of Nader Shah')
partof['partof']=partof['partof'].replace('polish-ottoman war (1620-1621)','Polish–Ottoman War (1620–21)')
partof['partof']=partof['partof'].replace('pacific theatre of world war ii','Pacific War')
partof['partof']=partof['partof'].replace('russian revolution of 1917','Russian Revolution')
partof['partof']=partof['partof'].replace('spanish conquest of peru','spanish conquest of the inca empire')
partof['partof']=partof['partof'].replace('invasion of poland (1939)','invasion of poland')
partof['partof']=partof['partof'].replace('romanian campaign (world war i)','Romania during World War I')
partof['partof']=partof['partof'].replace('sixth ottoman-venetian war','Morean War')
partof['partof']=partof['partof'].replace('balkan campaign (world war ii)','Balkans Campaign (World War II)')
partof['partof']=partof['partof'].replace('muslim conquest','Early Muslim conquests')
partof['partof']=partof['partof'].replace('fourth coalition','War of the Fourth Coalition')
partof['partof']=partof['partof'].replace('russo-turkish war of 1877-78','Russo-Turkish War (1877–1878)')
partof['partof']=partof['partof'].replace('second samnite war','Samnite Wars')
partof['partof']=partof['partof'].replace('nine years war (ireland)','Nine Years\' War (Ireland)')
partof['partof']=partof['partof'].replace('the deluge (polish history)','Deluge (history)')
partof['partof']=partof['partof'].replace('mongol invasions','Mongol invasions and conquests')
partof['partof']=partof['partof'].replace('campaign of the carolinas','carolinas campaign')
partof['partof']=partof['partof'].replace('italian war of 1542','Italian War of 1542–46')
partof['partof']=partof['partof'].replace('south-west africa campaign','South West Africa campaign')
partof['partof']=partof['partof'].replace('1814 campaign in north-east france','Campaign in north-east France (1814)')
partof['partof']=partof['partof'].replace('polish-bolshevik war','Polish–Soviet War')
partof['partof']=partof['partof'].replace('persian gulf war','Gulf War')
partof['partof']=partof['partof'].replace('the seven years war','Seven Years\' War')
partof['partof']=partof['partof'].replace('mongol conquests','Mongol invasions and conquests')
partof['partof']=partof['partof'].replace('serbian campaign (world war i)','Serbian Campaign of World War I')
partof['partof']=partof['partof'].replace('dano-swedish war (1658-1660)','dano-swedish war (1658-60)')
partof['partof']=partof['partof'].replace('turkoman invasions of georgia','Turkmen incursions into Georgia')
partof['partof']=partof['partof'].replace('middle-eastern theatre of world war i','Middle Eastern theatre of World War I')
partof['partof']=partof['partof'].replace('russo-turkish war of 1877-1878','russo-turkish war (1877-1878)')
partof['partof']=partof['partof'].replace('second italian war','Italian War of 1499–1504')
partof['partof']=partof['partof'].replace('siege of corfu (1716)','Ottoman–Venetian War (1714–1718)')
partof['partof']=partof['partof'].replace('ottoman-venetian war of 1714-18','Ottoman–Venetian War (1714–1718)')
partof['partof']=partof['partof'].replace('imjin war','Japanese invasions of Korea (1592–98)')
partof['partof']=partof['partof'].replace('anglo-turkish war (1807-1809)','Anglo-Turkish War (1807–09)')
partof['partof']=partof['partof'].replace('nato intervention in bosnia','NATO intervention in Bosnia and Herzegovina')
partof['partof']=partof['partof'].replace('turkish-venetian war (1714-1718)','ottoman-venetian war (1714-1718)')
partof['partof']=partof['partof'].replace('1383-1385 crisis','1383–1385 Portuguese interregnum')
partof['partof']=partof['partof'].replace('long war (ottoman wars)','Long Turkish War')
partof['partof']=partof['partof'].replace('post-invasion iraq, 2003-present','Iraq War')
partof['partof']=partof['partof'].replace('japanese invasions of korea (1592-1598)','japanese invasions of korea (1592-98)')
partof['partof']=partof['partof'].replace('polish-russian war (1658-1667)','Russo-Polish War (1654–1667)')
partof['partof']=partof['partof'].replace('serbian-ottoman wars','list of serbian-ottoman conflicts')
partof['partof']=partof['partof'].replace('first samnite war','Samnite Wars')
partof['partof']=partof['partof'].replace('second arab-khazar war','Arab–Khazar wars')
partof['partof']=partof['partof'].replace('japanese invasions of korea','japanese invasions of korea (1592-98)')
partof['partof']=partof['partof'].replace('colombian armed conflict','Colombian conflict')
partof['partof']=partof['partof'].replace('austro-turkish war (1663-1664)','austro-turkish war (1663-64)')
partof['partof']=partof['partof'].replace('muslim conquest in the indian subcontinent','Muslim conquests in the Indian subcontinent')
partof['partof']=partof['partof'].replace('muslim conquests','Early Muslim conquests')
partof['partof']=partof['partof'].replace('anglo-spanish war of 1779-1783','Spain and the American Revolutionary War')
partof['partof']=partof['partof'].replace('hideyoshi\'s invasions of korea','Japanese invasions of Korea (1592–1598)')
partof['partof']=partof['partof'].replace('argentine civil war','argentine civil wars')
partof['partof']=partof['partof'].replace('pontiac\'s rebellion','Pontiac\'s War')
partof['partof']=partof['partof'].replace('austro-turkish war of 1716-18','Austro-Turkish War (1716–1718)')
partof['partof']=partof['partof'].replace('goryeo-khitan wars','Goryeo–Khitan War')
partof['partof']=partof['partof'].replace('defeat and dissolution of the ottoman empire','Dissolution of the Ottoman Empire')
partof['partof']=partof['partof'].replace('montenegrin campaign (world war i)','Montenegrin Campaign of World War I')
partof['partof']=partof['partof'].replace('muslim conquest of syria','Muslim conquest of the Levant')
partof['partof']=partof['partof'].replace('polish-muscovite war (1605-1618)','polish-muscovite war (1605-18)')
partof['partof']=partof['partof'].replace('byzantine-sassanid war of 572-591','Byzantine–Sasanian War of 572–591')
partof['partof']=partof['partof'].replace('polish-muscovite war (1605-1618)','polish-muscovite war (1605-18)')
partof['partof']=partof['partof'].replace('muslim conquest of north africa','Muslim conquest of the Maghreb')
partof['partof']=partof['partof'].replace('mongol invasion of rus','mongol invasion of rus\'')
partof['partof']=partof['partof'].replace('siege of paris (1870-1871)','Siege of Paris (1870–71)')
partof['partof']=partof['partof'].replace('ethiopian-adal war','Abyssinian–Adal war')
partof['partof']=partof['partof'].replace('ottoman-portuguese conflicts (1538-57)','Ottoman–Portuguese conflicts (1538–1559)')
partof['partof']=partof['partof'].replace('franco-flemish war (1297-1305)','Franco-Flemish War')
partof['partof']=partof['partof'].replace('hundred years war','Hundred Years\' War')
partof['partof']=partof['partof'].replace('actions in inner mongolia (1933-1936)','Actions in Inner Mongolia (1933–36)')
partof['partof']=partof['partof'].replace('first turko-egyptian war','Egyptian–Ottoman War (1831–1833)')
partof['partof']=partof['partof'].replace('1868 expedition to abyssinia','British Expedition to Abyssinia')
partof['partof']=partof['partof'].replace('first franco-moroccan war','Franco-Moroccan War')
partof['partof']=partof['partof'].replace('russo-persian war (1804-1813)','Russo-Persian War (1804–13)')
partof['partof']=partof['partof'].replace('napoleon\'s invasion of russia','French invasion of Russia')
partof['partof']=partof['partof'].replace('lebanese civil war (1975-1977)','Lebanese Civil War')
partof['partof']=partof['partof'].replace('first scottish rebellion','First War of Scottish Independence')
partof['partof']=partof['partof'].replace('global war on terrorism','War on Terror')
partof['partof']=partof['partof'].replace('egyptian-ottoman war (1831-1833)','egyptian-ottoman war (1831-33)')
partof['partof']=partof['partof'].replace('iraqi insurgency (iraq war)','iraq war')
partof['partof']=partof['partof'].replace('third goryeo-khitan war','Third conflict in the Goryeo–Khitan War')
partof['partof']=partof['partof'].replace('rus\'-byzantine war (970-971)','Sviatoslav\'s invasion of Bulgaria')
partof['partof']=partof['partof'].replace('mongol invasion of kievan rus\'','mongol invasion of rus\'')
partof['partof']=partof['partof'].replace('roman conquest of italy','Roman expansion in Italy')
partof['partof']=partof['partof'].replace('chad-sudan conflict','chadian civil war (2005-2010)')
partof['partof']=partof['partof'].replace('War of the Polish Succession (1587–1588)','War of the Polish Succession (1587–88)')
partof['partof']=partof['partof'].replace('islamic conquest of persia','Muslim conquest of Persia')
partof['partof']=partof['partof'].replace('federalist riograndense revolution (brazil)','Federalist Riograndense Revolution')
partof['partof']=partof['partof'].replace('cpp-npa-ndf rebellion','Communist rebellion in the Philippines')
partof['partof']=partof['partof'].replace('chadian civil war (1965-1979)','Chadian Civil War (1965–79)')
partof['partof']=partof['partof'].replace('chadian-sudanese conflict','Chadian Civil War (2005–2010)')
partof['partof']=partof['partof'].replace('north-west europe campaign (world war ii)','North West Europe Campaign')
partof['partof']=partof['partof'].replace('revolution of 1905','1905 Russian Revolution')
partof['partof']=partof['partof'].replace('indian mutiny of 1857','Indian Rebellion of 1857')
partof['partof']=partof['partof'].replace('the french revolution','French Revolution')
partof['partof']=partof['partof'].replace('insurgency in north-east india','Insurgency in Northeast India')
partof['partof']=partof['partof'].replace('mamluk-ilkhanid war','Mongol invasions of the Levant')
partof['partof']=partof['partof'].replace('war in chad (2005-2010)','Chadian Civil War (2005–2010)')
partof['partof']=partof['partof'].replace('mongol-vietnamese war','Mongol invasions of Vietnam')
partof['partof']=partof['partof'].replace('conquest of baekje by silla and tang','Baekje–Tang War')
partof['partof']=partof['partof'].replace('third battle of ypres','Battle of Passchendaele')
partof['partof']=partof['partof'].replace('first coalition','War of the First Coalition')
partof['partof']=partof['partof'].replace('border war (1910-1918)','border war (1910-19)')
partof['partof']=partof['partof'].replace('balochistan conflict','Insurgency in Balochistan')
partof['partof']=partof['partof'].replace('italian war of 1551','Italian War of 1551–59')
partof['partof']=partof['partof'].replace('austro-russian-turkish war (1735-39)','Russo-Turkish War (1735–1739)')
partof['partof']=partof['partof'].replace('urabi revolt','\'Urabi revolt')
partof['partof']=partof['partof'].replace('conflict in afghanistan (1978-present)','war in afghanistan (1978-present)')
partof['partof']=partof['partof'].replace('ottoman-serbian wars','List of Serbian–Ottoman conflicts')
partof['partof']=partof['partof'].replace('sekigahara campaign','Battle of Sekigahara')
partof['partof']=partof['partof'].replace('zenkunen war','Former Nine Years\' War')
partof['partof']=partof['partof'].replace('ottoman-safavid war (1603-1618)','Ottoman–Safavid War (1603–18)')
partof['partof']=partof['partof'].replace('polish-swedish war (1600-1629)','polish-swedish war (1600-29)')
partof['partof']=partof['partof'].replace('anglo-spanish war (1625)','Anglo-Spanish War (1625–1630)')
partof['partof']=partof['partof'].replace('1948 palestine war','1947–1949 Palestine war')
partof['partof']=partof['partof'].replace('first italian war','Italian War of 1494–98')
partof['partof']=partof['partof'].replace('armenian resistance','Armenian resistance during the Armenian Genocide')
partof['partof']=partof['partof'].replace('the mahdist war','Mahdist War')
partof['partof']=partof['partof'].replace('battle of central burma','Battle of Meiktila and Mandalay')
partof['partof']=partof['partof'].replace('war of the polish succession (1587-1588)','War of the Polish Succession (1587–88)')
partof['partof']=partof['partof'].replace('war in somalia (2006-2009)','Somali Civil War (2006–2009)')
partof['partof']=partof['partof'].replace('war in afghanistan (2001-14)','war in afghanistan (2001-present)')
partof['partof']=partof['partof'].replace('british invasions of the río de la plata','British invasions of the River Plate')
partof['partof']=partof['partof'].replace('second war of schleswig','Second Schleswig War')
partof['partof']=partof['partof'].replace('thousand days war','Thousand Days\' War')
partof['partof']=partof['partof'].replace('pugachev war','Pugachev\'s Rebellion')
partof['partof']=partof['partof'].replace('ottoman-wahhabi war','Wahhabi War')
partof['partof']=partof['partof'].replace('ottoman-saudi war','Wahhabi War')
partof['partof']=partof['partof'].replace('georgian-abkhazian conflict','Abkhaz–Georgian conflict')
partof['partof']=partof['partof'].replace('shaba invasions','Angolan Civil War')
partof['partof']=partof['partof'].replace('insurgency in the maghreb','Insurgency in the Maghreb (2002–present)')
partof['partof']=partof['partof'].replace('herzegovina uprising (1875-78)','Herzegovina uprising (1875–1877)')
partof['partof']=partof['partof'].replace('war in afghanistan (2001-2014)','war in afghanistan (2001-present)')
partof['partof']=partof['partof'].replace('2008-2009 sla northern offensive','2008–09 Sri Lankan Army Northern offensive')
partof['partof']=partof['partof'].replace('2011 libyan civil war','Libyan Civil War (2011)')
partof['partof']=partof['partof'].replace('2012 northern mali conflict','Northern Mali conflict')
partof['partof']=partof['partof'].replace('2012 tuareg rebellion','Tuareg rebellion (2012)')
partof['partof']=partof['partof'].replace('2014 libyan civil war','Libyan Civil War (2014–present)')
partof['partof']=partof['partof'].replace('2014 libyan conflict','Libyan Civil War (2014–present)')
partof['partof']=partof['partof'].replace('2014 libyan uprising','Libyan Civil War (2014–present)')
partof['partof']=partof['partof'].replace('american-led intervention in syria','American-led intervention in the Syrian Civil War')
partof['partof']=partof['partof'].replace('an shi rebellion','An Lushan Rebellion')
partof['partof']=partof['partof'].replace('anglo-french war 1778','Anglo-French War (1778–1783)')
partof['partof']=partof['partof'].replace('anglo-spanish war (1654)','Anglo-Spanish War (1654–1660)')
partof['partof']=partof['partof'].replace('anglo-spanish war (1727)','Anglo-Spanish War (1727–1729)')
partof['partof']=partof['partof'].replace('anglo-spanish war (1761-1763)','Anglo-Spanish War (1762–63)')
partof['partof']=partof['partof'].replace('anglo-spanish war (1779)','Spain and the American Revolutionary War')
partof['partof']=partof['partof'].replace('atlantic u-boat campaign (world war i)','Atlantic U-boat campaign of World War I')
partof['partof']=partof['partof'].replace('1882 anglo-egyptian war','Anglo-Egyptian War')
partof['partof']=partof['partof'].replace('1891 chilean civil war','Chilean Civil War of 1891')
partof['partof']=partof['partof'].replace('1947-1948 civil war in mandatory palestine','1947-48 civil war in mandatory palestine')
partof['partof']=partof['partof'].replace('1989 tiananmen square protests','tiananmen square protests of 1989')
partof['partof']=partof['partof'].replace('1997 riots','Albanian Civil War')
partof['partof']=partof['partof'].replace('2000-2006 shebaa farms conflict','2000–06 Shebaa Farms conflict')
partof['partof']=partof['partof'].replace('2008-2009 israel-gaza conflict','Gaza War (2008–09)')
partof['partof']=partof['partof'].replace('2011 nafusa mountain campaign','2011 Nafusa Mountains campaign')
partof['partof']=partof['partof'].replace('2011 syrian uprising','Libyan Civil War (2011)')
partof['partof']=partof['partof'].replace('2014 russian military intervention in ukraine','Russian military intervention in Ukraine (2014–present)')
partof['partof']=partof['partof'].replace('2015 burundian unrest','Burundian unrest (2015–present)')
partof['partof']=partof['partof'].replace('2015 ramadan attacks','26 June 2015 Islamist attacks')
partof['partof']=partof['partof'].replace('2015-16 şırnak clashes','Şırnak clashes (2015–16)')
partof['partof']=partof['partof'].replace('2016-2017 gambian constitutional crisis','2016–17 Gambian constitutional crisis')
partof['partof']=partof['partof'].replace('adal-ethiopian war','Abyssinian–Adal war')
partof['partof']=partof['partof'].replace('aegean anti-piracy operation','Aegean Sea Anti-Piracy Operations of the United States')
partof['partof']=partof['partof'].replace('afghan war (2015-present)','War in Afghanistan (2001–present)')
partof['partof']=partof['partof'].replace('al anbar campaign','iraq war in anbar province')
partof['partof']=partof['partof'].replace('al hudaydah governorate offensive','Al Hudaydah offensive')
partof['partof']=partof['partof'].replace('albanian resistance of world war ii','World War II in Albania')
partof['partof']=partof['partof'].replace('albanian-venetian war (1447-1448)','Albanian–Venetian War')
partof['partof']=partof['partof'].replace('american-intervention in niger','Operation Juniper Shield - Niger')
partof['partof']=partof['partof'].replace('american-led intervention in iraq','American-led intervention in Iraq (2014–present)')
partof['partof']=partof['partof'].replace('eastern front of world war ii','Eastern Front (World War II)')
partof['partof']=partof['partof'].replace('iraq war (2014-present)','Iraqi Civil War (2014–2017)')
partof['partof']=partof['partof'].replace('kurdish-turkish conflict','Kurdish–Turkish conflict (1978–present)')
partof['partof']=partof['partof'].replace('military intervention against isil','International military intervention against ISIL')
partof['partof']=partof['partof'].replace('piracy in somalia','Piracy off the coast of Somalia')
partof['partof']=partof['partof'].replace('russian military intervention in syria','Russian military intervention in the Syrian Civil War')
partof['partof']=partof['partof'].replace('war in somalia (2009-present)','Somali Civil War (2009–present)')
partof['partof']=partof['partof'].replace('sino-vietnamese conflicts 1979-90','Sino-Vietnamese conflicts, 1979–1991')
partof['partof']=partof['partof'].replace('spanish-moroccan war (1859)','Hispano-Moroccan War (1859–60)')
partof['partof']=partof['partof'].replace('montenegrin-turkish war of 1876-1878','Montenegrin–Ottoman War (1876–78)')
partof['partof']=partof['partof'].replace('united nations stabilization mission in haiti','United Nations Stabilisation Mission in Haiti')
partof['partof']=partof['partof'].replace('gallic invasion of the balkans','celtic settlement of eastern europe')
partof['partof']=partof['partof'].replace('french invasion of the isle of wight (1545)','French invasion of the Isle of Wight')
partof['partof']=partof['partof'].replace('italian war of 1542-1546','italian war of 1542-46')
partof['partof']=partof['partof'].replace('north african theatre (world war i)','Military operations in North Africa during World War I')
partof['partof']=partof['partof'].replace('polish-swedish war (1626-1629)','polish-swedish war (1626-29)')
partof['partof']=partof['partof'].replace('prussian crusades','Prussian Crusade')
partof['partof']=partof['partof'].replace('the retribution operations','Reprisal operations')
partof['partof']=partof['partof'].replace('great prussian uprising','Prussian uprisings')
partof['partof']=partof['partof'].replace('indian mutiny','Indian Rebellion of 1857')
partof['partof']=partof['partof'].replace('diyala province campaign','Diyala campaign')
partof['partof']=partof['partof'].replace('islamic insurgency in the philippines','Moro conflict')
partof['partof']=partof['partof'].replace('mediterranean naval engagements during world war i','Naval warfare in the Mediterranean during World War I')
partof['partof']=partof['partof'].replace('eastern front (wwii)','Eastern Front (World War II)')
partof['partof']=partof['partof'].replace('rákóczi\'s war for independence','Rákóczi\'s War of Independence')
partof['partof']=partof['partof'].replace('unification of italy','Italian unification')
partof['partof']=partof['partof'].replace('war of jenkin\'s ear','War of Jenkins\' Ear')
partof['partof']=partof['partof'].replace('operation unceasing waves iii','Second Battle of Elephant Pass')
partof['partof']=partof['partof'].replace('turkey-pkk conflict','Kurdish–Turkish conflict (1978–present)')
partof['partof']=partof['partof'].replace('occupation of poland','Occupation of Poland (1939–1945)')
partof['partof']=partof['partof'].replace('italian campaign (world war i)','Italian Front (World War I)')
partof['partof']=partof['partof'].replace('soviet war in afghanistan','Soviet–Afghan War')
partof['partof']=partof['partof'].replace('troubles','The Troubles')
partof['partof']=partof['partof'].replace('iroquois wars','Beaver Wars')
partof['partof']=partof['partof'].replace('byzantine-arab wars','Arab–Byzantine wars')
partof['partof']=partof['partof'].replace('battle of sedan (1870)','Battle of Sedan')
partof['partof']=partof['partof'].replace('second carnatic war','carnatic wars')
partof['partof']=partof['partof'].replace('civil war in chad (2005-2010)','Chadian Civil War (2005–2010)')
partof['partof']=partof['partof'].replace('israel-gaza conflict','Gaza–Israel conflict')
partof['partof']=partof['partof'].replace('sectarian conflict in mandatory palestine','Intercommunal conflict in Mandatory Palestine')
partof['partof']=partof['partof'].replace('saudi-rashidi war','Saudi–Rashidi War (1903–1907)')
partof['partof']=partof['partof'].replace('insurgency in aceh (1976-2005)','Insurgency in Aceh')
partof['partof']=partof['partof'].replace('spanish-portuguese war (1761-1763)','Fantastic War')
partof['partof']=partof['partof'].replace('civil war in iraq (2006-07)','Sectarian violence in Iraq (2006–08)')
partof['partof']=partof['partof'].replace('macedonian front (world war i)','Macedonian Front')
partof['partof']=partof['partof'].replace('war of the seventh coalition','Hundred Days')
partof['partof']=partof['partof'].replace('peru-bolivian war','War of the Confederation')
partof['partof']=partof['partof'].replace('war of the gdańsk rebellion','Danzig rebellion')
partof['partof']=partof['partof'].replace('war in somalia (2006-present)','Somali Civil War (2006–2009)')
partof['partof']=partof['partof'].replace('french revolutionary wars: campaigns of 1797','French Revolutionary Wars')
partof['partof']=partof['partof'].replace('iraqi invasion of iran','Iraqi invasion of Iran (1980)')
partof['partof']=partof['partof'].replace('yugoslav front','World War II in Yugoslavia')
partof['partof']=partof['partof'].replace('polish-ottoman war (1672-1676)','polish-ottoman war (1672-76)')
partof['partof']=partof['partof'].replace('hawaiian rebellions (1887-1895)','hawaiian rebellions (1887-95)')
partof['partof']=partof['partof'].replace('history of the serbian-turkish wars','List of Serbian–Ottoman conflicts')
partof['partof']=partof['partof'].replace('first war of schleswig','First Schleswig War')
partof['partof']=partof['partof'].replace('colombian armed conflict (1964-present)','Colombian conflict')
partof['partof']=partof['partof'].replace('the crusades','Crusades')
partof['partof']=partof['partof'].replace('diyala province campaign','Diyala campaign')
partof['partof']=partof['partof'].replace('soviet westward offensive of 1918-1919','Soviet westward offensive of 1918–19')
partof['partof']=partof['partof'].replace('mongol invasion of china','Mongol conquest of China')
partof['partof']=partof['partof'].replace('new zealand land wars','New Zealand Wars')
partof['partof']=partof['partof'].replace('north caucasus insurgency 2009','Insurgency in the North Caucasus')
partof['partof']=partof['partof'].replace('ottoman-albanian wars','Skanderbeg\'s rebellion')
partof['partof']=partof['partof'].replace('war of the three kingdoms','Wars of the Three Kingdoms')
partof['partof']=partof['partof'].replace('gallic war','Gallic Wars')
partof['partof']=partof['partof'].replace('nien rebellion','Nian Rebellion')
partof['partof']=partof['partof'].replace('colombian civil war of 1885','Panama crisis of 1885')
partof['partof']=partof['partof'].replace('naderian wars','Campaigns of Nader Shah')
partof['partof']=partof['partof'].replace('egyptian-ottoman war (1839-1841)','egyptian-ottoman war (1839-41)')
partof['partof']=partof['partof'].replace('central somalia spring fighting of 2009','Battle for Central Somalia (2009)')
partof['partof']=partof['partof'].replace('third carnatic war','Carnatic Wars')
partof['partof']=partof['partof'].replace('dano-swedish war of 1808-1809','Dano-Swedish War of 1808–09')
partof['partof']=partof['partof'].replace('spanish invasion of portugal','Spanish invasion of Portugal (1762)')
partof['partof']=partof['partof'].replace('serbian-ottoman war (1876-1877)','Serbian-Turkish Wars (1876–1878)')
partof['partof']=partof['partof'].replace('shia insurgency in yemen','Houthi insurgency in Yemen')
partof['partof']=partof['partof'].replace('portuguese-mamluk war','Portuguese–Mamluk naval war')
partof['partof']=partof['partof'].replace('hawaiian revolutions','Hawaiian rebellions (1887–1895)')
partof['partof']=partof['partof'].replace('goguryeo-sui wars','Goguryeo–Sui War')
partof['partof']=partof['partof'].replace('romanian campaign','Romania during World War I')
partof['partof']=partof['partof'].replace('little war in hungary','Ottoman–Habsburg wars in Hungary (1526–1568)')
partof['partof']=partof['partof'].replace('non-co-operation movement','Non-cooperation movement')
partof['partof']=partof['partof'].replace('war in somalia (2009-)','Somali Civil War (2009–present)')
partof['partof']=partof['partof'].replace('egyptian campaign','French campaign in Egypt and Syria')
partof['partof']=partof['partof'].replace('korean expedition','United States expedition to Korea')
partof['partof']=partof['partof'].replace('ottoman-safavid war (1578-1590)','Ottoman–Safavid War (1578–90)')
partof['partof']=partof['partof'].replace('retreat from mons','Great Retreat')
partof['partof']=partof['partof'].replace('polish-cossack-tatar war (1666-1671)','Polish–Cossack–Tatar War (1666–71)')
partof['partof']=partof['partof'].replace('mediterranean theatre of world war ii','Mediterranean and Middle East theatre of World War II')
partof['partof']=partof['partof'].replace('father le loutre’s war','father le loutre\'s war')
partof['partof']=partof['partof'].replace('toungoo-hanthawaddy war (1534-41)','Taungoo–Hanthawaddy War (1534–41)')
partof['partof']=partof['partof'].replace('despenser wars','Despenser War')
partof['partof']=partof['partof'].replace('march on london','Invasion of England (1326)')
partof['partof']=partof['partof'].replace('normandy campaigns of 1202-1204','french invasion of normandy (1202-1204)')
partof['partof']=partof['partof'].replace('hundred years\' war (1415-1453)','Hundred Years\' War (1415–53)')
partof['partof']=partof['partof'].replace('tanker war','Iran–Iraq War')
partof['partof']=partof['partof'].replace('russian conquest of turkestan','Russian conquest of Central Asia')
partof['partof']=partof['partof'].replace('ottoman-safavid war (1532-1555)','Ottoman–Safavid War (1532–55)')
partof['partof']=partof['partof'].replace('ottoman-portuguese conflicts (1538-1557)','Ottoman–Portuguese conflicts (1538–1559)')
partof['partof']=partof['partof'].replace('wynaad insurrection','Cotiote War')
partof['partof']=partof['partof'].replace('father rale\'s war','Dummer\'s War')
partof['partof']=partof['partof'].replace('shia insurgency in yemen','Houthi insurgency in Yemen')
partof['partof']=partof['partof'].replace('yemeni al-qaeda crackdown','Al-Qaeda insurgency in Yemen')
partof['partof']=partof['partof'].replace('iraq war in al anbar governorate','iraq war in anbar province')
partof['partof']=partof['partof'].replace('second coalition','War of the Second Coalition')
partof['partof']=partof['partof'].replace('nicaraguan civil war (1912)','United States occupation of Nicaragua')
partof['partof']=partof['partof'].replace('operation neptune','Normandy landings')
partof['partof']=partof['partof'].replace('campaign of egypt','French campaign in Egypt and Syria')
partof['partof']=partof['partof'].replace('ferdinand wars','Fernandine Wars')
partof['partof']=partof['partof'].replace('venezuela crisis of 1902-1903','venezuelan crisis of 1902-1903')
partof['partof']=partof['partof'].replace('caucasus war','Caucasian War')
partof['partof']=partof['partof'].replace('reaper\'s war','Reapers\' War')
partof['partof']=partof['partof'].replace('brazilian naval revolt','Revolta da Armada')
partof['partof']=partof['partof'].replace('normandy campaigns of 1200-1204','French invasion of Normandy (1202–1204)')
partof['partof']=partof['partof'].replace('german invasion of greece','Battle of Greece')
partof['partof']=partof['partof'].replace('second anglo-egyptian war','Anglo-Egyptian War')
partof['partof']=partof['partof'].replace('petersburg campaign','Siege of Petersburg')
partof['partof']=partof['partof'].replace('luso-brazilian invasion','Portuguese conquest of the Banda Oriental')
partof['partof']=partof['partof'].replace('western front (wwii)','Western Front (World War II)')
partof['partof']=partof['partof'].replace('retribution operations','Reprisal operations')
partof['partof']=partof['partof'].replace('dzungar-qing war','Dzungar–Qing Wars')
partof['partof']=partof['partof'].replace('khuzestan conflict','Arab separatism in Khuzestan')
partof['partof']=partof['partof'].replace('idlib governorate clashes (september 2011-march 2012)','Idlib Governorate clashes (September 2011 – March 2012)')
partof['partof']=partof['partof'].replace('liège wars','Wars of Liège')
partof['partof']=partof['partof'].replace('maratha war of independence','Mughal–Maratha Wars')
partof['partof']=partof['partof'].replace('post-civil war violence in libya','Factional violence in Libya (2011–2014)')
partof['partof']=partof['partof'].replace('peasants\' revolt of 1834 (palestine)','Peasants\' revolt in Palestine')
partof['partof']=partof['partof'].replace('northern mali conflict (2012-present)','Northern Mali conflict')
partof['partof']=partof['partof'].replace('swedish-brandenburg war','Scanian War')
partof['partof']=partof['partof'].replace('cortina war','Cortina Troubles')
partof['partof']=partof['partof'].replace('franco-spanish war (1595-1598)','French Wars of Religion')
partof['partof']=partof['partof'].replace('islamic invasion of gaul','Umayyad invasion of Gaul')
partof['partof']=partof['partof'].replace('namibian war of independence','South African Border War')
partof['partof']=partof['partof'].replace('kurdish-iraqi conflict','Iraqi–Kurdish conflict')
partof['partof']=partof['partof'].replace('middle eastern theatre of the first world war','Middle Eastern theatre of World War I')
partof['partof']=partof['partof'].replace('iraqi insurgency (post-u.s. withdrawal)','Iraqi insurgency (2011–2013)')
partof['partof']=partof['partof'].replace('latin wars','Roman–Latin wars')
partof['partof']=partof['partof'].replace('palej uprising','Paliy uprising')
partof['partof']=partof['partof'].replace('philippine revolutionary war','Philippine Revolution')
partof['partof']=partof['partof'].replace('tang campaign against the western turks','Tang campaigns against the Western Turks')
partof['partof']=partof['partof'].replace('tang campaign against the oasis states','Emperor Taizong\'s campaign against the Western Regions')
partof['partof']=partof['partof'].replace('colombian conflict (1964-present)','Colombian conflict')
partof['partof']=partof['partof'].replace('fatah-hamas conflict (2006-07)','Fatah–Hamas conflict')
partof['partof']=partof['partof'].replace('indo-pakistani war of 1947','Indo-Pakistani War of 1947–1948')
partof['partof']=partof['partof'].replace('central african republic conflict (2012-present)','central african republic civil war (2012-2014)')
partof['partof']=partof['partof'].replace('ukraine crisis','Ukrainian crisis')
partof['partof']=partof['partof'].replace('guerrilla war in lithuania','Lithuanian partisans')
partof['partof']=partof['partof'].replace('northern seven years war','Northern Seven Years\' War')
partof['partof']=partof['partof'].replace('lubomirski rebellion','Lubomirski\'s rebellion')
partof['partof']=partof['partof'].replace('charles xii invasion of poland','swedish invasion of poland (1701-1706)')
partof['partof']=partof['partof'].replace('powder river expedition','Powder River Expedition (1865)')
partof['partof']=partof['partof'].replace('insurgency in the republic of macedonia','2001 insurgency in the Republic of Macedonia')
partof['partof']=partof['partof'].replace('salahuddin campaign (2014-15)','Salahuddin campaign')
partof['partof']=partof['partof'].replace('egyptian crisis (2011-present)','Egyptian Crisis (2011–14)')
partof['partof']=partof['partof'].replace('iranian-led intervention in iraq (2014-present)','Iranian intervention in Iraq (2014–present)')
partof['partof']=partof['partof'].replace('central african republic civil war (2012-present)','central african republic civil war (2012-2014)')
partof['partof']=partof['partof'].replace('spanish conquest of mexico','Spanish conquest of the Aztec Empire')
partof['partof']=partof['partof'].replace('moro insurgency in the philippines','Moro conflict')
partof['partof']=partof['partof'].replace('islamist insurgency in nigeria','Boko Haram insurgency')
partof['partof']=partof['partof'].replace('long war (1591-1606)','Long Turkish War')
partof['partof']=partof['partof'].replace('operation shingle','Battle of Anzio')
partof['partof']=partof['partof'].replace('south philippines insurgency','Civil conflict in the Philippines')
partof['partof']=partof['partof'].replace('islamist insurgency in west africa','Boko Haram insurgency')
partof['partof']=partof['partof'].replace('danish-hanseatic war (1426-1435)','Dano-Hanseatic War (1426–1435)')
partof['partof']=partof['partof'].replace('qatar-saudi arabia proxy conflict','Qatar–Saudi Arabia diplomatic conflict')
partof['partof']=partof['partof'].replace('maratha expeditions in bengal','Maratha invasions of Bengal')
partof['partof']=partof['partof'].replace('turkish military intervention in syria (august 2016 - march 2017)','Operation Euphrates Shield')
partof['partof']=partof['partof'].replace('iraq civil war (2014-present)','Iraqi Civil War (2014–2017)')
partof['partof']=partof['partof'].replace('shabwah governorate offensive (2014-2016)','Shabwah Governorate offensive (2014–present)')
partof['partof']=partof['partof'].replace('tyrone\'s rebellion','Nine Years\' War (Ireland)')
partof['partof']=partof['partof'].replace('german campaign (napoleonic wars)','German Campaign of 1813')
partof['partof']=partof['partof'].replace('pkk rebellion (2015-present)','Kurdish–Turkish conflict (2015–present)')
partof['partof']=partof['partof'].replace('battle of lang son (1979)','battle of lạng sơn (1979)')
partof['partof']=partof['partof'].replace('timurid wars','Timurid conquests and invasions')
partof['partof']=partof['partof'].replace('war in afghanistan (2015-present)','War in Afghanistan (2001–present)')
partof['partof']=partof['partof'].replace('global war on terror','War on Terror')
partof['partof']=partof['partof'].replace('russo-turkish war of 1787-1792','Russo-Turkish War (1787–1792)')
partof['partof']=partof['partof'].replace('first scottish war of independence','First War of Scottish Independence')
partof['partof']=partof['partof'].replace('third war of the diadochi','Wars of the Diadochi')
partof['partof']=partof['partof'].replace('serbian-ottoman war (1876-78)','Serbian-Turkish Wars (1876–1878)')
partof['partof']=partof['partof'].replace('charles i of austria\'s attempts to retake the throne of hungary','Charles IV of Hungary\'s attempts to retake the throne')
partof['partof']=partof['partof'].replace('turkish military intervention in syria (august 2016-march 2017)','Operation Euphrates Shield')
partof['partof']=partof['partof'].replace('wars of religion (france)','French Wars of Religion')
partof['partof']=partof['partof'].replace('first campaign in the goguryeo-tang war','First conflict of the Goguryeo–Tang War')
partof['partof']=partof['partof'].replace('second libyan civil war','Libyan Civil War (2014–present)')
partof['partof']=partof['partof'].replace('kaffir wars','Xhosa Wars')
partof['partof']=partof['partof'].replace('bosporan expansion wars','Bosporan wars of expansion')
partof['partof']=partof['partof'].replace('israeli-syrian military incidents during the syrian civil war','Israeli–Syrian ceasefire line incidents during the Syrian Civil War')
partof['partof']=partof['partof'].replace('the great war','World War I')
partof['partof']=partof['partof'].replace('eastern syria campaign (september 2017-present)','Eastern Syria campaign (September–December 2017)')
partof['partof']=partof['partof'].replace('crisis in venezuela (2012-present)','Crisis in Venezuela')
partof['partof']=partof['partof'].replace('insurgency in the republic of macedonia','2001 insurgency in the Republic of Macedonia')
partof['partof']=partof['partof'].replace('deir ez-zor offensive (september 2017-march 2018)','Deir ez-Zor campaign (2017–19)')
partof['partof']=partof['partof'].replace('iran-iraq_war','Iran–Iraq War')
partof['partof']=partof['partof'].replace('operation greeley','Battle of Dak To')
partof['partof']=partof['partof'].replace('war of the roses','Wars of the Roses')
partof['partof']=partof['partof'].replace('crisis in venezuela (2010-present)','Crisis in Venezuela')
partof['partof']=partof['partof'].replace('pompey\'s campaign in caucasian iberia and albania','pompey\'s georgian campaign')
partof['partof']=partof['partof'].replace('russian military intervention in ukraine','Russian military intervention in Ukraine (2014–present)')
partof['partof']=partof['partof'].replace('post-invasion iraq, 2003-2006','iraq war')
partof['partof']=partof['partof'].replace('south-oranese campaign','Pacification of Algeria')

#######I didn't change some of the titles like "polish-russian wars", because these are series of some wars and they seems not the great war we want, more like some summary
#######################################################So the wars that belong to a "bigger war" without being matched to a qid may means that  we don't want the "bigger war".



partof['partof']=partof['partof'].str.replace(r'–','-') # replace "–" to "-"
partof['partof']=partof['partof'].str.lower()
##############match the 'partof' to the wid##############
partof=partof.fillna(0)
partof_wid=pd.merge(partof,qid_subject,on='partof',how='left')
partof_wid.loc[partof_wid.partof=='franco-visigothic wars','qid']=60760993
partof_wid.loc[partof_wid.partof=='sennacherib\'s campaign in the levant','qid']=63431018
partof_wid.loc[partof_wid.partof=='gascon campaign of 1345','qid']=60667736
partof_wid.loc[partof_wid.partof=='iraqi insurgency (2017-present)','qid']=57890365
partof_wid.loc[partof_wid.partof=='second battle of the alps','qid']=22247939
partof_wid.loc[partof_wid.partof=='operation juniper shield - niger','qid']=22247939
partof_wid.loc[partof_wid.partof=='muhammad tapar\'s anti-nizari campaign','qid']=60769268




partof_wid=partof_wid.fillna(0)
partof_wid['qid']=partof_wid['qid'].astype(int)
partof_wid_y=partof_wid[partof_wid['qid']!=0]
x=partof_wid[partof_wid['qid']==0] #the wars that can not be matched to a qid, need further dealing
x['qid']=x['qid'].astype(int)
####change ****-** to ****-****


x['y1']=x['partof'].str.extract(r'(.*\d\d\d\d-)\d\d\d\d.*',expand=False)
x['z1']=x['partof'].str.extract(r'.*(\d\d)\d\d-\d\d\d\d.*',expand=False)
x['z2']=x['partof'].str.extract(r'.*\d\d\d\d-(\d\d)\d\d.*',expand=False)
x['y2']=x['partof'].str.extract(r'.*\d\d\d\d-\d\d(\d\d.*)',expand=False)
x['y3']=x['y1']+x['y2']
x=x.fillna(0)
def functionf(a,b,c,d):
    if c==d:
        if a!=0:
            return a
        else:
            return b
    else:
        return b
    
x['partof']=x.apply(lambda x: functionf(x.y3,x.partof,x.z1,x.z2), axis=1)
x=x.loc[:,['wid','title','partof']]
x=pd.merge(x,qid_subject,on='partof',how='left')

x['century']=x['partof'].str.extract(r'(\d\d)\d\d-\d\d(?!\d\d)',expand=False)
x['x1']=x['partof'].str.extract(r'(.*?\d\d\d\d-)\d\d(?!\d\d)',expand=False)
x['x2']=x['partof'].str.extract(r'\d\d\d\d-(\d\d(?!\d\d).*)',expand=False)

x['x3']=x['x1']+x['century']+x['x2']
x=x.fillna(0)

x['partof']=x.apply(lambda x: function0(x.x3,x.partof), axis=1)
x.drop(['x1','x2','x3','century'],axis=1, inplace=True)

###############################
x=pd.merge(x,qid_subject,on='partof',how='left')
x=x.fillna(0)
x['qid']=x.apply(lambda x: function0(x.qid_x,x.qid_y), axis=1)
x=x.loc[:,['wid','title','partof','qid']]

partof_wid=partof_wid_y.append(x,ignore_index=True)
partof_wid['qid']=partof_wid['qid'].astype(int)
partof_wid.drop_duplicates(inplace=True)
#######match qid to wid
partof_wid.rename(columns={'wid': 'wid_0'}, inplace=True) 
partof_wid=pd.merge(partof_wid,wid_to_qid,on='qid',how='left')
partof_wid.drop_duplicates(inplace=True)
partof_wid.rename(columns={'wid': 'wid_1'}, inplace=True) 
partof_wid=partof_wid.fillna(0)
partof_wid['wid_1']=partof_wid['wid_1'].astype(int)
####### in wid_to_qid, some qids were matched to more than one wid, so change those wids which are linked to non_English pages to the wids linked to English pages.
partof_wid.loc[partof_wid.qid==179275,'wid_1']=12336420
partof_wid.loc[partof_wid.qid==99717,'wid_1']=44534
partof_wid.loc[partof_wid.qid==957012,'wid_1']=426828
partof_wid.loc[partof_wid.qid==2985977,'wid_1']=7302065
partof_wid.loc[partof_wid.qid==25894038,'wid_1']=51077660

###### some qids are not matched to any wid, here we match them to wids
partof_wid.loc[partof_wid.qid==189266,'wid_1']=519489
partof_wid.loc[partof_wid.qid==63431018,'wid_1']=60618531
partof_wid.loc[partof_wid.qid==60769268,'wid_1']=58664954
#####
partof_wid.drop_duplicates(inplace=True)
partof_wid.rename(columns={'wid_0': 'wid'}, inplace=True) 
partof_wid.rename(columns={'qid':'partof_qid','wid_1':'partof_wid'}, inplace=True) 
#######
#partof_qid_no_wid=partof_wid[partof_wid['wid_1']==0]


#data1=partof_wid[partof_wid.duplicated(['wid_0','title','partof','qid'])]
#data1['wid_1']=data1['wid_1'].astype(int)
######
############ iterating
partof_wid_1=partof_wid.loc[:,['wid','partof','partof_qid','partof_wid']]

partof_wid_iterate=partof_wid.loc[:,['title','wid','partof','partof_qid','partof_wid']]

##################### very special case: A is part of B and B is part of A, remove them first
partof_wid_1=partof_wid_1[partof_wid_1['wid']!=1368339]#soviet westward offensive of 1918-19vslatvian war of independence 
partof_wid_1=partof_wid_1[partof_wid_1['wid']!=4212744]#soviet westward offensive of 1918-19vslatvian war of independence 
 
partof_wid_1=partof_wid_1[partof_wid_1['wid']!=27868253]#boko haram insurgency
partof_wid_1=partof_wid_1[partof_wid_1['wid']!=43517689]#international military intervention against isil 
 
partof_wid_1=partof_wid_1[partof_wid_1['wid']!=4126964]#Revolta da Armada
partof_wid_1=partof_wid_1[partof_wid_1['wid']!=22034327]#federalist riograndense revolution

partof_wid_1=partof_wid_1[partof_wid_1['wid']!=13727911]#second battle of elephant pass

partof_wid_1=partof_wid_1[partof_wid_1['wid']!=31142430]#2011 military intervention in Libya
partof_wid_1=partof_wid_1[partof_wid_1['wid']!=31279643]#operation unified protector

partof_wid_1=partof_wid_1[partof_wid_1['wid']!=4126964]#revolta da armada
partof_wid_1=partof_wid_1[partof_wid_1['wid']!=22034327]#federalist riograndense revolution

partof_wid_1=partof_wid_1[partof_wid_1['wid']!=37353977]#Komenda Wars

##########如何得到这些的：当迭代足够多次后，anyelse里剩下的，就几乎都是存在互为部分的项了

 
######################
partof_wid_1.rename(columns={'wid': 'partof_wid','partof':'partof_2','partof_qid':'partof_2_qid','partof_wid':'partof_2_wid'}, inplace=True) 
partof_wid_iterate=pd.merge(partof_wid_iterate,partof_wid_1,on='partof_wid',how='left')

partof_wid_1.rename(columns={'partof_wid':'partof_2_wid','partof_2':'partof_3','partof_2_qid':'partof_3_qid','partof_2_wid':'partof_3_wid'}, inplace=True) 
partof_wid_iterate=pd.merge(partof_wid_iterate,partof_wid_1,on='partof_2_wid',how='left')

partof_wid_1.rename(columns={'partof_2_wid':'partof_3_wid','partof_3':'partof_4','partof_3_qid':'partof_4_qid','partof_3_wid':'partof_4_wid'}, inplace=True) 
partof_wid_iterate=pd.merge(partof_wid_iterate,partof_wid_1,on='partof_3_wid',how='left')

partof_wid_1.rename(columns={'partof_3_wid':'partof_4_wid','partof_4':'partof_5','partof_4_qid':'partof_5_qid','partof_4_wid':'partof_5_wid'}, inplace=True) 
partof_wid_iterate=pd.merge(partof_wid_iterate,partof_wid_1,on='partof_4_wid',how='left')

partof_wid_1.rename(columns={'partof_4_wid':'partof_5_wid','partof_5':'partof_6','partof_5_qid':'partof_6_qid','partof_5_wid':'partof_6_wid'}, inplace=True) 
partof_wid_iterate=pd.merge(partof_wid_iterate,partof_wid_1,on='partof_5_wid',how='left')

partof_wid_1.rename(columns={'partof_5_wid':'partof_6_wid','partof_6':'partof_7','partof_6_qid':'partof_7_qid','partof_6_wid':'partof_7_wid'}, inplace=True) 
partof_wid_iterate=pd.merge(partof_wid_iterate,partof_wid_1,on='partof_6_wid',how='left')



######check whether we have had enough iteration: there should be nothing left in 'anyelse'
partof_wid_iterate=partof_wid_iterate.fillna(0)
anyelse=partof_wid_iterate[partof_wid_iterate['partof_7']!=0]


#checkcircle=anyelse[anyelse['wid_partof_4']!=32927]
#checkcircle=checkcircle[checkcircle['wid_partof_4']!=4764461]
#checkcircle=checkcircle[checkcircle['wid_partof_4']!=342640]
#checkcircle=checkcircle[checkcircle['wid_partof_4']!=519516]
#checkcircle=checkcircle[checkcircle['wid_partof_4']!=325329]

partof_wid_iterate.drop_duplicates(inplace=True)
partof_wid=partof_wid.loc[:,['title','wid','partof','partof_qid','partof_wid']]

for i in ['2','3','4','5','6','7']:
    partof_wid['partof_'+i]=0
    partof_wid['partof_'+i+'_qid']=0
    partof_wid['partof_'+i+'_wid']=0

partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==1368339],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==4212744],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==27868253],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==43517689],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==4126964],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==22034327],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==13727911],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==31142430],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==31279643],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==4126964],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==22034327],ignore_index=True)
partof_wid_iterate=partof_wid_iterate.append(partof_wid[partof_wid['wid']==37353977],ignore_index=True)

partof_wid_iterate=partof_wid_iterate.replace(0,np.nan)

partof_wid_iterate.to_csv(partof_path,index=False)
