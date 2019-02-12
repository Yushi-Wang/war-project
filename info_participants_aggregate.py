# -*- coding: utf-8 -*-
"""
Created on Sun Jan 27 16:20:05 2019

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
participant1_infobox=pd.read_csv(main_path+'output/participant1_infobox.csv')
participant2_infobox=pd.read_csv(main_path+'output/participant2_infobox.csv')
participant3_infobox=pd.read_csv(main_path+'output/participant3_infobox.csv')
participant4_infobox=pd.read_csv(main_path+'output/participant4_infobox.csv')
participant5_infobox=pd.read_csv(main_path+'output/participant5_infobox.csv')
participant1a_1_infobox=pd.read_csv(main_path+'output/participant1a_1_infobox.csv')
participant1a_1a_infobox=pd.read_csv(main_path+'output/participant1a_1a_infobox.csv')
participant1a_1b_infobox=pd.read_csv(main_path+'output/participant1a_1b_infobox.csv')
participant1a_2_infobox=pd.read_csv(main_path+'output/participant1a_2_infobox.csv')
participant1a_2a_infobox=pd.read_csv(main_path+'output/participant1a_2a_infobox.csv')
participant1a_2b_infobox=pd.read_csv(main_path+'output/participant1a_2b_infobox.csv')
participant1a_3_infobox=pd.read_csv(main_path+'output/participant1a_3_infobox.csv')
participant1a_3a_infobox=pd.read_csv(main_path+'output/participant1a_3a_infobox.csv')
participant1a_3b_infobox=pd.read_csv(main_path+'output/participant1a_3b_infobox.csv')

war_participant_infobox=participant1_infobox.append(participant2_infobox)
war_participant_infobox=war_participant_infobox.append(participant3_infobox)
war_participant_infobox=war_participant_infobox.append(participant4_infobox)
war_participant_infobox=war_participant_infobox.append(participant5_infobox)
war_participant_infobox=war_participant_infobox.append(participant1a_1_infobox)
war_participant_infobox=war_participant_infobox.append(participant1a_1a_infobox)
war_participant_infobox=war_participant_infobox.append(participant1a_1b_infobox)
war_participant_infobox=war_participant_infobox.append(participant1a_2_infobox)
war_participant_infobox=war_participant_infobox.append(participant1a_2a_infobox)
war_participant_infobox=war_participant_infobox.append(participant1a_2b_infobox)
war_participant_infobox=war_participant_infobox.append(participant1a_3_infobox)
war_participant_infobox=war_participant_infobox.append(participant1a_3a_infobox)
war_participant_infobox=war_participant_infobox.append(participant1a_3b_infobox)


war_participant_infobox.drop('Unnamed: 0',axis=1, inplace=True)
war_participant_infobox=war_participant_infobox.sort_index(by = ["wid",'side'])

war_participant_infobox['x1']=war_participant_infobox['participants'].str.extract(r'(war$)',expand=False)
war_participant_infobox=war_participant_infobox.fillna(0)          
war_participant_infobox=war_participant_infobox.loc[war_participant_infobox['x1']==0]
war_participant_infobox.drop('x1',axis=1, inplace=True)



war_participant_infobox['first_part']=war_participant_infobox['participants'].str.extract(r'(.*)\|',expand=False)
war_participant_infobox=war_participant_infobox.fillna(0)

def function(a,b):
    if a!=0:
        return a
    else:
        return b

war_participant_infobox['participants']=war_participant_infobox.apply(lambda x: function(x.first_part,x.participants), axis=1)
war_participant_infobox['participants']=war_participant_infobox['participants'].str.strip()
war_participant_infobox=war_participant_infobox.replace(0,np.nan)
war_participant_infobox.drop('first_part',axis=1, inplace=True)
war_participant_infobox.to_csv(main_path+'output/war_participant_infobox.csv',index=False)

x=war_participant_infobox.loc[:,'wid']
x.drop_duplicates(inplace=True)