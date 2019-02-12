# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 15:33:18 2019

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
commander1_infobox=pd.read_csv(main_path+'output/commander1_infobox.csv')
commander2_infobox=pd.read_csv(main_path+'output/commander2_infobox.csv')
commander3_infobox=pd.read_csv(main_path+'output/commander3_infobox.csv')
commander4_infobox=pd.read_csv(main_path+'output/commander4_infobox.csv')
commander5_infobox=pd.read_csv(main_path+'output/commander5_infobox.csv')

war_commander_infobox=commander1_infobox.append(commander2_infobox)
war_commander_infobox=war_commander_infobox.append(commander3_infobox)
war_commander_infobox=war_commander_infobox.append(commander4_infobox)
war_commander_infobox=war_commander_infobox.append(commander5_infobox)

war_commander_infobox=war_commander_infobox.sort_index(by = ["wid",'side'])

war_commander_infobox['x1']=war_commander_infobox['commander'].str.extract(r'(war$)',expand=False)
war_commander_infobox=war_commander_infobox.fillna(0)          
war_commander_infobox=war_commander_infobox.loc[war_commander_infobox['x1']==0]
war_commander_infobox.drop('x1',axis=1, inplace=True)


war_commander_infobox=war_commander_infobox.replace('0',np.nan)
war_commander_infobox=war_commander_infobox.replace(0,np.nan)
war_commander_infobox.to_csv(main_path+'output/war_commander_infobox.csv',index=False)

x=war_commander_infobox.loc[:,'wid']
x.drop_duplicates(inplace=True)
x=list(x)
missing_wid=writer[~writer.wid.isin(x)]
