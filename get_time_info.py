# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 21:20:34 2019

@author: leo
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Mar 13 23:20:17 2019

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



#################################commander1
info_time=writer.loc[:,['wid','title','tmp']]
def function0(a,b):
    if a!=0:
        return a
    else:
        return b

info_time['tmp']=info_time['tmp'].str.replace(r'\{\{Pufc\|1=\|date=.*?\}\}','')
info_time['tmp']=info_time['tmp'].str.replace(r'\{\{Dubious\|date=.*?\}\}','')
info_time['tmp']=info_time['tmp'].str.replace(r'\{\{page needed\|date=.*?\}\}','')


info_time['time']=info_time['tmp'].str.extract(r'(.*)',expand=False)


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




info_time['time']=info_time['time'].str.extract(r'(\| *date *=.*)',expand=False)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *place *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *conflict *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *casus *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *coordinates *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *part *of *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *image_size *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *result *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *map_type *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *image *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *territory *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)


info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *campaign *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)


info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *causes *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *cause *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *time *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *lat_deg *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *location *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *caption *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *commander1 *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *combatant1 *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

info_time['comb1_2']=info_time['time'].str.extract(r'(.*?)\| *status *=',expand=False)
info_time=info_time.fillna(0)
info_time['time']=info_time.apply(lambda x: function0(x.comb1_2,x.time), axis=1)
info_time.drop('comb1_2',axis=1, inplace=True)

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






#info_time['or']=info_time['time'].str.extract(r'( or )',expand=False)
#info_or=info_time[info_time['or']==' or ']

#info_time['special']=info_time['time'].str.extract(r'(\d{3,4}[–/-]\d{1,2}$)',expand=False)
#info_time['special']=info_time['special'].fillna(0)
#info_s=info_time[info_time['special']!=0]

 
 
 
 

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
#ad=bc
#need to change
 
#notwar
info_time_final=info_time_final[info_time_final['wid']!=288520]
info_time_final=info_time_final[info_time_final['wid']!=160665]
info_time_final=info_time_final[info_time_final['wid']!=160664]
info_time_final=info_time_final[info_time_final['wid']!=560948]
info_time_final=info_time_final[info_time_final['wid']!=2150520]
info_time_final=info_time_final[info_time_final['wid']!=16315254]
info_time_final=info_time_final[info_time_final['wid']!=207630]
info_time_final=info_time_final[info_time_final['wid']!=205658]
info_time_final['start_year']=info_time_final['start_year'].replace(0,np.nan)
info_time_final['end_year']=info_time_final['end_year'].replace(0,np.nan)

info_time_final.to_csv(main_path+'output/info_time.csv',index=False)








