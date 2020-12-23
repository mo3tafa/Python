import pandas as pd
import numpy as np
import pyodbc
import datetime as dt
import datetime 
import re
from math import floor

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=10.186.179.11;'
                      'Database=DashboardDB;' 
                     'UID=dadmin;PWD=Sadmin@123;') 
DetailOutage2g = pd.read_sql_query('Select * from [AvailabilityDetailOutage2G_NowRouz]', conn)
DetailOutage3g = pd.read_sql_query('Select * from [AvailabilityDetailOutage3G_NowRouz]', conn)
DetailOutage4g = pd.read_sql_query('Select * from [AvailabilityDetailOutage4G_NowRouz]', conn)
conn.close()

CellReff = pd.read_excel('G:\\Dashboard\\Dashboard\\Fluctuation Sites\\Cell Reff.xlsx')
NumberOfCellsPerSite = pd.read_excel('G:\\Dashboard\\Dashboard\\Fluctuation Sites\\Number of Cells Per Site-2G3G4G.xlsx', sheet_name = 'Sheet1')
#Concat 3 technology
DetailOutage = pd.concat([DetailOutage2g, DetailOutage3g, DetailOutage4g], axis = 0, sort = True)

DetailOutage['CellName'] = DetailOutage['CellName'].astype(str)
date = str(pd.to_datetime(DetailOutage['EndOfOutage']).dt.date.max())
Fluct_Date = pd.to_datetime(DetailOutage['EndOfOutage']).dt.date
DetailOutage.loc[DetailOutage['CellName'].str[:2] == 'XH' , 'CellName'] = 'TH' + DetailOutage.loc[DetailOutage['CellName'].str[:2] == 'XH' , 'CellName'].str[2:]
#Sort by StartOfOutage and then CellName 
DetailOutage = DetailOutage.sort_values(by = ['StartOfOutage'], ascending=True)
DetailOutage = DetailOutage.sort_values(by = ['CellName'], ascending=True)
#Delete incorrect Cell_Names(Numbers, TEST, MCIKANA,...)
pat_name = '\A[A-Z]{2}[1-9]*[GLU]{0,3}[0-9]{4}'#'[A-F]{1}'
DetailOutage['CellName'] = DetailOutage['CellName'].astype(str)
DetailOutage['CellName'] = DetailOutage['CellName'].str.upper()
DetailOutage['Sector'] = DetailOutage['CellName'].apply(lambda x:  ''.join(re.findall(pat_name, x)))
DetailOutage = DetailOutage[DetailOutage['Sector'] != '']
#Create SiteName
DetailOutage['SiteName'] = np.where(len(DetailOutage['Sector']) > 7, DetailOutage['Sector'].str[:8], DetailOutage['Sector'].str[:6])
DetailOutage['Province Index'] = DetailOutage['SiteName'].str[:2]
#Location
pat_tech = '[1-9]{1}[GLU]{1}'
DetailOutage['Location'] = DetailOutage['SiteName'].apply(lambda x: re.sub(pat_tech,'',x))
DetailOutage = DetailOutage[DetailOutage['Location'] != '']

DetailOutage = DetailOutage[['SiteName','CellName','Location','NE','Technology','Province Index','StartOfOutage','EndOfOutage','Down_Time(Seconds)','Hour']]
#Create Outage Day
DetailOutage['Outage Day'] = pd.to_datetime(DetailOutage['StartOfOutage']).dt.date
#Create Down_Count
DetailOutageUp60ns = DetailOutage[DetailOutage['Down_Time(Seconds)'] > 60]
DetailOutageUp60ns['Down_Count'] = 1
DetailOutageUp60ns['Sum_Down_Time'] = DetailOutageUp60ns['Down_Time(Seconds)']
DetailOutage2 = DetailOutageUp60ns.groupby(['CellName','Outage Day'], as_index = False).agg({'Down_Count':'sum','Sum_Down_Time' : 'sum'})
DetailOutageUp60ns = DetailOutageUp60ns.drop(['Down_Count','Sum_Down_Time'], axis = 1)
DetailOutageUp60ns = DetailOutageUp60ns.merge(DetailOutage2, on = ['CellName','Outage Day'], how = 'left')
#Remove Duplicates on CellName and OutageDay
DetailOutageUp60ns = DetailOutageUp60ns.drop_duplicates(subset =['CellName','Outage Day'],keep = 'first')
############متوسط تعداد قطعی سلهای هر سایت در یک روز 
DetailOutageUp60ns['AVG_Down_Count_Cell_PerDay'] = DetailOutageUp60ns['Down_Count']
DetailOutage3 = DetailOutageUp60ns.groupby(['SiteName','Outage Day'], as_index = False).agg({'AVG_Down_Count_Cell_PerDay':'mean'})
DetailOutageUp60ns = DetailOutageUp60ns.drop(['AVG_Down_Count_Cell_PerDay'], axis = 1)
DetailOutageUp60ns = DetailOutageUp60ns.merge(DetailOutage3, on = ['SiteName','Outage Day'], how = 'left')
#Filter on "Down Count" for more than or equalt to 2
DetailOutageUp60nsUp2DC = DetailOutageUp60ns[DetailOutageUp60ns['Down_Count'] > 1]
#Site Level
##Average Per Cell
###Sort on "Outage Day" from oldest to Newest.
DetailOutageUp60nsUp2DC = DetailOutageUp60nsUp2DC.sort_values(by = ['Outage Day'])
###Sort on "Site Name" from A to Z.
DetailOutageUp60nsUp2DC = DetailOutageUp60nsUp2DC.sort_values(by = ['SiteName'])
#Calculate the "_Fluc_Cellof_each_Site","AVG Down Count PER CELL" and " AVG (Sum Down Time(Second)) PER CELL"
DetailOutageUp60nsUp2DC['Fluc_Cellof_each_Site'] = 1
DetailOutageUp60nsUp2DC_PC = DetailOutageUp60nsUp2DC.groupby(['SiteName','Outage Day'], as_index = False).agg({'Down_Count':'mean','Sum_Down_Time' : 'mean', 'Fluc_Cellof_each_Site' : 'sum'})
DetailOutageUp60nsUp2DC_PerCell = DetailOutageUp60nsUp2DC.drop(['Down_Count','Sum_Down_Time', 'Fluc_Cellof_each_Site'], axis = 1)
DetailOutageUp60nsUp2DC_PerCell = DetailOutageUp60nsUp2DC_PerCell.merge(DetailOutageUp60nsUp2DC_PC, on = ['SiteName','Outage Day'], how = 'left')
#Remove Duplicates on SiteName and OutageDay
DetailOutageUp60nsUp2DC_PerCell = DetailOutageUp60nsUp2DC_PerCell.drop_duplicates(subset =['SiteName','Outage Day'],keep = 'first')
#Calculate the "_Fluc_Cellof_each_Site","AVG Down Count PER CELL" and " AVG (Sum Down Time(Second)) PER CELL"
DetailOutageUp60nsUp2DC['Fluc_Cellof_each_Site'] = 1
DetailOutageUp60nsUp2DC_PC = DetailOutageUp60nsUp2DC.groupby(['SiteName','Outage Day'], as_index = False).agg({'Down_Count':'mean','Sum_Down_Time' : 'mean', 'Fluc_Cellof_each_Site' : 'sum'})
DetailOutageUp60nsUp2DC_PerCell = DetailOutageUp60nsUp2DC.drop(['Down_Count','Sum_Down_Time', 'Fluc_Cellof_each_Site'], axis = 1)
DetailOutageUp60nsUp2DC_PerCell = DetailOutageUp60nsUp2DC_PerCell.merge(DetailOutageUp60nsUp2DC_PC, on = ['SiteName','Outage Day'], how = 'left')
#Remove Duplicates on SiteName and OutageDay
DetailOutageUp60nsUp2DC_PerCell = DetailOutageUp60nsUp2DC_PerCell.drop_duplicates(subset =['SiteName','Outage Day'],keep = 'first')
#Add Number Of Cells Per Site
DetailOutageUp60nsUp2DC_PerCell = DetailOutageUp60nsUp2DC_PerCell.merge(NumberOfCellsPerSite, on = ['SiteName'], how = 'left')
# Outage(Site Level Or Cell Level)
DetailOutageUp60nsUp2DC_PerCell['Site or Cell']=np.where(DetailOutageUp60nsUp2DC_PerCell['Fluc_Cellof_each_Site'] > (0.75 * DetailOutageUp60nsUp2DC_PerCell['Cell_Per_Site']), 'SITE', 'CELL')
#Filter on"SITE" 
DetailOutageUp60nsUp2DC_PerCell_SITE = DetailOutageUp60nsUp2DC_PerCell[DetailOutageUp60nsUp2DC_PerCell['Site or Cell'] == 'SITE']
DetailOutageUp60nsUp2DC_PerCell_SITE['NumOfFlucDay'] = 1
DetailOutageUp60nsUp2DC_PerCell_SITE['AvgNumOfOutagePerDay'] = DetailOutageUp60nsUp2DC_PerCell_SITE['AVG_Down_Count_Cell_PerDay']
DetailOutageUp60nsUp2DC_PerCell_S = DetailOutageUp60nsUp2DC_PerCell_SITE.groupby(['SiteName'], as_index = False).agg({'NumOfFlucDay' : 'sum', 'AvgNumOfOutagePerDay' : 'mean'})
DetailOutageUp60nsUp2DC_PerCell_SITE = DetailOutageUp60nsUp2DC_PerCell_SITE.drop(['NumOfFlucDay', 'AvgNumOfOutagePerDay'], axis = 1)
DetailOutageUp60nsUp2DC_PerCell_SITE = DetailOutageUp60nsUp2DC_PerCell_SITE.merge(DetailOutageUp60nsUp2DC_PerCell_S, on = ['SiteName'], how = 'left')
DetailOutageUp60nsUp2DC_PerCell_SITE['AvgNumOfOutagePerDay'] = DetailOutageUp60nsUp2DC_PerCell_SITE['AvgNumOfOutagePerDay'].apply(np.floor)

#Remove Duplicates on "Site Name"
DetailOutageUp60nsUp2DC_PerCell_SITE_Loc = DetailOutageUp60nsUp2DC_PerCell_SITE.drop_duplicates(subset =['SiteName'],keep = 'first')
#Filter on "#_Flactuating_Daysof each_Site" for "more than 5"
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day = DetailOutageUp60nsUp2DC_PerCell_SITE_Loc[DetailOutageUp60nsUp2DC_PerCell_SITE_Loc['NumOfFlucDay'] > 4]
#Add TAG Number Of Fluctuating Day
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['TAG'] = np.where(DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['NumOfFlucDay'] <= 10,'Between 05 and 10 days',\
                                                     np.where(DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['NumOfFlucDay'] <= 20,'Between 11 and 20 days',\
                                                                                                                                'Between 21 and 30 days'))
# Add Lat&Long and Category
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.merge(CellReff, on = ['Location'], how = 'left')
# Create Proince:
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 05 and 10 days'] = np.where(DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['TAG'] == 'Between 05 and 10 days', 1, 0)
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 11 and 20 days'] = np.where(DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['TAG'] == 'Between 11 and 20 days', 1, 0)
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 21 and 30 days'] = np.where(DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['TAG'] == 'Between 21 and 30 days', 1, 0)

Province = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.groupby(['Province Index'], as_index = False).agg({'Between 05 and 10 days' : 'sum', 'Between 11 and 20 days' : 'sum', 'Between 21 and 30 days' : 'sum'})
Province['Grand Total'] = Province['Between 05 and 10 days'] + Province['Between 11 and 20 days'] + Province['Between 21 and 30 days']
Province['Date'] = date
Fluct_Province = Province
#Create Dashboard
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day[['SiteName','CellName','Location','Province Index','Technology','NumOfFlucDay','AvgNumOfOutagePerDay','TAG','Between 05 and 10 days','Between 11 and 20 days','Between 21 and 30 days','Latitude','Longitude','Category','Site Type','Name']]
###Sort on "AvgNumOfOutagePerDay" from Largest to Smalest.
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.sort_values(by = ['AvgNumOfOutagePerDay'], ascending = False)
###Sort on "NumOfFlucDay" from Largest to Smalest.
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.sort_values(by = ['NumOfFlucDay'], ascending = False)
###Sort on "TAG" from Z to A.
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.sort_values(by = ['TAG'], ascending = False)
#Remove Duplicates on "Location"
DetailOutageUp60nsUp2DC_PerCell_SITE_Locs = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.drop_duplicates(subset =['Location'],keep = 'first')

col = ['Location','2G Site','3G Site','4G Site','Golden Site','City Site','Road Site']
ind = ['Between 05 and 10 days','Between 11 and 20 days','Between 21 and 30 days'] 
Dashboard = pd.DataFrame(columns=col,index=ind)

Dashboard['Location']['Between 05 and 10 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Locs.loc[DetailOutageUp60nsUp2DC_PerCell_SITE_Locs['Between 05 and 10 days'] != 0,'Location'].count()
Dashboard['Location']['Between 11 and 20 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Locs.loc[DetailOutageUp60nsUp2DC_PerCell_SITE_Locs['Between 11 and 20 days'] != 0,'Location'].count()
Dashboard['Location']['Between 21 and 30 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Locs.loc[DetailOutageUp60nsUp2DC_PerCell_SITE_Locs['Between 21 and 30 days'] != 0,'Location'].count()

###2G Site:
Dashboard['2G Site']['Between 05 and 10 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 05 and 10 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Technology'] == '2G')),'Between 05 and 10 days'].sum()
Dashboard['2G Site']['Between 11 and 20 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 11 and 20 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Technology'] == '2G')),'Between 11 and 20 days'].sum()
Dashboard['2G Site']['Between 21 and 30 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 21 and 30 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Technology'] == '2G')),'Between 21 and 30 days'].sum()

###3G Site:
Dashboard['3G Site']['Between 05 and 10 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 05 and 10 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Technology'] == '3G')),'Between 05 and 10 days'].sum()
Dashboard['3G Site']['Between 11 and 20 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 11 and 20 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Technology'] == '3G')),'Between 11 and 20 days'].sum()
Dashboard['3G Site']['Between 21 and 30 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 21 and 30 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Technology'] == '3G')),'Between 21 and 30 days'].sum()

###4G Site:
Dashboard['4G Site']['Between 05 and 10 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 05 and 10 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Technology'] == '4G')),'Between 05 and 10 days'].sum()
Dashboard['4G Site']['Between 11 and 20 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 11 and 20 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Technology'] == '4G')),'Between 11 and 20 days'].sum()
Dashboard['4G Site']['Between 21 and 30 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 21 and 30 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Technology'] == '4G')),'Between 21 and 30 days'].sum()

###Category(Golden and Silver Site):
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Category'] != 'Golden', 'Category'] = 'Silver'
Dashboard['Golden Site']['Between 05 and 10 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 05 and 10 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Category'] == 'Golden')),'Between 05 and 10 days'].sum()
Dashboard['Golden Site']['Between 11 and 20 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 11 and 20 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Category'] == 'Golden')),'Between 11 and 20 days'].sum()
Dashboard['Golden Site']['Between 21 and 30 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 21 and 30 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Category'] == 'Golden')),'Between 21 and 30 days'].sum()

###Site Type(Road and City Site):
DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Site Type'] != 'Road', 'Site Type'] = 'City'

Dashboard['City Site']['Between 05 and 10 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 05 and 10 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Site Type'] == 'City')),'Between 05 and 10 days'].sum()
Dashboard['City Site']['Between 11 and 20 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 11 and 20 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Site Type'] == 'City')),'Between 11 and 20 days'].sum()
Dashboard['City Site']['Between 21 and 30 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 21 and 30 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Site Type'] == 'City')),'Between 21 and 30 days'].sum()

Dashboard['Road Site']['Between 05 and 10 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 05 and 10 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Site Type'] == 'Road')),'Between 05 and 10 days'].sum()
Dashboard['Road Site']['Between 11 and 20 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 11 and 20 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Site Type'] == 'Road')),'Between 11 and 20 days'].sum()
Dashboard['Road Site']['Between 21 and 30 days'] = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day.loc[((DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Between 21 and 30 days'] != 0) & (DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day['Site Type'] == 'Road')),'Between 21 and 30 days'].sum()
Dashboard['Label'] = ind
Fluct_Dashboard = Dashboard
#Create Map
Map = DetailOutageUp60nsUp2DC_PerCell_SITE_Up5Day[['Province Index','Location','NumOfFlucDay','AvgNumOfOutagePerDay','TAG','Category','Site Type','Name','Latitude','Longitude']]
Map = Map.sort_values(by = ['TAG'], ascending = False)
Map = Map.drop_duplicates(subset =['Location'],keep = 'first')
Fluct_Map = Map.sort_values(by = ['Location'], ascending = True)
