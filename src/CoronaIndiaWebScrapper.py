from lxml import html
import requests
import urllib
import bs4
import ssl
import lxml
import pandas as pd
from pathlib import Path
import os
import pytz
from datetime import datetime


url = 'https://www.mohfw.gov.in/'
# Create unverified SSL context to avoid SSL Invalid Certificate errors
context = ssl._create_unverified_context()
page = urllib.request.urlopen(url, context=context)

# Create html tree scrapper
soup = bs4.BeautifulSoup(page, features="lxml")
lblairportscreened = 'Total number of passengers screened at airport '
screenedKey = 'Airpot Screened'
# dictionary to hold the metrics
metrics = {}

# Scan the page for total passangers scanned
infoblock = soup.select_one("div.information_row")
spantags = infoblock.find_all("span")
for span in spantags:
    value = span.text.strip().replace(" ", "").replace(",", "")
    metrics[screenedKey] = value
    break

# Extract Statewise table data
columns = []
tabledata = []
contentBlock = soup.select_one("div.content.newtab")
table = contentBlock.select_one("div.table-responsive")
trtags = table.find_all("tr")
count = 0
size = len(trtags) - 1
for trtag in trtags:
    if count == 0:
        cont = trtag.text.strip().replace("\n", "~")
        # Header row
        coltokens = cont.split("~")
        colCount = 0
        for coltoken in coltokens:
            if colCount > 0:
                thvalue = coltoken.strip().replace(" ", "").replace(",", "")
                columns.append(thvalue)
            colCount = colCount + 1
    elif 0 < count < size:
        # data row
        rwdata = []
        cont = trtag.text.strip().replace("\n", "~")
        coltokens = cont.split("~")
        colCount = 0
        for coltoken in coltokens:
            if colCount > 0:
                tdvalue = coltoken.strip().replace(" ", "").replace(",", "").replace("#","")
                try:
                    rwdata.append(int(tdvalue))
                except ValueError:
                    rwdata.append(tdvalue)
            colCount = colCount + 1
        tabledata.append(rwdata)
    count = count + 1

# Create Pandas Dataframe for data warrangling
dfRegion = pd.DataFrame(data=tabledata, columns=columns)

# Remove first column


# Compute statewise ToTal confirmed and motality rate
dfRegion['Totalconfirmed'] = dfRegion['TotalConfirmedcases(IndianNational)'] + dfRegion[
    'TotalConfirmedcases(ForeignNational)']
dfRegion["Totalconfirmed"] = pd.to_numeric(dfRegion["Totalconfirmed"])
dfRegion['MortalityRate'] = round(dfRegion['Death'] * 100 / (dfRegion['Totalconfirmed']), 2)
# Sort by the Statename
sortby = columns[1]
dfRegion.sort_values(by=sortby)
df = dfRegion.reset_index(drop=True)
dfRegion.drop([1])

# Compute metric at national level
sum_columns = dfRegion.sum(axis=0)
metrics['TotalConfirmed'] = int(sum_columns['TotalConfirmedcases(IndianNational)']) + int(sum_columns['TotalConfirmedcases(ForeignNational)'])
metrics['TotalDeaths'] = int(sum_columns['Death'])
metrics['TotalRecovered'] = int(sum_columns['Cured/Discharged/Migrated'])
metrics['TotalLocalTransmissions'] = int(sum_columns['TotalConfirmedcases(IndianNational)'])
metrics['TotalExternalTransmission'] = int(sum_columns['TotalConfirmedcases(ForeignNational)'])
metrics['MortalityRate%'] = int(round(sum_columns['Death'] * 100 / sum_columns['Totalconfirmed'], 2))


# Initialize the file names
dataPath = Path.parent
print("Data Path: " ,dataPath)

metrics_ts_file = "../datasets/india/metrics_ts.csv"
metrics_file = "../datasets/india/metrics.csv"
region_file = Path("../datasets/india/regions.csv")
region_ts_file = "../datasets/india/regions_ts.csv"

# Write current region information
region_data_csv = dfRegion.to_csv(index=False)
f = open(region_file, "w")
f.write(region_data_csv)
f.close()

#Wite region wise data in Timeseries file
utc = pytz.utc
timestamp = datetime.date(datetime.now(tz=utc))
dfRegion.insert(0, "Date", timestamp)
dfRegion.set_index("Date")

try:
    tsdata = pd.read_csv(region_ts_file)
    frames = [tsdata, dfRegion]
    tsdata.drop(tsdata[tsdata['Date'] >= timestamp.strftime("%Y-%m-%d")].index, inplace=True)
    tsdata = pd.concat(frames,sort=True,ignore_index=True)
except ValueError:
    tsdata = dfRegion

# Write to a file
f = open(region_ts_file, "w")
f.write(tsdata.to_csv(index=False,columns=["Date","NameofState/UT","Totalconfirmed","TotalConfirmedcases(ForeignNational)","TotalConfirmedcases(IndianNational)","Cured/Discharged/Migrated","Death","MortalityRate"]))
f.close()

# Preview the first 5 lines of the loaded data

# Write metrics information to CSV
headerstr=""
mtrcstr=""
for k,v in metrics.items():
    if mtrcstr=="":
        headerstr=k
        mtrcstr=v
    else:
        headerstr=headerstr + "," + k
        mtrcstr = mtrcstr + "," + str(v)



#Write dictionary to file
f = open(metrics_file, "w")
f.writelines(headerstr + '\n')
f.writelines(mtrcstr + '\n')
f.close()

#Write Dictionary TS File
try:
    tsdata = pd.read_csv(metrics_ts_file)
    tsdata.drop(tsdata[tsdata['Date'] >= timestamp.strftime("%Y-%m-%d")].index, inplace=True)
    tsdata = pd.concat(frames,sort=True,ignore_index=True)
except ValueError:
    f = open(metrics_ts_file, "w")
    f.writelines( "Date," + headerstr + '\n')
    f.writelines(timestamp.strftime("%Y-%m-%d") + ","+ mtrcstr + '\n')
    f.close()


print("End...")
