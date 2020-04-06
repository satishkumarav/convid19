import pandas as pd
import pytz
from datetime import datetime
from pathlib import Path

from src.Utils import Util

print('Webscrapper to CSV Execution Begin')
# Get India data
dfRegion,metrics = Util.getIndiaData()

# Initialize the file names
dataPath = Path.parent
# print("Data Path: " ,dataPath)

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


print("End of Execution")