import pandas as pd
import pytz
import schedule
import time
from datetime import datetime

from Utils import TimescaleUtil
from Utils import Util


def getInt(value):
    try:
        return int(value)
    except:
        return 0


def loadfromcsv(region_ts_file, locationParent, locationtype,clean):
    # Extract Statewise table data
    # columns = ['TimestampUTC', 'NameofState/UT', 'Totalconfirmed', 'Death', 'Cured/Discharged/Migrated',
    #            'TotalConfirmedcases(IndianNational)', 'TotalConfirmedcases(ForeignNational)', 'MortalityRate']

    columns = TimescaleUtil.getSpreadColumnNames()

    # Wite region wise data in Timeseries file
    utc = pytz.utc
    timestamp = datetime.date(datetime.now(tz=utc))

    try:
        tabledata = pd.read_csv(region_ts_file)

        dfRegion = pd.DataFrame(columns=columns)
        for key, row in tabledata.iterrows():
            rwdata = {};
            datestr = row[1] + " " + row[2]
            rwtimestamp = Util.getUTC(datestr, '%d/%m/%y %I:%M %p')
            rwdata[TimescaleUtil.ColoumnName.TimestampUTC.value] = rwtimestamp
            rwdata[TimescaleUtil.ColoumnName.NameofState_UT.value] = row[3]
            rwdata[TimescaleUtil.ColoumnName.Totalconfirmed.value] = int(row[8])
            rwdata[TimescaleUtil.ColoumnName.Death.value] = getInt(row[7])
            rwdata[TimescaleUtil.ColoumnName.Cured_Discharged_Migrated.value] = getInt(row[6])
            rwdata[TimescaleUtil.ColoumnName.TotalConfirmedcases_IndianNational.value] = getInt(row[4])
            rwdata[TimescaleUtil.ColoumnName.TotalConfirmedcases_ForeignNational.value] = getInt(row[5])
            rwdata[TimescaleUtil.ColoumnName.MortalityRate.value] = getInt(getInt(row[7]) / getInt(row[8]))
            dfRegion = dfRegion.append(rwdata, ignore_index=True)

        print(dfRegion.head(10))
        TimescaleUtil.insert2spread(dfRegion, locationParent, locationtype, usetimestampfromdataframe=True,
                                    cleanbeforeload=clean)
    except ValueError:
        print("Error : ", ValueError)

    # dfRegion = pd.DataFrame(data=tabledata, columns=columns)


def scheduleIT():
    # Define scheduler job
    locationParent = "India"
    locationType = TimescaleUtil.LocationType.State
    region_ts_file = "../datasets/india/covid_19_indiav1.csv"
    loadfromcsv(region_ts_file, locationParent, locationType)
    schedule.every(3).hours.do(loadfromcsv(region_ts_file, locationParent, locationType))

    # run the scheduler
    while True:
        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        time.sleep(1)


def ondemand():
    #load statewise data
    print ('Loading statewide data for India')
    locationParent = "India"
    locationType = TimescaleUtil.LocationType.State
    region_ts_file = "../datasets/india/covid_19_indiav1.csv"
    loadfromcsv(region_ts_file, locationParent, locationType,True)
    print("done")
    # Load countrywise data
    print('Loading India wide data')
    #locationParent = "World"
    locationType = TimescaleUtil.LocationType.Country
    region_ts_file = "../datasets/india/covid_19_India_summary.csv"
    loadfromcsv(region_ts_file, "World", locationType,True)
    print ('Done')



# Invoke in on-demand mode
ondemand()
