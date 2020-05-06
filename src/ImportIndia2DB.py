import pandas as pd
import pytz
import schedule
import time
from datetime import datetime

from src.Utils import TimescaleUtil
from src.Utils import Util


def getInt(value):
    try:
        return int(value)
    except:
        return 0


def loadfromcsvformatV1(region_ts_file, locationParent, locationtype, clean):
    columns = TimescaleUtil.getSpreadColumnNames()

    # Write region wise data in Timeseries file
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

        TimescaleUtil.insert2spread(dfRegion, locationParent, locationtype, usetimestampfromdataframe=True,
                                    cleanbeforeload=clean)
    except ValueError:
        print("Error : ", ValueError)

    # dfRegion = pd.DataFrame(data=tabledata, columns=columns)


def isNaN(num):
    return num != num


def loadfromcsvformatV2(region_ts_file, locationParent, locationtype, clean=False, datefrom='2020-01-01',
                        dateto="2020-04-18"):
    columns = TimescaleUtil.getSpreadColumnNames()

    # Write region wise data in Timeseries file
    # utc = pytz.utc
    # timestamp = datetime.date(datetime.now(tz=utc))

    try:
        tabledata = pd.read_csv(region_ts_file)

        dfRegion = pd.DataFrame(columns=columns)
        for key, row in tabledata.iterrows():
            rwdata = {};
            if not isNaN(row[0]):
                dtecompare = datetime.strptime(row[0], '%Y-%m-%d').strftime('%Y-%m-%d')
                dtestr = datetime.strptime(row[0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
                location = row[1]
                confirmed = getInt(row[2])
                recovered = getInt(row[3])
                dead = getInt(row[4])
                motality = 0

                rwdata[TimescaleUtil.ColoumnName.TimestampUTC.value] = dtestr
                rwdata[TimescaleUtil.ColoumnName.NameofState_UT.value] = location
                rwdata[TimescaleUtil.ColoumnName.Totalconfirmed.value] = confirmed
                rwdata[TimescaleUtil.ColoumnName.Death.value] = dead
                rwdata[TimescaleUtil.ColoumnName.Cured_Discharged_Migrated.value] = recovered
                rwdata[TimescaleUtil.ColoumnName.TotalConfirmedcases_IndianNational.value] = 0
                rwdata[TimescaleUtil.ColoumnName.TotalConfirmedcases_ForeignNational.value] = 0

                if confirmed > 0:
                    motality = format(dead / confirmed, '.2f')
                rwdata[TimescaleUtil.ColoumnName.MortalityRate.value] = motality

                if datefrom <= dtecompare <= dateto:
                    dfRegion = dfRegion.append(rwdata, ignore_index=True)

                    # if not location == 'India':
                    #     dfRegion = dfRegion.append(rwdata, ignore_index=True)
                    # else:
                    #     print("Igonore row for ",location)

        print(dfRegion)
        TimescaleUtil.insert2spread(dfRegion, locationParent, locationtype, usetimestampfromdataframe=True,
                                    cleanbeforeload=clean,source="JHCSEE")
    except ValueError:
        print("Error : ", ValueError)


def loadhistoricalIndiastatedata():
    # load statewise data
    print('Loading statewide data for India')
    locationParent = "India"
    locationType = TimescaleUtil.LocationType.State
    region_ts_file = "../datasets/india/covid_19_indiav1.csv"
    loadfromcsvformatV1(region_ts_file, locationParent, locationType, True)
    print("done")
    # Load countrywise data
    print('Loading India wide data')
    # locationParent = "World"
    locationType = TimescaleUtil.LocationType.Country
    region_ts_file = "../datasets/india/covid_19_India_summary.csv"
    loadfromcsvformatV1(region_ts_file, "World", locationType, True)
    print('Done')


def loadhistoricalWorldData():
    datafrom = '2020-04-28'  # Load from this date
    datato = '2020-04-30'  # Load from this date
    region_ts_file = "../datasets/world/all.csv"
    locationParent = "World"
    locationType = TimescaleUtil.LocationType.Country
    loadfromcsvformatV2(region_ts_file, "World", locationType, False, datafrom, datato)
    print('Done')


# Invoke in on-demand mode
# loadhistoricalIndiastatedata()
loadhistoricalWorldData()
