import pandas as pd
import schedule
import time
from src.Utils import TimescaleUtil
from src.Utils import Util
from src.Utils.DataAdapterFactory import DataSourceAdapter


# Version 0.5
def IndiaScrap2DB():
    # Get India data
    dfRegion, metrics = Util.getIndiaData()

    # Insert statewise records for India
    TimescaleUtil.insert2spread(dfRegion, "India", TimescaleUtil.LocationType.State, usetimestampfromdataframe=False,
                                cleanbeforeload=False, deletetodaysrecordsforparentlocation=True)
    # Insert India Summary Record
    columns = TimescaleUtil.getSpreadColumnNames()
    dfRegion = pd.DataFrame(columns=columns)
    dfRegion = dfRegion.append(metrics, ignore_index=True)

    TimescaleUtil.insert2spread(dfRegion, "World", TimescaleUtil.LocationType.Country, usetimestampfromdataframe=False,
                                cleanbeforeload=False, deletetodaysrecordsforlocation=True, location='India')


def Telangana2DB():
    dfRegion, dte = Util.getTelananaDistrictData()
    # Insert statewise records for India
    TimescaleUtil.insert2spread(dfRegion, "Telangana", TimescaleUtil.LocationType.District,
                                usetimestampfromdataframe=True,
                                cleanbeforeload=False, deletetodaysrecordsforparentlocation=False, source="MOHT")
    print("Done & Dusted: Data should be there in db")


def Rajasthan2DB():
    adapter = Util.getRajasthanData()
    # Since it is districtwise records, cleanup all records having same locationparent and source
    TimescaleUtil.insert2spread(adapter.dfRegion, adapter.parentregion, TimescaleUtil.LocationType.District,
                                usetimestampfromdataframe=True,
                                cleanbeforeload=False, deletetodaysrecordsforparentlocation=True,
                                source=adapter.sourceofdata)

    # Since it is state record, cleanup  record with location supplied and source
    TimescaleUtil.insert2spread(adapter.dfSummary, "India", TimescaleUtil.LocationType.State,
                                usetimestampfromdataframe=True,
                                cleanbeforeload=False, deletetodaysrecordsforlocation=True,
                                source=adapter.sourceofdata)

    print("Done & Dusted: Data should be there in db")


def Punjab2DB():
    adapter = Util.getPunjabData()
    print(adapter.dfRegion)
    print(adapter.dfSummary)
    # Since it is district wise records, cleanup all records having same locationparent and source
    TimescaleUtil.insert2spread(adapter.dfRegion, adapter.parentregion, TimescaleUtil.LocationType.District,
                                usetimestampfromdataframe=True,
                                cleanbeforeload=False, deletetodaysrecordsforparentlocation=True,
                                source=adapter.sourceofdata)

    # Since it is state record, cleanup  record with location supplied and source
    TimescaleUtil.insert2spread(adapter.dfSummary, "India", TimescaleUtil.LocationType.State,
                                usetimestampfromdataframe=True,
                                cleanbeforeload=False, deletetodaysrecordsforlocation=True,
                                source=adapter.sourceofdata)

    print("Done & Dusted: Data should be there in db")

def scrapstates():
    Punjab2DB()
    Rajasthan2DB()
    IndiaScrap2DB()


def scheduleIT():
    # Define scheduler job
    schedule.every(1).hours.do(scrapstates)

    # run the scheduler
    while True:
        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        time.sleep(1)


def telangana2File():
    Util.telanganaWriteToday()


def ondemand():
    IndiaScrap2DB()


def test():
    # telangana2File()
    # Rajasthan2DB()
    Punjab2DB()


# Invoke in on-demand mode
# ondemand()
scheduleIT()
# test()
