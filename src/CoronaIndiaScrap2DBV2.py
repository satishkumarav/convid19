import pandas as pd
import schedule
import time
from src.Utils import TimescaleUtil
from src.Utils import Util
from src.Utils.DataAdapterFactory import DataSourceAdapter


# Version 0.5
def IndiaScrap2DB():
    # Get India data
    try:
        dfRegion, metrics = Util.getIndiaData()

        # Insert statewise records for India
        TimescaleUtil.insert2spread(dfRegion, "India", TimescaleUtil.LocationType.State,
                                    usetimestampfromdataframe=False,
                                    cleanbeforeload=False, deletetodaysrecordsforparentlocation=True, source="MOHI")
        # Insert India Summary Record
        columns = TimescaleUtil.getSpreadColumnNames()
        dfRegion = pd.DataFrame(columns=columns)
        dfRegion = dfRegion.append(metrics, ignore_index=True)

        TimescaleUtil.insert2spread(dfRegion, "World", TimescaleUtil.LocationType.Country,
                                    usetimestampfromdataframe=False,
                                    cleanbeforeload=False, deletetodaysrecordsforlocation=True, location='India',
                                    source="MOHI")
    except Exception as e:
        print("Getting Error for India", e)


def Telangana2DB():
    dfRegion, dte = Util.getTelananaDistrictData()
    # Insert statewise records for India
    TimescaleUtil.insert2spread(dfRegion, "Telangana", TimescaleUtil.LocationType.District,
                                usetimestampfromdataframe=True,
                                cleanbeforeload=False, deletetodaysrecordsforparentlocation=False, source="MOHT")
    print("Done & Dusted: Data should be there in db")


def Rajasthan2DB():
    try:
        adapter = Util.getRajasthanData()
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
    except Exception as e:
        print("Getting Error for Rajasthan", e)


def Punjab2DB():
    try:
        adapter = Util.getPunjabData()
        # print(adapter.dfRegion)
        # print(adapter.dfSummary)
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
    except Exception as e:
        print("Getting Error for Punjab", e)


def World2DB():
    try:
        adapter = Util.getWorldData()
        print(adapter.dfRegion)
        print(adapter.dfSummary)
        # Since it is district wise records, cleanup all records having same locationparent and source
        # TimescaleUtil.insert2spread(adapter.dfRegion, adapter.parentregion, TimescaleUtil.LocationType.Country,
        #                             usetimestampfromdataframe=True,
        #                             cleanbeforeload=False, deletetodaysrecordsforparentlocation=True,
        #                             source=adapter.sourceofdata)
        #
        # # Since it is state record, cleanup  record with location supplied and source
        # TimescaleUtil.insert2spread(adapter.dfSummary, "World", TimescaleUtil.LocationType.World,
        #                             usetimestampfromdataframe=True,
        #                             cleanbeforeload=False, deletetodaysrecordsforlocation=True,
        #                             source=adapter.sourceofdata)
        # print("Done & Dusted: Data should be there in db")
    except Exception as e:
        print("Getting Error for World", e)


def scrapstates():
    # print("Punjab")
    Punjab2DB()
    # print("Rajasthan")
    # print("India")
    IndiaScrap2DB()


# World2DB()


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
    scrapstates()


    # def test():
    #     # Punjab2DB()


    # Invoke in on-demand mode
    # ondemand()


scheduleIT()
