import pandas as pd
import schedule
import time
from src.Utils import Util
from src.Utils import TimescaleUtil

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


def scheduleIT():
    # Define scheduler job
    schedule.every(3).hours.do(IndiaScrap2DB)

    # run the scheduler
    while True:
        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        time.sleep(1)


def ondemand():
    IndiaScrap2DB()


def test():
    location = 'India'
    print(TimescaleUtil.getLocations(jsonformat=True,location=location))


# Invoke in on-demand mode
ondemand()
#scheduleIT()test()

#test()
