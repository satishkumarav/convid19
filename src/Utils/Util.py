import bs4
import pandas as pd
import pytz
import ssl
import urllib
from datetime import datetime

from src.Utils import TimescaleUtil

# returns UTC Datatime object for given date
def getUTC(date_string, format='%d/%m/%y %I:%M %p'):
    my_date = datetime.strptime(date_string, format)
    localtz = pytz.timezone("Asia/Kolkata")
    local_datetime = localtz.localize(my_date, is_dst=None)
    utc_datetime = local_datetime.astimezone(pytz.utc)
    return utc_datetime

# Get Data for India
def getIndiaData():
    # Read Configuration Information
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
    # infoblock = soup.select_one("div.information_row")
    # spantags = infoblock.find_all("span")
    # for span in spantags:
    #     value = span.text.strip().replace(" ", "").replace(",", "")
    #     metrics[screenedKey] = value
    #     break

    # Extract Statewise table data
    columns = []
    tabledata = []
    #contentBlock = soup.select_one("div.content.newtab")
    #table = contentBlock.select_one("div.table-responsive")
    table = soup.find('table', {"class": "table table-striped"})  # Use dictionary to pass key : value pair
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
                    if thvalue.startswith('TotalConfirmedcases'):
                        thvalue="TotalConfirmedcases*"
                    columns.append(thvalue)
                colCount = colCount + 1
        elif 0 < count < size - 1:
            # data row
            rwdata = []
            cont = trtag.text.strip().replace("\n", "~")
            coltokens = cont.split("~")
            colCount = 0
            for coltoken in coltokens:
                if colCount > 0:
                    tdvalue = coltoken.strip().replace(" ", "").replace(",", "").replace("#", "")
                    try:
                        rwdata.append(int(tdvalue))
                    except ValueError:
                        if tdvalue == 'Telengana':
                            tdvalue = 'Telangana'
                        rwdata.append(tdvalue)
                colCount = colCount + 1
            tabledata.append(rwdata)
        count = count + 1

    # Create Pandas Dataframe for data warrangling
    dfRegion = pd.DataFrame(data=tabledata, columns=columns)

    # Compute statewise ToTal confirmed and motality rate
    # dfRegion['Totalconfirmed'] = dfRegion['TotalConfirmedcases(IndianNational)'] + dfRegion['TotalConfirmedcases(ForeignNational)']

    dfRegion[TimescaleUtil.ColoumnName.Totalconfirmed.value] = pd.to_numeric(dfRegion["TotalConfirmedcases*"])
    dfRegion[TimescaleUtil.ColoumnName.MortalityRate.value] = round(
        dfRegion[TimescaleUtil.ColoumnName.Death.value] * 100 / (
        dfRegion[TimescaleUtil.ColoumnName.Totalconfirmed.value]), 2)
    dfRegion[TimescaleUtil.ColoumnName.TotalConfirmedcases_ForeignNational.value] = 0
    dfRegion[TimescaleUtil.ColoumnName.TotalConfirmedcases_IndianNational.value] = dfRegion[
        TimescaleUtil.ColoumnName.Totalconfirmed.value]

    # Sort by the Statename
    sortby = columns[1]
    dfRegion.sort_values(by=sortby)
    df = dfRegion.reset_index(drop=True)
    dfRegion.drop([1])

    # Compute metric at national level
    sum_columns = dfRegion.sum(axis=0)
    external_default_infected = 47
    metrics[TimescaleUtil.ColoumnName.NameofState_UT.value] = "India"
    metrics[TimescaleUtil.ColoumnName.Totalconfirmed.value] = int(
        sum_columns[TimescaleUtil.ColoumnName.Totalconfirmed.value])
    metrics[TimescaleUtil.ColoumnName.Death.value] = int(sum_columns[TimescaleUtil.ColoumnName.Death.value])
    metrics[TimescaleUtil.ColoumnName.Cured_Discharged_Migrated.value] = int(
        sum_columns[TimescaleUtil.ColoumnName.Cured_Discharged_Migrated.value])
    metrics[TimescaleUtil.ColoumnName.TotalConfirmedcases_IndianNational.value] = metrics[
                                                                                      TimescaleUtil.ColoumnName.Totalconfirmed.value] - external_default_infected
    metrics[TimescaleUtil.ColoumnName.TotalConfirmedcases_ForeignNational.value] = external_default_infected
    metrics[TimescaleUtil.ColoumnName.MortalityRate.value] = int(round(
        sum_columns[TimescaleUtil.ColoumnName.Death.value] * 100 / sum_columns[
            TimescaleUtil.ColoumnName.Totalconfirmed.value], 2))

    dfRegion.drop(['TotalConfirmedcases*'], axis=1, inplace=True)
    #dfRegion.drop([''], axis=1, inplace=True)

    return dfRegion, metrics
