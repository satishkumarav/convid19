import bs4
import pandas as pd
import pytz
import ssl
import urllib
from datetime import datetime
import feedparser
import tabula

from src.Utils import TimescaleUtil


# Version 0.6
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
    # lblairportscreened = 'Total number of passengers screened at airport '
    # screenedKey = 'Airpot Screened'
    # dictionary to hold the metrics
    metrics = {}

    # Extract Statewise table data
    columns = []
    tabledata = []
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
                        thvalue = "TotalConfirmedcases*"
                    columns.append(thvalue)
                colCount = colCount + 1
        elif 0 < count < size - 3:
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
    # dfRegion.drop([''], axis=1, inplace=True)

    return dfRegion, metrics


def getTelananaDistrictData():
    # Parse RSS feed
    newsfeed = telanganaRSSFeed()

    # Run through the links with PDF files and parse them to pick file with with atleast one table
    for entry in newsfeed.entries:
        link = entry.link
        dte = cleanse(entry.title)
        dtpdf = datetime.strptime(dte, '%d %B %Y')
        if ".pdf" in link:
            if "#new_tab" in link:
                link = link.replace("#new_tab", "")
            df = tabula.read_pdf(link, pages=1, area=[362, 0, 900, 590], multiple_tables=False, lattice=True)
            if len(df) > 0:
                break

    return telanganaparseV2(link, df), dtpdf


def telanganaRSSFeed():
    url = 'https://covid19.telangana.gov.in/announcements/media-bulletins/feed/'
    # Create unverified SSL context to avoid SSL Invalid Certificate errors
    # Parse RSS feed
    newsfeed = feedparser.parse(url)
    return newsfeed


def telanganaparseV1(link, df):
    # Initalize metrics
    print(df)
    columns = TimescaleUtil.getSpreadColumnNamesWoTS()
    metrics = {}
    tabledata = []
    # Extract data from dataframe
    for vdr in df[0].values:
        v = str(vdr)
        v.replace("[", "").replace("]", "").strip()
        if len(v) > 0:
            if not ("No." in v or "S.No" in v or "**" in v or "nan" in v):
                rwdata = []
                pary = vdr[1].split()
                location = pary[0]
                confirmed = int(pary[len(pary) - 1].strip())
                dead = 0
                recovered = int(vdr[2].strip())
                confirmed_internal = 0
                confirmed_external = 0
                motalityrate = dead / confirmed
                rwdata.append(location)
                rwdata.append(confirmed)
                rwdata.append(dead)
                rwdata.append(recovered)
                rwdata.append(confirmed_internal)
                rwdata.append(confirmed_external)
                rwdata.append(motalityrate)
                tabledata.append(rwdata)

    # Create Pandas Dataframe for data warrangling
    dfRegion = pd.DataFrame(data=tabledata, columns=columns)


def telanganaparseV2(link, df):
    # Initalize metrics
    columns = TimescaleUtil.getSpreadColumnNamesWoTS()
    metrics = {}
    tabledata = []
    # Extract data from dataframe
    for vdr in df[0].values:
        v = str(vdr)
        # v.replace("[", "").replace("]", "").strip()
        if len(v) > 0:
            if not ("No." in v or "S.No" in v or "**" in v or "nan" in v):
                rwdata = []
                # pary = vdr[1].split()
                location = replace(vdr[1].replace("'", ""))
                confirmed = vdr[2] + int(vdr[3])
                dead = 0
                recovered = int(vdr[3])
                confirmed_internal = 0
                confirmed_external = 0
                motalityrate = dead / confirmed
                rwdata.append(location)
                rwdata.append(confirmed)
                rwdata.append(dead)
                rwdata.append(recovered)
                rwdata.append(confirmed_internal)
                rwdata.append(confirmed_external)
                rwdata.append(motalityrate)
                tabledata.append(rwdata)

    # Create Pandas Dataframe for data warrangling
    dfRegion = pd.DataFrame(data=tabledata, columns=columns)
    return dfRegion


def cleanse(sourcestr):
    clensewords = ["Media Bulletin – ", "#new_tab", "th", ",", "-", "Media", "Bulletin", "_"]
    for word in clensewords:
        sourcestr = sourcestr.replace(word, "")

    return sourcestr.strip()


def replace(sourcestr):
    repdict = {" ": "", "\n": "", "\r": "", "GHMC": "Hyderabad", "(Non-Hyderabad)": ""}
    for findstr in repdict.keys():
        sourcestr = sourcestr.replace(findstr, repdict[findstr])

    return sourcestr.strip()


def telanganaFileWrite(dfRegion, dte):
    region_file = "../datasets/india/telangana_" + dte.strftime("%Y_%m_%d") + "_.csv"
    # Write current region information
    region_data_csv = dfRegion.to_csv(index=False)
    f = open(region_file, "w")
    f.write(region_data_csv)
    f.close()


def telanganaWriteToday():
    dfRegion, dte = getTelananaDistrictData()
    print("Date: ", dte)
    print(dfRegion)
    telanganaFileWrite(dfRegion, dte)


def telanganaWriteHistory():
    newsfeed = telanganaRSSFeed()

    # Run through the links with PDF files and parse them to pick file with with atleast one table
    for entry in newsfeed.entries:
        link = entry.link
        dte = cleanse(entry.title)
        dtpdf = datetime.strptime(dte, '%d %B %Y')
        if ".pdf" in link:
            if "#new_tab" in link:
                link = link.replace("#new_tab", "")
            df = tabula.read_pdf(link, pages=1, area=[362, 0, 900, 590], multiple_tables=False, lattice=True)
            if len(df) > 0:
                break

    return telanganaparseV2(link, df), dtpdf
