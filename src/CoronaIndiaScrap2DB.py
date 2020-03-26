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
import psycopg2
from pgcopy import CopyManager
import schedule
import time

def coronaindia():
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

    # Write data to Timescale DB
    CONNECTIONURI = "postgres://tsdbadmin:pxn5kcqhhpft1b3z@tsdb-corona-avsk71-959a.a.timescaledb.io:28164/defaultdb?sslmode=require"
    locationparent = "India"
    locationtype = "State"  # World,Country,State, Region, SubRegion, Zone, Ward
    utc_datetime = datetime.utcnow()
    tmpstamp = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
    print("timestamp utc %s, localtime %s", utc_datetime, datetime.now())
    # Create DB Connection
    try:
        # Create connection and cursor
        connection = psycopg2.connect(CONNECTIONURI)
        cursor = connection.cursor()
        # Writing the statewise records
        for idx, row in dfRegion.iterrows():
            location = row['NameofState/UT']
            totalconfirmation = row['Totalconfirmed']
            totaldeath = row['Death']
            totalrecovered = row['Cured/Discharged/Migrated']
            totallocaltransmission = row['TotalConfirmedcases(IndianNational)']
            totalexternaltransmission = row['TotalConfirmedcases(ForeignNational)']
            motalityrate = row['MortalityRate']
            locationKey = locationparent + "." + location
            print(location, locationKey, locationparent, locationtype, totalconfirmation, totaldeath, totalrecovered,
                  totallocaltransmission, totalexternaltransmission, motalityrate)
            cursor.execute(
                "INSERT INTO spread (timestampz,location, locationKey, locationparent,locationtype,totalconfirmation,totaldeath,totalrecovered,totallocaltransmission,totalexternaltransmission,motalityrate) VALUES (%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (tmpstamp,location, locationKey, locationparent, locationtype, totalconfirmation, totaldeath, totalrecovered,
                 totallocaltransmission, totalexternaltransmission, motalityrate))

        # Commit only if the national and statewise records are all successfully inserted
        cursor.execute(
            "INSERT INTO spread (timestampz,location, locationKey, locationparent,locationtype,totalconfirmation,totaldeath,totalrecovered,totallocaltransmission,totalexternaltransmission,motalityrate) VALUES (%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (tmpstamp, "India", "World.India", "World", "World", metrics['TotalConfirmed'], metrics['TotalDeaths'], metrics['TotalRecovered'],
             metrics['TotalLocalTransmissions'], metrics['TotalExternalTransmission'], metrics['MortalityRate%']))

        connection.commit()


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Define scheduler job
schedule.every(3).hours.do(coronaindia)

#run the scheduler
while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    time.sleep(1)