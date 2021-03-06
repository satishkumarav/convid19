from typing import List

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
import configparser
import enum
import json
import os
from psycopg2.extras import RealDictCursor, DictCursor, NamedTupleCursor


# Version 0.5
# Enumeration consisting of column names
class ColoumnName(enum.Enum):
    TimestampUTC = "TimestampUTC"
    NameofState_UT = "NameofState/UT"
    Totalconfirmed = "Totalconfirmed"
    Death = "Death"
    Cured_Discharged_Migrated = "Cured/Discharged/Migrated"
    TotalConfirmedcases_IndianNational = "TotalConfirmedcases(IndianNational)"
    TotalConfirmedcases_ForeignNational = "TotalConfirmedcases(ForeignNational)"
    MortalityRate = "MortalityRate"


# Get column Names of of table
def getSpreadColumnNames():
    columns = [ColoumnName.TimestampUTC.value, ColoumnName.NameofState_UT.value, ColoumnName.Totalconfirmed.value,
               ColoumnName.Death.value, ColoumnName.Cured_Discharged_Migrated.value,
               ColoumnName.TotalConfirmedcases_IndianNational.value,
               ColoumnName.TotalConfirmedcases_ForeignNational.value, ColoumnName.MortalityRate.value]
    return columns

def getSpreadColumnNamesWoTS():
    columns = [ColoumnName.NameofState_UT.value, ColoumnName.Totalconfirmed.value,
               ColoumnName.Death.value, ColoumnName.Cured_Discharged_Migrated.value,
               ColoumnName.TotalConfirmedcases_IndianNational.value,
               ColoumnName.TotalConfirmedcases_ForeignNational.value, ColoumnName.MortalityRate.value]
    return columns

# Enumeration of Location Type
class LocationType(enum.Enum):
    World = "World"
    Country = "Country"
    State = "State"
    District = "District"
    Mandal = "Mandal"
    City = "City"
    Zone = "Zone"
    Area = "Area"


def getLocations(location=None, breakdown=False, historical=False, limit=1000, totime=None, fromtime=None):
    # Read Configuration Information
    config = getConfigParser()
    CONNECTIONURI = config['DB']['DBURL']
    jsonformat = True
    timeflag = False
    try:

        # Create connection and cursor
        connection = psycopg2.connect(CONNECTIONURI)
        if jsonformat:
            # Todo: Add logic to deal with prepared statement
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            # cursor = connection.cursor(cursor_factory=NamedTupleCursor)
            # cursor = connection.cursor()

        else:
            cursor = connection.cursor()

        if not historical:
            selectFragment = config['DBQUERIES']['QRY_LATEST_SELECT_FRAGMENT'] + " "
            bylocationFragment = config['DBQUERIES']['QRY_BYLOCATION']
            bylocationParentFragment = config['DBQUERIES']['QRY_BYPARENTLOCATION']
        else:
            selectFragment = config['DBQUERIES']['QRY_TS_SELECT_FRAGMENT'] + " "
            if totime is None and fromtime is None:
                bylocationFragment = config['DBQUERIES']['QRY_TS_BYLOCATION']
                bylocationParentFragment = config['DBQUERIES']['QRY_TS_BYPARENTLOCATION']
            else:
                if totime is None: totime = datetime.now().strftime('%Y-%m-%d')
                if fromtime is None: fromtime = datetime.date(datetime(year=1970, month=1, day=1)).strftime('%Y-%m-%d')
                bylocationFragment = config['DBQUERIES']['QRY_TS_BYLOCATION_BYDATE']
                bylocationParentFragment = config['DBQUERIES']['QRY_TS_BYPARENTLOCATION_BYDATE']
                timeflag = True

        if location is None:
            query = selectFragment + config['DBQUERIES']['QRY_ALL']
            # print(query)
            cursor.execute(query)
        else:
            if not breakdown:
                query = selectFragment + bylocationFragment
                if not timeflag:
                    cursor.execute(query, {'location': location})
                else:
                    cursor.execute(query, ({'location': location, 'fromtime': fromtime, 'totime': totime}))
            else:
                query = selectFragment + bylocationParentFragment
                if not timeflag:
                    cursor.execute(query, {'locationparent': location})
                else:
                    cursor.execute(query, ({'locationparent': location, 'fromtime': fromtime, 'totime': totime}))

        if jsonformat:
            res = transform(cursor.fetchall())
            result = json.dumps(res, default=str)
            return result
        else:
            return cursor.fetchall()

    except (Exception, psycopg2.Error) as error:
        print("Error in executing query :", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()

def transform(result):
    finalRes = {}
    # print(result)
    for x in result:
        if x['location'] in finalRes:
        #if finalRes[x["location"]]:
            finalRes[x["location"]].append(x)
        else:
            finalRes[x["location"]] = [x]
    return finalRes


# Inserts the record to spread table
def insert2spread(dfRegion, locationparent="India", locationtype=LocationType.State, usetimestampfromdataframe=False,
                  cleanbeforeload=False, deletetodaysrecordsforparentlocation=False,
                  deletetodaysrecordsforlocation=False, location="",source=None):
    # Read Configuration Information
    config = getConfigParser()
    CONNECTIONURI = config['DB']['DBURL']

    # Write data to Timescale DB
    # CONNECTIONURI = DBURL
    utc_datetime = datetime.utcnow()
    tmpstampUTC = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
    # Create DB Connection
    try:
        # Create connection and cursor
        connection = psycopg2.connect(CONNECTIONURI)
        cursor = connection.cursor()
        # Cleanse the the table before reload
        if source is None:
            source = getSourceQueryString(location)

        if cleanbeforeload:
            query = "DELETE from SPREAD WHERE locationparent = %s and source = %s"
            cursor.execute(query, (locationparent,source))
        if deletetodaysrecordsforparentlocation:
            query = "DELETE from spread where DATE_TRUNC('day',timestampz) >= DATE_TRUNC('day',now()) AND locationparent = %s and source = %s"
            cursor.execute(query, (locationparent, source))
        if deletetodaysrecordsforlocation:
            query = "DELETE from spread where DATE_TRUNC('day',timestampz) >= DATE_TRUNC('day',now()) AND location = %s and source = %s"
            cursor.execute(query, (location, source))

        # Writing the statewise records
        for idx, row in dfRegion.iterrows():
            location = row[ColoumnName.NameofState_UT.value].strip().replace(" ", "").replace(",", "")
            # coltoken.strip().replace(" ", "").replace(",", "")
            totalconfirmation = row[ColoumnName.Totalconfirmed.value]
            totaldeath = row[ColoumnName.Death.value]
            totalrecovered = row[ColoumnName.Cured_Discharged_Migrated.value]
            totallocaltransmission = row[ColoumnName.TotalConfirmedcases_IndianNational.value]
            totalexternaltransmission = row[ColoumnName.TotalConfirmedcases_ForeignNational.value]
            motalityrate = row[ColoumnName.MortalityRate.value]
            locationKey = locationparent + "." + location
            if usetimestampfromdataframe:
                tmpstamp = row[ColoumnName.TimestampUTC.value]
            else:
                tmpstamp = utc_datetime.replace(microsecond=0)

            cursor.execute(
                "INSERT INTO SPREAD (timestampz,location, locationKey, locationparent,locationtype,totalconfirmation,totaldeath,totalrecovered,totallocaltransmission,totalexternaltransmission,motalityrate,source) VALUES (%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (tmpstamp, location, locationKey, locationparent, locationtype.value, totalconfirmation, totaldeath,
                 totalrecovered,
                 totallocaltransmission, totalexternaltransmission, motalityrate,source))

        connection.commit()
        print("Data inserted into the database")


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Returns default source string
def getSourceQueryString(location):
    srcqrystr = None
    # MOHI
    # JHCSEE
    # MOHRJ
    # MOHT
    srcDict = dict({"India": "MOHI", "Telangana": "MOHT", "Rajasthan": "MOHRJ", "World": "JHCSEE"})
    srcQry = srcDict[location]
    # Todo: Incase in abmigious situation, you search whole data base
    if srcQry == None:
        srcQry = "MOHI"

    return srcQry

# Get Configuration Parser
def getConfigParser():
    try:
        # Read Configuration Information
        config = configparser.ConfigParser()
        basedir = os.path.abspath(os.path.dirname(__file__))
        fpath = os.path.join(basedir, "environment.properties")
        print ("looking for environment.properties file in ", fpath)
        config.read(fpath)
        return config
    except Exception as error:
        print("Unable to read configuration file due to " ,error)