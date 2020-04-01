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


# Enumeration of Location Type
class LocationType(enum.Enum):
    World = "World"
    Country = "Country"
    State = "State"
    District = "District"
    Zone = "Zone"


# Inserts the record to spread table
def insert2spread(dfRegion, locationparent="India", locationtype=LocationType.State, usetimestampfromdataframe=False,
                  cleanbeforeload=False, deletetodaysrecordsforparentlocation=False,
                  deletetodaysrecordsforlocation=False, location=""):
    # Read Configuration Information
    config = configparser.ConfigParser()
    config.read('../environment.properties')
    DBURL = config['DB']['DBURL']

    # Write data to Timescale DB
    CONNECTIONURI = DBURL
    utc_datetime = datetime.utcnow()
    tmpstampUTC = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
    # Create DB Connection
    try:
        # Create connection and cursor
        connection = psycopg2.connect(CONNECTIONURI)
        cursor = connection.cursor()
        # Cleanse the the table before reload
        if cleanbeforeload:
            query = "DELETE from SPREAD WHERE locationparent = %s"
            cursor.execute(query, (locationparent,))
        if deletetodaysrecordsforparentlocation:
            query = "DELETE from spread where DATE_TRUNC('day',timestampz) >= DATE_TRUNC('day',now()) AND locationparent = %s"
            cursor.execute(query, (locationparent,))
        if deletetodaysrecordsforlocation:
            query = "DELETE from spread where DATE_TRUNC('day',timestampz) >= DATE_TRUNC('day',now()) AND location = %s"
            cursor.execute(query, (location,))

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
                "INSERT INTO SPREAD (timestampz,location, locationKey, locationparent,locationtype,totalconfirmation,totaldeath,totalrecovered,totallocaltransmission,totalexternaltransmission,motalityrate) VALUES (%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (tmpstamp, location, locationKey, locationparent, locationtype.value, totalconfirmation, totaldeath,
                 totalrecovered,
                 totallocaltransmission, totalexternaltransmission, motalityrate))

        connection.commit()


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
