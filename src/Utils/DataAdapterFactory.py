from abc import ABC, abstractmethod
from pandas import pandas as pd
from datetime import datetime
import configparser
import enum
import simplejson as json
import os
import pandas as pd
import pytz
import ssl
import urllib
from lxml import html
import requests
from src.Utils import TimescaleUtil
from src.Utils.TimescaleUtil import ColoumnName


# Enumeration of concrete implementation of source adapters
class SourceAdapterNames(enum.Enum):
    RajasthanSourceAdapter = "RajasthanSourceDataAdapter"
    PunjabSourceDataAdapter = "PunjabSourceDataAdapter"
    IndiaSourceAdapter = "IndiaSourceDataAdapter"
    WorldSourceAdapter = "WorldSourceDataAdapter"


# Abstract class for DataSource Adapter
class DataSourceAdapter(ABC):

    def __init__(self):
        # Parse configuration information
        self.config = Configuration().getConfigParser()
        self.parentregion = None
        self.dfRegion = None
        self.dfSummary = None
        self.metrics = []
        self.sourceofdata = None
        utc = pytz.utc
        timestamp = datetime.date(datetime.now(tz=utc))
        self.dtestr = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        self.columns = TimescaleUtil.getSpreadColumnNames()
        print("Loaded the configuration information")

    @abstractmethod
    def getParentRegion(self) -> str:
        pass

    @abstractmethod
    def getSourceOfData(self) -> str:
        pass

    @abstractmethod
    def loadRegionalData(self):
        pass

    def load(self):
        self.parentregion = self.getParentRegion()
        self.sourceofdata = self.getSourceOfData()
        self.loadRegionalData()
        summarydf = pd.DataFrame(columns=self.columns)
        # Initalize variables
        tabledata = []
        rwdata = []

        # Derive values
        location = self.parentregion
        confirmed = self.dfRegion[ColoumnName.Totalconfirmed.value].sum()
        dead = self.dfRegion[ColoumnName.Death.value].sum()
        recovered = self.dfRegion[ColoumnName.Cured_Discharged_Migrated.value].sum()
        confirmed_internal = self.dfRegion[ColoumnName.TotalConfirmedcases_IndianNational.value].sum()
        confirmed_external = self.dfRegion[ColoumnName.TotalConfirmedcases_ForeignNational.value].sum()
        motalityrate = dead / confirmed
        # Add it to the list
        rwdata.append(self.dtestr)
        rwdata.append(location)
        rwdata.append(confirmed)
        rwdata.append(dead)
        rwdata.append(recovered)
        rwdata.append(confirmed_internal)
        rwdata.append(confirmed_external)
        rwdata.append(motalityrate)
        # Add to master List
        tabledata.append(rwdata)
        # Assign it master dataframe
        self.dfSummary = pd.DataFrame(data=tabledata, columns=self.columns)


# Concrete implementation for the state of Rajasthan
class RajasthanSourceDataAdapter(DataSourceAdapter):

    def getParentRegion(self) -> str:
        # parent region is mandatory,pease return a string
        return "Rajasthan"

    def getSourceOfData(self) -> str:
        return "MOHRJ"

    def loadRegionalData(self):
        # parent region is mandatory
        url = self.config['RajasthanSourceAdapter']['RAJURL']
        # URL = self.config['RajasthanSourceAdapter']['RAJURL']
        context = ssl._create_unverified_context()
        page = urllib.request.urlopen(url, context=context)
        if page.status == 200:
            content = page.read().decode('utf-8')
            data = json.loads(content)
            tabledata = []
            for item in data["features"]:
                attributes = item["attributes"]
                rwdata = []
                locationparent = "Rajasthan"
                location = attributes["GIS.DISTRICT.DISTRICT_NAME_EN"]
                confirmed = attributes["GIS.V_COVID_MEDICALSCREENINGDETAIL.POSTIVE_CASES"]
                dead = attributes["GIS.V_COVID_MEDICALSCREENINGDETAIL.DEATHS"]
                recovered = attributes["GIS.V_COVID_MEDICALSCREENINGDETAIL.CURED_CASES"]
                confirmed_internal = 0
                confirmed_external = 0
                if confirmed > 0:
                    motalityrate = dead / confirmed
                else:
                    motalityrate = 0
                rwdata.append(self.dtestr)
                rwdata.append(location)
                rwdata.append(confirmed)
                rwdata.append(dead)
                rwdata.append(recovered)
                rwdata.append(confirmed_internal)
                rwdata.append(confirmed_external)
                rwdata.append(motalityrate)
                tabledata.append(rwdata)
            dfRegion = pd.DataFrame(data=tabledata, columns=self.columns)
            self.dfRegion = dfRegion
        else:
            print("Error Reading the URL: ", url)


# Concrete implementation for the state of Punjab
class PunjabSourceDataAdapter(DataSourceAdapter):

    def getParentRegion(self) -> str:
        # parent region is mandatory,pease return a string
        return "Punjab"

    def getSourceOfData(self) -> str:
        return "MOHPJB"

    def loadRegionalData(self):
        # parent region is mandatory
        url = self.config['PunjabSourceAdapter']['PJBURL']
        context = ssl._create_unverified_context()
        page = urllib.request.urlopen(url, context=context)
        if page.status == 200:
            content = page.read().decode('utf-8')
            data = json.loads(content)
            tabledata = []
            for item in data["features"]:
                attributes = item["attributes"]
                rwdata = []
                locationparent = "Punjab"
                location = attributes["District"]
                confirmed = attributes["Confir_Cas"]
                dead = attributes["Death_Coun"]
                recovered = attributes["Recov_Cas"]
                confirmed_internal = 0
                confirmed_external = 0
                if confirmed > 0 :
                    motalityrate = dead / confirmed
                else:
                    motalityrate = 0
                rwdata.append(self.dtestr)
                rwdata.append(location)
                rwdata.append(confirmed)
                rwdata.append(dead)
                rwdata.append(recovered)
                rwdata.append(confirmed_internal)
                rwdata.append(confirmed_external)
                rwdata.append(motalityrate)
                tabledata.append(rwdata)
            dfRegion = pd.DataFrame(data=tabledata, columns=self.columns)
            self.dfRegion = dfRegion
        else:
            print("Error Reading the URL: ", url)


class WorldSourceDataAdapter(DataSourceAdapter):

    def getParentRegion(self) -> str:
        # parent region is mandatory,please return a string
        return "WORLD"

    def getSourceOfData(self) -> str:
        return "JHCSEE"

    def loadRegionalData(self):
        # parent region is mandatory
        url = self.config['WorldSourceAdapter']['WRLDURL']
        context = ssl._create_unverified_context()
        page = urllib.request.urlopen(url, context=context)
        if page.status == 200:
            content = page.read().decode('utf-8')
            data = json.loads(content)
            # Extract date
            #print(data["dt"])
            timestamp = datetime.strptime(data["dt"],"%m-%d-%Y")
            self.dtestr = timestamp.strftime("%Y-%m-%d %H:%M:%S")

            tabledata = []
            for item in data["data"]:
                rwdata = []
                locationparent = "World"
                location = item["location"]
                confirmed = item["confirmed"]
                dead = item["deaths"]
                recovered = item["recovered"]
                confirmed_internal = 0
                confirmed_external = 0
                if confirmed > 0 :
                    motalityrate = dead / confirmed
                else:
                    motalityrate = 0
                rwdata.append(self.dtestr)
                rwdata.append(location)
                rwdata.append(confirmed)
                rwdata.append(dead)
                rwdata.append(recovered)
                rwdata.append(confirmed_internal)
                rwdata.append(confirmed_external)
                rwdata.append(motalityrate)
                tabledata.append(rwdata)
            dfRegion = pd.DataFrame(data=tabledata, columns=self.columns)
            self.dfRegion = dfRegion
        else:
            print("Error Reading the URL: ", url)




# Class represents the application configuration class
class Configuration():
    config = None

    def getConfigParser(self):
        try:
            # Read Configuration Information
            config = configparser.ConfigParser()
            basedir = os.path.abspath(os.path.dirname(__file__))
            fpath = os.path.join(basedir, "environment.properties")
            print("looking for environment.properties file in ", fpath)
            config.read(fpath)
            return config
        except Exception as error:
            print("Unable to read configuration file due to ", error)


# Factory Class returns the list of data adapters
class DataSourceAdapterFactory():
    def getDataSourceAdapter(adaptername=None) -> DataSourceAdapter:
        if adaptername == SourceAdapterNames.RajasthanSourceAdapter.value:
            return RajasthanSourceDataAdapter()

        if adaptername == SourceAdapterNames.PunjabSourceDataAdapter.value:
            return PunjabSourceDataAdapter()

        if adaptername == SourceAdapterNames.WorldSourceAdapter.value:
            return WorldSourceDataAdapter()

        else:
            return None


def test():
    print("Test program")
    adapter = DataSourceAdapterFactory.getDataSourceAdapter(SourceAdapterNames.WorldSourceAdapter.value)
    adapter.load()
    # print(adapter.dfRegion)
    # print(adapter.dfSummary)


#test()
