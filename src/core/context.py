from dotenv import load_dotenv
import os
load_dotenv()

import sys

class Context:
    """ Context class to propagate year, month and day info
    """
    def __init__(self, year: str, month: str, day: str):
        self.__year = year
        self.__month = month
        self.__day = day

    @property
    def year(self):
        return self.__year
    @year.setter
    def year(self, value):
        self.__year = value

    @property
    def month(self):
        return self.__month
    @month.setter
    def month(self, value):
        self.__month = value

    @property
    def day(self):
        return self.__day
    @day.setter
    def day(self, value):
        self.__day = value
    
class ExtractContext(Context):
    """
    """
    def __init__(self, year: str, month: str, day: str):
        super().__init__(year, month, day)
        self.__data_source = os.environ[f"MOVILIDAD_RAW_{year}"] \
            if year in ["2020", "2021"] else sys.exit("Invalid Year")
        self.__raw_pings_target = os.environ[f"RAW_PINGS_TARGET"]
        self.__payload = None
    
    @property
    def data_source(self):
        return self.__data_source
    @data_source.setter
    def data_source(self, value):
        self.__data_source = value

    @property
    def raw_pings_target(self):
        return self.__raw_pings_target
    @raw_pings_target.setter
    def payload(self, value):
        self.__raw_pings_target = value
    
    @property
    def payload(self):
        return self.__payload
    @payload.setter
    def payload(self, value):
        self.__payload = value

class TransformContext(ExtractContext):
    """
    """
    def __init__(self, year: str, month: str, day: str):
        super().__init__(year, month, day)
        self.__ageb_catalog = os.environ["AGEB_CATALOG"]
        self.__ntl_pings_target = os.environ["NTL_PINGS_TARGET"]

    @property
    def ageb_catalog(self):
        return self.__ageb_catalog
    @ageb_catalog.setter
    def ageb_catalog(self, value):
        self.__ageb_catalog = value
    
    @property
    def ntl_pings_target(self):
        return self.__ntl_pings_target
    @ntl_pings_target.setter
    def ntl_pings_target(self, value):
        self.__ntl_pings_target = value
    