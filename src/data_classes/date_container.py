from dotenv import load_dotenv
import os
load_dotenv()

import sys

from utils.path_utils import create_if_not_exists

class DateContext:
    """ 
    """
    def __init__(self, year: str, month: str, day: str):
        self.__year = year
        self.__month = month
        self.__day = day
        self.__base_dir = \
            create_if_not_exists(
                f"/datos/EpiTeam/insumos_redes/year={year}/month={month}/day={day}"
            )

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

    @property
    def base_dir(self):
        return self.__base_dir
    @base_dir.setter
    def base_dir(self, value):
        self.__base_dir = value

    def as_tuple(self) -> tuple[str, str, str]:
        return (self.__year, self.__month, self.__day)
    
    def __str__(self) -> str:
        return f"{self.__year}-{self.__month}-{self.__day}"
    
    def __repr__(self):
        return self.__str__()
