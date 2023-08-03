import datetime
import logging
import sys

import click

sys.path.append("/home/hmora/network-gen-pipeline/src")
import utils.DateUtils as du
import utils.path_utils as pu


class DateArgsCore:
    """
    """
    def __init__(self, year:str, month: str, day: str
                 , target: str
                 , logs: str) -> None:
        self.__year = year
        self.__month = month
        self.__day = day
        self.__target = pu.create_if_not_exists(f"{target}/{self.__year}_{self.__month}_{self.__day}")

        logging.basicConfig(filename=f"{logs}/{datetime.datetime.now()}.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
        self.__logger = logging.getLogger(f"Network Generator: {self.__year}-{self.__month}-{self.__day}")

    
    def main(self)