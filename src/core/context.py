from dotenv import load_dotenv
import os
load_dotenv()

import logging

class Context:
    """
    """
    def __init__(self, year: str, month: str, day: str, spark, log_file: str):
        self.__year = year
        self.__month = month
        self.__day = day
        self.__spark = spark
        self.__parquet_path = os.environ[f"MOVILIDAD_RAW_{self.__year}"]
        logging.basicConfig(filename=f"{os.environ['LOG_FILE']}/{log_file}",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d -- %H:%M:%S',
                    level=logging.INFO)
        self.__logger = logging.getLogger(f'{log_file}')
        self.__ageb_catalog = os.environ["AGEB_CATALOG"]
        self.__df = None
        self.__home_ageb_catalog = None
        self.__duck = None
        self.__interactions_table = None

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
    def spark(self):
        return self.__spark
    @spark.setter
    def spark(self, value):
        self.__spark = value
    
    @property
    def parquet_path(self):
        return self.__parquet_path
    @parquet_path.setter
    def parquet_path(self, value):
        self.__parquet_path = value
    
    @property
    def logger(self):
        return self.__logger
    @logger.setter
    def logger(self, value):
        self.__logger = value
    
    @property
    def ageb_catalog(self):
        return self.__ageb_catalog
    @ageb_catalog.setter
    def ageb_catalog(self, value):
        self.__ageb_catalog = value

    @property
    def df(self):
        return self.__df
    @df.setter
    def df(self, value):
        self.__df = value
    
    @property
    def home_ageb_catalog(self):
        return self.__home_ageb_catalog
    @home_ageb_catalog.setter
    def home_ageb_catalog(self, value):
        self.__home_ageb_catalog = value

    @property
    def duck(self):
        return self.__duck
    @duck.setter
    def duck(self, value):
        self.__duck = value

    @property
    def interactions_table(self):
        return self.__interactions_table
    @interactions_table.setter
    def interactions_table(self, value):
        self.__interactions_table = value

