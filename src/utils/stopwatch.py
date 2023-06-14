import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import logging

from utils.duckaccess import DuckSession

class Stopwatch():
    def __init__(self) -> None:
        pass

    def __enter__(self):
        self.start = time.time()
        print(f"Started at {str(datetime.now())}")

    def __exit__(self, type, value, traceback):
        self.end = time.time()
        elapsed = self.end - self.start
        print(f"Ended at {str(datetime.now())}")
        print(f"Elapsed time: {str(timedelta(seconds=elapsed))}")

class Benchmark():
    """
    """
    def __init__(self, program_name: str, log_file: str, db_name: str) -> None:
        self.__program_name = program_name
        self.__log_file = log_file
        self.__db_name = db_name
        logging.basicConfig(filename=self.__log_file,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
        self.__logger = logging.getLogger(f"Benchmarking: {program_name}")

    def __enter__(self):
        self.__start_time = time.time()
        self.__start_date = datetime.now(ZoneInfo('America/Mexico_City'))
        self.__logger.info(f"Started at {str(self.__start_date)}")

        with DuckSession(self.__db_name) as duck:
            duck.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.__program_name} AS
            SELECT NULL::TIMESTAMPTZ AS start_date, NULL::TIMESTAMPTZ AS end_date, NULL::DOUBLE AS elapsed_time 
            """)


    def __exit__(self, type, value, traceback):
        self.__end = time.time()
        self.__end_date = datetime.now(ZoneInfo('America/Mexico_City'))
        self.__elapsed = self.__end - self.__start_time
        self.__logger.info(f"Ended at {str(self.__end_date)}")
        self.__logger.info(f"Elapsed time: {str(timedelta(seconds=self.__elapsed))}")

        with DuckSession(self.__db_name) as duck:
            duck.execute(f"""
            INSERT INTO {str(self.__program_name)} VALUES ('{str(self.__start_date)}', '{str(self.__end_date)}', '{str(self.__elapsed)}')
            """)