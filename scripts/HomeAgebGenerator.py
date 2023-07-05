import datetime
import logging
import sys

import click
sys.path.append("/home/hmora/network-gen-pipeline/src")

import utils.DateUtils as du
import utils.path_utils as pu
from utils.stopwatch import Stopwatch
from utils.duckaccess import DuckSession
from utils.sparkaccess import SparkAccess
from data_classes.date_container import DateContext

import duckdb
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from geopandas import datasets, GeoDataFrame, read_file, GeoSeries
from pandas import concat

from shapely.geometry import shape
import shapely.wkt

import json

from h3 import h3

import os
from pathlib import Path

from dotenv import load_dotenv
import os
load_dotenv()

import multiprocessing as mp

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

import time

import numpy as np

import concurrent.futures

from pathlib import Path

class LocalWinnerGenerator:

    def __init__(self, year:str, month: str, day: str
                 , target: str
                 , logs: str) -> None:
        self.__year = year
        self.__month = month
        self.__day = day
        self.__target = f"{target}/{self.__year}_{self.__month}_{self.__day}"
        self.__db = pu.create_if_not_exists("/home/hmora/network-gen-pipeline/temp/disposable_stores") \
            + f"/duck_{datetime.datetime.now()}.duckdb"
        
        logging.basicConfig(filename=f"{logs}/home_ageb_etl_{datetime.datetime.now()}.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
        self.__logger = logging.getLogger(f"Home Ageb Generator: {self.__year}-{self.__month}-{self.__day}")

    def get_local_winner(self, arrow_table: pa.Table):
        """
        """
        with DuckSession() as duck:
            df = duck.sql("""
            WITH
            /* Table that filters out rows that have NULL cve_geo */
            not_null_cve_geo AS (
                SELECT *
                FROM arrow_table
                WHERE cve_geo IS NOT NULL
                -- USING SAMPLE 0.5 PERCENT (bernoulli)
            )

            /* Table that filters out rows that aren't in night time */
            , only_in_nighttime_hours AS (
                SELECT *
                FROM not_null_cve_geo
                WHERE (DATEPART('hour', cdmx_datetime) >= 22)
                    OR (DATEPART('hour', cdmx_datetime) < 6)
            )

            /* Table that adds a label (cluster) to a row based on which 10 min 
            bucket it belongs across the night time interval */
            , clustering_by_time_window AS (
                SELECT *
                    , TIME_BUCKET(INTERVAL '600 seconds'
                        , cdmx_datetime::TIMESTAMP, min_datetime::TIMESTAMP) AS tw_cluster
                FROM (
                    SELECT *
                        , MIN(cdmx_datetime) OVER() AS min_datetime
                    FROM only_in_nighttime_hours
                )
            )

            /* Table that computes the distinct cluster count of the above table */
            , total_tws AS (
                SELECT COUNT(DISTINCT(tw_cluster)) AS total_clusters
                FROM clustering_by_time_window
            )

            /* Table that computes a score for each caid and cve_geo based on
            the number of distinct clusters found in cve_geo */
            , pre_scores AS (
                SELECT caid, cve_geo
                    , COUNT(DISTINCT tw_cluster) AS pre_score
                FROM clustering_by_time_window
                GROUP BY 1, 2
            )

            /* Table that computes a score based on the number of distinct clusters 
            found in cve_geo given the total posibble clusters */
            , scores AS (
                SELECT *
                    , (pre_score::FLOAT / total_clusters::FLOAT) * 100.0 AS score
                FROM pre_scores, total_tws
            )

            /* Table that ranks every row by it's score */
            , ranked AS (
                SELECT *
                    , ROW_NUMBER() OVER(PARTITION BY caid ORDER BY score DESC) AS rank
                FROM scores
            )

            /* Table that counts the total pings per caid */
            , pings_count AS (
                SELECT caid
                    , COUNT(*) AS total_pings
                FROM only_in_nighttime_hours
                GROUP BY 1
            )

            /* Table than chooses the highest scored cve_geo per caid */
            , best_ranked AS (
                SELECT *
                FROM ranked
                WHERE rank = 1
            )

            /* Projection of required data */
            , pre_final AS (
                SELECT 
                    a.caid
                    , a.cve_geo AS home_ageb
                    , a.score
                    , b.total_pings
                    , 'computed' AS type
                FROM 
                    best_ranked AS a
                    LEFT JOIN
                    pings_count AS b
                    ON a.caid = b.caid
            )

            /* Table that computes a confidence value (high, low) based on the
            absolute difference between score and total_pings while penalyzes low total_pings*/ 
            , with_confidence_score AS (
                SELECT * EXCLUDE (score, total_pings, confidence_score, avg_confidence_score)
                    , IF(confidence_score < avg_confidence_score, 'low', 'high') AS confidence
                FROM (
                    SELECT *
                        , AVG(confidence_score) OVER() AS avg_confidence_score
                    FROM (
                        SELECT *
                            , (1 - 0.5) * ABS(score - total_pings) + (0.75 * total_pings) AS confidence_score
                        FROM pre_final
                    )
                )
            )

            SELECT *
            FROM with_confidence_score
            """).arrow().combine_chunks()

            self.__logger.info(f"NO OF ROWS: {duck.sql('SELECT COUNT(*) FROM df').fetchone()[0]:,}") 
            self.__logger.info(f"DISTINCT CAIDS: {duck.sql('SELECT COUNT(DISTINCT caid) FROM df').fetchone()[0]:,}")

        return df

    def driver(self):
        """
        """
        arrow_table = \
            (ds.dataset(f"{os.environ['WAREHOUSE_PATH']}/fact_pings_base/{self.__year}_{self.__month}_{self.__day}_pings")) \
                .to_table().combine_chunks()

        self.__logger.info(f"COMPUTING LOCAL WINNER ...")
        result = self.get_local_winner(arrow_table)
        self.__logger.info(f"... DONE")

        output_dir = pu.create_if_not_exists(self.__target)
        self.__logger.info(f"WRITING DATA INTO: {output_dir}")
        pq.write_to_dataset(result, root_path=output_dir, partition_cols=["type","confidence"])
        self.__logger.info(f"... DONE")

        return

########################################################################################################

def execute(p: LocalWinnerGenerator): p.driver()

@click.command(help="""
               Tool to generate the set of work ageb's for all the pings registered
               during the given date/period
               """)
@click.option("--date", default=None
              , help="""
              A string date like %Y-%m-%d that represents the set of pings
              to assign a home_ageb field based on last 15 day's data.
              """)
@click.option("--period", default=None
              , help="""
              A string date like %Y-%m that represents the set of pings for
              the specified period to assign a home_ageb field based on the
              last 15 day's data.
              """)
@click.option('-t', "--target"
              , default=f"{os.environ['WAREHOUSE_PATH']}/home_ageb_repository"
              , help="""
              The target directory in which the data would be written.
              """)
@click.option("--logs"
              , default=f"/home/hmora/network-gen-pipeline/logs"
              , help="""
              The target directory in which the data would be written.
              """)
def main(date: str, period: str, target: str, logs: str) -> None:
    """
    """
    if (date is not None and period is not None) \
        or (date is None and period is None):
        click.echo("Provide --date %Y-%m-%d or --period %Y-%m")
        sys.exit()

    if date is not None:
        year, month, day = date.split('-')

        p = LocalWinnerGenerator(year, month, day, target, logs)
        p.driver()

    else:
        year, month = period.split('-')
        start, end = du.first_and_last_days(year, month)

        batches = \
            [LocalWinnerGenerator(year, month, str(i).zfill(2), target, logs) \
             for i in range(start, end+1)]
        
        with mp.Pool(2) as p:
            p.map(execute, batches)

if __name__ == "__main__":

    with Stopwatch():
        main()