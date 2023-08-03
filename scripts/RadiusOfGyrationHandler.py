import datetime
import logging
import sys

import click
sys.path.append("/home/hmora/network-gen-pipeline/src")

import utils.DateUtils as du
import utils.path_utils as pu
from utils.stopwatch import Stopwatch, LoggerStopwatch
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

class RadiusOfGyrationHVersion:

    def __init__(self, year:str, month: str, day: str
                 , target: str
                 , logs: str) -> None:
        self.__year = year
        self.__month = month
        self.__day = day
        self.__target = f"{target}/{self.__year}_{self.__month}_{self.__day}"
        
        logging.basicConfig(filename=f"{logs}/radius_of_gyration{datetime.datetime.now()}.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
        self.__logger = logging.getLogger(f"Radius of Gyration: {self.__year}-{self.__month}-{self.__day}")

    def prepare_agebs_with_centroid(self) -> pd.DataFrame:
        """"
        """
        self.__logger.info(f"GETTING CENTROIDS ...")
        ageb_catalog = pd.read_parquet(f"{os.environ['AGEB_CATALOG']}")
        ageb_catalog["geometry"] = \
            ageb_catalog["geometry"].apply(lambda x: shape(json.loads(x)))
        ageb_catalog = GeoDataFrame(ageb_catalog, geometry='geometry', crs="EPSG:6365")

        # Se re-proyecta el polígono a "WGS84/EPSG:4326 authalic radius"
        # Esa es la proyección con la que está alineado H3
        ageb_catalog["centroid"] = ageb_catalog["geometry"].to_crs("EPSG:4326").centroid

        ageb_catalog["centroid"] = \
            ageb_catalog["centroid"].apply(lambda p: h3.geo_to_h3(p.y, p.x, 15))
        
        ageb_catalog = ageb_catalog.drop(columns=["geometry"])
        self.__logger.info(f"... DONE")
        self.__logger.info(f"RES:\n{ageb_catalog}")
        
        return ageb_catalog

    def add_home_ageb_centroid(self, agebs_with_centroid: pd.DataFrame, pings: pa.Table) -> pa.Table:
        """
        """
        self.__logger.info(f"APPENDING CENTROIDS...")
        centroids_arrow_df = pa.Table.from_pandas(agebs_with_centroid, preserve_index=False)
        with DuckSession() as duck:
            df = duck.sql("""
            WITH
            only_with_home_agebs AS (
                SELECT *
                FROM pings
                WHERE home_ageb != '0000000000000'
            )

            , distinct_traversals AS (
                SELECT DISTINCT caid, home_ageb, h3index_15
                FROM only_with_home_agebs
            )

            , pings_with_centroids AS (
                SELECT a.caid, b.centroid AS ha_centroid, a.h3index_15
                FROM 
                    distinct_traversals AS a
                    LEFT JOIN
                    centroids_arrow_df AS b
                    ON a.home_ageb = b.cve_geo
            )

            SELECT DISTINCT *
            FROM pings_with_centroids
            """).df()

            # self.__logger.info(f"RES:\n{duckdb.arrow(df)}")
            self.__logger.info(f"... DONE")
            self.__logger.info(f"RES:\n{df}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM df').fetchone()[0]:,}")
            self.__logger.info(f"DISTINCT CAIDS: {duck.sql('SELECT COUNT(DISTINCT caid) FROM df').fetchone()[0]:,}")

            # pq.write_table(df, "./temp/pings_with_centroid.parquet")
            # df.to_parquet("pings_with_centroid.parquet")

        return df
    
    def compute_ris(self, ris_table: pd.DataFrame):
        """
        """
        self.__logger.info(f"COMPUTING H3 DISTANCES...")

        ris_table["r_i"] = ris_table[["h3index_15", "ha_centroid"]] \
            .apply(lambda x : h3.h3_distance(x["h3index_15"], x["ha_centroid"]), axis=1)
        
        with DuckSession() as duck:
            self.__logger.info(f"... DONE")
            self.__logger.info(f"RES:\n{ris_table}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM ris_table').fetchone()[0]:,}")
            self.__logger.info(f"DISTINCT CAIDS: {duck.sql('SELECT COUNT(DISTINCT caid) FROM ris_table').fetchone()[0]:,}")
        
        return ris_table
    
    def get_mass_center_table(self, ris_table: pd.DataFrame):
        """
        """
        self.__logger.info(f"COMPUTING MASS CENTER ...")

        arrow_ris_table = pa.Table.from_pandas(ris_table)
        with DuckSession() as duck:
            df = duck.sql("""
            WITH
            /* Obtención del número de celdas distintas dentro de las cuáles se movió el caid */
            locations_per_caid AS (
                SELECT caid
                    , COUNT(DISTINCT h3index_15) AS n
                FROM arrow_ris_table
                GROUP BY 1
            )

            /* Suma de r_i's por caid */
            , sum_of_ris AS (
                SELECT caid
                    , SUM(r_i) AS sum_of_ris
                FROM arrow_ris_table
                GROUP BY 1
            )

            /* Obtención del centro de masa como la suma de las distancias desde el origen  
                sobre el número de lugares */
            , caids_with_mass_center AS (
                SELECT a.caid
                    , a.sum_of_ris
                    , b.n
                    , (a.sum_of_ris::FLOAT / b.n::FLOAT) AS mass_center
                FROM 
                    sum_of_ris AS a
                    LEFT JOIN
                    locations_per_caid AS b
                    ON a.caid = b.caid
            )

            SELECT *
            FROM caids_with_mass_center
            """).arrow().combine_chunks()

            self.__logger.info(f"... DONE")
            self.__logger.info(f"RES:\n{duckdb.arrow(df)}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM df').fetchone()[0]:,}")
            self.__logger.info(f"DISTINCT CAIDS: {duck.sql('SELECT COUNT(DISTINCT caid) FROM df').fetchone()[0]:,}")

        return df
    
    def get_sum_of_diffs(self, ris_table: pa.Table, mass_center_table: pa.Table):
        """
        """
        self.__logger.info(f"COMPUTING SUM OF DIFFS...")

        with DuckSession() as duck:
            df = duck.sql("""
            WITH
            /* Join de la tabla de r_i's con el catálogo de centros de masa */
            ris_and_mass_center AS (
                SELECT 
                    a.caid
                    , a.r_i
                    , b.mass_center
                FROM 
                    ris_table AS a
                    INNER JOIN
                    mass_center_table AS b
                    ON a.caid = b.caid
            )

            /* Cálculo del valor absoluto de las distancias entre cada r_i con el centro
                de masa correspondiente */
            , ris_diffs AS (
                SELECT caid
                    , POW(ABS(r_i - mass_center), 2) AS ri_diff
                FROM ris_and_mass_center
            )

            SELECT caid
                , SUM(ri_diff) AS ri_diff_sum
            FROM ris_diffs
            GROUP BY 1
            """).arrow().combine_chunks()

            self.__logger.info(f"... DONE")
            self.__logger.info(f"RES:\n{duckdb.arrow(df)}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM df').fetchone()[0]:,}")
            self.__logger.info(f"DISTINCT CAIDS: {duck.sql('SELECT COUNT(DISTINCT caid) FROM df').fetchone()[0]:,}")

        return df
    
    def get_radius_of_gyration_catalog(self, ris_table: pa.Table, diffs_table: pa.Table):
        """
        """
        self.__logger.info(f"COMPUTING SUM OF DIFFS...")

        with DuckSession() as duck:
            df = duck.sql("""
            WITH
            /* Obtención del número de celdas distintas por las que se movió el usuario */
            locations_per_caid AS (
                SELECT caid
                    , COUNT(DISTINCT h3index_15) AS n
                FROM ris_table
                GROUP BY 1
            )

            /* Cálculo del radio de giro como la raíz cuadrada de la suma de las distancias con respecto al
                centro de masa dividido sobre el número de celdas por caid */
            , caids_radius_of_gyration AS (
                SELECT a.caid
                    , SQRT(a.ri_diff_sum / b.n) AS rg
                FROM 
                    diffs_table AS a
                    INNER JOIN
                    locations_per_caid AS b
                    ON a.caid = b.caid
            )

            SELECT *
            FROM caids_radius_of_gyration
            """).arrow().combine_chunks()

            self.__logger.info(f"... DONE")
            self.__logger.info(f"RES:\n{duckdb.arrow(df)}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM df').fetchone()[0]:,}")
            self.__logger.info(f"DISTINCT CAIDS: {duck.sql('SELECT COUNT(DISTINCT caid) FROM df').fetchone()[0]:,}")

        return df

    def driver(self):
        """
        """
        with LoggerStopwatch(self.__logger) as sw:

            self.__logger.info(f"RETRIEVING DATA ...")
            arrow_table = \
                (ds.dataset(f"{os.environ['WAREHOUSE_PATH']}/pings_with_home_ageb/year={self.__year}/month={self.__month}/day={self.__day}/cve_zm=09.01")) \
                    .to_table().combine_chunks()
            #arrow_table = \
            #    pq.ParquetDataset(f"{os.environ['WAREHOUSE_PATH']}/pings_with_home_ageb/year={self.__year}/month={self.__month}/day={self.__day}/"
            #                      , use_legacy_dataset=False
            #                      , filters=[("cve_zm", '=', "09.01")]) \
            #        .read().combine_chunks()
            self.__logger.info(f"... DONE")
            with DuckSession() as duck:
                self.__logger.info(f"RES:\n{duckdb.arrow(arrow_table)}")
                self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM arrow_table').fetchone()[0]:,}")
                self.__logger.info(f"DISTINCT CAIDS: {duck.sql('SELECT COUNT(DISTINCT caid) FROM arrow_table').fetchone()[0]:,}")

            sw.report_til_here()

            agebs_with_centroid = self.prepare_agebs_with_centroid()
            sw.report_til_here()

            traversals_with_centroid = \
                self.add_home_ageb_centroid(agebs_with_centroid, arrow_table)
            sw.report_til_here()

            ris_table = self.compute_ris(traversals_with_centroid)
            sw.report_til_here()

            mass_center_table = self.get_mass_center_table(ris_table)
            sw.report_til_here()

            sum_of_diffs = self.get_sum_of_diffs(ris_table, mass_center_table)
            sw.report_til_here()

            rg_catalog = self.get_radius_of_gyration_catalog(ris_table, sum_of_diffs)
            sw.report_til_here()

            output_dir = pu.create_if_not_exists(self.__target)
            self.__logger.info(f"WRITING DATA INTO: {output_dir}")
            pq.write_to_dataset(rg_catalog, root_path=output_dir)
            self.__logger.info(f"... DONE")

        return

########################################################################################################

def execute(p: RadiusOfGyrationHVersion): p.driver()

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
              , default=f"{os.environ['WAREHOUSE_PATH']}/radius_of_gyration"
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

        p = RadiusOfGyrationHVersion(year, month, day, target, logs)
        p.driver()

    else:
        year, month = period.split('-')
        start, end = du.first_and_last_days(year, month)

        batches = \
            [RadiusOfGyrationHVersion(year, month, str(i).zfill(2), target, logs) \
             for i in range(start, end+1)]
        
        with mp.Pool(2) as p:
            p.map(execute, batches)

if __name__ == "__main__":

    with Stopwatch():
        main()