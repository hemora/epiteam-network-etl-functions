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

def get_mun_catalog(year: str, month: str, day: str) -> pa.Table:
    """
    """
    arrow_table = \
        (ds.dataset(f"/datos/EpiTeam/warehouse/pings_with_home_ageb/year={year}/month={month}/day={day}/cve_zm=09.01")) \
            .to_table().combine_chunks()

    arrow_table  = arrow_table.filter(pc.not_equal(arrow_table["home_ageb"], "0000000000000"))

    with DuckSession() as duck:
        df = duck.sql("""
        WITH
        all_caids_in_mun AS (
            SELECT DISTINCT caid, home_ageb[:5] AS cve_mun
            FROM arrow_table
        )
                      
        SELECT *
        FROM all_caids_in_mun
        """).arrow().combine_chunks()

    return df

def aggregate_rg(year: str, month: str, day:str, caids_in_muns: pa.Table) -> pa.Table:
    """
    """
    radius_of_gyration_repo = \
        (ds.dataset(f"/datos/EpiTeam/warehouse/radius_of_gyration/{year}_{month}_{day}")) \
            .to_table().combine_chunks()
    
    with DuckSession() as duck:
        df = duck.sql(f"""
        WITH
        rg_aggregated AS (
            SELECT
                cve_mun
                , AVG(rg) AS rg_avg
                , STDDEV(rg) AS rg_stddev
            FROM 
                caids_in_muns AS a
                INNER JOIN 
                radius_of_gyration_repo AS b
                ON a.caid = b.caid
            GROUP BY 1
        )
        
        SELECT *
            , '{year}-{month}-{day}' AS __date
        FROM rg_aggregated
        WHERE cve_mun[:2] IN ('09', '13', '15')
        """).arrow().combine_chunks()
    
    return df

def driver(t: tuple[str]):

    year, month, day = t

    mun_catalog = get_mun_catalog(year, month, day)

    df = aggregate_rg(year, month, day, mun_catalog)

    return df

if __name__ == "__main__":

    with Stopwatch():
        
        date_range = du.date_range("2020", "01", "16", 720)
        # date_range = du.date_range("2020", "01", "16", 16)

        with mp.Pool(10) as p:
            
            aggregates = p.map(driver, date_range)

        big_union = pa.concat_tables(aggregates)

        df = big_union.to_pandas()

        df.to_parquet("./temp/aggregated_rg_by_mun_day.parquet")
