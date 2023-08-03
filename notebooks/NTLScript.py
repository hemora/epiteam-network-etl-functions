import sys
sys.path.append("/home/hmora/network-gen-pipeline/src")

import utils.DateUtils as du
import utils.path_utils as pu

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

import matplotlib.pyplot as plt
import matplotlib.patheffects as patheffects

import os
from pathlib import Path

from dotenv import load_dotenv
import os
load_dotenv()

import multiprocessing as mp
from multiprocessing import Pool

from utils.duckaccess import DuckSession

from utils.stopwatch import Stopwatch

from dataclasses import dataclass

from datetime import timedelta, date, datetime
from dateutil.relativedelta import *
from dateutil.rrule import rrule, DAILY

def get_unique_caids(year, month, day):
    """
    Función que devuelve un dataframe con los caids únicos que contiene
    """
    with DuckSession() as duck:

        unique_caids = duck.sql(f"""
        WITH 
        raw_data AS (
            SELECT *
            FROM read_parquet('{os.environ["WAREHOUSE_PATH"]}/semi_raw_pings_with_dates/{year}_{month}_{day}_pings.parquet')
        ) 

        SELECT DISTINCT caid
        FROM raw_data
        """).df()
    return unique_caids

def get_last_n_days_range(year: str, month: str, day: str, offset: int):
    """
    """
    aux_day = date(int(year), int(month), int(day))
    start_day = aux_day - timedelta(days=offset)
    end_day = aux_day - timedelta(days=1)

    print(f"{start_day} <----> {end_day}")

    date_range = [dt for dt in rrule(DAILY, dtstart=start_day, until=end_day) \
                  if dt >= datetime(2020, 1, 1) \
                    if dt <= datetime(2022, 1, 1) ]
    
    parsed_dates = [(str(d.year), str(d.month).zfill(2), str(d.day).zfill(2)) for d in date_range]
    
    return parsed_dates

def retrive_data(t: tuple[str, str, str, pd.DataFrame]):
    """
    Función encargada de obtener los datos atribuidos al día especificado
    """
    year, month, day, uniques = t

    with DuckSession() as duck:

        curr_path = \
            f"{os.environ['WAREHOUSE_PATH']}/semi_raw_pings_with_dates/{year}_{month}_{day}_pings.parquet"

        print(curr_path)

        result = duck.sql(f"""
        WITH
        raw_data AS (
            SELECT *
            FROM read_parquet('{curr_path}')
        )

        , only_caids_of_interest AS (
            SELECT b.*
            FROM uniques AS a
                INNER JOIN 
                raw_data AS b
                ON a.caid = b.caid
        )

        SELECT utc_timestamp, cdmx_datetime
            , caid
            , latitude, longitude, horizontal_accuracy
        FROM only_caids_of_interest
        WHERE (DATEPART('hour', cdmx_datetime) >= 22 OR DATEPART('hour', cdmx_datetime) < 6)
        """).df()

    return result

def locate_in_agebs(df: pd.DataFrame):
    """
    Función encargada de localizar cada ping dentro de su correspondiente AGEB
    """
    df["geometry"] = df[["latitude", "longitude"]] \
        .apply(lambda x : Point(x["longitude"], x["latitude"]), axis=1)

    gdf_L = GeoDataFrame(df, geometry='geometry', crs="EPSG:6365")

    with DuckSession() as duck:

        ageb_catalog = duck.sql(f"""
        SELECT DISTINCT cve_geo, geometry
        FROM read_parquet('{os.environ['AGEB_CATALOG']}')
        """).df()

        ageb_catalog["geometry"] = \
            ageb_catalog["geometry"].apply(lambda x: shape(json.loads(x)))
        
        gdf_R = GeoDataFrame(ageb_catalog, geometry='geometry', crs="EPSG:6365")

        joined = gdf_L.sjoin(gdf_R, how="left")
        joined = joined.drop(columns=["geometry", "index_right"])

    return joined

def get_local_winner(df: pd.DataFrame):
    """
    Función encargada de calcular el night time location por cada día
    """
    with DuckSession() as duck:

        winners = duck.sql("""
        WITH
        pre AS (
            SELECT STRFTIME(cdmx_datetime, '%Y-%m-%d') AS cdmx_date
                , caid, cve_geo
            FROM df
        )

        , pings_per_day AS (
            SELECT caid, cve_geo
                , COUNT(*) AS pings_per_day
            FROM pre
            GROUP BY 1, 2
        )

        , with_total_pings AS (
            SELECT *
                , (SUM(pings_per_day) OVER (PARTITION BY caid))::INTEGER AS total_pings
            FROM pings_per_day
        )

        SELECT *
            , ROW_NUMBER() OVER (PARTITION BY caid ORDER BY pings_per_day DESC) AS rank
        FROM with_total_pings
        ORDER BY caid
        """).df()

    return winners

def get_home_ageb_catalog(uniques: pd.DataFrame, df: pd.DataFrame):
    """
    Función encargada de obtener un catálogo de home_agebs
    a partir de los ganadores locales
    """
    with DuckSession() as duck:

        ranked_agebs = duck.sql("""
        WITH 
        scored AS (
            SELECT caid, cve_geo
                , COUNT(*) AS global_score
            FROM df
            GROUP BY 1, 2
        )

        , ranked AS (
            SELECT *
                , ROW_NUMBER() OVER (PARTITION BY caid ORDER BY global_score DESC) AS rank 
            FROM scored
        )

        SELECT caid, cve_geo
        FROM ranked
        WHERE rank = 1;
        """).df()

        home_ageb_catalog = duck.sql("""
        SELECT
            a.caid
            , IF(b.cve_geo IS NULL, '0000000000000', b.cve_geo) AS home_ageb
            , 'trusted' AS type
        FROM 
            uniques AS a
            LEFT JOIN
            ranked_agebs AS b
            ON a.caid = b.caid

        """).df()

    return home_ageb_catalog

if __name__ == "__main__":

    YEAR, MONTH, DAY = ("2020", "01", "16")

    ### Obtención de los caids únicos en el día de interés
    with Stopwatch():

        unique_caids_in_day = get_unique_caids(YEAR, MONTH, DAY)

        print(unique_caids_in_day)
        print(unique_caids_in_day.shape)

    ### Se obtienen las fechas atribuidas a 15 días anteriores
    with Stopwatch():

        last_15_days = get_last_n_days_range(YEAR, MONTH, DAY, 15)

        print(last_15_days)

    ### Se obtienen los pings atribuidos a las fechas anteriores y los caids de interés
    with Stopwatch():

        # Se calcula una lista de tuplas que procesar funcionalmente
        dates_to_retrieve = [(d[0], d[1], d[2], unique_caids_in_day) for d in last_15_days]

        # Se obtienen los datos concurrentemente
        with mp.Pool(5) as p:

            last_15_days_data = p.map(retrive_data, dates_to_retrieve)

        #print(last_15_days_data)

    ### Se localizan los pings dentro de su correspondiente AGEB
    with Stopwatch():

        with mp.Pool(5) as p:

            located_pings = p.map(locate_in_agebs, last_15_days_data)
        
        # print(located_pings)

    ### Se obtienen los ganadores locales para cada día
    with Stopwatch():

        with mp.Pool(5) as p:

            local_winners = p.map(get_local_winner, located_pings)

            # print(local_winners)

    ### Se obtiene el ganador global a partir de los
    ### ganadores locales
    with Stopwatch():

        big_union = pd.concat(local_winners)

        home_ageb_catalog = get_home_ageb_catalog(unique_caids_in_day, big_union)

        home_ageb_catalog.to_parquet(f"{os.environ['WAREHOUSE_PATH']}/home_ageb_catalog_daily/{YEAR}_{MONTH}_{DAY}_home_agebs.parquet")

        print(home_ageb_catalog)


