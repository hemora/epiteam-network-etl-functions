import sys
sys.path.append("/home/hmora/network-gen-pipeline/src")

import utils.DateUtils as du
import utils.path_utils as pu
from utils.stopwatch import Stopwatch
from utils.duckaccess import DuckSession
from utils.sparkaccess import SparkAccess

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

import time

import numpy as np

import concurrent.futures

def get_df_with_geometry(df: pd.DataFrame):
    """
    """
    df["geometry"] = df[["latitude", "longitude"]] \
        .apply(lambda x : Point(x["longitude"], x["latitude"]), axis=1)
    
    return df

def locate_in_agebs(df: pd.DataFrame):
    """
    Función encargada de localizar cada ping dentro de su correspondiente AGEB
    """
    print("Partition")
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
        joined = joined.drop(columns=["index_right"])

        zm_catalog = duck.sql(f"""
        SELECT cve_geo AS cve_mun
            , cve_zm, geometry
        FROM read_parquet('{os.environ["ZM_CATALOG"]}')
        """).df()
        zm_catalog["geometry"] = zm_catalog["geometry"].apply(lambda x: shape(json.loads(x)))

        gdf_R = GeoDataFrame(zm_catalog, geometry='geometry', crs="EPSG:6365")

        joined = joined.sjoin(gdf_R, how="left")

        joined = joined.drop(columns=["geometry", "index_right"])

        joined["h3index_15"] = joined[["latitude", "longitude"]] \
            .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 15), axis=1)

    return joined

def locate_in_agebs_arrow(tuple):
    """
    Función encargada de localizar cada ping dentro de su correspondiente AGEB
    """
    i, arrow_batch = tuple
    print(f"Processing {i} batch")
    df = arrow_batch.to_pandas()
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
        joined = joined.drop(columns=["index_right"])

        zm_catalog = duck.sql(f"""
        SELECT cve_geo AS cve_mun
            , cve_zm, geometry
        FROM read_parquet('{os.environ["ZM_CATALOG"]}')
        """).df()
        zm_catalog["geometry"] = zm_catalog["geometry"].apply(lambda x: shape(json.loads(x)))

        gdf_R = GeoDataFrame(zm_catalog, geometry='geometry', crs="EPSG:6365")

        joined = joined.sjoin(gdf_R, how="left")

        joined = joined.drop(columns=["geometry", "index_right"])

        joined["h3index_15"] = joined[["latitude", "longitude"]] \
            .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 15), axis=1)

    return joined

def driver(i: int):
    """
    """
    YEAR, MONTH, DAY = ("2021", "01", str(i).zfill(2))


    output_dir = pu.create_if_not_exists(
       f"{os.environ['WAREHOUSE_PATH']}/fact_pings_base/{YEAR}_{MONTH}_{DAY}_pings"
    )

    print(output_dir)

    #arrow_dataset = pq.ParquetFile(f"/datos/EpiTeam/warehouse/daily_semi_raw_pings/{YEAR}_{MONTH}_{DAY}.parquet")
    arrow_dataset = ds.dataset(f"/datos/EpiTeam/warehouse/daily_semi_raw_pings/{YEAR}_{MONTH}_{DAY}.parquet")

    print(type(arrow_dataset))

    with DuckSession() as duck:

        duck_dataset = duckdb.arrow(arrow_dataset)

        distinct_coordinates = duck.sql("""
        SELECT DISTINCT latitude, longitude
        FROM arrow_dataset
        """).arrow()

        #print(distinct_coordinates)
        #print(type(distinct_coordinates))

    print(len(distinct_coordinates.to_batches()))

    batches = distinct_coordinates.to_batches()

    #batches = list(arrow_dataset.iter_batches())
    batches = [(i, batch) for i, batch in enumerate(batches)]
    # For TEST PORPOUSES
    # batches = batches[:10]

    with mp.Pool(10) as p:

        located_pings = p.map(locate_in_agebs_arrow, batches)

    result = concat(located_pings)
    #print(type(result))
    #print(result.head(10))

    result = pa.Table.from_pandas(result, preserve_index=False)
    #print(type(result))

    with DuckSession() as duck:

        duck_dataset = duckdb.arrow(arrow_dataset)

        final = duck.sql("""
        SELECT 
            a.utc_timestamp
            , a.cdmx_datetime
            , a.caid
            , a.latitude
            , a.longitude
            , a.horizontal_accuracy
            , b.cve_geo
            , b.cve_mun
            , b.cve_zm
            , b.h3index_15
        FROM
            duck_dataset AS a
            LEFT JOIN
            result AS b
            ON a.latitude = b.latitude
                AND a.longitude = b.longitude
        """).arrow()

        # print(final)

    pq.write_to_dataset(final, root_path=output_dir, partition_cols=["cve_zm"])
    
    return


if __name__ == "__main__":

    with Stopwatch():

        YEAR, MONTH = ("2021", "01")

        start, end = du.first_and_last_days(YEAR, MONTH)

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(driver, range(start, end+1))
        # [driver(i) for i in range(1,32)]
        # for i in range(start, end+1):
        #     driver(i)


    # print("") 
        #driver(13)