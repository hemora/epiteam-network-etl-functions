from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context
from utils.duckaccess import DuckSession

from shapely.geometry import Point
from geopandas import datasets, GeoDataFrame, read_file, GeoSeries
from pandas import concat

from shapely.geometry import shape
import shapely.wkt

import json

from h3 import h3
import numpy as np

from dotenv import load_dotenv
import os
load_dotenv()

class HomeAgebLocator(AbstractHandler):

    def locate(self, context: Context):

        with DuckSession() as duck:

            pings_with_home_ageb = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/pings_with_home_ageb.parquet')
            WHERE home_h3index_12 != '000000000000000'
            """).df()

            pings_without_home_ageb = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/pings_with_home_ageb.parquet')
            WHERE home_h3index_12 = '000000000000000'
            """).df()

            pings_with_home_ageb["aux_latitude"] = pings_with_home_ageb["home_h3index_12"] \
                .apply(lambda x : h3.h3_to_geo(x)[0])
            pings_with_home_ageb["aux_longitude"] = pings_with_home_ageb["home_h3index_12"] \
                .apply(lambda x : h3.h3_to_geo(x)[1])
            pings_with_home_ageb["geometry"] = \
                pings_with_home_ageb[["aux_latitude", "aux_longitude"]] \
                .apply(lambda x : Point(x["aux_longitude"], x["aux_latitude"]), axis=1)
            
            agebs = duck.sql(f"""
            SELECT *
            FROM read_parquet('{os.environ['AGEB_CATALOG']}')
            """).df()
            agebs["geometry"] = agebs["geometry"].apply(lambda x: shape(json.loads(x)))

            gdf_L = GeoDataFrame(pings_with_home_ageb, geometry='geometry', crs="EPSG:4326")
            gdf_R = GeoDataFrame(agebs, geometry='geometry', crs="EPSG:4326")

            joined = gdf_L.sjoin(gdf_R, how="left")
            joined["h3index_12"] = joined[["latitude", "longitude"]] \
                .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 12), axis=1)
            joined["h3index_15"] = joined[["latitude", "longitude"]] \
                .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 15), axis=1)
            
            located_df = joined[["utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude"
                         , "horizontal_accuracy", "h3index_12", "h3index_15", "home_h3index_12"
                         , "cve_geo"]]
            located_df = located_df \
                .rename(columns={"cve_geo": "home_ageb"})

            pings_without_home_ageb["h3index_12"] = pings_without_home_ageb[["latitude", "longitude"]] \
                    .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 12), axis=1)
            pings_without_home_ageb["h3index_15"] = pings_without_home_ageb[["latitude", "longitude"]] \
                    .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 15), axis=1)
            pings_without_home_ageb["home_ageb"] = "0000000000000"
            pings_without_home_ageb = pings_without_home_ageb[["utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude"
                 , "horizontal_accuracy", "h3index_12", "h3index_15", "home_h3index_12"
                 , "home_ageb"]]
            
            all_pings = concat([located_df, pings_without_home_ageb])
            all_pings.to_parquet(f"{context.base_dir}/located_pings.parquet")

            context.payload = ("home_h3index_12", "home_ageb", all_pings)
        
        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.locate(request))

class PingLocator(AbstractHandler):

    def locate(self, context: Context):
        """
        """
        with DuckSession() as duck:

            pings_base = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/located_pings.parquet')
            """).df()
            pings_base["geometry"] = pings_base[["latitude", "longitude"]] \
                .apply(lambda x : Point(x["longitude"], x["latitude"]), axis=1)
            
            agebs = duck.sql(f"""
            SELECT *
            FROM read_parquet('{os.environ['AGEB_CATALOG']}')
            """).df()
            agebs["geometry"] = agebs["geometry"].apply(lambda x: shape(json.loads(x)))

            gdf_L = GeoDataFrame(pings_base, geometry='geometry', crs="EPSG:4326")
            gdf_R = GeoDataFrame(agebs, geometry='geometry', crs="EPSG:4326")

            joined = gdf_L.sjoin(gdf_R, how="left")

            located_df = joined[["utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude"
                , "horizontal_accuracy", "h3index_12", "h3index_15", "home_h3index_12"
                , "home_ageb", "cve_geo"]]
            
            located_df.to_parquet(f"{context.base_dir}/located_pings.parquet")

            context.payload = ("h3index_12", "cve_geo", located_df)

            return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.locate(request))