from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context
from wrappers.DuckWrapper import DuckWrapper

from shapely.geometry import Point
from geopandas import datasets, GeoDataFrame, read_file, GeoSeries

from shapely.geometry import shape
import shapely.wkt

import json


from h3 import h3

class LocalizationTransform(AbstractHandler):

    def locate(self, payload: Context):

        if payload.duck is None:
            payload.duck = DuckWrapper().get_session()

        pings = payload.duck.sql("""
            WITH
            pre AS (
                SELECT *
                FROM read_parquet('./temp/procesed_pings.parquet/*.parquet')
                WHERE home_ageb != '000000000000000'
            )

            SELECT *
            FROM pre
        """).df()
        pings["aux_latitude"] = pings["home_ageb"].apply(lambda x : h3.h3_to_geo(x)[0])
        pings["aux_longitude"] = pings["home_ageb"].apply(lambda x : h3.h3_to_geo(x)[1])
        pings["geometry"] = pings[["aux_latitude", "aux_longitude"]].apply(lambda x : Point(x["aux_longitude"], x["aux_latitude"]), axis=1)

        agebs = payload.duck.sql("""
            WITH
            pre AS (
                SELECT *
                FROM read_parquet('./raw_data/utils/ageb_catalog/*.parquet')
            )

            SELECT *
            FROM pre
        """).df()
        agebs["geometry"] = agebs["geometry"].apply(lambda x: shape(json.loads(x)))

        gdf_L = GeoDataFrame(pings, geometry='geometry', crs="EPSG:4326")
        gdf_R = GeoDataFrame(agebs, geometry='geometry', crs="EPSG:4326")

        joined = gdf_L.sjoin(gdf_R, how="left")

        located_df = joined[["utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude", 
                             "horizontal_accuracy", "h3index_12", "h3index_15", "home_ageb"
                             , "cve_geo", "cve_agee", "nom_agee", "nom_agem"]]

        payload.duck.sql("""
            SELECT utc_timestamp, cdmx_datetime, caid
                , latitude, longitude, horizontal_accuracy, h3index_12, h3index_15
                , home_ageb AS home_h3index_12
                , cve_geo AS home_ageb
                , cve_agee AS home_agee
                , nom_agee AS home_agee_nom
                , nom_agem AS home_agem_nom
            FROM located_df
        """).write_parquet("./temp/located_df.parquet")

        payload.df = located_df

        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.locate(request))
        
        return self.locate(request)