from typing import Any
from core.core_abstract import AbstractHandler
from core.context import TransformContext

from queries.ntl_queries import NTLQueries
from queries.extractqueries import ExtractQueries

from utils.duckaccess import DuckSession
import utils.DateUtils as du

from shapely.geometry import Point, shape
from geopandas import GeoDataFrame
from pandas import concat
import numpy as np
from h3 import h3
import json

from pandas import concat

class NTLPreparation(AbstractHandler):

    def prepare(self, context: TransformContext):
        """
        """
        with DuckSession() as duck:
            unique_caids = duck.sql(
                NTLQueries.UNIQUE_CAIDS(context.raw_pings_target)
            ).df()

        print(f"Unique caids shape: {unique_caids.shape}")

        date_str, dates = du.get_last_dates(context.year, context.month, context.day, 15)

        dfs = []

        for d in dates:
            curr_path = \
                f"{context.data_source}/month={str(d.month).zfill(2)}/day={str(d.day).zfill(2)}"

            with DuckSession() as duck:
                result = duck.sql(
                    NTLQueries \
                        .EXTRACT_IN_DATE_RANGE(date_str, curr_path)
                ).df()

                if not result.empty:
                    dfs.append(result)
        
        last_n_days_data = concat(dfs)
        print(last_n_days_data.shape)
        with DuckSession() as duck:
            last_n_days_data = duck.sql("""
            SELECT b.*
            FROM unique_caids AS a
                INNER JOIN 
                last_n_days_data AS b
                ON a.caid = b.caid
            """
            ).df()
        
        last_n_days_data["h3index_12"] = last_n_days_data[["latitude", "longitude"]] \
                        .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 12), axis=1)
        print(last_n_days_data.shape)

        context.payload = last_n_days_data

        return context


    def handle(self, request: Any) -> Any:
        return super().handle(self.prepare(request))
    
class NTLWinners(AbstractHandler):

    def get_winners(self, context: TransformContext):

        last_n_days_data = context.payload

        with DuckSession() as duck:

            candidates = duck.sql(
                NTLQueries.WINNERS("last_n_days_data")
            ).df()

        print(candidates.shape)

        context.payload = candidates

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.get_winners(request))

class NTLJoiner(AbstractHandler):

    def join(self, context: TransformContext):
        
        home_ageb_catalog = context.payload
        print(f"Home Ageb catalog shape: {home_ageb_catalog.shape}")

        with DuckSession() as duck:

            pings_with_agebs = duck.sql(
                NTLQueries.JOIN(context.raw_pings_target)
            ).df()

        pings_with_agebs.to_parquet(context.ntl_pings_target)
            
        context.payload = pings_with_agebs

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.join(request))

class NTLLocator(AbstractHandler):

    def locate(self, context: TransformContext):
        
        with DuckSession() as duck:

            pings = duck.sql(
                NTLQueries.SELECT_IF_EXISTS(context.ntl_pings_target)
            ).df()
            pings["aux_latitude"] = pings["home_h3index_12"].apply(lambda x : h3.h3_to_geo(x)[0])
            pings["aux_longitude"] = pings["home_h3index_12"].apply(lambda x : h3.h3_to_geo(x)[1])
            pings["geometry"] = pings[["aux_latitude", "aux_longitude"]].apply(lambda x : Point(x["aux_longitude"], x["aux_latitude"]), axis=1)

            agebs = duck.sql(f"""
                WITH
                pre AS (
                    SELECT *
                    FROM read_parquet('{context.ageb_catalog}')
                )

                SELECT *
                FROM pre
            """).df()
            agebs["geometry"] = agebs["geometry"].apply(lambda x: shape(json.loads(x)))

        gdf_L = GeoDataFrame(pings, geometry='geometry', crs="EPSG:4326")
        gdf_R = GeoDataFrame(agebs, geometry='geometry', crs="EPSG:4326")

        joined = gdf_L.sjoin(gdf_R, how="left")
        joined["h3index_12"] = joined[["latitude", "longitude"]] \
            .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 12), axis=1)
        joined["h3index_15"] = joined[["latitude", "longitude"]] \
            .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 15), axis=1)

        located_df = joined[["utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude"
                             , "horizontal_accuracy", "h3index_12", "h3index_15", "home_h3index_12"
                             , "cve_geo", "cve_agee", "nom_agee", "nom_agem"]]
        located_df = located_df.rename(columns={"cve_geo": "home_ageb", "cve_agee" : "home_agee", "nom_agee" : "home_agee_nom", "nom_agem" : "home_agem_nom"})

        print(located_df)
        print(located_df.columns)

        located_df["geometry"] = pings[["latitude", "longitude"]].apply(lambda x : Point(x["longitude"], x["latitude"]), axis=1)
        located_df = GeoDataFrame(located_df, geometry="geometry", crs="EPSG:4326")
        located_df = located_df.sjoin(gdf_R, how="left")
        located_df = located_df[["utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude"
                             , "horizontal_accuracy", "h3index_12", "h3index_15", "home_h3index_12"
                             , "home_ageb", "home_agee", "home_agee_nom", "home_agem_nom", "cve_agee"]]

        
        if not context.only_if_exists :

            with DuckSession() as duck:

                not_existant = duck.sql(
                    NTLQueries.SELECT_NOT_EXISTS(context.ntl_pings_target)
                ).df()

                not_existant["h3index_12"] = joined[["latitude", "longitude"]] \
                    .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 12), axis=1)
                not_existant["h3index_15"] = joined[["latitude", "longitude"]] \
                    .apply(lambda x : h3.geo_to_h3(x["latitude"], x["longitude"], 15), axis=1)
                
                not_existant["home_ageb"] = np.nan
                not_existant["home_agee"] = np.nan
                not_existant["home_agee_nom"] = np.nan
                not_existant["home_agem_nom"] = np.nan
                not_existant["cve_agee"] = np.nan

                not_existant = not_existant[["utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude"
                             , "horizontal_accuracy", "h3index_12", "h3index_15", "home_h3index_12"
                             , "home_ageb", "home_agee", "home_agee_nom", "home_agem_nom", "cve_agee"]]
                
            located_df = concat([not_existant, located_df])

            print(located_df.columns)
            print(located_df)
        
        with DuckSession() as duck:
        
            located_df = duck.sql("""
                SELECT utc_timestamp, cdmx_datetime, caid
                    , latitude, longitude, horizontal_accuracy
                    , h3index_12, h3index_15
                    , home_h3index_12
                    , home_ageb
                    , home_agee
                    , home_agee_nom
                    , home_agem_nom
                    , cve_agee
                FROM located_df
            """).df()

        located_df.to_parquet("./temp/located_pings.parquet")
        
        context.payload = located_df

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.locate(request))