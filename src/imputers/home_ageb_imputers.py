from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context
from utils.duckaccess import DuckSession
from queries.interaction_queries import InteractionQueries

from h3 import h3
from shapely.geometry import Point
from geopandas import datasets, GeoDataFrame, read_file, GeoSeries
from pandas import concat

from shapely.geometry import shape
import json

from dotenv import load_dotenv
import os
load_dotenv()


class NullLocationImputer(AbstractHandler):

    def impute(self, context: Context):

        h3index_col, ageb_col, df = context.payload

        with DuckSession() as duck:

            not_located_pings = duck.sql(f"""
            SELECT DISTINCT caid, {h3index_col}
            FROM read_parquet('{context.base_dir}/located_pings.parquet')
            WHERE {ageb_col} IS NULL
            """).df()

            xs = []

            for i in range(5):

                not_located_pings["h3_neighbours"] = \
                    not_located_pings[h3index_col].apply(lambda x: list(h3.k_ring(x, i+1)))
                
                not_located_pings = duck.sql(f"""
                SELECT a.caid, a.{h3index_col}, b.h3_neighbours
                FROM not_located_pings AS a, UNNEST(h3_neighbours) AS b
                ORDER BY caid ASC
                """).df()
                not_located_pings["aux_latitude"] = \
                    not_located_pings["h3_neighbours"] \
                    .apply(lambda x : h3.h3_to_geo(x)[0])
                not_located_pings["aux_longitude"] = \
                    not_located_pings["h3_neighbours"] \
                    .apply(lambda x : h3.h3_to_geo(x)[1])
                not_located_pings["geometry"] = \
                    not_located_pings[["aux_latitude", "aux_longitude"]] \
                    .apply(lambda x : Point(x["aux_longitude"], x["aux_latitude"]), axis=1)
                
                agebs = duck.sql(f"""
                SELECT *
                FROM read_parquet('{os.environ['AGEB_CATALOG']}')
                """).df()
                agebs["geometry"] = agebs["geometry"].apply(lambda x: shape(json.loads(x)))

                gdf_L = GeoDataFrame(not_located_pings, geometry='geometry', crs="EPSG:4326")
                gdf_R = GeoDataFrame(agebs, geometry='geometry', crs="EPSG:4326")

                joined = gdf_L.sjoin(gdf_R, how="left")

                aux = joined.drop(columns=["geometry"])

                aux = duck.sql(f"""
                SELECT *
                FROM (
                    SELECT caid, {h3index_col}, h3_neighbours, cve_geo
                        , ROW_NUMBER() OVER (PARTITION BY CAID ORDER BY cve_geo NULLS LAST) AS rank
                    FROM aux
                )
                WHERE rank = 1
                """).df()

                imputed = duck.sql(f"""
                SELECT caid, cve_geo
                FROM aux
                WHERE cve_geo IS NOT NULL
                """).df()

                xs.append(imputed)

                remains = duck.sql(f"""
                SELECT caid, {h3index_col}
                FROM aux
                WHERE cve_geo IS NULL
                """).df()

                if remains.empty:
                    break

                not_located_pings = remains
            #END for

            ageb_catalog = concat(xs)

            all_pings = duck.sql(f"""
            SELECT a.* EXCLUDE ({ageb_col})
                , IF(a.{ageb_col} IS NULL, b.cve_geo, a.{ageb_col}) AS {ageb_col}
            FROM 
                read_parquet('{context.base_dir}/located_pings.parquet') AS a
                LEFT JOIN
                ageb_catalog AS b
                ON a.caid = b.caid
            """).df()

            all_pings.to_parquet(f"{context.base_dir}/located_pings.parquet")

        context.payload = all_pings
        
        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.impute(request))

class NeighbourFrequencyImputer(AbstractHandler):

    def impute(self, context: Context):
        
        with DuckSession() as duck:

            for i in range(6):

                print(f"{i}st try")

                interactions_table = duck.sql(
                    InteractionQueries \
                        .INTERACTIONS(f"{context.base_dir}/located_pings.parquet")
                )

                interactions_table.to_parquet(f"{context.base_dir}/interactions_table.parquet")

                interactions_base = duck.sql(f"""
                WITH
                interactions_base AS (
                    SELECT DISTINCT
                        [LEAST(a_caid, b_caid), GREATEST(a_caid, b_caid)] AS connected_component
                        , IF(a_home_ageb = '0000000000000', b_home_ageb, a_home_ageb) AS home_ageb
                    FROM read_parquet('{context.base_dir}/interactions_table.parquet')
                )

                SELECT *
                FROM interactions_base
                """).df() 

                caids_without_home_ageb = duck.sql(f"""
                SELECT DISTINCT caid
                FROM read_parquet('{context.base_dir}/located_pings.parquet')
                WHERE home_ageb = '0000000000000'
                """).df()

                ageb_catalog = duck.sql("""
                WITH
                joined_faltantes AS (
                    SELECT *
                    FROM 
                        caids_without_home_ageb AS a
                        INNER JOIN
                        interactions_base AS b
                        ON ARRAY_CONTAINS(b.connected_component, a.caid)
                )

                , scored_home_ageb AS (
                    SELECT *
                        , ROW_NUMBER() OVER (PARTITION BY caid ORDER BY score DESC) AS rank_score
                    FROM (
                        SELECT caid, home_ageb, COUNT(*) AS score
                        FROM joined_faltantes
                        GROUP BY 1, 2
                    )
                    WHERE home_ageb != '0000000000000'
                    ORDER BY 1 DESC, 3 DESC
                )

                SELECT *
                FROM scored_home_ageb
                WHERE rank_score = 1
                """).df()

                all_pings = duck.sql(f"""
                SELECT a.* EXCLUDE (home_ageb)
                    , CASE
                        WHEN a.home_ageb = '0000000000000' AND b.home_ageb != '0000000000000'
                            THEN b.home_ageb
                        ELSE a.home_ageb
                    END AS home_ageb
                FROM 
                    read_parquet('{context.base_dir}/located_pings.parquet') AS a
                    LEFT JOIN
                    ageb_catalog AS b
                    ON a.caid = b.caid
                """).df()
            #END for

            all_pings.to_parquet(f"{context.base_dir}/located_pings.parquet")
        
        context.payload = all_pings

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.impute(request))