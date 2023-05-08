from typing import Any
from core.core_abstract import AbstractHandler
from core.context import TransformContext

from queries.ntl_queries import NTLQueries
from queries.extractqueries import ExtractQueries

from utils.duckaccess import DuckSession
import utils.DateUtils as du

from h3 import h3

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

        dates = du.get_last_dates(context.year, context.month, context.day, 15)

        dfs = []

        for d in dates:
            curr_path = \
                f"{context.data_source}/month={str(d.month).zfill(2)}/day={str(d.day).zfill(2)}"

            with DuckSession() as duck:
                result = duck.sql(
                    ExtractQueries \
                        .PARQUET_READER(context.year, str(d.month).zfill(2), str(d.day).zfill(2), curr_path)
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

        print(type(NTLQueries.WINNERS))
        
        with DuckSession() as duck:

            candidates = duck.sql(
                NTLQueries.WINNERS("last_n_days_data")
            ).df()

        print(candidates.shape)

        context.payload = candidates

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.get_winners(request))