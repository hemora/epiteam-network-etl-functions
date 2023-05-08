from typing import Any
from core.core_abstract import AbstractHandler
from core.context import ExtractContext
from utils.duckaccess import DuckSession

import utils.DateUtils as du

from queries.extractqueries import ExtractQueries

from pandas import concat

class ParquetExtractor(AbstractHandler):
    """
    """
    def extract(self, context: ExtractContext):
        """
        """
        dates = du.trusted_range(context.year, context.month, context.day, 10)

        dfs = []

        for d in dates:
            curr_path = \
                f"{context.data_source}/month={str(d.month).zfill(2)}/day={str(d.day).zfill(2)}"

            with DuckSession() as duck:
                result = duck.sql(
                    ExtractQueries \
                        .PARQUET_READER(context.year, context.month, context.day, curr_path)
                ).df()

                if not result.empty:
                    dfs.append(result)
        
        big_union = concat(dfs)

        big_union.to_parquet("../temp/raw_pings.parquet")

        context.payload = big_union

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.extract(request))
