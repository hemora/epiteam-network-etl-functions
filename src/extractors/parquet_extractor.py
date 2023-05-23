from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context
from utils.duckaccess import DuckSession

import utils.DateUtils as du

from queries.extractqueries import ExtractQueries

from pandas import concat

from dotenv import load_dotenv
import os
load_dotenv()

class ParquetExtractor(AbstractHandler):
    """
    """
    def extract(self, context: Context):
        """
        """
        dates = du.trusted_range(context.year, context.month, context.day, 10)

        dfs = []

        for d in dates:
            curr_path = \
                f"{os.environ[f'MOVILIDAD_RAW_{context.year}']}/month={str(d.month).zfill(2)}/day={str(d.day).zfill(2)}"

            with DuckSession() as duck:

                result = duck.sql(
                    ExtractQueries \
                        .PARQUET_READER(context.year, context.month, context.day, curr_path)
                ).df()

                if not result.empty:
                    dfs.append(result)
        
        big_union = concat(dfs)

        big_union.to_parquet(f"{context.base_dir}/raw_pings.parquet")

        context.payload = big_union

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.extract(request))