from typing import Any
from core.core_abstract import AbstractHandler
from core.context import ExtractContext
from utils.duckaccess import DuckSession

import utils.DateUtils as du

from extractors.extractqueries import ExtractQueries

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

        context.payload = dfs

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.extract(request))
