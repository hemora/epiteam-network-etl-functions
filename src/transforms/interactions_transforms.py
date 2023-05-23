from typing import Any
from core.core_abstract import AbstractHandler
from core.context import InteractionsContext, Context

from queries.interaction_queries import InteractionQueries

from utils.duckaccess import DuckSession

class VMInteractionsCompute(AbstractHandler):
    """
    """
    def compute(self, context: Context):
        """
        """
        with DuckSession() as duck:

            interactions_table = duck.sql(
                InteractionQueries. \
                    INTERACTIONS_IN_VM(f"{context.base_dir}/located_pings.parquet")
            ).df()
        
        interactions_table.to_parquet(f"{context.base_dir}/interactions_table.parquet")

        context.payload = interactions_table
        
        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.compute(request))