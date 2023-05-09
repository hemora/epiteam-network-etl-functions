from typing import Any
from core.core_abstract import AbstractHandler
from core.context import InteractionsContext

from queries.interaction_queries import InteractionQueries

from utils.duckaccess import DuckSession

class InteractionsCompute(AbstractHandler):
    """
    """
    def compute(self, context: InteractionsContext):
        """
        """

        print(context.in_vm)
        with DuckSession() as duck:

            interactions_table = duck.sql(
                InteractionQueries.INTERACTIONS_IN_VM(context.pings_base) \
                    if context.in_vm else \
                        InteractionQueries.ALL_INTERACTIONS(context.pings_base)
            ).df()
        
        interactions_table.to_parquet("./temp/interactions_table.parquet")

        context.payload = interactions_table
        
        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.compute(request))