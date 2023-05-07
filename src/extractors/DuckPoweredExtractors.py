from typing import Any
from core.core_abstract import AbstractHandler
from core.context import ExtractContext

import utils.DateUtils as du

class ParquetExtractor(AbstractHandler):
    """
    """
    def extract(self, context: ExtractContext):
        pass

    def handle(self, request: Any) -> Any:
        return super().handle(self.extract(request))
