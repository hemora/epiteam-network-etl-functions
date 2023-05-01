
from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context, ParquetContext

class NTLPreparation(AbstractHandler):

    def __init__(self, spark_wrapper, context: Context):
        self.spark_wrapper = spark_wrapper
        self.context = context

    def handle(self, request: Any) -> Any:
        return super().handle(request)