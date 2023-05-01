from extractors.ParquetExtractor import ParquetExtractor
from wrappers.SparkWrapper import SparkWrapper
from core.context import Context, ParquetContext
from pyspark.sql import functions as F

from dotenv import load_dotenv
import os
load_dotenv()


if __name__ == "__main__":

    sc = SparkWrapper("Test")
    ctxt = Context("2020", "01", "01", sc.get_session())

    ptxt = ParquetContext(ctxt, os.environ[f"MOVILIDAD_RAW_{ctxt.year}"])
    pe = ParquetExtractor()

    result = pe.handle(ptxt)

    print(type(result))
    print(result.show(10))
    print(result.select(F.to_date(F.col("cdmx_datetime"))).distinct().show(10))
