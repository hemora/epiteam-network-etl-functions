from extractors.ParquetExtractor import ParquetExtractor
from wrappers.SparkWrapper import SparkWrapper
from core.context import Context, ParquetContext
from pyspark.sql import functions as F

from dotenv import load_dotenv
import os
load_dotenv()


if __name__ == "__main__":

    sc = SparkWrapper("Test")
    ctxt = Context("2020", "01", "01")


    #print(ctxt.year)
    #print(ctxt.month)
    #print(ctxt.day)

    ptxt = ParquetContext(ctxt, os.environ[f"MOVILIDAD_RAW_{ctxt.year}"])
    pe = ParquetExtractor(sc.get_session(), ptxt)

    result = pe.handle(ptxt)

    print(type(result))
    print(result.select(F.to_date(F.col("cdmx_datetime"))).distinct().show(10))

    #print(ptxt.parquet_path)
    #print(ptxt.day)
    #print(ptxt.year)
    #print(ptxt.month)