from extractors.ParquetExtractor import ParquetExtractor
from transforms.NTLTransforms import NTLPreparation, NTLLocalWinner, LocalizationStage
from wrappers.SparkWrapper import SparkWrapper
from core.context import Context, ParquetContext
from pyspark.sql import functions as F

from dotenv import load_dotenv
import os
load_dotenv()


if __name__ == "__main__":

    sc = SparkWrapper("Test")
    ctxt = Context("2020", "01", "16", sc.get_session())
    ptxt = ParquetContext(ctxt, os.environ[f"MOVILIDAD_RAW_{ctxt.year}"])

    pe = ParquetExtractor()
    ntl1 = NTLPreparation()
    ntl2 = NTLLocalWinner()
    #ntl3 = LocalizationStage()

    pe.set_next(ntl1).set_next(ntl2)

    result = pe.handle(ptxt)

    print(type(result))
    print(result.show())
    #print(result.select(F.to_date(F.col("cdmx_datetime"))).distinct().show(10))
