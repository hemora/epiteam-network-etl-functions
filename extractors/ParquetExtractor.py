from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context, ParquetContext

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, DoubleType

from datetime import timedelta, date, datetime
from dateutil.relativedelta import *
from dateutil.rrule import rrule, DAILY

import os

class ParquetExtractor(AbstractHandler):
    """
    """
    def __init__(self, spark_wrapper, context: Context):
        self.spark_wrapper = spark_wrapper
        self.context = context

    def trust_range_date(self, interval: int):

        aux_day = date(int(self.context.year), int(self.context.month), int(self.context.day))
        start_day = aux_day - timedelta(days=int(interval / 2))
        end_day = aux_day + timedelta(days=+int(interval / 2))

        print(f"{start_day} <----> {end_day}")

        date_range = [dt for dt in rrule(DAILY, dtstart=start_day, until=end_day)]

        return date_range
        
    
    def extract(self, payload: ParquetContext):
        """
        """
        schema = StructType([
            StructField('utc_timestamp', StringType(), True)
            , StructField('cdmx_datetime', StringType(), True)
            , StructField('caid', StringType(), True)
            , StructField('latitude', DoubleType(), True)
            , StructField('longitude', DoubleType(), True)
            , StructField('horizontal_accuracy', DoubleType(), True)
        ])

        # Se crea un DataFrame vacío que sirva como acumulador
        emptyRDD = self.spark_wrapper.sparkContext.emptyRDD()
        emptyDF = self.spark_wrapper.createDataFrame(emptyRDD,schema)
        df_acc = emptyDF

        dates = self.trust_range_date(10)

        # Se unen iterativamente los dataframes que contienen al ageb candidato ganador por día
        for d in dates:

            if d < datetime(2020, 1, 1):
                continue

            curr_df = self.spark_wrapper.read.option("header", True) \
                .parquet(f"{payload.parquet_path}/month={str(d.month).zfill(2)}/day={str(d.day).zfill(2)}") \
                .where(F.col("horizontal_accuracy") >= F.lit(100.0)) \
                .distinct() \
                .withColumn("utc_datetime", F.from_unixtime(F.col("utc_timestamp"))) \
                .withColumn("cdmx_datetime", F.from_utc_timestamp(F.col("utc_datetime"), "America/Mexico_City")) \
                .select("utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude", "horizontal_accuracy") \
                .where(
                    F.to_date(F.col("cdmx_datetime")) <= F.lit(date(int(payload.year), int(payload.month), int(payload.day)))
                )

            df_acc = df_acc.union(curr_df)
        
        return df_acc

    def handle(self, request: Any) -> Any:
        return self.extract(request)

