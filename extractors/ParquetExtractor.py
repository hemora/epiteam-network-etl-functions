from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context

from pyspark.sql.types import StringType, StructField, StructType, DoubleType

from datetime import timedelta, date, datetime
from dateutil.relativedelta import *
from dateutil.rrule import rrule, DAILY

from pyspark.sql.functions import col, from_unixtime, lit, to_date, from_utc_timestamp

from h3_pyspark.indexing import index_shape
from h3_pyspark import geo_to_h3

import os

class ParquetExtractor(AbstractHandler):
    """
    """
    #def __init__(self, spark_wrapper, context: Context):
    #    self.spark_wrapper = spark_wrapper
    #    self.context = context

    def trust_range_date(self, year:str, month:str, day: str,interval: int):

        aux_day = date(int(year), int(month), int(day))
        start_day = aux_day - timedelta(days=int(interval / 2))
        end_day = aux_day + timedelta(days=+int(interval / 2))

        print(f"{start_day} <----> {end_day}")

        date_range = [dt for dt in rrule(DAILY, dtstart=start_day, until=end_day)]

        return date_range
        
    
    def extract(self, payload: Context):
        """
        """
        payload.logger.info("Extract Stage")

        schema = StructType([
            StructField('utc_timestamp', StringType(), True)
            , StructField('cdmx_datetime', StringType(), True)
            , StructField('caid', StringType(), True)
            , StructField('latitude', DoubleType(), True)
            , StructField('longitude', DoubleType(), True)
            , StructField('horizontal_accuracy', DoubleType(), True)
            , StructField('h3index_12', StringType(), True)
            , StructField('h3index_15', StringType(), True)
        ])

        # Se crea un DataFrame vacío que sirva como acumulador
        emptyRDD = payload.spark.sparkContext.emptyRDD()
        emptyDF = payload.spark.createDataFrame(emptyRDD,schema)
        df_acc = emptyDF

        dates = self.trust_range_date(payload.year, payload.month, payload.day, 10)

        # Se unen iterativamente los dataframes que contienen al ageb candidato ganador por día
        for d in dates:

            if d < datetime(2020, 1, 1):
                continue

            curr_df = payload.spark.read.option("header", True) \
                .parquet(f"{payload.parquet_path}/month={str(d.month).zfill(2)}/day={str(d.day).zfill(2)}") \
                .where(col("horizontal_accuracy") >= lit(100.0)) \
                .distinct() \
                .withColumn("utc_datetime", from_unixtime(col("utc_timestamp"))) \
                .withColumn("cdmx_datetime", from_utc_timestamp(col("utc_datetime"), "America/Mexico_City")) \
                .withColumn("h3index_12", geo_to_h3("latitude", "longitude", lit(12))) \
                .withColumn("h3index_15", geo_to_h3("latitude", "longitude", lit(15))) \
                .select("utc_timestamp", "cdmx_datetime"
                        , "caid", "latitude", "longitude", "horizontal_accuracy"
                        , "h3index_12", "h3index_15") \
                .where(
                    to_date(col("cdmx_datetime")) == lit(date(int(payload.year), int(payload.month), int(payload.day)))
                )

            df_acc = df_acc.union(curr_df)
        
        payload.df = df_acc
        
        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.extract(request))
        
        return self.extract(request)

