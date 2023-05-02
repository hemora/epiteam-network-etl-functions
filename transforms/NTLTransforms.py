from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context, ParquetContext, NTLContext, LocalizationContext

from datetime import timedelta, date, datetime
from dateutil.relativedelta import *
from dateutil.rrule import rrule, DAILY

from pyspark.sql.window import Window
from pyspark.sql.functions import col, from_unixtime, lit, row_number, from_utc_timestamp, hour, explode
from pyspark.sql.types import StringType, StructField, StructType, DoubleType

from h3_pyspark.indexing import index_shape
from h3_pyspark import geo_to_h3, h3_to_parent


class NTLPreparation(AbstractHandler):

    def get_last_dates(self, year: str, month: str, day: str, offset: int):
        """
        """
        aux_day = date(int(year), int(month), int(day))
        start_day = aux_day - timedelta(days=offset)
        end_day = aux_day - timedelta(days=1)

        print(f"{start_day} <----> {end_day}")

        date_range = [dt for dt in rrule(DAILY, dtstart=start_day, until=end_day)]

        return date_range


    def prepare(self, payload: NTLContext):
        """
        """
        ## Se obtienen los CAIDS distintos
        unique_caids = payload.df.select("caid").distinct()

        ## Se leen los datos de 15 días atrás
        last_15_dates = self.get_last_dates(payload.year, payload.month, payload.day, 15)

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

        # Se unen iterativamente los dataframes que contienen al ageb candidato ganador por día
        for d in last_15_dates:

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
                    (hour(col("cdmx_datetime")) >= 22) | (hour(col("cdmx_datetime")) < 6)
                )

            df_acc = df_acc.union(curr_df)


        ## Se hace un INNER JOIN para obtener los datos atribuidos
        ## a los CAIDs de interés
        prepared_df = unique_caids.alias("a") \
            .join(df_acc.alias("b")
                  , col("a.caid") == col("b.caid")
                  , how="inner") \
            .select(col("b.*"))
        
        return NTLContext(payload.context, prepared_df)

    def handle(self, request: Any) -> Any:
        #return self.prepare(request)
        return super().handle(self.prepare(request))
    
class NTLLocalWinner(AbstractHandler):

    def local_winner(self, payload: NTLContext):
        """
        """
        w = Window().partitionBy("caid").orderBy(col("count").desc())
        candidates = payload.df.groupBy("caid", "h3index_12").count() \
            .withColumn("rank", row_number().over(w)) \
            .where(col("rank") == 1) \
            .select("caid", "h3index_12", h3_to_parent(col("h3index_12"), lit(5)).alias("h3index_5"))

        #return LocalizationContext(payload.year, payload.month, payload.day, payload.spark
        #                           , "./utils/ageb_catalog/", candidates)
        return candidates

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.local_winner(request))
        
        return self.local_winner(request)
    
class LocalizationStage(AbstractHandler):
    """
    """
    def locate(self, payload: LocalizationContext):
        """
        """
        ageb_df = payload.spark.read.parquet(payload.catalog_path) \
            .select("cve_geo", "geometry") \
            .withColumn("h3polyfill_5", index_shape("geometry", lit(5))) \
            .withColumn("h3polyfill_5", explode("h3polyfill_5")) \
            .withColumn("h3polyfill_12", index_shape("geometry", lit(12))) \
            .withColumn("h3polyfill_12", explode("h3polyfill_12"))
        
        candidates_unique = payload.df.select("h3index_5", "h3index_12").distinct()

        located_pre = candidates_unique.alias("a") \
            .join(ageb_df.alias("b")
                  , col("a.h3index_5") == col("b.h3polyfill_5")
                  , how="inner") \
            .where(col("a.h3index_12") == col("b.h3polyfill_12"))
        
        return located_pre



    def handle(self, request: Any) -> Any:
        return self.locate(request)
    