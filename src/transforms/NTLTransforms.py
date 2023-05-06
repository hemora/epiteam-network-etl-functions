from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context

from datetime import timedelta, date, datetime
from dateutil.relativedelta import *
from dateutil.rrule import rrule, DAILY

from pyspark.sql.window import Window
from pyspark.sql.functions import col, isnull, lit, row_number, sum, count
from pyspark.sql.functions import when, window
from pyspark.sql.functions import from_unixtime, from_utc_timestamp, hour, to_date
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

    def prepare(self, payload: Context):
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
    
        payload.home_ageb_catalog = prepared_df
        
        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.prepare(request))
        
        return self.prepare(request)
    
class NTLCountWinner(AbstractHandler):

    def local_winner(self, payload: Context):
        """
        """
        payload.logger.info("Computing Candidates with Sum")
        #payload.logger.info(f"Number of distinct caids: {payload.home_ageb_catalog.persist().select('caid').distinct().count()}")

        w = Window().partitionBy("caid").orderBy(col("count").desc())
        w_by_caids = Window().partitionBy("caid")
        candidates = payload.home_ageb_catalog.groupBy(
                "caid", "h3index_12", to_date(col("cdmx_datetime")).alias("cdmx_date")
            ) \
            .agg(count(col("*")).alias("pings_per_day")) \
            .withColumn("total_pings", sum(col("pings_per_day")).over(w_by_caids)) \
            .where(
                (col("total_pings") >= lit(10)) & (col("pings_per_day") >= lit(5))
            )
        
        w_by_score = Window().partitionBy("caid").orderBy(col("score").desc())
        winners = candidates.groupBy(
                "caid", "h3index_12"
            ) \
            .agg(sum(col("pings_per_day")).alias("score")) \
            .withColumn("rank", row_number().over(w_by_score)) \
            .where(col("rank") == lit(1))

        #payload.logger.info(f"Number of distinct caids: {winners.select('caid').distinct().count()}")
        #payload.logger.info(f"Number of regs: {winners.count()}")

        payload.home_ageb_catalog = winners

        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.local_winner(request))
        
        return self.local_winner(request)
    

class NTLJoiner(AbstractHandler):

    def join(self, payload: Context):
        """
        """
        payload.logger.info("Adding home_agebs")
        #payload.logger.info(f"Total Caids: {payload.df.select('caid').distinct().count()}" )

        joined_df = payload.df.alias("a").join(
                payload.home_ageb_catalog.alias("b")
                , col("a.caid") == col("b.caid")
                , how="left"
            ) \
            .select(col("a.*")
                    , when(isnull(col("b.h3index_12")), lit("000000000000000")).otherwise(col("b.h3index_12")).alias("home_ageb")) 

        payload.df = joined_df

        payload.df.write.mode("overwrite").parquet("./temp/procesed_pings.parquet")

        #payload.logger.info(
        #    f"Caids with home_ageb: {joined_df.where(col('home_ageb') != lit('000000000000000')).select('caid').distinct().count()}"
        #    )
        #payload.logger.info(
        #    f"Caids without home_ageb: {joined_df.where(col('home_ageb') == lit('000000000000000')).select('caid').distinct().count()}"
        #    )

        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.join(request))
        
        return self.join(request)