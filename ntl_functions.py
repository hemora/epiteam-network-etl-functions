import pyspark
import h3_pyspark
from pyspark.sql import SparkSession
from h3_pyspark.indexing import index_shape
from pyspark.sql.window import Window
from pyspark.sql.functions import col, pandas_udf, lit, to_date, when, array
from pyspark.sql import functions as F
from datetime import timedelta, date
from pyspark.sql.types import LongType, StringType, StructField, StructType, ArrayType, IntegerType
import os

SparkDataFrame = pyspark.sql.dataframe.DataFrame

class NTLClass:

    def __init__(self) -> None:
        self.spark = SparkSession.builder \
            .config("spark.jars", "./utils/postgresql-42.5.1.jar") \
            .master("local[40]") \
            .config("spark.driver.memory", "50g") \
            .config("spark.executor.memory", "50g") \
            .appName("Generacion de Redes") \
            .getOrCreate()

        zonas_metropolitanas_2015 = self.spark.read.option("header", True).csv("./raw_data/zonas_metropolitanas_2015.csv")
        zonasMetropolitanas_vm = zonas_metropolitanas_2015.where(col("CVE_ZM") == lit("09.01"))
        all_agebs = self.spark.read.parquet("./raw_data/utils/ageb_catalog/")

        self.agebs_vm = zonas_metropolitanas_2015.join(all_agebs
                , zonas_metropolitanas_2015.NOM_MUN == all_agebs.nom_agem
                , how="left") \
            .where(
                (col("cve_agee") == lit("09")) | (col("cve_agee") == lit("13")) | (col("cve_agee") == lit("15"))
            ) \
            .select("cve_geo", "cve_agee", "cve_agem", "cve_loc", "cve_ageb"
            , "nom_agee", "nom_agem", "geometry", "type", col("CVE_ENT").alias("cve_ent"))


    def single_ntl(self, movilidad_df: SparkDataFrame, ageb_df: SparkDataFrame) -> SparkDataFrame:
        """ Función encargada de obtener la colección de NTL ganadores por día de actividad
        movilidad_df (SparkDataFrame) : Un data frame de Spark con el registro de actividad en el día d
        ageb_df (SparkDataFrame) : Catálogo de AGEBS, debe de contener el shape ("geometry") de cada AGEB
        """

        # Se filtran los pings para obtener aquellos dentro de la ventana 10pm - 6am
        filtered_df = movilidad_df.where(
            (F.hour(col("cdmx_datetime")) >= 22) | (F.hour(col("cdmx_datetime")) < 6)
        )

        # Se localizan los pings dentro de su correspondiente AGEB
        indexed_df = filtered_df.withColumn("h3index_5", h3_pyspark.geo_to_h3("latitude", "longitude", lit(5))) \
            .withColumn("h3index_12", h3_pyspark.geo_to_h3("latitude", "longitude", lit(12))) \
            .withColumn("h3index_15", h3_pyspark.geo_to_h3("latitude", "longitude", lit(15)))

        agebs_indexed = ageb_df.select("cve_geo", "geometry") \
            .withColumn("h3polyfill_5", index_shape("geometry", lit(5))) \
            .withColumn("h3polyfill_5", F.explode("h3polyfill_5")) \
            .withColumn("h3polyfill_12", index_shape("geometry", lit(12))) \
            .withColumn("h3polyfill_12", F.explode("h3polyfill_12"))

        located_df = indexed_df.join(agebs_indexed
                , indexed_df.h3index_5 == agebs_indexed.h3polyfill_5
                , how="left") \
            .where(
                (col("h3index_12") == col("h3polyfill_12"))
            ) \
            .select("utc_timestamp", "cdmx_datetime", "caid", "h3index_15", "cve_geo")

        # Se agrupan los distintos pings existentes en ventanas de 10 minutos
        clustered_df = located_df.withColumn("time_window", F.window("cdmx_datetime", "600 seconds"))

        # Se calcula el número de clusters por AGEB que posee cada dispositivo
        scored_df = clustered_df.groupBy("caid", "cve_geo") \
            .agg(F.count("time_window").alias("score"))

        # El ageb con mayor número de clusters registrados es denominado como ganador para ese día
        w = Window().partitionBy("caid").orderBy(col("score").desc())
        winners_df = scored_df.withColumn("rank", F.row_number().over(w)) \
            .where(col("rank") == 1) \
            .select("caid", "cve_geo", "score")

        return winners_df

    def last_n_days_ntl(self, day: int, month: int, year: int, days_offset: int = 15) -> SparkDataFrame :

        # Se calcula la ventana de tiempo correspondiente a days_offset atrás
        curr_day = date(year, month, day)
        print(curr_day)
        n_days_ago = timedelta(days=days_offset)
        print(n_days_ago)
        d0 = curr_day - n_days_ago
        dn = curr_day - timedelta(days=1)
        print(d0, dn)

        date_range = [d0 + timedelta(days=x) for x in range(0, days_offset)]

        schema = StructType([
            StructField('caid', StringType(), True),
            StructField('cve_geo', StringType(), True),
            StructField('score', IntegerType(), True)
        ])


        emptyRDD = self.spark.sparkContext.emptyRDD()
        emptyDF = self.spark.createDataFrame(emptyRDD,schema)


        df_acc = emptyDF

        for d in date_range:

            curr_df = self.spark.read.option("header", True).parquet(f'{os.environ[f"MOVILIDAD_RAW_{d.year}"]}/month={str(d.month).zfill(2)}/day={str(d.day).zfill(2)}') \
                .distinct() \
                .where(col("horizontal_accuracy") >= lit(100.0)) \
                .withColumn("utc_datetime", F.from_unixtime(col("utc_timestamp"))) \
                .withColumn("cdmx_datetime", F.from_utc_timestamp(col("utc_datetime"), "America/Mexico_City")) \
                .select("utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude", "horizontal_accuracy")


            curr_ntl = self.single_ntl(curr_df, self.agebs_vm)

            df_acc = df_acc.union(curr_ntl)

        last_n_days_candidates = df_acc.groupBy("caid", "cve_geo").count()

        # TODO: Revisar que sean únicos las ntl
        w = Window().partitionBy("caid").orderBy(col("count").desc())
        ntl_df = last_n_days_candidates.withColumn("rank", F.row_number().over(w)) \
            .where(col("rank") == 1) \
            .select("caid", "cve_geo")

        return ntl_df