from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context

from wrappers.DuckWrapper import DuckWrapper

from pyspark.sql.functions import col, isnull, lit, window
from pyspark.sql import functions as F
from h3_pyspark.indexing import index_shape

class VMFilter(AbstractHandler):

    def filter(self, payload):
        """
        """
        payload.logger.info("Filtering to only VM")

        movilidad_indexed = payload.spark.read \
            .option("header", True) \
            .parquet("./temp/procesed_pings.parquet/") \

        zonas_metropolitanas_2015 = payload.spark.read \
            .option("header", True) \
            .csv("./raw_data/zonas_metropolitanas_2015.csv")
        zonasMetropolitanas_vm = zonas_metropolitanas_2015.where(col("CVE_ZM") == lit("09.01"))
        all_agebs = payload.spark.read.parquet("./utils/ageb_catalog/")

        agebs_vm = zonasMetropolitanas_vm.join(all_agebs
                , zonasMetropolitanas_vm.NOM_MUN == all_agebs.nom_agem
                , how="left") \
            .where(
                (col("cve_agee") == lit("09")) | (col("cve_agee") == lit("13")) | (col("cve_agee") == lit("15"))
            ) \
            .select("cve_geo", "cve_agee", "cve_agem", "cve_loc", "cve_ageb"
            , "nom_agee"
            , F.when(F.isnull(col("nom_agem")), col("NOM_MUN")).otherwise(col("nom_agem")).alias("nom_agem")
            , "geometry", "type", col("CVE_ENT").alias("cve_ent"))
        
        agebs_indexed = agebs_vm.select("cve_geo", "nom_agem", "geometry") \
            .withColumn("h3polyfill_12", index_shape("geometry", lit(12))) \
            .withColumn("no_h3res12", F.size(col("h3polyfill_12"))) \
            .withColumn("h3polyfill_12", F.explode("h3polyfill_12"))

        # Se hace un inner para eliminar aquellos que no son del valle de México
        movilidad_in_VM = movilidad_indexed.join(agebs_indexed
                , movilidad_indexed.h3index_12 == agebs_indexed.h3polyfill_12
                , how="inner"
            ) \
            .select("utc_timestamp", "cdmx_datetime", "caid", "h3index_12", "h3index_15", "cve_geo", "home_ageb")
        
        movilidad_in_VM.write.mode("overwrite").parquet("./temp/pings_in_vm.parquet")

        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.filter(request))
        
        return self.filter(request)

class InteractionsCompute(AbstractHandler):
    """
    """
    def compute(self, payload: Context):
        """
        """
        payload.logger.info("Computing Interactions")

        if payload.duck is None:
            payload.duck = DuckWrapper().get_session()

        payload.logger.info(payload.duck.sql("SELECT COUNT(*) FROM read_parquet('./temp/located_df.parquet')").fetchall())

        payload.interactions_table = payload.duck.sql("""
            WITH
            pre AS (
                SELECT *
                    , MIN(cdmx_datetime) OVER() AS min_datetime
                FROM read_parquet('./temp/located_df.parquet')
                WHERE home_ageb IS NOT NULL
                    AND home_agee IN ('09', '13', '15')
            )

            , pings_base AS (
                SELECT *
                    , TIME_BUCKET(INTERVAL '600 seconds', cdmx_datetime::TIMESTAMP, min_datetime::TIMESTAMP) AS tw
                FROM pre
            )
            
            SELECT  DISTINCT a.caid AS a_caid, a.home_ageb AS a_home_ageb
                , b.caid AS b_caid, b.home_ageb AS b_home_ageb
            FROM pings_base AS a
                INNER JOIN
                pings_base AS b
                ON a.h3index_15 = b.h3index_15
                    AND a.tw = b.tw
                WHERE a.caid != b.caid
            ;
            """).df()
        
        payload.interactions_table.to_parquet("./temp/interactions_table.parquet")
        
        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.compute(request))
        
        return self.compute(request)