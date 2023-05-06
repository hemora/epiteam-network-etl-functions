from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context

from wrappers.DuckWrapper import DuckWrapper

class SizesMatrix(AbstractHandler):

    def get_sizes_matrix(self, payload: Context):
        payload.logger.info("Computing sizes matrix")

        if payload.duck is None:
            payload.duck = DuckWrapper().get_session()

        pandas_du = payload.df

        result = payload.duck.sql("""
            WITH
            pre AS (
                SELECT *
                    , MIN(cdmx_datetime) OVER() AS min_datetime
                FROM read_parquet('./temp/located_df.parquet')
                WHERE home_ageb IS NOT NULL
                    AND home_agee IN ('09', '13', '15')
            )

            SELECT home_ageb, COUNT(DISTINCT caid) AS cardinalidad
            FROM pre
            GROUP BY 1
            ORDER BY 1 ASC
        """).df()
        
        result.to_parquet("./temp/sizes.parquet")

        payload.df = result

        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.get_sizes_matrix(request))
        
        return self.get_sizes_matrix(request)

class TotalEdgesMatrix(AbstractHandler):

    def get_total_edges(self, payload: Context):
        payload.logger.info("Computing all posibilities")

        if payload.duck is None:
            payload.duck = DuckWrapper().get_session()

        result = payload.duck.sql("""
            WITH
            pre AS (
                SELECT *
                FROM './temp/sizes.parquet'
            )

            SELECT a.home_ageb AS bloque_a, b.home_ageb AS bloque_b
                , CEIL(a.cardinalidad * b.cardinalidad)::INTEGER AS total_enlaces
            FROM pre AS a
                CROSS JOIN
                pre AS b
        """).df()
        
        result.to_parquet("./temp/all_possible.parquet")

        payload.df = result

        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.get_total_edges(request))
        
        return self.get_total_edges(request)

class ObservedMatrix(AbstractHandler):

    def get_observed(self, payload: Context):
        payload.logger.info("Computing observed")

        if payload.duck is None:
            payload.duck = DuckWrapper().get_session()
        
        #pandas_df = payload.interactions_table

        result = payload.duck.sql("""
            WITH
            interactions_base AS (
                SELECT * 
                FROM read_parquet('./temp/interactions_table.parquet')
            )

            SELECT a_home_ageb, b_home_ageb, COUNT(*) AS no_contactos
            FROM interactions_base
            GROUP BY 1,2
            ORDER BY 1,2 ASC
        """).df()
        
        result.to_parquet("./temp/observed.parquet")

        payload.df = result

        return payload

    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.get_observed(request))
        
        return self.get_observed(request)

class TotalVsObservedMatrix(AbstractHandler):

    def get_total_vs_observed(self, payload):
        """
        """
        payload.logger.info("Getting total vs observed")

        if payload.duck is None:
            payload.duck = DuckWrapper().get_session()
        
        payload.duck.sql("""
            WITH
            all_possible AS (
                SELECT * 
                FROM read_parquet('./temp/all_possible.parquet')
            )

            , observed AS (
                SELECT * 
                FROM read_parquet('./temp/observed.parquet')    
            )

            SELECT 
                bloque_a, bloque_b
                , total_enlaces
                , IF(no_contactos IS NULL, 0, no_contactos) AS no_contactos
            FROM 
                all_possible AS a
                LEFT JOIN
                observed AS b
                ON a.bloque_a = b.a_home_ageb
                    AND a.bloque_b = b.b_home_ageb
            ORDER BY 1, 2 ASC
        """).write_parquet("./temp/total_vs_observed.parquet")
        
        return payload


    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.get_total_vs_observed(request))
        
        return self.get_total_vs_observed(request)
    
class ProbAndSizesMatrix(AbstractHandler):

    def get_total_vs_observed(self, payload):
        """
        """
        payload.logger.info("Getting Sizes and Probability Matrixes")

        if payload.duck is None:
            payload.duck = DuckWrapper().get_session()
        
        # Computing block sizes
        sizes = payload.duck.sql("""
            SELECT cardinalidad
            FROM read_parquet('./temp/sizes.parquet')
        """).df()
        sizes = list(sizes["cardinalidad"])

        # Computing total numpy matrix
        total_m = payload.duck.sql("""
            SELECT bloque_a, bloque_b, total_enlaces
            FROM read_parquet('./temp/total_vs_observed.parquet')
        """).df()
        total_m = total_m.pivot(index="bloque_a", columns="bloque_b", values="total_enlaces")  \
            .reset_index() \
            .drop(columns=["bloque_a"]) \
            .to_numpy()

        # Computing total numpy matrix
        observed_m = payload.duck.sql("""
            SELECT bloque_a, bloque_b, no_contactos
            FROM read_parquet('./temp/total_vs_observed.parquet')
        """).df()
        observed_m = observed_m.pivot(index="bloque_a", columns="bloque_b", values="no_contactos")  \
            .reset_index() \
            .drop(columns=["bloque_a"]) \
            .to_numpy()
        
        matriz_probs = observed_m / total_m

        payload.df = (sizes, matriz_probs)
        
        return payload


    def handle(self, request: Any) -> Any:
        if self.has_next():
            return super().handle(self.get_total_vs_observed(request))
        
        return self.get_total_vs_observed(request)