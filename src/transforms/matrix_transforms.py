from typing import Any
from core.core_abstract import AbstractHandler
from core.context import MatrixContext

from queries.matrix_queries import MatrixQueries
from utils.duckaccess import DuckSession

import networkx as nx


class SizesMatrix(AbstractHandler):

    def get_sizes_matrix(self, context: MatrixContext):

        with DuckSession() as duck:

            result = duck.sql(
                MatrixQueries.SIZES_VM(context.pings_base) \
                    if context.in_vm \
                        else MatrixQueries.ALL_SIZES(context.pings_base)
            ).df()
        
        result.to_parquet("./temp/sizes.parquet")

        context.payload = result

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.get_sizes_matrix(request))

class TotalEdgesMatrix(AbstractHandler):

    def get_total_edges(self, context: MatrixContext):

        with DuckSession() as duck:

            result = duck.sql("""
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

        context.payload = result

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.get_total_edges(request))

class ObservedMatrix(AbstractHandler):

    def get_observed(self, context: MatrixContext):

        with DuckSession() as duck:

            result = duck.sql("""
                WITH
                interactions_base AS (
                    SELECT * 
                    FROM read_parquet('./temp/interactions_table.parquet')
                )

                SELECT a_home_ageb, b_home_ageb, COUNT(*) AS no_contactos
                FROM interactions_base
                GROUP BY 1, 2
                ORDER BY 1, 2 ASC
            """).df()
        
        result.to_parquet("./temp/observed.parquet")

        context.payload = result

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.get_observed(request))

class TotalVsObservedMatrix(AbstractHandler):

    def get_total_vs_observed(self, context: MatrixContext):
        """
        """
        
        with DuckSession() as duck:
            duck.sql("""
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
        
        return context


    def handle(self, request: Any) -> Any:
        return super().handle(self.get_total_vs_observed(request))
    
class ProbAndSizesMatrix(AbstractHandler):

    def get_probs_and_sizes(self, context: MatrixContext):
        """
        """

        with DuckSession() as duck:
        
            # Computing block sizes
            sizes = duck.sql("""
                SELECT cardinalidad
                FROM read_parquet('./temp/sizes.parquet')
            """).df()
            sizes = list(sizes["cardinalidad"])

            # Computing total numpy matrix
            total_m = duck.sql("""
                SELECT bloque_a, bloque_b, total_enlaces
                FROM read_parquet('./temp/total_vs_observed.parquet')
            """).df()
            total_m = total_m.pivot(index="bloque_a", columns="bloque_b", values="total_enlaces")  \
                .reset_index() \
                .drop(columns=["bloque_a"]) \
                .to_numpy()

            # Computing total numpy matrix
            observed_m = duck.sql("""
                SELECT bloque_a, bloque_b, no_contactos
                FROM read_parquet('./temp/total_vs_observed.parquet')
            """).df()

            observed_m = observed_m.pivot(index="bloque_a", columns="bloque_b", values="no_contactos")  \
                .reset_index() \
                .drop(columns=["bloque_a"]) \
                .to_numpy()
        
        matriz_probs = observed_m / total_m

        context.payload = (sizes, matriz_probs)
        
        return context


    def handle(self, request: Any) -> Any:
        return super().handle(self.get_probs_and_sizes(request))
    
class NetworkGen(AbstractHandler):
    """
    """
    def gen_network(self, context: MatrixContext):
        
        sizes, matriz_probs = context.payload

        print(len(sizes))
        print(matriz_probs.shape)

        sbm = nx.stochastic_block_model(sizes, matriz_probs)

        context.payload = sbm

        n=len(list(sbm.nodes())) ##lista con los todos los nodos del modelo generado
        enlaces=list(sbm.edges())

        ##Con la info de arriba se hace el nuevo grafo que ya puede escribirse en graphml
        g2=nx.empty_graph(n) ##le pongo los mismos nodos
        g2.add_edges_from(enlaces) ##le pongo los mismos enlaces

        ##Escribo el graphml
        nx.write_graphml(g2,'./temp/graficas/20200116.graphml')

        return context


    def handle(self, request: Any) -> Any:
        return super().handle(self.gen_network(request))