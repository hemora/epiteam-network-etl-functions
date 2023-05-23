from typing import Any
from core.core_abstract import AbstractHandler
from core.context import MatrixContext, Context

from queries.matrix_queries import MatrixQueries
from utils.duckaccess import DuckSession

from pandas import DataFrame

import networkx as nx

import numpy as np
from collections import Counter
from pandas import DataFrame, merge
import matplotlib.pyplot as plt

class VMSizesMatrix(AbstractHandler):

    def get_sizes_matrix(self, context: Context):

        with DuckSession() as duck:

            result = duck.sql(
                MatrixQueries.SIZES_VM(f"{context.base_dir}/located_pings.parquet")
            ).df()
        
        result.to_parquet(f"{context.base_dir}/sizes.parquet")

        context.payload = result

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.get_sizes_matrix(request))

class VMTotalEdgesMatrix(AbstractHandler):

    def get_total_edges(self, context: MatrixContext):

        with DuckSession() as duck:

            result = duck.sql(f"""
                WITH
                pre AS (
                    SELECT *
                    FROM '{context.base_dir}/sizes.parquet'
                )

                SELECT a.home_ageb AS ageb_a, b.home_ageb AS ageb_b
                    , CEIL(a.size * b.size)::INT64 AS total_enlaces
                FROM pre AS a
                    CROSS JOIN
                    pre AS b
            """).df()
        
        result.to_parquet(f"{context.base_dir}/enlaces_posibles.parquet")

        context.payload = result

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.get_total_edges(request))

class VMObservedMatrix(AbstractHandler):

    def get_observed(self, context: Context):

        with DuckSession() as duck:

            result = duck.sql(f"""
            WITH
            interactions_base AS (
                SELECT * 
                FROM read_parquet('{context.base_dir}/interactions_table.parquet')
            )

            SELECT a_home_ageb AS ageb_a, b_home_ageb AS ageb_b, COUNT(*) AS no_contactos
            FROM interactions_base
            GROUP BY 1, 2
            """).df()
        
        result.to_parquet(f"{context.base_dir}/enlaces_observados.parquet")

        context.payload = result

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.get_observed(request))

class VMTotalVsObservedMatrix(AbstractHandler):

    def get_total_vs_observed(self, context: Context):
        """
        """
        with DuckSession() as duck:

            total_vs_observados = duck.sql(f"""
            WITH
            totales AS (
                SELECT * 
                FROM read_parquet('{context.base_dir}/enlaces_posibles.parquet')
            )

            , observados AS (
                SELECT * 
                FROM read_parquet('{context.base_dir}/enlaces_observados.parquet')    
            )

            SELECT 
                a.ageb_a, a.ageb_b
                , total_enlaces
                , IF(no_contactos IS NULL, 0, no_contactos) AS no_contactos
            FROM 
                totales AS a
                LEFT JOIN
                observados AS b
                ON a.ageb_a = b.ageb_a
                    AND a.ageb_b = b.ageb_b
            ORDER BY 1, 2 ASC
            """).df()

        total_vs_observados.to_parquet(f"{context.base_dir}/posibles_vs_observados.parquet")
        
        context.payload = total_vs_observados
        
        return context


    def handle(self, request: Any) -> Any:
        return super().handle(self.get_total_vs_observed(request))

class ProbAndSizesMatrix(AbstractHandler):

    def get_probs_and_sizes(self, context: MatrixContext):
        """
        """
        with DuckSession() as duck:
        
            # Computing total numpy matrix
            total_m = duck.sql(f"""
                SELECT ageb_a, ageb_b, total_enlaces
                FROM read_parquet('{context.base_dir}/posibles_vs_observados.parquet')
            """).df()
            total_m = total_m.pivot(index="ageb_a", columns="ageb_b", values="total_enlaces")  \
                .reset_index() \
                .drop(columns=["ageb_a"]) \
                .to_numpy()

            # Computing total numpy matrix
            observed_m = duck.sql(f"""
                SELECT ageb_a, ageb_b, no_contactos
                FROM read_parquet('{context.base_dir}/posibles_vs_observados.parquet')
            """).df()
            observed_m = observed_m.pivot(index="ageb_a", columns="ageb_b", values="no_contactos")  \
                .reset_index() \
                .drop(columns=["ageb_a"]) \
                .to_numpy()
        
        matriz_probs = observed_m / total_m
        
        sizes, sizes_and_tags = context.payload
        print(sizes)

        print(len(sizes))
        print(matriz_probs.shape)

        context.payload = (sizes, sizes_and_tags, matriz_probs)
        
        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.get_probs_and_sizes(request))
    
class NetworkGen(AbstractHandler):
    """
    """
    def gen_network(self, context: Context):

        sizes, zipped, matrix_probs = context.payload

        for i in range(100):
        
            print("Generating Model")
            sbm = nx.stochastic_block_model(sizes, matrix_probs, seed=i)

            print("Writing to Graphml")
            aux_columns = ["home_ageb", "cardinalidad"]
            ageb_tags_pre = DataFrame(zipped, columns=aux_columns)
            ageb_tags_pre.reset_index()

            ageb_tags = ageb_tags_pre.loc[ageb_tags_pre.index.repeat(ageb_tags_pre.cardinalidad)]
            tags_final = list(ageb_tags["home_ageb"])

            assert len(tags_final) == sum(sizes)

            n=len(list(sbm.nodes())) ##lista con los todos los nodos del modelo generado
            enlaces=list(sbm.edges())
            bloque= nx.get_node_attributes(sbm, "block") ##diccionario con keys:nombre nodo, value: bloque al que pertenece el nodo
            bloque= list(bloque.values()) ## lista con solo los values

            ##nuevo diccionario
            test_keys=list(sbm.nodes())
            nuevo_diccionario =  {test_keys[i]: tags_final[i] for i in range(len(test_keys))}

            ##Con la info de arriba se hace el nuevo grafo que ya puede escribirse en graphml
            g2=nx.empty_graph(n) ##le pongo los mismos nodos
            g2.add_edges_from(enlaces) ##le pongo los mismos enlaces
            nx.set_node_attributes(g2, nuevo_diccionario, "block") ##le agrego un atributo (bloque) a cada nodo

            ##Escribo el graphml
            nx.write_graphml(g2, f'/datos/EpiTeam/redes/year={context.year}/month={context.month}/day={context.day}/{context.day}_{context.month}_{context.year}_SEED_{i}.graphml')

        context.payload = ageb_tags

        return context


    def handle(self, request: Any) -> Any:
        return super().handle(self.gen_network(request))