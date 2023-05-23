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

class ScaleTo10000(AbstractHandler):

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , ((size * 10000) / {N})::INTEGER AS scaled_size
            FROM original_sizes
            """).df()
            scaled_size = duck.sql("SELECT SUM(scaled_size) from scaled""").fetchone()[0]
            print(f"Scaled Size: {scaled_size}")

            scale_aux = list(scaled["scaled_size"])
            for i, row in scaled.iterrows():
                if sum(scale_aux) == 10000:
                    break

                if row["size"] > 0 and scale_aux[i] == 0:
                    scale_aux[i] = 1

            scale_aux_size = sum(scale_aux)
            print(f"Scaled Size Fixed: {scale_aux_size}")
            assert scale_aux_size == 10000

            tags = list(original_sizes["home_ageb"])
        
        context.payload = (scale_aux, list(zip(tags, scale_aux)))

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleTo10000_2(AbstractHandler):

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , ((size * 10000) / {N})::INTEGER AS scaled_size
            FROM original_sizes
            """).df()
            scaled_size = duck.sql("SELECT SUM(scaled_size) from scaled""").fetchone()[0]
            print(f"Scaled Size: {scaled_size}")

            scale_aux = list(scaled["scaled_size"])
            for i, row in scaled.iterrows():
                if sum(scale_aux) == 10000:
                    break
                
                if row["size"] > 1 and scale_aux[i] < 2:
                    scale_aux[i] = 2

                if row["size"] > 0 and scale_aux[i] == 0:
                    scale_aux[i] = 1

            scale_aux_size = sum(scale_aux)
            print(f"Scaled Size Fixed: {scale_aux_size}")

            tags = list(original_sizes["home_ageb"])
        
        context.payload = (scale_aux, list(zip(tags, scale_aux)))
            
        assert scale_aux_size == 10000

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleTo10000_3(AbstractHandler):

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , ((size * 10000) / {N})::INTEGER AS scaled_size
            FROM original_sizes
            """).df()
            scaled_size = duck.sql("SELECT SUM(scaled_size) from scaled""").fetchone()[0]
            print(f"Scaled Size: {scaled_size}")

            scale_aux = list(scaled["scaled_size"])
            for i, row in scaled.iterrows():
                if sum(scale_aux) == 10000:
                    break
                
                if row["size"] > 1 and scale_aux[i] < 2:
                    scale_aux[i] = 2

                if row["size"] > 0 and scale_aux[i] == 0:
                    scale_aux[i] = 1

            scale_aux_size = sum(scale_aux)
            print(f"Scaled Size Fixed: {scale_aux_size}")

            top_10_greatest = sorted(scale_aux, reverse=True)[:10]
            top_10_greatest_index = list(map(lambda x: top_10_greatest.index(x), top_10_greatest))

            increments = int((10000 - scale_aux_size) / len(top_10_greatest))

            for i in top_10_greatest_index:
                scale_aux[i] = scale_aux[i] + increments

            print(f"Double Scaled Size Fixed: {sum(scale_aux)}")

            scale_aux[top_10_greatest_index[0]] = scale_aux[top_10_greatest_index[0]] + 2

            tags = list(original_sizes["home_ageb"])
        
        context.payload = (scale_aux, list(zip(tags, scale_aux)))
            
        assert sum(scale_aux) == 10000

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleTo(AbstractHandler):

    def __init__(self, scale_size=10000) -> None:
        super().__init__()
        self.scale_size = scale_size

    def scale(self, context: Context):

        with DuckSession() as duck:

            print(self.scale_size)

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , FLOOR((size * {self.scale_size}) / {N})::INTEGER AS scaled_size
            FROM original_sizes
            """).df()
            scale_aux = list(scaled["scaled_size"])

            print(f"Scaled Size: {sum(scale_aux)}")

            tags = list(original_sizes["home_ageb"])
        
        context.payload = (scale_aux, list(zip(tags, scale_aux)))
            
        assert sum(scale_aux) >= self.scale_size

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleBySample(AbstractHandler):

    def __init__(self, sample_size=10000) -> None:
        super().__init__()
        self.__sample_size = sample_size

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            """).df()
            #plt.bar(original_sizes["home_ageb"], original_sizes["size"])
            #original_sizes.hist("size")
            #plt.savefig("./images/original_dist.png")

            sizes_with_probs = duck.sql("""
            SELECT *
                , (size / total)::FLOAT AS prob
            FROM (
                SELECT *
                    , (SUM(size) OVER())::DECIMAL(18,3) AS total
                FROM original_sizes
            )
            """).df()
            #assert int(sum(list(sizes_with_probs["prob"]))) == 1

            agebs_list = list(sizes_with_probs["home_ageb"])
            probs_list = list(sizes_with_probs["prob"])

            np.random.seed(3696)
            sample = np.random.choice(agebs_list, size=self.__sample_size, p=probs_list, replace=True,)
            assert len(sample) == self.__sample_size


            ageb_counter = Counter(list(sample))
            scaled_df = DataFrame(list(ageb_counter.items()))
            print(scaled_df.dtypes)
            scaled_df = scaled_df.rename(columns={scaled_df.columns[0] : "home_ageb"
                                        , scaled_df.columns[1]: "scaled_size"})
            
            
            print(f"Sum scaled_size: {sum(list(scaled_df['scaled_size']))}")

            final_scale = merge(original_sizes, scaled_df, left_on="home_ageb", right_on="home_ageb", how="left")
            final_scale["scaled_size"] = final_scale["scaled_size"].apply(lambda x: 0 if np.isnan(x) else int(x))
            final_scale = final_scale[["home_ageb", "scaled_size"]].sort_values(by="home_ageb")
            #plt.bar(final_scale["home_ageb"], final_scale["scaled_size"])
            #plt.clf()
            #final_scale.hist("scaled_size")
            #plt.savefig("./images/scaled_dist.png")

            assert original_sizes.shape == final_scale.shape

            final_scale = list(final_scale["scaled_size"])
            assert sum(list(final_scale)) == self.__sample_size

            tags = list(original_sizes["home_ageb"])

            context.payload = (final_scale, list(zip(tags, final_scale)))

        return context
        

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))

    
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