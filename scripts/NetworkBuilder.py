from collections import Counter
import click

from dotenv import load_dotenv
import os

import numpy as np
load_dotenv()

import datetime
import logging

import sys
sys.path.append("/home/hmora/network-gen-pipeline/src")

from utils.stopwatch import Stopwatch, LoggerStopwatch
from utils.duckaccess import DuckSession
import utils.path_utils as pu

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

import duckdb

import pandas as pd

import networkx as nx

import multiprocessing as mp

########################################################################################################

class NetworkBuilder:
    """
    """
    def __init__(self, year:str, month: str, day: str
                 , zm: str
                 , target: str
                 , logs: str) -> None:
        self.__year = year
        self.__month = month
        self.__day = day
        self.__zm = zm
        self.__target = f"{str(target).rstrip('/')}/year={self.__year}/month={self.__month}/day={self.__day}"

        logging.basicConfig(filename=f"{logs}/network_build_{datetime.datetime.now()}.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
        self.__logger = logging.getLogger(f"Network Builder: {self.__year}-{self.__month}-{self.__day}")

    def get_pings_dataset(self) -> list[pa.Table]:
        """
        """
        self.__logger.info("RETRIEVING PINGS DATASET...")

        if self.__zm is not None:
            arrow_table = \
                (ds.dataset(f"{os.environ['WAREHOUSE_PATH']}/pings_with_home_ageb/year={self.__year}/month={self.__month}/day={self.__day}/cve_zm={self.__zm}")) \
                    .to_table().combine_chunks()
        else :
            arrow_table = \
                (ds.dataset(f"{os.environ['WAREHOUSE_PATH']}/pings_with_home_ageb/year={self.__year}/month={self.__month}/day={self.__day}")) \
                    .to_table().combine_chunks()
        
        arrow_table  = arrow_table.filter(pc.not_equal(arrow_table["home_ageb"], "0000000000000"))
        # FOR TEST PURPOSES
        # arrow_table = arrow_table.slice(length=1000)
            
        self.__logger.info("...DONE")

        with DuckSession() as duck:
            self.__logger.info(f"RES:\n{duckdb.arrow(arrow_table)}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM arrow_table').fetchone()[0]:,}")
            self.__logger.info(f"DISTINCT CAIDS: {duck.sql('SELECT COUNT(DISTINCT caid) FROM arrow_table').fetchone()[0]:,}")

        return arrow_table
    
    def get_ageb_sizes(self, arrow_table: pa.Table) -> pa.Table:
        """
        """
        self.__logger.info("COMPUTING AGEB SIZES")

        with DuckSession() as duck:
            df = duck.sql(f"""
            WITH
            only_valid_home_ageb AS (
                SELECT *
                FROM arrow_table
            )

            SELECT home_ageb
                , COUNT(DISTINCT caid) AS size
            FROM arrow_table
            GROUP BY 1
            ORDER BY 1 ASC
            """).arrow().combine_chunks()

            self.__logger.info("...DONE")
            self.__logger.info(f"RES:\n{duckdb.arrow(df)}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM df').fetchone()[0]:,}")
            self.__logger.info(f"SUM SIZES: {duck.sql('SELECT SUM(size) FROM df').fetchone()[0]:,}")


        output_dir = pu.create_if_not_exists(
            f"{self.__target}/side_results/"
        )

        self.__logger.info(f"WRITING INTO: {output_dir}")

        pq.write_table(df, f"{output_dir}/ageb_sizes.parquet")

        return df
    
    def get_total_contacts(self, arrow_table: pa.Table) -> pa.Table:
        """
        """
        self.__logger.info("COMPUTING TOTAL CONTACTS...")

        with DuckSession() as duck:
            df = duck.sql(f"""
            SELECT 
                a.home_ageb AS a_home_ageb
                , b.home_ageb AS b_home_ageb
                , (a.size * b.size) AS total_contacts
            FROM 
                arrow_table AS a
                CROSS JOIN
                arrow_table AS b
            """).arrow().combine_chunks()

            self.__logger.info("...DONE")
            self.__logger.info(f"RES:\n{duckdb.arrow(df)}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM df').fetchone()[0]:,}")
        
        output_dir = pu.create_if_not_exists(
            f"{self.__target}/side_results/"
        )

        self.__logger.info(f"WRITING INTO: {output_dir}")

        pq.write_table(df, f"{output_dir}/all_contacts.parquet")

        return df
    
    def get_observed_contacts(self, arrow_table: pa.Table) -> pa.Table:
        """
        """
        self.__logger.info("COMPUTING OBSERVED CONTACTS...")

        with DuckSession() as duck:
            df = duck.sql(f"""
            WITH
            pings_with_tw AS (
                SELECT *
                    , TIME_BUCKET(INTERVAL '600 seconds'
                        , cdmx_datetime::TIMESTAMP
                        , min_datetime::TIMESTAMP) AS tw
                FROM (
                    SELECT *
                        , MIN(cdmx_datetime) 
                          OVER() AS min_datetime
                    FROM arrow_table
                )
            )

            , contacts_table AS (
                SELECT DISTINCT *
                FROM (
                    SELECT
                        a.home_ageb AS a_home_ageb
                        , b.home_ageb AS b_home_ageb
                        , CONCAT(a.caid, '-', b.caid) AS contacts
                    FROM
                        pings_with_tw AS a
                        INNER JOIN
                        pings_with_tw AS b
                        ON a.tw = b.tw 
                            AND a.h3index_15 = b.h3index_15
                            AND a.caid != b.caid
                )
            )
                          
            SELECT a_home_ageb, b_home_ageb
                , COUNT(contacts) AS observed_contacts
            FROM contacts_table
            GROUP BY 1, 2
            """).arrow().combine_chunks()

            self.__logger.info("...DONE")
            self.__logger.info(f"RES:\n{duckdb.arrow(df)}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM df').fetchone()[0]:,}")
        
        output_dir = pu.create_if_not_exists(
            f"{self.__target}/side_results/"
        )

        self.__logger.info(f"WRITING INTO: {output_dir}")

        pq.write_table(df, f"{output_dir}/observed_contacts.parquet")
        
        return df
    
    def get_total_vs_observed(self) -> pa.Table:
        """
        """
        self.__logger.info("COMPUTING TOTAL VS OBSERVED TABLE...")

        tables_path = f"{self.__target}/side_results"

        self.__logger.info(f"SUPPLIES PATH: {tables_path}")

        with DuckSession() as duck:
            df = duck.sql(f"""
            WITH
            all_contacts AS (
                SELECT * 
                FROM read_parquet('{tables_path}/all_contacts.parquet')
            )

            , observed_contacts AS (
                SELECT * 
                FROM read_parquet('{tables_path}/observed_contacts.parquet')
            )

            SELECT 
                a.a_home_ageb
                , a.b_home_ageb
                , a.total_contacts
                , IF(b.observed_contacts IS NULL, 0, b.observed_contacts) AS observed_contacts
            FROM 
                all_contacts AS a
                LEFT JOIN
                observed_contacts AS b
                ON a.a_home_ageb = b.a_home_ageb
                    AND a.b_home_ageb = b.b_home_ageb
            ORDER BY 1,2 ASC
            """).df()

            self.__logger.info("...DONE")
            self.__logger.info(f"RES:\n{df}")
            self.__logger.info(f"NO. ROWS: {duck.sql('SELECT COUNT(*) FROM df').fetchone()[0]:,}")
            self.__logger.info(f"NO. TOTAL GT OBSERVED: {duck.sql('SELECT COUNT(*) FROM df WHERE total_contacts < observed_contacts').fetchone()[0]:,}")
        
        output_dir = pu.create_if_not_exists(
            f"{self.__target}/side_results/"
        )

        self.__logger.info(f"WRITING INTO: {output_dir}")

        df.to_parquet(f"{output_dir}/total_vs_observed.parquet")
        
        return df
    
    def get_tables(self):
        """
        """
        with LoggerStopwatch(self.__logger) as sw:

            base_pings = self.get_pings_dataset()
            sw.report_til_here()

            ageb_sizes = self.get_ageb_sizes(base_pings)
            sw.report_til_here()

            self.get_total_contacts(ageb_sizes)
            sw.report_til_here()

            self.get_observed_contacts(base_pings)
            sw.report_til_here()

            self.get_total_vs_observed()
            sw.report_til_here()

        return
    
    def get_probs_matrix(self) :
        """
        """
        self.__logger.info("COMPUTING PROB MATRIX...")

        tables_path = f"{self.__target}/side_results"

        self.__logger.info(f"SUPPLIES PATH: {tables_path}")

        pdf = pd.read_parquet(f"{tables_path}/total_vs_observed.parquet")

        total_m = pdf[["a_home_ageb", "b_home_ageb", "total_contacts"]]
        total_m = total_m.pivot(index="a_home_ageb", columns="b_home_ageb", values="total_contacts") \
            .reset_index() \
            .drop(columns=["a_home_ageb"]) \
            .to_numpy()

        self.__logger.info(f"RES:\n{total_m}")
        self.__logger.info(f"NAN VALUES:\n{total_m[np.isnan(total_m)]}")
        np.savetxt(f"{tables_path}/total_matrix.npy", total_m)
        
        observed_m = pdf[["a_home_ageb", "b_home_ageb", "observed_contacts"]]
        observed_m = observed_m.pivot(index="a_home_ageb", columns="b_home_ageb", values="observed_contacts")  \
           .reset_index() \
           .drop(columns=["a_home_ageb"]) \
           .to_numpy()
        
        self.__logger.info(f"RES:\n{observed_m}")
        self.__logger.info(f"NAN VALUES:\n{observed_m[np.isnan(observed_m)]}")
        np.savetxt(f"{tables_path}/observed_matrix.npy", observed_m)

        probs = observed_m / total_m
        self.__logger.info(f"RES:\n{probs}")
        self.__logger.info(f"NAN VALUES:\n{probs[np.isnan(probs)]}")

        self.__logger.info("...DONE")

        np.savetxt(f"{tables_path}/probs_matrix.npy", probs)
        
        return probs
    
    def scale(self, scale_size=None):
        """
        """
        if scale_size is None:
            self.__logger.info("NOT SCALING, USING ORIGINAL SIZES")
        else:
            self.__logger.info(f"SCALING TO {scale_size}")

        tables_path = f"{self.__target}/side_results"

        read_sizes = pd.read_parquet(f"{tables_path}/ageb_sizes.parquet")

        with DuckSession() as duck:

            # 1. Get the sizes table. Ensure an ascending ordering
            original_sizes = duck.sql(f"""
            SELECT *
                , 1 AS single_representant
            FROM read_sizes
            ORDER BY 1 ASC
            """).df()

            if scale_size is not None:
                scale_size = int(scale_size)
                np.set_printoptions(precision=10)

                # 2. Compute the probabilities asociated with each Ageb
                original_sizes_arr = np.array(list(original_sizes["size"]))
                total = np.sum(original_sizes_arr)
                probs = np.array(list(original_sizes["size"])) / total
                print(type(probs))
                probs /= probs.sum()
                print(total)
                print(sum(probs))

                ## Probs must sum 1
                # assert np.sum(probs) == 1.0

                # 3. Compute the difference between the sum of the single representants
                ## and the expected size
                #sum_of_single_rep = sum(list(original_sizes["single_representant"]))
                #difference = 10000 - sum_of_single_rep
                #print(sum_of_single_rep)
                #print(difference)

                # 4. Obtain a sample of size equal to the difference
                agebs_list = list(original_sizes["home_ageb"])
                np.random.seed(3696)
                sample = np.random.choice(agebs_list, size=scale_size, p=probs, replace=True)

                assert len(sample) == scale_size

                # 5. Count the ocurrences in the difference
                ageb_counter = Counter(list(sample))
                scaled_df = pd.DataFrame(list(ageb_counter.items()))
                print(scaled_df.dtypes)
                scaled_df = scaled_df.rename(columns={scaled_df.columns[0] : "home_ageb"
                                            , scaled_df.columns[1]: "aux_size"})
            
                #print(f"Sum scaled_size: {sum(list(scaled_df['aux_size']))}")

                ## 6. Join the sampled size with the size due to single representants
                final_scale = pd.merge(original_sizes, scaled_df, left_on="home_ageb", right_on="home_ageb", how="left")
                final_scale["scaled_size"] = final_scale["aux_size"].apply(lambda x: 0 if np.isnan(x) else int(x))
                print(final_scale)
                #final_scale["scaled_size"] = final_scale[["single_representant", "aux_size"]].apply(lambda x: x["single_representant"] \
                #                                                                                  if np.isnan(x["aux_size"]) \
                #                                                                                    else x["single_representant"] + x["aux_size"], axis=1)
                #original_sizes = original_sizes[["home_ageb", "size"]]
                final_scale = final_scale[["home_ageb", "scaled_size"]].sort_values(by="home_ageb")

                #scale_aux = list(final_scale["scaled_size"])

                print(original_sizes.shape)
                print(final_scale.shape)

                # assert original_sizes.shape == final_scale.shape

                final_scale = list(final_scale["scaled_size"])
                print(sum(list(final_scale)))
                assert sum(list(final_scale)) == scale_size

                tags = list(original_sizes["home_ageb"])
            
            else:
                final_scale = list(read_sizes["size"])

                tags = list(read_sizes["home_ageb"])

        self.__logger.info(f"FINAL SCALE: {final_scale}")
        self.__logger.info(f"TAGS: {tags}")

        return final_scale, list(zip(tags, final_scale))


    #def gen_network(self, scaled_size, zipped):
    def gen_network(self, t):
        """
        """
        self.__logger.info("GENERATING MODEL")

        scaled_size, zipped, i = t

        tables_path = f"{self.__target}/side_results"

        # obs_m = np.loadtxt(f"{tables_path}/observed_matrix.npy")
        # tot_m = np.loadtxt(f"{tables_path}/total_matrix.npy")

        probs = np.loadtxt(f"{tables_path}/probs_matrix.npy")


        sbm = nx.stochastic_block_model(scaled_size, probs, seed=i)
        self.__logger.info("WRITING TO GRAPHML")

        aux_columns = ["home_ageb", "cardinalidad"]
        ageb_tags_pre = pd.DataFrame(zipped, columns=aux_columns)
        ageb_tags_pre.reset_index()

        ageb_tags = ageb_tags_pre.loc[ageb_tags_pre.index.repeat(ageb_tags_pre.cardinalidad)]
        tags_final = list(ageb_tags["home_ageb"])

        assert len(tags_final) == sum(scaled_size)

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
        nx.write_graphml(g2, f"{self.__target}/{self.__year}_{self.__month}_{self.__day}_SEED_{i}.graphml")

        return
    
    def build_network(self, size):
        """
        """
        scaled_size, tags = self.scale(size)

        payload = [(scaled_size, tags, i) for i in range(100)]

        with mp.Pool(10) as p:
            p.map(self.gen_network, payload)

        # self.gen_network(scaled_size, tags)


########################################################################################################
#@click.command(help="""
#               Tool to generate networks trough a series of stages
#               """)
@click.command()
@click.argument("date", required=True)
@click.option('-t', "--target"
              , default=f"{os.environ['NETWORK_WORKDIR']}"
              , help="""
              The target directory in which the results and side results would be written
              """)
@click.option("--logs"
              , default=f"/home/hmora/network-gen-pipeline/logs"
              , help="""
              The target directory in which the logs would be written.
              """)
@click.option("--zm", default=None
              , help="""
              A ZM string (e.g. "09.01") used to filter the set of pings to the
              desired zona metropolitana. If none is provided then the whole dataset is used.
              """)
@click.option("--gen_tables"
              , is_flag=True, default=False
              , help="""
              Use it to compute the interactions, sizes and observed tables.
              """)
@click.option("--prob_matrix"
              , is_flag=True, default=False
              , help="""
              Use it to compute the probability matrix for this set of pings.
              """)
@click.option("--build_network"
              , is_flag=True, default=False
              , help="""
              Use it to generate a series of 100 networks using the probability matrix
              trough the stochatic block model approach.
              """)
@click.option("--scale", default=None
              , help="""
              A ZM string (e.g. "09.01") used to filter the set of pings to the
              desired zona metropolitana. If none is provided then the whole dataset is used.
              """)
def main(date: str, target: str, logs: str
         , zm: str
         , gen_tables: bool
         , prob_matrix: bool
         , build_network: bool
         , scale: int) -> None:
    """ Tool to generate networks trough a series of stages

        DATE: A string date like YY-mm-dd that represents the set of pings used to build
        the network.
    """
    year, month, day = date.split('-')
    
    nb = NetworkBuilder(year, month, day, zm, target, logs)

    if gen_tables:
        nb.get_tables()
    
    if prob_matrix:
        nb.get_probs_matrix()

    if build_network:
        nb.build_network(scale)

    
if __name__ == "__main__":

    with Stopwatch():
        main()