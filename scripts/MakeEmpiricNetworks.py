import networkx as nx
import pandas as pd

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

from dotenv import load_dotenv
load_dotenv()
import os

import duckdb

duck = duckdb.connect()

import sys
sys.path.append("/home/hmora/network-gen-pipeline/src")
from utils.stopwatch import Stopwatch

if __name__ == "__main__":

    with Stopwatch():

        YEAR, MONTH, DAY = str(sys.argv[1]).split("-")

        arrow_table = \
            (ds.dataset(f"{os.environ['WAREHOUSE_PATH']}/pings_with_home_ageb/year={YEAR}/month={MONTH}/day={DAY}/cve_zm=09.01")) \
                .to_table().combine_chunks()
        arrow_table  = arrow_table.filter(pc.not_equal(arrow_table["home_ageb"], "0000000000000"))

        interactions_table = duck.sql(f"""
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
                                      
        , interactions_table AS (
            SELECT
                a.caid AS a_caid
                , b.caid AS b_caid
            FROM
                pings_with_tw AS a
                LEFT JOIN
                pings_with_tw AS b
                ON a.tw = b.tw 
                    AND a.h3index_15 = b.h3index_15
                    AND a.caid != b.caid
        )
                                      
        SELECT *
        FROM interactions_table
        """).df()

        not_connected = duck.sql("""
        WITH
        not_connected AS (
            SELECT DISTINCT *
            FROM (
                SELECT *
                FROM interactions_table
                WHERE b_caid IS NULL
            )
        )
        
        SELECT * 
        FROM not_connected
        """).df()
                                      
        connected = duck.sql("""
        WITH
        connected AS (
            SELECT DISTINCT * 
            FROM (
                SELECT 
                    LEAST(a_caid, b_caid) AS a_caid
                    , GREATEST(a_caid, b_caid) AS b_caid
                FROM interactions_table
                WHERE b_caid IS NOT NULL
            )
        )
        
        SELECT *
        FROM connected
        """).df()

        # print(interactions_table)


        home_ageb_df = duck.sql(f"""
        SELECT DISTINCT caid, home_ageb
        FROM arrow_table
        """).df()

        print("HEAVY PROCESSING ENDS HERE...")
        
        
        print("COMPUTING NETWORK FROM PANDAS...")
        G = nx.from_pandas_edgelist(connected, "a_caid", "b_caid")

        print("ADDING ISOLATED NODES...")
        G.add_nodes_from(list(not_connected["a_caid"]))

        node_caids = \
            {node : node for node in G.nodes()} 

        labels_dict = home_ageb_df.to_dict("records")
        node_home_agebs = \
            { d["caid"] : d["home_ageb"]  for d in labels_dict}
        
        nx.set_node_attributes(G, values=node_caids, name="caid")
        nx.set_node_attributes(G, values=node_home_agebs, name="home_ageb")

        print(f"NO. NODES: {G.number_of_nodes()}")
        print(f"NO. EDGES: {G.number_of_edges()}")

        nx.write_graphml(G, f"/home/hmora/network-gen-pipeline/temp/empiric_networks/empiric_{YEAR}_{MONTH}_{DAY}.graphml")




