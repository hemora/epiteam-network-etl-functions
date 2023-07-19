import pandas as pd

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

import networkx as nx

from pathlib import Path

import sys
sys.path.append("/home/hmora/network-gen-pipeline/src")
from utils.duckaccess import DuckSession
from utils.stopwatch import Stopwatch

import multiprocessing as mp

import duckdb
duck = duckdb.connect()

YEAR, MONTH, DAY = ("2020", "02", "04")
GRAPH_ROOT_PATH = f"/datos/EpiTeam/redes/year={YEAR}/month={MONTH}/day={DAY}"
SIM_ROOT_PATH = f"/datos/EpiTeam/simulaciones_redes/year={YEAR}/month={MONTH}/day={DAY}"

all_node_status = \
    [str(b) for a in Path(f"{SIM_ROOT_PATH}").glob("*") for b in Path(a).glob("*_node_status.csv")]

all_tabla_de_grafica = \
    [str(b) for a in Path(f"{SIM_ROOT_PATH}").glob("*") for b in Path(a).glob("*_tabla_de_grafica.csv")]

single_ns = all_node_status[100]
single_tg = all_tabla_de_grafica[0]

def get_graph(node_status_path: str):
    """
    """
    graph_path = f"{GRAPH_ROOT_PATH}/{node_status_path.split('/')[7]}.graphml"

    return nx.read_graphml(graph_path)

def get_graph_agebs(g: nx.classes.graph.Graph):
    """ Funcion que obtiene los nodos de una gráfica junto con el ageb al que pertenecen
    
    Parameters:
    graph_path (str): path hacia el graphml a procesar

    Returns:
    (Dataframe): Un Dataframe de pandas
    """

    aux_pdf = \
        pd.DataFrame([(x,y["block"]) for x,y in g.nodes(data=True)])

    aux_pdf = aux_pdf.rename(columns={0: "node_id", 1: "ageb_id"})
    
    aux_pdf["node_id"] = pd.to_numeric(aux_pdf["node_id"])
    aux_pdf["ageb_id"] = aux_pdf["ageb_id"].astype(str)

    return aux_pdf

def node_status_indexing(node_status_path: str):
    """
    """
    g = get_graph(node_status_path)
    node_id_catalog = get_graph_agebs(g)

    with DuckSession() as duck:
        df = duck.sql(f"""
        WITH
        node_status_df AS (
            SELECT column0 AS node_id
                , * EXCLUDE(column0)
            FROM read_csv_auto('{node_status_path}')
        )

        SELECT a.*, b.ageb_id
        FROM 
            node_status_df AS a
            LEFT JOIN
            node_id_catalog AS b
            ON a.node_id = b.node_id
        ORDER BY 1 ASC
        """).df()
    
    return df

def compute_infected_aggr(node_status_path: str):
    """
    """
    node_status_catalog = node_status_indexing(node_status_path)

    with DuckSession() as duck:
        df = duck.sql("""
        SELECT ageb_id
            , I
            , COUNT(DISTINCT node_id) AS infected_people
        FROM node_status_catalog
        GROUP BY 1, 2
        HAVING I NOT NULL
        ORDER BY 1, 2 ASC
        """).df()

    return df

def avg_infected_by_time(aggr_pdf: pd.DataFrame) -> pd.DataFrame:
    """
    """
    with DuckSession() as duck:
        df = duck.sql("""
        SELECT 
            ageb_id
            , I
            , AVG(infected_people) AS avg_infected_people
            , STDDEV(Infected_people) AS stddev_infected_people
        FROM aggr_pdf
        GROUP BY 1, 2
        ORDER BY 1, 2 ASC
        """).df()

    return df

if __name__ == "__main__":

    with Stopwatch():

        with mp.Pool(20) as p:
            indexed_status = p.map(compute_infected_aggr, all_node_status)

        avg_infection_per_ageb = \
            avg_infected_by_time(pd.concat(indexed_status))
        
        avg_infection_per_ageb.to_csv(f"/datos/EpiTeam/no_infectados_por_dia_ageb/{YEAR}_{MONTH}_{DAY}.csv", index=False)